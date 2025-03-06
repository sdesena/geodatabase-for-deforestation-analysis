# %% [markdown]
# # Contrução de Banco de Dados Espaciais para Monitoramento de Desmatamento e Gestão Fundiária

# %%
# Instalar dependências necessárias para o script

# %pip install pyarrow -q
# %pip install geopandas -q
# %pip install geoarrow-pyarrow -q
# %pip install geoarrow-pandas -q
# %pip install pyspark -q
# %pip install findspark -q
# %pip install duckdb -q
# %pip install folium -q
# %pip install matplotlib -q
# %pip install mapclassify -q
# %pip install lonboard -q
# %pip install psycopg2 -q
# %pip install sqlalchemy -q
# %pip install geoalchemy2 -q
# %pip install seaborn -q

# %%
from pyspark import SparkConf
from pyspark.sql import SparkSession
from IPython.display import display
from pyspark.sql.functions import round
from shapely.validation import make_valid
from shapely.geometry import MultiPolygon
from shapely.ops import unary_union
from IPython.display import display, HTML
from lonboard import viz
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

import psycopg2
import seaborn as sns
import pyspark.sql.functions as F
import geopandas as gpd
import matplotlib.pyplot as plt
import findspark
import duckdb
import pandas as pd
import os
import shapely
import fiona
import zipfile

# %% [markdown]
# ## Extração

# %%
def extract_zip_files(source_dir, dest_dir):
    """
    Extrai arquivos ZIP de um diretório de origem para um diretório de destino.
    
    :param source_dir: Caminho para o diretório que contém os arquivos ZIP.
    :param dest_dir: Caminho para o diretório onde os arquivos serão extraídos.
    """
    print("Iniciando a extração de arquivos ZIP...")
    try:
        # Verificar se o diretório de origem existe
        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"O diretório de origem não existe: {source_dir}")

        # Criar o diretório de destino, se necessário
        os.makedirs(dest_dir, exist_ok=True)

        # Iterar sobre os arquivos no diretório de origem
        for file_name in os.listdir(source_dir):
            if file_name.endswith(".zip"):
                zip_path = os.path.join(source_dir, file_name)
                output_subdir = os.path.join(dest_dir, os.path.splitext(file_name)[0])  # Subdiretório com o nome do ZIP

                try:
                    print(f"Extraindo: {zip_path} para {output_subdir}...")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(output_subdir)  # Extrair para o subdiretório correspondente
                    print(f"Extração concluída: {zip_path}")
                except zipfile.BadZipFile:
                    print(f"Erro: O arquivo {zip_path} não é um ZIP válido. Ignorando...")
                except Exception as e:
                    print(f"Erro ao extrair {zip_path}: {e}")

        print("Extração concluída para todos os arquivos ZIP.")
    except Exception as e:
        print(f"Erro durante a extração: {e}")

# %%
# Diretórios
raw_dir = r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw"
extracted_dir = os.path.join(raw_dir, "extracted")
output_dir = os.path.join(raw_dir, "output")

# %%
def extract():
    """
    Função principal para executar o pipeline de extração de arquivos ZIP.
    """
    # Criar diretórios de saída, se necessário
    try:
        os.makedirs(extracted_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        print("Diretórios criados ou já existentes.")
    except Exception as e:
        print(f"Erro ao criar diretórios: {e}")
        return

    # Pipeline
    print("Iniciando o pipeline...")
    try:
        print("Iniciando a extração de arquivos ZIP...")
        extract_zip_files(raw_dir, extracted_dir)
        print("Extração concluída com sucesso!")
    except Exception as e:
        print(f"Erro crítico no pipeline: {e}")

# %%
# Executar o pipeline no notebook
extract()


# %% [markdown]
# ## Transformação

# %% [markdown]
# ### Transformar para GeoParquet

# %%
def transform_to_parquet(source_dir, output_dir):
    """
    Process shapefiles and geopackages, converting them to GeoParquet.
    """
    print("Iniciando a conversão para GeoParquet...")
    
    for root, _, files in os.walk(source_dir):
        for file_name in files:
            if file_name.endswith(".shp") or file_name.endswith(".gpkg"):
                file_path = os.path.join(root, file_name)

                try:
                    if file_name.endswith(".gpkg"):
                        # Obter a lista de camadas corretamente
                        layers = fiona.listlayers(file_path)

                        for layer in layers:
                            print(f"Processando camada: {layer} no arquivo {file_name}...")
                            gdf = gpd.read_file(file_path, layer=layer)
                            output_file = f"{layer}.parquet"
                            output_path = os.path.join(output_dir, output_file)
                            gdf.to_parquet(output_path)
                            print(f"Salvo: {output_path}")

                    else:
                        # Processar normalmente os Shapefiles
                        print(f"Processando arquivo: {file_name}...")
                        gdf = gpd.read_file(file_path)
                        output_file = os.path.splitext(file_name)[0] + ".parquet"
                        output_path = os.path.join(output_dir, output_file)
                        gdf.to_parquet(output_path)
                        print(f"Salvo: {output_path}")

                except Exception as e:
                    print(f"Erro ao processar {file_name}: {e}")
                    raise

# Definir diretórios e executar no notebook

transform_to_parquet(extracted_dir, output_dir)

# %% [markdown]
# ### Validar Geometrias

# %%
def validate_geometries(gdf):
    """
    Checks if geometries are valid, attempts to fix invalid geometries,
    and drops geometries that remain invalid or are null.
    
    Parameters:
        gdf (GeoDataFrame): The input GeoDataFrame.
    
    Returns:
        GeoDataFrame: The cleaned GeoDataFrame with only valid geometries.
    """    
    # Remover geometrias nulas antes de qualquer operação
    gdf = gdf[gdf.geometry.notnull()]
    
    # Verificar geometrias inválidas
    invalid_mask = ~gdf.geometry.is_valid
    print(f"Invalid geometries before fix: {invalid_mask.sum()}")

    # Aplicar make_valid() somente em geometrias não nulas
    gdf.loc[invalid_mask, "geometry"] = gdf.loc[invalid_mask, "geometry"].apply(lambda geom: make_valid(geom) if geom else None)

    # Verificar novamente após a tentativa de correção
    invalid_mask_after = ~gdf.geometry.is_valid
    print(f"Invalid geometries after fix: {invalid_mask_after.sum()}")

    # Remover geometrias que ainda são inválidas ou nulas
    gdf = gdf[~invalid_mask_after & gdf.geometry.notnull()]
    
    print(f"Final valid geometries count: {len(gdf)}")
    
    return gdf

# %%

# Áreas embargadas pelo IBAMA
gdf_areas_embargadas = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\adm_embargo_ibama_a.parquet")

# %%
gdf_areas_embargadas = validate_geometries(gdf_areas_embargadas)

# %% [markdown]
# ### Junção Espacial

# %%
# Limites administrativos por Estado e Municípios
gdf_BR_UF = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\BR_UF_2023.parquet")
gdf_BR_Municipios = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\BR_Municipios_2023.parquet")

# %%
# Realizar a junção espacial para filtrar áreas que intersectam o Brasil
gdf_areas_embargadas = gpd.sjoin(gdf_areas_embargadas, gdf_BR_UF, how="inner", predicate="within")

# Remover colunas adicionais da junção, se necessário
gdf_areas_embargadas = gdf_areas_embargadas.drop(columns=["index_right"])

# %% [markdown]
# ### Limpeza e Concatenação

# %%
# Áreas prioritárias para conservação	
gdf_APC_amazonia = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\Amazonia_2a_atualizacao.parquet")
gdf_APC_caatinga = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\Caatinga_2a_atualizacao.parquet")
gdf_APC_mata_atlantica = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\MataAtlantica_2a_atualizacao.parquet")
gdf_APC_cerrado_pantanal = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\Cerrado_Pantanal_2a_atualizacao.parquet")
gdf_APC_pampa = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\Pampa_2a_atualizacao.parquet")

datasets = [gdf_APC_pampa, gdf_APC_mata_atlantica, gdf_APC_cerrado_pantanal, gdf_APC_caatinga, gdf_APC_amazonia]

# %%
def concatenate_geodataframes(gdf_list):
    """
    Concatenates a list of GeoDataFrames into a single GeoDataFrame.
    
    Parameters:
        gdf_list (list): A list of GeoDataFrames to concatenate.

    Returns:
        GeoDataFrame: A concatenated GeoDataFrame.
    """
    if not gdf_list:
        raise ValueError("The list of GeoDataFrames is empty.")
    
    gdf_combined = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True))
    return gdf_combined

# %%
gdf_APC_combined = concatenate_geodataframes(datasets)

# %%
gdf_APC_combined["Import_bio"].value_counts()

# %%
# Substituir o valor incorreto "Muita Alta" por "Muito Alto"
gdf_APC_combined["Import_bio"] = gdf_APC_combined["Import_bio"].replace("Muita Alta", "Muito Alta")

# Verificar se a substituição foi feita
print(gdf_APC_combined["Import_bio"].value_counts())

# %% [markdown]
# ### Padronização de geometrias

# %% [markdown]
# - Verifica o tipo de geometria → Garante que seja um único tipo (Polygon, MultiPolygon, etc.).
# - Checa coordenadas Z → Se houver Z, pode precisar remover.
# - Remove geometrias vazias → Evita erros na importação para o PostGIS.
# - Remove a dimensão Z → Evita problemas se o PostGIS estiver esperando apenas X, Y.

# %%
# Verificar o tipo de geometria
print(gdf_APC_combined.geom_type.unique())

# Checar se existem dimensões Z na geometria
print(gdf_APC_combined['geometry'].has_z.any())

# %%
gdf_APC_combined = gdf_APC_combined[gdf_APC_combined.is_empty==False]

# %%
import shapely
func = lambda geom: shapely.wkb.loads(shapely.wkb.dumps(geom, output_dimension=2))
gdf_APC_combined['geometry'] = gdf_APC_combined['geometry'].apply(func)

# %% [markdown]
# ### Reprojeção do SRC

# %%
# Malha fundiária
gdf_malha_fundiaria = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\pa_br_landtenure_imaflora_2021.parquet")

# %%
# Biomas brasileiros
gdf_biomas = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\biomas_5000.parquet")

# %%
# Desmatamento (PRODES)
gdf_desmatamento_prodes = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\pa_br_deforestation_prodes_2002-2022.parquet")

# %%
# Desmatamento (DETER)
gdf_desmatamento_deter = gpd.read_parquet(r"C:\Users\sandr\Documents\GitHub\spatial-data-management-postgis\venv\raw\output\deter-amz-deter-public.parquet")

# %%
def reproject_to_sirgas(gdf):
    """
    Reprojects a GeoDataFrame to SIRGAS 2000 Polyconic (EPSG:5880).

    Parameters:
        gdf (GeoDataFrame): The input GeoDataFrame.

    Returns:
        GeoDataFrame: The reprojected GeoDataFrame.
    """
    target_crs = "EPSG:5880"  # SIRGAS 2000 Polyconic

    if gdf.crs is None:
        raise ValueError("Input GeoDataFrame does not have a defined CRS.")

    return gdf.to_crs(target_crs) if gdf.crs != target_crs else gdf

# %%
# Reprojetar todos os GeoDataFrames para SIRGAS 2000 Polyconic
gdf_areas_embargadas = reproject_to_sirgas(gdf_areas_embargadas)
gdf_APC_combined = reproject_to_sirgas(gdf_APC_combined)
gdf_BR_UF = reproject_to_sirgas(gdf_BR_UF)
gdf_BR_Municipios = reproject_to_sirgas(gdf_BR_Municipios)
gdf_biomas = reproject_to_sirgas(gdf_biomas)

# %%
# Reprojetar todos os GeoDataFrames para SIRGAS 2000 Polyconic
gdf_malha_fundiaria = reproject_to_sirgas(gdf_malha_fundiaria)

# %%
# Reprojetar todos os GeoDataFrames para SIRGAS 2000 Polyconic
gdf_desmatamento_prodes = reproject_to_sirgas(gdf_desmatamento_prodes)

# %%
# Reprojetar todos os GeoDataFrames para SIRGAS 2000 Polyconic
gdf_desmatamento_deter = reproject_to_sirgas(gdf_desmatamento_deter)

# %% [markdown]
# ## Carga

# %% [markdown]
# ### Carregar variáveis de conexão

# %%
# Configurações
from dotenv import load_dotenv

# Carregar as variáveis do arquivo .env
load_dotenv()

# Recuperar as variáveis de ambiente
db_config = {
    "db_user": os.getenv("DB_USER"),
    "db_password": os.getenv("DB_PASSWORD"),
    "db_host": os.getenv("DB_HOST"),
    "db_port": os.getenv('DB_PORT'),
    "db_name": os.getenv("DB_NAME")
}

# %% [markdown]
# ### Conexão com o Banco de Dados

# %%
def create_database_connection(db_user, db_password, db_host, db_port, db_name):
    """Cria uma conexão com o banco de dados PostgreSQL."""
    conn = psycopg2.connect(
        dbname="postgres",  # Conexão inicial ao banco padrão
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True
    return conn

# %%
def initialize_database(cursor, db_name):
    """Inicializa o banco de dados, criando-o se necessário."""
    try:
        cursor.execute(f"CREATE DATABASE {db_name};")
        print(f"Banco de dados '{db_name}' criado com sucesso!")
    except psycopg2.errors.DuplicateDatabase:
        print(f"O banco de dados '{db_name}' já existe.")

# %%
# Inicialização
conn = create_database_connection(**db_config)
cur = conn.cursor()
initialize_database(cur, db_config["db_name"])
conn.close()

engine = create_engine(
    f"postgresql://{db_config['db_user']}:{db_config['db_password']}@{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"
)

# %% [markdown]
# ### Criar Esquemas e extensões

# %%

def create_schemas(engine, schemas):
    """Cria os esquemas necessários no banco de dados."""
    with engine.begin() as conn:
        for schema in schemas:
            try:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
                print(f"Esquema '{schema}' criado com sucesso!")
            except Exception as e:
                print(f"Erro ao criar o esquema '{schema}': {e}")

# %%
def create_extensions(engine, extensions):
    """Habilita as extensões necessárias no banco de dados."""
    with engine.begin() as conn:
        for ext in extensions:
            try:
                conn.execute(text(ext))
                print(f"Extensão executada com sucesso: {ext}")
            except Exception as e:
                print(f"Erro ao criar extensão: {ext} -> {e}")

# %%
# Extensões e esquemas
extensions = [
    "CREATE EXTENSION IF NOT EXISTS postgis;",
    "CREATE EXTENSION IF NOT EXISTS postgis_raster;",
    "CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;"
]
schemas = ["ibge", "imaflora", "ibama", "inpe", "icmbio","sicar"]

#%%
create_extensions(engine, extensions)
create_schemas(engine, schemas)


# %% [markdown]
# ### Exportar para o PostGIS

# %%

def export_to_postgis(gdf, table_name, schema, engine):
    """
    Exporta o GeoDataFrame para o banco de dados PostgreSQL.
    """
    gdf.to_postgis(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",  # ou "append" dependendo da lógica
        index=False,
    )
    print(f"Dados exportados com sucesso para a tabela {schema}.{table_name}!")


# %%
def export_partitioned_to_postgis(gdf, engine, schema, column, prefix):
    """
    Partitions a GeoDataFrame by a given column (e.g., state abbreviation) and exports each partition to PostGIS.
    
    Parameters:
        gdf (GeoDataFrame): The input GeoDataFrame containing the spatial data.
        engine (SQLAlchemy engine): The connection engine to the PostGIS database.
        schema (str): The target schema in the database.
        column (str): The column used for partitioning (for example: "sigla_uf").
        prefix (str): The prefix for table names (for example: "base_fundiaria").
    
    Returns:
        None
    """
    # Obter valores únicos da coluna de partição
    unique_values = gdf[column].unique()

    # Percorrer cada valor único e exportar separadamente
    for value in unique_values:
        # Filtrar os dados pelo valor atual
        gdf_partition = gdf[gdf[column] == value]

        # Nome da tabela no banco
        table_name = f"{prefix}_{value}"

        # Enviar para o banco de dados
        gdf_partition.to_postgis(name=table_name, con=engine, schema=schema, if_exists='replace', index=False)
        print(f"Tabela '{table_name}' enviada com sucesso!")

# %%
# Exportar para o PostgreSQL/PostGIS
export_to_postgis(gdf_areas_embargadas, "areas_embargadas", "ibama", engine)
export_to_postgis(gdf_APC_combined, "areas_prioritarias_conservacao", "ibama", engine)
export_to_postgis(gdf_BR_UF, "estados_br", "ibge", engine)
export_to_postgis(gdf_BR_Municipios, "municipios_br", "ibge", engine)
export_to_postgis(gdf_biomas, "biomas", "icmbio", engine)


# %%
# Exportar a camada de destamatamento PRODES particionado por Bioma
export_partitioned_to_postgis(gdf_desmatamento_prodes,engine,'inpe','source','desmatamento')

# %%
# Exportar a camada de destamatamento DETER particionado por Estado
export_partitioned_to_postgis(gdf_desmatamento_deter,engine,'inpe','UF','desmatamento_deter')

# %%
# Exportar a malha fundiária particionada por Estado
export_partitioned_to_postgis(gdf_malha_fundiaria,engine,'imaflora','sigla_uf','malha_fundiaria')

# %% [markdown]
# ### Fechar as conexões

# %%
# Fechar a conexão administrativa com o PostgreSQL
cur.close()
conn.close()

# %% [markdown]
# ## Análise Exploratória

# %% [markdown]
# 


