# **Banco de Dados Geoespacial para Análise do Desmatamento no Brasil**  

## **Objetivo do Projeto**  
Este projeto visa a construção de um banco de dados geoespacial robusto e estruturado para análise do desmatamento no Brasil. A iniciativa tem como foco integrar e processar informações provenientes de diferentes fontes para oferecer uma visão abrangente sobre a dinâmica do desmatamento no país.

> O desmatamento é um fator crítico para a formulação de políticas públicas e a definição de estratégias de desenvolvimento sustentável. Dados geoespaciais permitem monitorar a evolução do desmatamento ao longo do tempo, identificar áreas de maior risco e embasar decisões governamentais e privadas. Essas informações são amplamente utilizadas para o cumprimento de acordos ambientais, avaliação da efetividade de ações de fiscalização e certificação de cadeias produtivas sustentáveis, além de serem um recurso essencial para planejamento territorial e conservação dos recursos naturais.

Para isso, foram utilizados os dados dos principais sistemas de monitoramento por satélite desenvolvidos pelo Instituto Nacional de Pesquisas Espaciais (**INPE**):  

- **PRODES** (*Projeto de Monitoramento do Desmatamento na Amazônia Legal por Satélite*) — responsável pelo mapeamento anual das taxas de desmatamento na Amazônia Legal, sendo uma das principais referências para políticas ambientais e compromissos internacionais.  
- **DETER** (*Sistema de Detecção do Desmatamento em Tempo Real*) — opera em tempo quase real para detectar alterações na vegetação, permitindo ações rápidas de fiscalização e combate ao desmatamento ilegal.

Para enriquecer a análise espacial e contextualizar os dados de desmatamento, integramos informações de diversas fontes complementares:  

- **IMAFLORA (Instituto de Manejo e Certificação Florestal e Agrícola):**  
  Base fundiária do Atlas Agropecuário, que inclui dados sobre:  
  - Cadastro Ambiental Rural (**CAR**)  
  - Sistema de Gestão Fundiária (**SIGEF**)  
  - Assentamentos do Instituto Nacional de Colonização e Reforma Agrária (**INCRA**)  
  - Sistema Nacional de Imóveis Rurais (**SNIR**)  
  - Terras Indígenas (**FUNAI**)  
  - Unidades de Conservação (**MMA**)  

- **ICMBio (Instituto Chico Mendes de Conservação da Biodiversidade):**  
  - Dados sobre biomas brasileiros  
  - Áreas prioritárias para conservação, essenciais para identificar impactos do desmatamento sobre a biodiversidade  

- **IBAMA (Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renováveis):**  
  - Informações sobre áreas embargadas devido a infrações ambientais, permitindo cruzar dados de desmatamento com zonas de restrição ambiental  

- **IBGE (Instituto Brasileiro de Geografia e Estatística):**  
  - Limites administrativos de municípios e estados brasileiros, fundamentais para análises espaciais em diferentes escalas geográficas 


## 1. Análise Exploratória dos Dados (EDA)

Nesta etapa, realizamos um estudo aprofundado para entender a qualidade, a distribuição e as particularidades dos dados disponíveis:

- **Coleta e Integração dos Dados:**
  - Identificação e extração das fontes (IBGE, IMAFLORA, INPE, IBAMA, ICMBio).
- **Limpeza e Preparação:**
  - Verificação de inconsistências, tratamento de dados faltantes e identificação da dimensão, estrutura e formato dos dados (GeoPandas, PySpark, Pandas).
- **Visualização Inicial:**
  - Geração de mapas, histogramas e gráficos para compreender a distribuição espacial e temporal dos dados (Matplotlib, Seaborn, Folium).
- **Análise Estatística:**
  - Cálculo de métricas descritivas para cada fonte, a fim de identificar padrões e tendências iniciais no desmatamento.

## 2. Pipeline ETL

Para organizar e integrar os dados de forma eficiente, o pipeline ETL foi estruturado em três grandes etapas:

### Extração

- **Função `extract_zip_files`:**
  - Automatiza a extração de arquivos ZIP contendo os dados brutos, transferindo-os de um diretório de origem para um diretório de destino.

### Transformação

- **Conversão para GeoParquet:**
  - A função `transform_to_parquet` converte shapefiles e geopackages para o formato GeoParquet, otimizando o desempenho na manipulação de grandes volumes de dados geoespaciais.
- **Validação de Geometrias:**
  - A função `validate_geometries` verifica e corrige geometrias inválidas garantindo a integridade espacial dos dados.
- **Junção Espacial:**
  - Realiza a integração entre os diferentes conjuntos de dados, filtrando e cruzando informações que se interceptam com a área de estudo (Brasil).
- **Padronização das Geometrias:**
  - Uniformiza os formatos e sistemas de referência de coordenadas das geometrias, para assegurar a consistência dos dados transformados.

### Carga

- **Conexão com o Banco de Dados:**
  - Estabelece uma conexão com o PostgreSQL, preparado com a extensão PostGIS para suporte a dados geoespaciais.
- **Criação de Esquemas e Extensões:**
  - Configura o ambiente no banco de dados, criando esquemas específicos e habilitando as extensões necessárias.
- **Exportação para PostGIS:**
  - Carrega os dados transformados para o banco de dados, tornando-os acessíveis para análises no QGIS.

## 3. Consultas e Análises no Banco de Dados utilizando QGIS

Após a carga dos dados, a etapa de consultas e análises é realizada através de:

- **Consultas Espaciais:**
  - Utilização de funções nativas do PostGIS para realizar operações como intersecção, buffer, união e agregações espaciais.
- **Análise de Dados no QGIS:**
  - Integração do banco de dados com o QGIS para:
    - Visualização e simbologia dos dados geoespaciais;
    - Execução de consultas SQL diretamente na interface do QGIS;
    - Geração de mapas temáticos que evidenciam áreas de desmatamento, áreas prioritárias para conservação e zonas de embargo.
- **Geração de Relatórios e Mapas:**
  - Criação de dashboards e relatórios que suportem a tomada de decisão e a proposição de políticas públicas com base nos dados analisados.

## Tecnologias Utilizadas

- **Banco de Dados:** PostgreSQL com a extensão PostGIS
- **Linguagem de Programação:** Python e SQL
- **Ferramentas de Visualização e Análise:** Geopandas, PySpark, Matplotlib, Seaborn, Folium, QGIS
- **Formatos de Dados:** GeoParquet, Shapefile, GeoPackage

## Contribuição

Contribuições, sugestões e correções são bem-vindas! Para colaborar com o projeto:

1. Faça um fork do repositório.
2. Crie uma branch para a sua feature (`git checkout -b minha-feature`).
3. Realize as modificações e faça o commit (`git commit -am 'Adiciona nova feature'`).
4. Envie sua branch para o repositório remoto (`git push origin minha-feature`).
5. Abra um Pull Request detalhando as mudanças realizadas.

