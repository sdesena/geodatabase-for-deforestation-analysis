-- Verificar qual é a coluna de geometria e o SRID
SELECT f_table_name, f_geometry_column, srid, type 
FROM geometry_columns 
WHERE f_table_name = 'desmatamento_prodes_amz';


-- Identificar as geometrias invalidas
SELECT * 
FROM inpe.desmatamento_prodes_amz 
WHERE NOT ST_IsValid(geometry);


-- Verificar o motivo das geometrias invalidas
SELECT 
    id,  -- ou outra chave primária
    ST_IsValidReason(geometry) AS motivo_erro
FROM inpe.desmatamento_prodes_amz 
WHERE NOT ST_IsValid(geometry);


--  Adicionar uma nova coluna de geometria
ALTER TABLE inpe.desmatamento_prodes_amz ADD COLUMN geom geometry;


--  Preencher a nova coluna geom com geometrias corrigidas
UPDATE inpe.desmatamento_prodes_amz 
SET geom = ST_MakeValid(geometry)
WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;


-- Criar índice GiST na coluna geom da tabela ibama.areas_embargadas
CREATE INDEX desmatamento_prodes_amz_geom_gist 
ON inpe.desmatamento_prodes_amz
USING GIST (geom);



-- Calcular a área de interseção entre as duas camadas e realizar uma agregação espacial
SELECT 
	m.desc_subclass,
    COUNT(d.id) AS num_eventos_desmatamento,  -- Contagem de eventos de desmatamento dentro de cada parcela de terra
    SUM(ST_Area(ST_Intersection(d.geom, m.geom))) / 1000000 AS area_total_desmatada  -- Área de interseção em quilômetros quadrados
FROM 
    inpe.desmatamento_prodes_amz d
JOIN 
    imaflora."malha_fundiaria_AC" m
ON 
    ST_Intersects(d.geom, m.geom)  -- Verifica a interseção
GROUP BY 
    m.desc_subclass  -- Agrupa pelas parcelas de terra
ORDER BY 
	area_total_desmatada;
	

	
-- Usando Expressões de Tabelas Comuns (CTEs) para filtrar apenas áreas com mais de 10 km² desmatado
WITH area_desmatamento AS (
    SELECT 
        m.desc_subclass, 
        COUNT(d.id) AS num_eventos_desmatamento, 
        SUM(ST_Area(ST_Intersection(d.geom, m.geom))) / 1000000 AS area_total_desmatada
    FROM 
        inpe.desmatamento_prodes_amz d
    JOIN 
        imaflora."malha_fundiaria_AC" m
    ON 
        ST_Intersects(d.geom, m.geom)
    GROUP BY 
        m.desc_subclass
)
SELECT * 
FROM area_desmatamento
WHERE area_total_desmatada > 10;  -- Exemplo de condição para filtrar áreas com mais de 10 km² de desmatamento


-- Usando Expressões de Tabelas Comuns (CTEs) para filtrar apenas áreas com mais de 10 km² desmatado
WITH area_desmatamento AS (
    SELECT 
        m.desc_subclass, 
        COUNT(d.id) AS num_eventos_desmatamento, 
        SUM(ST_Area(ST_Intersection(d.geom, m.geom))) / 1000000 AS area_total_desmatada
    FROM 
        inpe.desmatamento_prodes_amz d
    JOIN 
        imaflora."malha_fundiaria_AC" m
    ON 
        ST_Intersects(d.geom, m.geom)
    GROUP BY 
        m.desc_subclass
)
SELECT 
    desc_subclass,
    area_total_desmatada,
    (area_total_desmatada / SUM(area_total_desmatada) OVER ()) * 100 AS perc_total
FROM area_desmatamento
WHERE area_total_desmatada > 10  -- Mantém o filtro de 10 km²
ORDER BY perc_total DESC;





--  Adicionar uma nova coluna de geometria
ALTER TABLE ibge.municipios_br ADD COLUMN geom geometry;


--  Preencher a nova coluna geom com geometrias corrigidas
UPDATE ibge.municipios_br 
SET geom = ST_MakeValid(geometry)
WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;


-- Criar índice GiST na coluna geom da tabela ibama.areas_embargadas
CREATE INDEX municipios_br_geom_gist 
ON ibge.municipios_br
USING GIST (geom);


-- Calcular desmatamento no município de Feijó, dentro do CAR, e classificar por tamanho

WITH area_desmatamento AS (
    SELECT 
        m.id AS id_malha,  -- ID da malha fundiária para referência
        m.geom AS geom_malha,  -- Geometria da malha fundiária para exportação
        SUM(ST_Area(ST_Intersection(d.geom, m.geom))) / 10000 AS area_desmatada_ha  -- Área desmatada em hectares
    FROM 
        inpe.desmatamento_prodes_amz d
    JOIN 
        ibge.municipios_br mun 
        ON ST_Intersects(d.geom, mun.geom) AND mun."NM_MUN" = 'Feijó'  -- Filtra apenas Feijó
    JOIN 
        imaflora."malha_fundiaria_AC" m 
        ON ST_Intersects(d.geom, m.geom) 
    WHERE 
        m.desc_subclass = 'Cadastro Ambiental Rural'  -- Filtra apenas CAR
    GROUP BY 
        m.id, m.geom
)
SELECT 
    id_malha,
    geom_malha,
    area_desmatada_ha,
    CASE 
        WHEN area_desmatada_ha BETWEEN 1 AND 10 THEN '1-10 ha'
        WHEN area_desmatada_ha BETWEEN 10 AND 50 THEN '10-50 ha'
        WHEN area_desmatada_ha BETWEEN 50 AND 100 THEN '50-100 ha'
        WHEN area_desmatada_ha BETWEEN 100 AND 500 THEN '100-500 ha'
        WHEN area_desmatada_ha BETWEEN 500 AND 1000 THEN '500-1000 ha'
        ELSE '1000-5000 ha'
    END AS classificacao_desmatamento
FROM area_desmatamento
-- WHERE area_desmatada_ha > 1  -- Filtra apenas polígonos com desmatamento acima de 1 ha
ORDER BY area_desmatada_ha DESC;
