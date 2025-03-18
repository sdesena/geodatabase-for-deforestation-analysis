-- Verificar qual é a coluna de geometria e o SRID
SELECT f_table_name, f_geometry_column, srid, type 
FROM geometry_columns 
WHERE f_table_name = 'areas_embargadas';


-- Identificar as geometrias invalidas
SELECT * 
FROM ibama.areas_embargadas 
WHERE NOT ST_IsValid(geometry);


-- Verificar o motivo das geometrias invalidas
SELECT 
    objectid,  -- ou outra chave primária
    ST_IsValidReason(geometry) AS motivo_erro
FROM ibama.areas_embargadas 
WHERE NOT ST_IsValid(geometry);


--  Adicionar uma nova coluna de geometria
ALTER TABLE ibama.areas_embargadas ADD COLUMN geom geometry;


--  Preencher a nova coluna geom com geometrias corrigidas
UPDATE ibama.areas_embargadas 
SET geom = ST_MakeValid(geometry)
WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;


-- Criar índice GiST na coluna geom da tabela ibama.areas_embargadas
CREATE INDEX areas_embargadas_geom_gist 
ON ibama.areas_embargadas 
USING GIST (geom);


-- Retorna a interseção entre as áreas embargadas e a malha fundiária agrupada por classe
SELECT 
    m.desc_subclass, 
    COUNT(a.objectid) AS qtd_areas_embargadas, 
    SUM(ST_Area(ST_Intersection(a.geom, m.geom)))/1000000 AS area_total_intersecao_km2
FROM ibama.areas_embargadas a
JOIN imaflora."malha_fundiaria_AC" m 
ON ST_Intersects(a.geom, m.geom)
WHERE ST_Area(ST_Intersection(a.geom, m.geom)) > 10000
GROUP BY m.desc_subclass
ORDER BY area_total_intersecao_km2 DESC;



-- Verificar o tipo do dado de data

SELECT column_name, data_type 

FROM information_schema.columns 

WHERE table_schema = 'ibama' AND table_name = 'areas_embargadas' AND column_name = 'dat_embarg';


-- Criar a coluna ano
ALTER TABLE ibama.areas_embargadas ADD COLUMN ano INTEGER;


-- Atualizar a coluna ano com o ano extraído de dat_embarg
UPDATE ibama.areas_embargadas 
SET ano = EXTRACT(YEAR FROM TO_TIMESTAMP(dat_embarg, 'DD-MM-YY HH24:MI:SS'));


-- Verifique o processo
SELECT ano, COUNT(*) 
FROM ibama.areas_embargadas 
GROUP BY ano 
ORDER BY ano;


-- Retorna a interseção entre as áreas embargadas e a malha fundiária agrupada por classe e ano
SELECT 
    m.desc_subclass, 
	a.ano,
    COUNT(a.objectid) AS qtd_areas_embargadas, 
    SUM(ST_Area(ST_Intersection(a.geom, m.geom)))/1000000 AS area_total_intersecao_km2
FROM ibama.areas_embargadas a
JOIN imaflora."malha_fundiaria_AC" m 
ON ST_Intersects(a.geom, m.geom)
WHERE ST_Area(ST_Intersection(a.geom, m.geom)) > 10000
GROUP BY m.desc_subclass, a.ano
ORDER BY area_total_intersecao_km2 DESC;


-- Qual percentual de desmatamento ocorreu dentro de áreas embargadas?
SELECT 
    SUM(ST_Area(ST_Intersection(d.geom, a.geom))) / SUM(ST_Area(d.geom)) * 100 AS perc_desmat_embargado
FROM inpe."desmatamento_deter_AC" d
JOIN ibama.areas_embargadas a
ON ST_Intersects(d.geom, a.geom)
WHERE ST_Area(ST_Intersection(d.geom, a.geom)) > 10000; -- Excluir interseções irrelevantes



-- Existe um padrão temporal entre áreas embargadas e desmatamento?
SELECT 
    a.ano,
    COUNT(*) AS num_eventos_desmatamento,
    COUNT(DISTINCT a.objectid) AS num_areas_embargadas
FROM inpe."desmatamento_deter_AC" d
LEFT JOIN ibama.areas_embargadas a 
ON ST_Intersects(d.geom, a.geom)
GROUP BY a.ano
ORDER BY a.ano;


-- Adicionar coluna de geometria na tabela areas prioritarias conservacao
ALTER TABLE ibama.areas_prioritarias_conservacao ADD COLUMN geom geometry;


--  Preencher a nova coluna geom com geometrias corrigidas
UPDATE ibama.areas_prioritarias_conservacao 
SET geom = ST_MakeValid(geometry)
WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;


-- Criar índice GiST na coluna geom da tabela ibama.areas_embargadas
CREATE INDEX areas_prioritarias_conservacao_geom_gist 
ON ibama.areas_prioritarias_conservacao 
USING GIST (geom);


-- Qual a quantidade de a área de areas_embargadas por areas_prioritarias_conservacao ?
SELECT 
    ac."Import_bio",
    COUNT(DISTINCT a.objectid) AS num_areas_embargadas,  -- Número de áreas embargadas
    SUM(ST_Area(a.geom))/1000000 AS total_area_embargadas_km2  -- Área total das áreas embargadas em km²
FROM
    ibama.areas_embargadas a
JOIN 
    ibama.areas_prioritarias_conservacao ac
ON 
    ST_Intersects(a.geom, ac.geom)  -- Verifica a interseção entre as áreas embargadas e as áreas prioritárias
GROUP BY 
    ac."Import_bio";  -- Agrupa os resultados pela coluna Import_bio

