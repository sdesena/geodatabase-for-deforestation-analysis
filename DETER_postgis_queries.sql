-- Verifica qual é a coluna de geometria e seu SRID

SELECT f_table_name, f_geometry_column, srid, type 

FROM geometry_columns 

WHERE f_table_name = 'malha_fundiaria_AC';



-- Identifica quais geometrias são inválidas

SELECT *

FROM imaflora."malha_fundiaria_AC"

WHERE NOT ST_IsValid(geometry);


-- Mostra o motivo do erro em cada geometria inválida

SELECT 

    id,  -- ou outra chave primária

    ST_IsValidReason(geometry) AS motivo_erro

FROM imaflora."malha_fundiaria_AC"

WHERE NOT ST_IsValid(geometry);



-- Antes de atualizar as geometrias, podemos simular a correção e verificar se ela realmente resolve os problemas

SELECT 

    id,  -- ou outra chave primária

    ST_IsValid(geometry) AS antes, 

    ST_IsValid(ST_MakeValid(geometry)) AS depois, 

    ST_IsValidReason(geometry) AS motivo_erro

FROM imaflora."malha_fundiaria_AC"

WHERE NOT ST_IsValid(geometry);



-- Adiciona a nova coluna de geometria na malha fundiária

ALTER TABLE imaflora."malha_fundiaria_AC" ADD COLUMN geom geometry;



-- Preenche a nova coluna 'geom' com geometrias corrigidas (se necessário)

UPDATE imaflora."malha_fundiaria_AC" 

SET geom = ST_MakeValid(geometry)

WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;



-- Adiciona a nova coluna de geometria na camada de desmatamento

ALTER TABLE inpe."desmatamento_deter_AC" ADD COLUMN geom geometry;



-- Preenche a nova coluna 'geom' com geometrias corrigidas (se necessário)

UPDATE inpe."desmatamento_deter_AC" 

SET geom = ST_MakeValid(geometry)

WHERE NOT ST_IsValid(geometry) OR geometry IS NOT NULL;





-- Criar um índice SP-GiST e GiST na coluna geom para acelerar consultas espaciais

CREATE INDEX idx_malha_fundiaria_geom

ON imaflora."malha_fundiaria_AC"

USING SPGIST (geom);



CREATE INDEX idx_desmatamento_geom

ON inpe."desmatamento_deter_AC"

USING GIST (geom);



-- Calcular o desmatamento por classe subclasse da malha fundiária

SELECT 

  mf.desc_subclass,

  SUM(ST_Area(ST_Intersection(d.geom, mf.geom))) / 1000000 AS area_desmatada_km2  -- Convertendo de m² para km²

FROM inpe."desmatamento_deter_AC" d

JOIN imaflora."malha_fundiaria_AC" mf

ON ST_Intersects(d.geom, mf.geom)

GROUP BY mf.desc_subclass

ORDER BY area_desmatada_km2 DESC;




-- Calcular o desmatamento por classe e subclasse da malha fundiária

SELECT 

  mf.desc_subclass,

  d."CLASSNAME",

  SUM(ST_Area(ST_Intersection(d.geom, mf.geom))) / 1000000 AS area_desmatada_km2  -- Convertendo de m² para km²

FROM inpe."desmatamento_deter_AC" d

JOIN imaflora."malha_fundiaria_AC" mf

ON ST_Intersects(d.geom, mf.geom)

GROUP BY mf.desc_subclass, d."CLASSNAME"  -- Agrupando também pela classe do desmatamento

ORDER BY area_desmatada_km2 DESC;


-- Verificar o tipo do dado de data

SELECT column_name, data_type 

FROM information_schema.columns 

WHERE table_schema = 'inpe' AND table_name = 'desmatamento_deter_AC' AND column_name = 'VIEW_DATE';



-- Normalizar coluna VIEW_DATE para futuros agrupamentos por data

ALTER TABLE inpe."desmatamento_deter_AC" ADD COLUMN ano INT;

ALTER TABLE inpe."desmatamento_deter_AC" ADD COLUMN mes INT;



-- Preencher as colunas ano e mes com os valores extraídos da VIEW_DATE

UPDATE inpe."desmatamento_deter_AC"

SET 

  ano = EXTRACT(YEAR FROM "VIEW_DATE"),

  mes = EXTRACT(MONTH FROM "VIEW_DATE");


-- Calcular o desmatamento por classe da malha fundiária e ano

SELECT 

  mf.desc_subclass,

  d.ano,

  SUM(ST_Area(ST_Intersection(d.geom, mf.geom))) / 1000000 AS area_desmatada_km2  -- Convertendo de m² para km²

FROM inpe."desmatamento_deter_AC" d

JOIN imaflora."malha_fundiaria_AC" mf

ON ST_Intersects(d.geom, mf.geom)

GROUP BY mf.desc_subclass, d.ano  -- Agrupando também pela classe do desmatamento

ORDER BY area_desmatada_km2 DESC;






-- Calcular a Área Desmatada por Mês
CREATE TEMP TABLE desmatamento_por_mes AS
SELECT 
    ano,
    mes,
    SUM(ST_Area(geom)) / 1000000 AS area_desmatada
FROM inpe."desmatamento_deter_AC"
GROUP BY ano, mes;


-- Calcular a Variação Percentual Mensal
SELECT 
    ano,
    mes,
    area_desmatada,
    LAG(area_desmatada) OVER (PARTITION BY ano ORDER BY mes) AS desmatamento_anterior,
    (area_desmatada - LAG(area_desmatada) OVER (PARTITION BY ano ORDER BY mes)) / 
    NULLIF(LAG(area_desmatada) OVER (PARTITION BY ano ORDER BY mes), 0) * 100 AS variacao_percentual
FROM desmatamento_por_mes
ORDER BY ano, mes;

-- Calcular a Média Móvel de 3 Meses
SELECT 
    ano,
    mes,
    area_desmatada,
    AVG(area_desmatada) OVER (PARTITION BY ano ORDER BY mes ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS media_movel_3meses
FROM desmatamento_por_mes
ORDER BY ano, mes;
