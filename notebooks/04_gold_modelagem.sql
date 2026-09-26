-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Modelagem → Gold (Esquema Estrela)
-- MAGIC
-- MAGIC ```
-- MAGIC                  dim_corrida (inclui circuito e era)
-- MAGIC                        │
-- MAGIC   dim_piloto ── fato_resultado ── dim_equipe          fato_pit_stop ── (mesmas dimensões)
-- MAGIC                        │
-- MAGIC                    dim_status
-- MAGIC ```
-- MAGIC
-- MAGIC **Duas fatos, dimensões compartilhadas (conformadas):**
-- MAGIC - `fato_resultado`: **1 linha por piloto × corrida** (grid, posição, pontos, status, paradas…)
-- MAGIC - `fato_pit_stop`: **1 linha por parada nos boxes**
-- MAGIC
-- MAGIC **Decisões de modelagem**
-- MAGIC - **Estrela, não snowflake:** o circuito é desnormalizado dentro de `dim_corrida` (nome, cidade, país, coordenadas),
-- MAGIC   então as análises por circuito/país precisam de um único join.
-- MAGIC - **Chaves:** os ids do Ergast (`race_id`, `driver_id`…) já são inteiros sem significado de negócio e estáveis entre
-- MAGIC   versões do dataset. Por isso são reaproveitados como chaves das dimensões, sem gerar novas chaves.
-- MAGIC - **Era regulamentar** em `dim_corrida`: a F1 muda de regulamento em blocos. Comparar 1960 com 2020 sem esse
-- MAGIC   contexto leva a conclusões erradas.
-- MAGIC - **Medidas derivadas na fato** (`posicoes_ganhas`, `flag_vitoria`, `flag_em_casa`, `idade_piloto`…): calculadas uma
-- MAGIC   vez na carga, deixam as consultas de análise simples.
-- MAGIC - **PK/FK informativas** do Unity Catalog documentam o modelo e habilitam o diagrama de relacionamentos no Catalog Explorer.
-- MAGIC - Carga completa e idempotente (`CREATE OR REPLACE`); as fatos são removidas primeiro por causa das FKs.

-- COMMAND ----------

USE CATALOG mvp_f1;

-- COMMAND ----------

DROP TABLE IF EXISTS gold.fato_resultado;
DROP TABLE IF EXISTS gold.fato_pit_stop;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_corrida

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_corrida (
  sk_corrida               INT     NOT NULL COMMENT 'PK. Id da corrida no Ergast (race_id).',
  ano                      INT              COMMENT 'Temporada. Domínio: 1950-2024. Origem: silver.corridas.ano.',
  decada                   INT              COMMENT 'Década da temporada (1950, 1960...). Derivado de ano.',
  era_regulamentar         STRING           COMMENT 'Bloco de regulamento técnico (ver notebook 04). Domínio: 9 eras de 1950-1967 a 2022-2024. Derivado de ano.',
  rodada                   INT              COMMENT 'Etapa dentro da temporada (>= 1). Origem: silver.corridas.rodada.',
  total_rodadas_temporada  INT              COMMENT 'Número de corridas da temporada. Derivado.',
  flag_ultima_corrida      BOOLEAN          COMMENT 'TRUE se é a última etapa da temporada (usada para o campeonato final). Derivado.',
  nome_gp                  STRING           COMMENT 'Nome do Grande Prêmio. Origem: silver.corridas.nome_gp.',
  data                     DATE             COMMENT 'Data da corrida. Origem: silver.corridas.data.',
  fim_de_semana_sprint     BOOLEAN          COMMENT 'TRUE se o fim de semana teve sprint. Origem: silver.corridas.',
  circuito_id              INT              COMMENT 'Id do circuito no Ergast. Origem: silver.circuitos.circuit_id.',
  circuito_nome            STRING           COMMENT 'Nome do circuito. Origem: silver.circuitos.nome.',
  circuito_cidade          STRING           COMMENT 'Cidade do circuito. Origem: silver.circuitos.cidade.',
  circuito_pais            STRING           COMMENT 'País do circuito (padronizado). Origem: silver.circuitos.pais.',
  latitude                 DOUBLE           COMMENT 'Latitude do circuito. Origem: silver.circuitos.',
  longitude                DOUBLE           COMMENT 'Longitude do circuito. Origem: silver.circuitos.',
  CONSTRAINT pk_dim_corrida PRIMARY KEY (sk_corrida)
)
COMMENT 'Dimensão de corridas (GPs) com o circuito desnormalizado e a era regulamentar. Linhagem: silver.corridas JOIN silver.circuitos.';

INSERT INTO gold.dim_corrida
SELECT
  c.race_id,
  c.ano,
  CAST(FLOOR(c.ano / 10) * 10 AS INT),
  CASE WHEN c.ano <= 1967 THEN '1950-1967 Clássica'
       WHEN c.ano <= 1982 THEN '1968-1982 Aerofólios e efeito solo'
       WHEN c.ano <= 1988 THEN '1983-1988 Turbo'
       WHEN c.ano <= 1994 THEN '1989-1994 Aspirados e eletrônica'
       WHEN c.ano <= 2005 THEN '1995-2005 V10'
       WHEN c.ano <= 2013 THEN '2006-2013 V8'
       WHEN c.ano <= 2021 THEN '2014-2021 Híbrida V6 turbo'
       ELSE '2022-2024 Novo efeito solo' END,
  c.rodada,
  CAST(MAX(c.rodada) OVER (PARTITION BY c.ano) AS INT),
  c.rodada = MAX(c.rodada) OVER (PARTITION BY c.ano),
  c.nome_gp,
  c.data,
  c.fim_de_semana_sprint,
  ci.circuit_id,
  ci.nome,
  ci.cidade,
  ci.pais,
  ci.latitude,
  ci.longitude
FROM silver.corridas c
LEFT JOIN silver.circuitos ci ON c.circuit_id = ci.circuit_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_piloto, dim_equipe, dim_status

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_piloto (
  sk_piloto        INT    NOT NULL COMMENT 'PK. Id do piloto no Ergast (driver_id).',
  nome_completo    STRING          COMMENT 'Nome e sobrenome. Origem: silver.pilotos.nome_completo.',
  codigo           STRING          COMMENT 'Código de 3 letras (ex.: HAM); NULL para pilotos antigos. Origem: silver.pilotos.codigo.',
  nacionalidade    STRING          COMMENT 'Nacionalidade padronizada. Origem: silver.pilotos.nacionalidade.',
  pais             STRING          COMMENT 'País da nacionalidade (mesmo padrão de dim_corrida.circuito_pais). Origem: silver.pilotos.pais.',
  data_nascimento  DATE            COMMENT 'Data de nascimento. Origem: silver.pilotos.data_nascimento.',
  CONSTRAINT pk_dim_piloto PRIMARY KEY (sk_piloto)
)
COMMENT 'Dimensão de pilotos. Linhagem: silver.pilotos.';

INSERT INTO gold.dim_piloto
SELECT driver_id, nome_completo, codigo, nacionalidade, pais, data_nascimento FROM silver.pilotos;

CREATE OR REPLACE TABLE gold.dim_equipe (
  sk_equipe      INT    NOT NULL COMMENT 'PK. Id da equipe no Ergast (constructor_id).',
  nome           STRING          COMMENT 'Nome da equipe. Origem: silver.equipes.nome.',
  nacionalidade  STRING          COMMENT 'Nacionalidade da equipe. Origem: silver.equipes.nacionalidade.',
  CONSTRAINT pk_dim_equipe PRIMARY KEY (sk_equipe)
)
COMMENT 'Dimensão de equipes (construtores). Linhagem: silver.equipes.';

INSERT INTO gold.dim_equipe
SELECT constructor_id, nome, nacionalidade FROM silver.equipes;

CREATE OR REPLACE TABLE gold.dim_status (
  sk_status         INT    NOT NULL COMMENT 'PK. Id do status no Ergast (status_id).',
  status            STRING          COMMENT 'Status original (ex.: Finished, Engine, +1 Lap). Origem: silver.status.status.',
  categoria_status  STRING          COMMENT 'FINALIZOU, ACIDENTE, FALHA_MECANICA, PILOTO, NAO_LARGOU_OU_DESCLASSIFICADO, OUTROS. Origem: silver.status.categoria_status.',
  CONSTRAINT pk_dim_status PRIMARY KEY (sk_status)
)
COMMENT 'Dimensão de situação final do piloto na corrida. Linhagem: silver.status.';

INSERT INTO gold.dim_status
SELECT status_id, status, categoria_status FROM silver.status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fato_resultado

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fato_resultado (
  id_resultado             INT     NOT NULL COMMENT 'Id do resultado no Ergast (dimensão degenerada). Origem: silver.resultados.result_id.',
  sk_corrida               INT     NOT NULL COMMENT 'FK para dim_corrida.',
  sk_piloto                INT     NOT NULL COMMENT 'FK para dim_piloto.',
  sk_equipe                INT     NOT NULL COMMENT 'FK para dim_equipe.',
  sk_status                INT              COMMENT 'FK para dim_status.',
  grid                     INT              COMMENT 'Posição de largada (>= 1); NULL = largou dos boxes ou sem grid. Origem: silver.resultados.grid.',
  posicao_classificacao    INT              COMMENT 'Posição no qualifying (>= 1); disponível só no período coberto pela tabela qualifying. Origem: silver.classificacao_grid.',
  posicao_final            INT              COMMENT 'Ordem de chegada, sempre preenchida (>= 1). Origem: silver.resultados.posicao_final.',
  posicao_oficial          INT              COMMENT 'Posição oficial; NULL se não classificado. Origem: silver.resultados.posicao_oficial.',
  posicoes_ganhas          INT              COMMENT 'grid - posicao_final (positivo = ganhou posições). NULL se grid NULL. Derivado.',
  pontos                   DOUBLE           COMMENT 'Pontos no campeonato (>= 0). Origem: silver.resultados.pontos.',
  voltas                   INT              COMMENT 'Voltas completadas. Origem: silver.resultados.voltas.',
  idade_piloto             DOUBLE           COMMENT 'Idade do piloto (anos) na data da corrida. Derivado de dim_piloto.data_nascimento e dim_corrida.data.',
  qtd_pit_stops            INT              COMMENT 'Número de paradas nos boxes; NULL fora do período coberto por pit_stops. Derivado de silver.pit_stops.',
  tempo_medio_pit_ms       DOUBLE           COMMENT 'Duração média das paradas do piloto na corrida, sem paradas atípicas (ms). Derivado de silver.pit_stops.',
  flag_pole                BOOLEAN          COMMENT 'TRUE se grid = 1.',
  flag_vitoria             BOOLEAN          COMMENT 'TRUE se posicao_oficial = 1.',
  flag_podio               BOOLEAN          COMMENT 'TRUE se posicao_oficial <= 3.',
  flag_largou              BOOLEAN          COMMENT 'FALSE se categoria_status = NAO_LARGOU_OU_DESCLASSIFICADO (não qualificou, retirou-se, desclassificado).',
  flag_finalizou           BOOLEAN          COMMENT 'TRUE se categoria_status = FINALIZOU (inclui quem terminou com voltas de atraso).',
  flag_em_casa             BOOLEAN          COMMENT 'TRUE se o país do piloto = país do circuito. Derivado de dim_piloto.pais e dim_corrida.circuito_pais.',
  CONSTRAINT fk_res_corrida FOREIGN KEY (sk_corrida) REFERENCES gold.dim_corrida,
  CONSTRAINT fk_res_piloto  FOREIGN KEY (sk_piloto)  REFERENCES gold.dim_piloto,
  CONSTRAINT fk_res_equipe  FOREIGN KEY (sk_equipe)  REFERENCES gold.dim_equipe,
  CONSTRAINT fk_res_status  FOREIGN KEY (sk_status)  REFERENCES gold.dim_status
)
COMMENT 'Fato de resultados. Granularidade: 1 linha por piloto x corrida (em raros casos dos anos 1950, 2 linhas quando o piloto dividiu carros). Linhagem: silver.resultados + classificacao_grid + pit_stops + status + pilotos + corridas.';

INSERT INTO gold.fato_resultado
WITH pits AS (
  SELECT race_id, driver_id,
         COUNT(*)                                                        AS qtd,
         AVG(CASE WHEN NOT flag_parada_atipica THEN duracao_ms END)      AS media_ms
  FROM silver.pit_stops GROUP BY race_id, driver_id
),
cobertura_pits AS (SELECT DISTINCT race_id FROM silver.pit_stops),
quali AS (
  SELECT race_id, driver_id, MIN(posicao_classificacao) AS posicao
  FROM silver.classificacao_grid GROUP BY race_id, driver_id
)
SELECT
  r.result_id, r.race_id, r.driver_id, r.constructor_id, r.status_id,
  r.grid,
  q.posicao,
  r.posicao_final,
  r.posicao_oficial,
  r.grid - r.posicao_final,
  r.pontos,
  r.voltas,
  ROUND(DATEDIFF(c.data, p.data_nascimento) / 365.25, 2),
  CASE WHEN cp.race_id IS NOT NULL THEN COALESCE(pt.qtd, 0) END,
  pt.media_ms,
  COALESCE(r.grid = 1, FALSE),
  COALESCE(r.posicao_oficial = 1, FALSE),
  COALESCE(r.posicao_oficial <= 3, FALSE),
  s.categoria_status <> 'NAO_LARGOU_OU_DESCLASSIFICADO',
  s.categoria_status = 'FINALIZOU',
  COALESCE(p.pais = ci.pais, FALSE)
FROM silver.resultados r
JOIN silver.corridas c        ON r.race_id = c.race_id
LEFT JOIN silver.circuitos ci ON c.circuit_id = ci.circuit_id
LEFT JOIN silver.pilotos p    ON r.driver_id = p.driver_id
LEFT JOIN silver.status s     ON r.status_id = s.status_id
LEFT JOIN quali q             ON r.race_id = q.race_id AND r.driver_id = q.driver_id
LEFT JOIN pits pt             ON r.race_id = pt.race_id AND r.driver_id = pt.driver_id
LEFT JOIN cobertura_pits cp   ON r.race_id = cp.race_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## fato_pit_stop

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fato_pit_stop (
  sk_corrida           INT     NOT NULL COMMENT 'FK para dim_corrida.',
  sk_piloto            INT     NOT NULL COMMENT 'FK para dim_piloto.',
  sk_equipe            INT              COMMENT 'FK para dim_equipe (equipe do piloto naquela corrida, via silver.resultados).',
  numero_parada        INT              COMMENT 'Ordem da parada do piloto na corrida (1, 2, 3...). Origem: silver.pit_stops.',
  volta                INT              COMMENT 'Volta da parada. Origem: silver.pit_stops.',
  duracao_ms           BIGINT           COMMENT 'Tempo total no pit lane (ms, > 0). Origem: silver.pit_stops.duracao_ms.',
  flag_parada_atipica  BOOLEAN          COMMENT 'TRUE se duração > Q3 + 3*IQR da temporada (bandeira vermelha/reparo). Origem: silver.pit_stops.',
  CONSTRAINT fk_pit_corrida FOREIGN KEY (sk_corrida) REFERENCES gold.dim_corrida,
  CONSTRAINT fk_pit_piloto  FOREIGN KEY (sk_piloto)  REFERENCES gold.dim_piloto,
  CONSTRAINT fk_pit_equipe  FOREIGN KEY (sk_equipe)  REFERENCES gold.dim_equipe
)
COMMENT 'Fato de paradas nos boxes. Granularidade: 1 linha por parada. Linhagem: silver.pit_stops + silver.resultados (equipe).';

INSERT INTO gold.fato_pit_stop
SELECT ps.race_id, ps.driver_id, e.constructor_id, ps.numero_parada, ps.volta, ps.duracao_ms, ps.flag_parada_atipica
FROM silver.pit_stops ps
LEFT JOIN (SELECT race_id, driver_id, MIN(constructor_id) AS constructor_id
           FROM silver.resultados GROUP BY race_id, driver_id) e
  ON ps.race_id = e.race_id AND ps.driver_id = e.driver_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Agregado: resumo de cada temporada
-- MAGIC Base da pergunta sobre competitividade. Usa a classificação do campeonato **após a última corrida** de cada ano.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_temporada
COMMENT 'Resumo por temporada: corridas, vencedores distintos, domínio da equipe mais vitoriosa e margem do campeão. Linhagem: fato_resultado + dim_corrida + silver.campeonato_pilotos.'
AS
WITH vitorias AS (
  SELECT d.ano, f.sk_piloto, f.sk_equipe
  FROM gold.fato_resultado f JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
  WHERE f.flag_vitoria
),
por_equipe AS (
  SELECT ano, sk_equipe, COUNT(*) AS vitorias,
         ROW_NUMBER() OVER (PARTITION BY ano ORDER BY COUNT(*) DESC) AS rn
  FROM vitorias GROUP BY ano, sk_equipe
),
venc AS (
  SELECT ano, COUNT(DISTINCT sk_piloto) AS n FROM vitorias GROUP BY ano
),
final AS (
  SELECT d.ano, cp.driver_id, cp.pontos_acumulados, cp.posicao_campeonato
  FROM silver.campeonato_pilotos cp
  JOIN gold.dim_corrida d ON cp.race_id = d.sk_corrida AND d.flag_ultima_corrida
)
SELECT
  d.ano,
  MAX(d.era_regulamentar)                                                    AS era_regulamentar,
  COUNT(DISTINCT d.sk_corrida)                                               AS corridas,
  MAX(vc.n)                                                                  AS vencedores_distintos,
  MAX(e.nome)                                                                AS equipe_mais_vitoriosa,
  ROUND(100.0 * MAX(pe.vitorias) / COUNT(DISTINCT d.sk_corrida), 1)          AS pct_vitorias_equipe_top,
  MAX(CASE WHEN fi.posicao_campeonato = 1 THEN p.nome_completo END)          AS campeao,
  MAX(CASE WHEN fi.posicao_campeonato = 1 THEN fi.pontos_acumulados END)     AS pontos_campeao,
  MAX(CASE WHEN fi.posicao_campeonato = 2 THEN fi.pontos_acumulados END)     AS pontos_vice,
  ROUND(100.0 * (MAX(CASE WHEN fi.posicao_campeonato = 1 THEN fi.pontos_acumulados END)
               - MAX(CASE WHEN fi.posicao_campeonato = 2 THEN fi.pontos_acumulados END))
               / MAX(CASE WHEN fi.posicao_campeonato = 1 THEN fi.pontos_acumulados END), 1) AS margem_campeao_pct
FROM gold.dim_corrida d
LEFT JOIN venc vc        ON vc.ano = d.ano
LEFT JOIN por_equipe pe  ON pe.ano = d.ano AND pe.rn = 1
LEFT JOIN gold.dim_equipe e ON pe.sk_equipe = e.sk_equipe
LEFT JOIN final fi       ON fi.ano = d.ano
LEFT JOIN gold.dim_piloto p ON fi.driver_id = p.sk_piloto
GROUP BY d.ano;

-- COMMAND ----------

ALTER TABLE gold.agg_temporada ALTER COLUMN ano                     COMMENT 'Temporada (1950-2024).';
ALTER TABLE gold.agg_temporada ALTER COLUMN era_regulamentar        COMMENT 'Era regulamentar da temporada. Origem: dim_corrida.';
ALTER TABLE gold.agg_temporada ALTER COLUMN corridas                COMMENT 'Número de corridas na temporada (> 0).';
ALTER TABLE gold.agg_temporada ALTER COLUMN vencedores_distintos    COMMENT 'Quantidade de pilotos diferentes que venceram ao menos uma corrida.';
ALTER TABLE gold.agg_temporada ALTER COLUMN equipe_mais_vitoriosa   COMMENT 'Equipe com mais vitórias na temporada.';
ALTER TABLE gold.agg_temporada ALTER COLUMN pct_vitorias_equipe_top COMMENT '% das corridas vencidas pela equipe mais vitoriosa (0-100). Indicador de domínio.';
ALTER TABLE gold.agg_temporada ALTER COLUMN campeao                 COMMENT 'Piloto campeão (posição 1 após a última corrida).';
ALTER TABLE gold.agg_temporada ALTER COLUMN pontos_campeao          COMMENT 'Pontos do campeão ao fim da temporada.';
ALTER TABLE gold.agg_temporada ALTER COLUMN pontos_vice             COMMENT 'Pontos do vice-campeão ao fim da temporada.';
ALTER TABLE gold.agg_temporada ALTER COLUMN margem_campeao_pct      COMMENT '(pontos campeão - pontos vice) / pontos campeão x 100. Normaliza as mudanças de sistema de pontos.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Evidência: contagens e integridade referencial

-- COMMAND ----------

SELECT 'dim_corrida' AS tabela, COUNT(*) AS linhas FROM gold.dim_corrida
UNION ALL SELECT 'dim_piloto', COUNT(*) FROM gold.dim_piloto
UNION ALL SELECT 'dim_equipe', COUNT(*) FROM gold.dim_equipe
UNION ALL SELECT 'dim_status', COUNT(*) FROM gold.dim_status
UNION ALL SELECT 'fato_resultado', COUNT(*) FROM gold.fato_resultado
UNION ALL SELECT 'fato_pit_stop', COUNT(*) FROM gold.fato_pit_stop
UNION ALL SELECT 'agg_temporada', COUNT(*) FROM gold.agg_temporada;

-- COMMAND ----------

-- Todas as colunas devem ser 0 (nenhuma linha órfã nas fatos)
SELECT
  (SELECT COUNT(*) FROM gold.fato_resultado f LEFT ANTI JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida) AS res_sem_corrida,
  (SELECT COUNT(*) FROM gold.fato_resultado f LEFT ANTI JOIN gold.dim_piloto d  ON f.sk_piloto = d.sk_piloto)   AS res_sem_piloto,
  (SELECT COUNT(*) FROM gold.fato_resultado f LEFT ANTI JOIN gold.dim_equipe d  ON f.sk_equipe = d.sk_equipe)   AS res_sem_equipe,
  (SELECT COUNT(*) FROM gold.fato_resultado f LEFT ANTI JOIN gold.dim_status d  ON f.sk_status = d.sk_status)   AS res_sem_status,
  (SELECT COUNT(*) FROM gold.fato_pit_stop f  LEFT ANTI JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida) AS pit_sem_corrida,
  (SELECT COUNT(*) FROM gold.fato_pit_stop f  LEFT ANTI JOIN gold.dim_piloto d  ON f.sk_piloto = d.sk_piloto)   AS pit_sem_piloto;

-- COMMAND ----------

-- Cobertura temporal de cada fonte (importante para interpretar P2 e P4)
SELECT 'resultados' AS dado, MIN(d.ano) AS de, MAX(d.ano) AS ate FROM gold.fato_resultado f JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
UNION ALL
SELECT 'qualifying', MIN(d.ano), MAX(d.ano) FROM gold.fato_resultado f JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida WHERE f.posicao_classificacao IS NOT NULL
UNION ALL
SELECT 'pit stops', MIN(d.ano), MAX(d.ano) FROM gold.fato_pit_stop f JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida;
