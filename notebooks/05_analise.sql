-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · Análise: respondendo às perguntas de negócio
-- MAGIC
-- MAGIC Todas as consultas usam a camada **Gold**. Depois de rodar, registre abaixo de cada pergunta (célula "Discussão")
-- MAGIC o que os números mostram, e tire um screenshot do resultado/gráfico para o README.

-- COMMAND ----------

USE CATALOG mvp_f1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P1 · Largar na pole decide a corrida? Isso mudou ao longo das eras e varia por circuito?

-- COMMAND ----------

-- Conversão pole → vitória e correlação grid × chegada, por era
SELECT d.era_regulamentar,
       COUNT(DISTINCT d.sk_corrida)                                              AS corridas,
       ROUND(100 * AVG(CASE WHEN f.flag_pole THEN CAST(f.flag_vitoria AS INT) END), 1) AS pct_pole_vence,
       ROUND(100 * AVG(CASE WHEN f.flag_vitoria THEN CAST(f.grid <= 3 AS INT) END), 1) AS pct_vencedor_largou_top3,
       ROUND(AVG(CASE WHEN f.flag_vitoria THEN f.grid END), 2)                   AS grid_medio_do_vencedor,
       ROUND(CORR(f.grid, f.posicao_final), 3)                                   AS correlacao_grid_chegada
FROM gold.fato_resultado f
JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
WHERE f.flag_largou
GROUP BY d.era_regulamentar
ORDER BY d.era_regulamentar;

-- COMMAND ----------

-- Circuitos onde a pole mais (e menos) vira vitória (mínimo de 10 corridas)
SELECT d.circuito_nome, d.circuito_pais,
       COUNT(*)                                            AS corridas,
       ROUND(100 * AVG(CAST(f.flag_vitoria AS INT)), 1)    AS pct_pole_vence
FROM gold.fato_resultado f
JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
WHERE f.flag_pole
GROUP BY d.circuito_nome, d.circuito_pais
HAVING COUNT(*) >= 10
ORDER BY pct_pole_vence DESC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC df = spark.sql("""
-- MAGIC   SELECT d.decada, CAST(100 * AVG(CAST(f.flag_vitoria AS INT)) AS DOUBLE) AS pct
-- MAGIC   FROM mvp_f1.gold.fato_resultado f JOIN mvp_f1.gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
-- MAGIC   WHERE f.flag_pole GROUP BY d.decada ORDER BY d.decada
-- MAGIC """).toPandas()
-- MAGIC ax = df.plot.bar(x="decada", y="pct", legend=False, figsize=(10, 4), color="#1f4e79")
-- MAGIC ax.set_title("% de corridas vencidas por quem largou na pole, por década"); ax.set_xlabel(""); ax.set_ylabel("%")
-- MAGIC for i, v in enumerate(df.pct): ax.text(i, v + 1, f"{v:.0f}%", ha="center")
-- MAGIC plt.tight_layout(); plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P1:** _(preencher)_ — a pole ficou mais decisiva na era moderna? quais circuitos são "de pole"
-- MAGIC (difíceis de ultrapassar, como Mônaco) e onde a largada importa menos?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P2 · Os pit stops ficaram mais rápidos? E a estratégia de menos paradas compensa?
-- MAGIC A duração registrada é o **tempo total no pit lane** (entrada → saída), não só o tempo parado.
-- MAGIC Paradas atípicas (bandeira vermelha, reparos) são excluídas.

-- COMMAND ----------

SELECT d.ano,
       COUNT(*)                                                        AS paradas,
       ROUND(PERCENTILE_APPROX(p.duracao_ms, 0.5) / 1000, 2)           AS mediana_pit_lane_s,
       ROUND(MIN(p.duracao_ms) / 1000, 2)                              AS parada_mais_rapida_s,
       ROUND(COUNT(*) / COUNT(DISTINCT p.sk_corrida, p.sk_piloto), 2)  AS paradas_por_piloto_que_parou
FROM gold.fato_pit_stop p
JOIN gold.dim_corrida d ON p.sk_corrida = d.sk_corrida
WHERE NOT p.flag_parada_atipica
GROUP BY d.ano
ORDER BY d.ano;

-- COMMAND ----------

-- Estratégia × resultado: pilotos que terminaram a prova, agrupados pelo nº de paradas
SELECT d.era_regulamentar, f.qtd_pit_stops,
       COUNT(*)                          AS resultados,
       ROUND(AVG(f.posicoes_ganhas), 2)  AS media_posicoes_ganhas,
       ROUND(AVG(f.posicao_final), 2)    AS media_posicao_final,
       ROUND(100 * AVG(CAST(f.flag_podio AS INT)), 1) AS pct_podio
FROM gold.fato_resultado f
JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
WHERE f.flag_finalizou AND f.qtd_pit_stops BETWEEN 1 AND 4 AND f.grid IS NOT NULL
GROUP BY d.era_regulamentar, f.qtd_pit_stops
ORDER BY d.era_regulamentar, f.qtd_pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P2:** _(preencher)_ — o tempo de pit lane caiu ou subiu (o fim do reabastecimento em 2010 muda
-- MAGIC tudo)? parar menos vezes está associado a ganhar posições? atenção: correlação não é causalidade (quem está na
-- MAGIC frente tende a fazer a estratégia "ideal").

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P3 · A F1 ficou mais confiável? Como evoluíram abandonos por falha mecânica e por acidente?
-- MAGIC Base: pilotos que largaram (exclui não qualificados, retirados e desclassificados).

-- COMMAND ----------

SELECT d.decada,
       COUNT(*)                                                                             AS largadas,
       ROUND(100 * AVG(CASE WHEN s.categoria_status = 'FINALIZOU' THEN 1 ELSE 0 END), 1)      AS pct_terminou,
       ROUND(100 * AVG(CASE WHEN s.categoria_status = 'FALHA_MECANICA' THEN 1 ELSE 0 END), 1) AS pct_falha_mecanica,
       ROUND(100 * AVG(CASE WHEN s.categoria_status = 'ACIDENTE' THEN 1 ELSE 0 END), 1)       AS pct_acidente,
       ROUND(100 * AVG(CASE WHEN s.categoria_status IN ('PILOTO', 'OUTROS') THEN 1 ELSE 0 END), 1) AS pct_outros
FROM gold.fato_resultado f
JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
JOIN gold.dim_status s  ON f.sk_status = s.sk_status
WHERE f.flag_largou
GROUP BY d.decada
ORDER BY d.decada;

-- COMMAND ----------

-- Quais componentes mais quebravam em cada era? (top 3 por era)
WITH falhas AS (
  SELECT d.era_regulamentar, s.status, COUNT(*) AS ocorrencias,
         ROW_NUMBER() OVER (PARTITION BY d.era_regulamentar ORDER BY COUNT(*) DESC) AS rn
  FROM gold.fato_resultado f
  JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
  JOIN gold.dim_status s  ON f.sk_status = s.sk_status
  WHERE s.categoria_status = 'FALHA_MECANICA'
  GROUP BY d.era_regulamentar, s.status
)
SELECT era_regulamentar, rn AS ranking, status, ocorrencias
FROM falhas WHERE rn <= 3
ORDER BY era_regulamentar, rn;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC df = spark.sql("""
-- MAGIC   SELECT d.decada,
-- MAGIC          CAST(100 * AVG(CASE WHEN s.categoria_status = 'FINALIZOU' THEN 1 ELSE 0 END) AS DOUBLE)      AS terminou,
-- MAGIC          CAST(100 * AVG(CASE WHEN s.categoria_status = 'FALHA_MECANICA' THEN 1 ELSE 0 END) AS DOUBLE) AS falha_mecanica,
-- MAGIC          CAST(100 * AVG(CASE WHEN s.categoria_status = 'ACIDENTE' THEN 1 ELSE 0 END) AS DOUBLE)       AS acidente
-- MAGIC   FROM mvp_f1.gold.fato_resultado f
-- MAGIC   JOIN mvp_f1.gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
-- MAGIC   JOIN mvp_f1.gold.dim_status s ON f.sk_status = s.sk_status
-- MAGIC   WHERE f.flag_largou GROUP BY d.decada ORDER BY d.decada
-- MAGIC """).toPandas().set_index("decada")
-- MAGIC ax = df.plot(figsize=(10, 4), marker="o", linewidth=2)
-- MAGIC ax.set_title("Destino dos pilotos que largaram, por década (%)"); ax.set_xlabel(""); ax.set_ylabel("%")
-- MAGIC ax.grid(alpha=.3); plt.tight_layout(); plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P3:** _(preencher)_ — em que década a confiabilidade deu o salto? motor e câmbio deixaram de ser o
-- MAGIC principal problema? os acidentes caíram na mesma proporção?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P4 · Carro ou piloto? Quem mais bateu o próprio companheiro de equipe na classificação?
-- MAGIC Com o mesmo carro, a diferença entre companheiros isola (em parte) o fator piloto. Compara-se a posição no
-- MAGIC qualifying de pares de pilotos da **mesma equipe na mesma corrida** (período com dados de qualifying).

-- COMMAND ----------

WITH q AS (
  SELECT sk_corrida, sk_equipe, sk_piloto, posicao_classificacao
  FROM gold.fato_resultado
  WHERE posicao_classificacao IS NOT NULL
),
duelos AS (
  SELECT a.sk_piloto, a.sk_corrida,
         CASE WHEN a.posicao_classificacao < b.posicao_classificacao THEN 1 ELSE 0 END AS venceu
  FROM q a
  JOIN q b ON a.sk_corrida = b.sk_corrida AND a.sk_equipe = b.sk_equipe AND a.sk_piloto <> b.sk_piloto
)
SELECT p.nome_completo, p.nacionalidade,
       COUNT(*)                        AS duelos,
       SUM(venceu)                     AS vitorias_no_duelo,
       ROUND(100 * AVG(venceu), 1)     AS pct_vitorias_sobre_companheiro
FROM duelos d
JOIN gold.dim_piloto p ON d.sk_piloto = p.sk_piloto
GROUP BY p.nome_completo, p.nacionalidade
HAVING COUNT(*) >= 50
ORDER BY pct_vitorias_sobre_companheiro DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P4:** _(preencher)_ — os campeões aparecem no topo? algum piloto com poucos títulos domina os
-- MAGIC companheiros (talento em carro ruim)? limitação: quem teve companheiro fraco é favorecido.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P5 · Existe vantagem de correr em casa?
-- MAGIC Comparação **pareada**: para cada piloto e temporada em que ele correu em casa, compara o desempenho no GP de casa
-- MAGIC com a média dele nas demais corridas **da mesma temporada** (mesmo carro, mesmo ano).

-- COMMAND ----------

WITH base AS (
  SELECT f.sk_piloto, d.ano, f.flag_em_casa, f.posicao_final, f.pontos,
         CAST(f.flag_podio AS INT) AS podio, CAST(f.flag_finalizou AS INT) AS terminou
  FROM gold.fato_resultado f
  JOIN gold.dim_corrida d ON f.sk_corrida = d.sk_corrida
  WHERE f.flag_largou
),
por_piloto_ano AS (
  SELECT sk_piloto, ano,
         AVG(CASE WHEN flag_em_casa THEN posicao_final END)      AS pos_casa,
         AVG(CASE WHEN NOT flag_em_casa THEN posicao_final END)  AS pos_fora,
         AVG(CASE WHEN flag_em_casa THEN podio END)              AS podio_casa,
         AVG(CASE WHEN NOT flag_em_casa THEN podio END)          AS podio_fora,
         AVG(CASE WHEN flag_em_casa THEN terminou END)           AS term_casa,
         AVG(CASE WHEN NOT flag_em_casa THEN terminou END)       AS term_fora
  FROM base GROUP BY sk_piloto, ano
  HAVING pos_casa IS NOT NULL AND pos_fora IS NOT NULL
)
SELECT COUNT(*)                                      AS pares_piloto_temporada,
       ROUND(AVG(pos_fora - pos_casa), 2)            AS ganho_medio_posicoes_em_casa,
       ROUND(100 * AVG(CASE WHEN pos_casa < pos_fora THEN 1 ELSE 0 END), 1) AS pct_pares_melhor_em_casa,
       ROUND(100 * AVG(podio_casa), 1)               AS pct_podio_casa,
       ROUND(100 * AVG(podio_fora), 1)               AS pct_podio_fora,
       ROUND(100 * AVG(term_casa), 1)                AS pct_terminou_casa,
       ROUND(100 * AVG(term_fora), 1)                AS pct_terminou_fora
FROM por_piloto_ano;

-- COMMAND ----------

-- Por país do piloto (mínimo de 30 largadas em casa)
SELECT p.pais,
       SUM(CASE WHEN f.flag_em_casa THEN 1 ELSE 0 END)                              AS largadas_em_casa,
       ROUND(AVG(CASE WHEN f.flag_em_casa THEN f.posicao_final END), 2)             AS pos_media_casa,
       ROUND(AVG(CASE WHEN NOT f.flag_em_casa THEN f.posicao_final END), 2)         AS pos_media_fora
FROM gold.fato_resultado f
JOIN gold.dim_piloto p ON f.sk_piloto = p.sk_piloto
WHERE f.flag_largou
GROUP BY p.pais
HAVING SUM(CASE WHEN f.flag_em_casa THEN 1 ELSE 0 END) >= 30
ORDER BY pos_media_fora - pos_media_casa DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P5:** _(preencher)_ — a diferença é relevante ou é ruído? em quais países aparece mais?
-- MAGIC limitação: pilotos locais convidados (carros fracos) aparecem só no GP de casa e são excluídos pela comparação pareada.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P6 · As temporadas ficaram mais ou menos competitivas?

-- COMMAND ----------

SELECT era_regulamentar,
       COUNT(*)                                  AS temporadas,
       ROUND(AVG(vencedores_distintos), 1)       AS media_vencedores_distintos,
       ROUND(AVG(pct_vitorias_equipe_top), 1)    AS media_pct_vitorias_equipe_top,
       ROUND(AVG(margem_campeao_pct), 1)         AS media_margem_campeao_pct,
       SUM(CASE WHEN margem_campeao_pct < 5 THEN 1 ELSE 0 END) AS temporadas_decididas_por_menos_de_5pct
FROM gold.agg_temporada
GROUP BY era_regulamentar
ORDER BY era_regulamentar;

-- COMMAND ----------

-- Temporadas mais dominadas e mais disputadas
(SELECT 'mais dominada' AS tipo, ano, equipe_mais_vitoriosa, pct_vitorias_equipe_top, campeao, margem_campeao_pct
 FROM gold.agg_temporada ORDER BY pct_vitorias_equipe_top DESC LIMIT 5)
UNION ALL
(SELECT 'mais disputada', ano, equipe_mais_vitoriosa, pct_vitorias_equipe_top, campeao, margem_campeao_pct
 FROM gold.agg_temporada WHERE margem_campeao_pct IS NOT NULL ORDER BY margem_campeao_pct ASC LIMIT 5);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC df = spark.sql("""SELECT ano, CAST(pct_vitorias_equipe_top AS DOUBLE) AS dominio,
-- MAGIC                          CAST(vencedores_distintos AS DOUBLE) AS vencedores
-- MAGIC                   FROM mvp_f1.gold.agg_temporada ORDER BY ano""").toPandas()
-- MAGIC fig, ax1 = plt.subplots(figsize=(11, 4))
-- MAGIC ax1.plot(df.ano, df.dominio, color="#c62828", linewidth=2, label="% vitórias da equipe top")
-- MAGIC ax1.set_ylabel("% vitórias da equipe top")
-- MAGIC ax2 = ax1.twinx(); ax2.bar(df.ano, df.vencedores, alpha=.3, color="#1f4e79", label="vencedores distintos")
-- MAGIC ax2.set_ylabel("vencedores distintos")
-- MAGIC ax1.set_title("Domínio por temporada: % de vitórias da equipe mais vitoriosa × nº de vencedores")
-- MAGIC fig.tight_layout(); plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Discussão P6:** _(preencher)_ — a era híbrida foi a mais dominada? as mudanças de regulamento (2014, 2022)
-- MAGIC criaram novos dominantes? houve temporadas decididas por margem mínima?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Discussão geral
-- MAGIC _(preencher)_ — conecte as respostas ao problema: o que mais decide uma corrida e um campeonato (grid, carro,
-- MAGIC piloto, confiabilidade, estratégia) e como o peso de cada fator mudou entre as eras.
