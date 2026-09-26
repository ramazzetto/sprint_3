-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Análise
-- MAGIC
-- MAGIC Respondendo as 6 perguntas do projeto usando só a Gold. Em quase todas as consultas filtro `flag_largou` para não contar quem nem largou.

-- COMMAND ----------

USE CATALOG mvp_f1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 1 - Largar na pole decide a corrida? Mudou com as eras? Depende do circuito?

-- COMMAND ----------

-- quanto a pole vira vitória em cada era
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

-- por circuito (só os que tiveram pelo menos 10 corridas)
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
-- MAGIC **Conclusão:** A pole pesa mais hoje do que antes: virava vitória em ~37% das vezes nos anos 50-70, 28% nos anos 80 (muita quebra) e passou de 50% a partir dos anos 2000. A correlação largada x chegada foi de 0,33 nos anos 80 para 0,63 hoje.
-- MAGIC Por circuito, Barcelona, Abu Dhabi e Singapura são os mais "de pole". Monza e Spa, com reta longa, os menos. Mônaco ficou no meio (45%), o que eu não esperava.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 2 - Os pit stops ficaram mais rápidos? Parar menos compensa?
-- MAGIC Cuidado na leitura: a duração no dataset é o tempo total dentro do pit lane, não só o carro parado. As paradas atípicas ficaram de fora.

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

-- só quem terminou a prova, agrupado pelo número de paradas
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
-- MAGIC **Conclusão:** O tempo no pit lane quase não mudou entre 2011 e 2024 (22 a 24s). Esse dado mede o pit lane inteiro, não só a troca de pneus, então depende mais do circuito do que da equipe.
-- MAGIC O que caiu foi o número de paradas (2,55 por piloto em 2011, primeiro ano da Pirelli). Quem para menos ganha mais posições, mas isso não prova que parar menos seja melhor: quem para muito geralmente teve algum problema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 3 - A F1 ficou mais confiável? Como evoluíram os abandonos por quebra e por acidente?

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

-- o que mais quebrava em cada era (top 3)
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
-- MAGIC **Conclusão:** Maior mudança do projeto. Até os anos 90 só metade dos pilotos terminava a corrida, hoje são 86%. A queda veio principalmente das quebras (41% nos anos 80, 6% hoje). O motor sempre foi o que mais quebrou, mas passou de 513 quebras (1968-82) para 22 (2022-24).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 4 - Carro ou piloto? Quem mais bateu o companheiro de equipe no treino classificatório?
-- MAGIC A ideia é que com o mesmo carro a diferença fica mais por conta do piloto. Comparo a posição no qualifying dos dois pilotos da mesma equipe em cada corrida.
-- MAGIC Só vale para o período que tem dado de qualifying, e coloquei mínimo de 50 duelos para não aparecer piloto com poucas corridas.

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
-- MAGIC **Conclusão:** Häkkinen, Verstappen, Russell e Alonso lideram. O Alonso chama atenção pelos 385 duelos. Hamilton ficou em 62%, mas teve companheiros fortes. Aparecem também pilotos sem título (Buemi, Panis, Albon).
-- MAGIC Limitação: só tem qualifying de 1994 em diante, e o resultado depende de quem foi o companheiro.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 5 - Piloto corre melhor em casa?
-- MAGIC Comparar a média geral de quem corre em casa com quem corre fora mistura muita coisa (piloto local convidado com carro ruim, por exemplo).
-- MAGIC Por isso comparo o mesmo piloto na mesma temporada: GP de casa contra a média dele nas outras corridas do ano.

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

-- por país (mínimo de 30 largadas em casa)
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
-- MAGIC **Conclusão:** A vantagem existe, mas é pequena: 0,09 posição em média e 55% dos casos melhores em casa. Pódio sobe de 12,7% para 14%. Não daria pra afirmar que não é ruído.
-- MAGIC A tabela por país mostra por que precisei da comparação pareada: EUA e Reino Unido aparecem piores em casa por causa de piloto local que só corria o GP de casa.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 6 - As temporadas ficaram mais ou menos disputadas?

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

-- top 5 mais dominadas e top 5 mais disputadas
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
-- MAGIC ax1.set_title("% de vitórias da equipe mais vitoriosa x número de vencedores, por temporada")
-- MAGIC fig.tight_layout(); plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Conclusão:** A era híbrida foi a mais dominada (71% das vitórias para a equipe top) e 2023 foi a temporada mais dominada de todas (Red Bull com 95,5%). A mais equilibrada foi 1968-1982.
-- MAGIC Domínio de equipe não quer dizer campeonato fácil: em 2016 a Mercedes venceu 90% das corridas e o título foi decidido por 1,3%, com a briga entre os dois pilotos dela. O título mais apertado foi 1984 (Lauda por meio ponto).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusão geral
-- MAGIC Até os anos 90 o que mais decidia era chegar ao fim, porque metade do grid quebrava. Com os carros confiáveis, o peso foi para a classificação: largar na frente nunca valeu tanto. O carro continua sendo o fator principal (eras longas de uma equipe só), o piloto aparece mais contra o companheiro, e estratégia de pit e correr em casa pesam pouco perto disso.
