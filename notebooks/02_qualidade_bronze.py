# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Qualidade dos dados (Bronze)
# MAGIC
# MAGIC Antes de sair limpando, quis entender o que tinha de errado no dado bruto. Separei a análise por dimensão de qualidade (completude, unicidade, integridade, consistência, acurácia e outliers).
# MAGIC Tudo que aparece aqui vira alguma regra de tratamento no notebook 03. Este notebook só lê, não grava nada.

# COMMAND ----------

CATALOGO = "mvp_f1"
B = f"{CATALOGO}.bronze"
from pyspark.sql import functions as F

TABELAS = ["circuits", "constructors", "drivers", "races", "results", "qualifying",
           "pit_stops", "status", "driver_standings", "constructor_standings",
           "lap_times", "sprint_results", "constructor_results", "seasons"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude
# MAGIC Primeira surpresa: o Ergast não usa nulo de verdade, ele grava o texto `\N`. Então contei `\N`, string vazia e NULL juntos, por coluna.

# COMMAND ----------

linhas = []
for t in TABELAS:
    df = spark.table(f"{B}.{t}")
    cols = [c for c in df.columns if not c.startswith("_")]
    total = df.count()
    agg = df.agg(*[F.sum((F.col(c).isNull() | (F.trim(F.col(c)).isin("\\N", ""))).cast("int")).alias(c) for c in cols]).collect()[0]
    for c in cols:
        linhas.append((t, c, total, agg[c] or 0, round(100 * (agg[c] or 0) / total, 2) if total else 0.0))

completude = spark.createDataFrame(linhas, "tabela string, coluna string, total long, nulos long, pct_nulos double")
display(completude.filter("nulos > 0").orderBy(F.desc("pct_nulos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unicidade

# COMMAND ----------

chaves = {
    "circuits": ["circuitId"], "constructors": ["constructorId"], "drivers": ["driverId"],
    "races": ["raceId"], "status": ["statusId"], "results": ["resultId"],
    "qualifying": ["qualifyId"], "pit_stops": ["raceId", "driverId", "stop"],
    "driver_standings": ["driverStandingsId"], "lap_times": ["raceId", "driverId", "lap"],
}
res = []
for t, k in chaves.items():
    df = spark.table(f"{B}.{t}")
    total = df.count()
    distintos = df.select(*k).distinct().count()
    res.append((t, ", ".join(k), total, distintos, total - distintos))
# chave de negócio: um piloto deveria ter no máximo 1 resultado por corrida
r = spark.table(f"{B}.results")
res.append(("results (negócio)", "raceId, driverId", r.count(),
            r.select("raceId", "driverId").distinct().count(),
            r.count() - r.select("raceId", "driverId").distinct().count()))
display(spark.createDataFrame(res, "tabela string, chave string, linhas long, distintos long, duplicados long"))

# COMMAND ----------

# MAGIC %md
# MAGIC Os ids são únicos, mas apareceram pilotos com mais de um resultado na mesma corrida. Fui pesquisar e é coisa dos anos 50: era permitido dois pilotos dividirem o mesmo carro, e às vezes o piloto trocava de carro no meio da prova. Não é erro, então vou manter.

# COMMAND ----------

display(spark.sql(f"""
SELECT r.raceId, ra.year, ra.name, r.driverId, COUNT(*) AS n
FROM {B}.results r JOIN {B}.races ra ON r.raceId = ra.raceId
GROUP BY ALL HAVING COUNT(*) > 1 ORDER BY ra.year LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integridade referencial
# MAGIC Procurando registros órfãos (chave estrangeira que não existe na tabela de origem).

# COMMAND ----------

display(spark.sql(f"""
SELECT 'results -> races'        AS relacao, COUNT(*) AS orfaos FROM {B}.results x LEFT ANTI JOIN {B}.races y        ON x.raceId = y.raceId
UNION ALL SELECT 'results -> drivers',      COUNT(*) FROM {B}.results x LEFT ANTI JOIN {B}.drivers y      ON x.driverId = y.driverId
UNION ALL SELECT 'results -> constructors', COUNT(*) FROM {B}.results x LEFT ANTI JOIN {B}.constructors y ON x.constructorId = y.constructorId
UNION ALL SELECT 'results -> status',       COUNT(*) FROM {B}.results x LEFT ANTI JOIN {B}.status y       ON x.statusId = y.statusId
UNION ALL SELECT 'races -> circuits',       COUNT(*) FROM {B}.races x   LEFT ANTI JOIN {B}.circuits y     ON x.circuitId = y.circuitId
UNION ALL SELECT 'qualifying -> races',     COUNT(*) FROM {B}.qualifying x LEFT ANTI JOIN {B}.races y     ON x.raceId = y.raceId
UNION ALL SELECT 'pit_stops -> results',    COUNT(*) FROM {B}.pit_stops x LEFT ANTI JOIN {B}.results y    ON x.raceId = y.raceId AND x.driverId = y.driverId
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consistência
# MAGIC Como tudo chegou como texto, conferi se datas, tempos e números seguem o mesmo formato.

# COMMAND ----------

display(spark.sql(f"""
SELECT 'races.date fora de yyyy-MM-dd' AS verificacao,
       SUM(CASE WHEN date NOT RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 1 ELSE 0 END) AS ocorrencias FROM {B}.races
UNION ALL
SELECT 'drivers.dob fora de yyyy-MM-dd',
       SUM(CASE WHEN dob <> '\\\\N' AND dob NOT RLIKE '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 1 ELSE 0 END) FROM {B}.drivers
UNION ALL
SELECT 'qualifying.q1 fora de m:ss.sss (exceto \\\\N/vazio)',
       SUM(CASE WHEN q1 NOT IN ('\\\\N', '') AND q1 NOT RLIKE '^[0-9]+:[0-9]{{2}}\\\\.[0-9]{{3}}$' THEN 1 ELSE 0 END) FROM {B}.qualifying
UNION ALL
SELECT 'results.grid não numérico',
       SUM(CASE WHEN TRY_CAST(grid AS INT) IS NULL THEN 1 ELSE 0 END) FROM {B}.results
UNION ALL
SELECT 'results.points não numérico',
       SUM(CASE WHEN TRY_CAST(points AS DOUBLE) IS NULL THEN 1 ELSE 0 END) FROM {B}.results
UNION ALL
SELECT 'pit_stops.duration no formato m:ss.sss (paradas > 60 s)',
       SUM(CASE WHEN duration LIKE '%:%' THEN 1 ELSE 0 END) FROM {B}.pit_stops
UNION ALL
SELECT 'drivers.nationality com espaços extras',
       SUM(CASE WHEN nationality <> TRIM(nationality) THEN 1 ELSE 0 END) FROM {B}.drivers
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Olhando os valores distintos de país e nacionalidade atrás de grafias diferentes para a mesma coisa:

# COMMAND ----------

display(spark.table(f"{B}.circuits").groupBy("country").count().orderBy("country"))

# COMMAND ----------

display(spark.table(f"{B}.drivers").groupBy("nationality").count().orderBy("nationality"))

# COMMAND ----------

# MAGIC %md
# MAGIC O `status` tem mais de 130 valores diferentes (Engine, Gearbox, +1 Lap, Accident...). Do jeito que está não dá pra analisar abandono, vou agrupar em categorias na Silver.

# COMMAND ----------

display(spark.sql(f"""
SELECT s.status, COUNT(*) AS resultados
FROM {B}.results r JOIN {B}.status s ON r.statusId = s.statusId
GROUP BY s.status ORDER BY resultados DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia
# MAGIC Checando se os valores fazem sentido: grid, posição, pontos, voltas e idade do piloto na corrida.

# COMMAND ----------

display(spark.sql(f"""
WITH r AS (
  SELECT TRY_CAST(res.grid AS INT)          AS grid,
         TRY_CAST(res.positionOrder AS INT) AS pos,
         TRY_CAST(res.points AS DOUBLE)     AS pontos,
         TRY_CAST(res.laps AS INT)          AS voltas,
         DATEDIFF(TRY_CAST(ra.date AS DATE), TRY_CAST(NULLIF(d.dob, '\\\\N') AS DATE)) / 365.25 AS idade
  FROM {B}.results res
  JOIN {B}.races ra  ON res.raceId = ra.raceId
  JOIN {B}.drivers d ON res.driverId = d.driverId
)
SELECT MIN(grid) AS grid_min, MAX(grid) AS grid_max,
       SUM(CASE WHEN grid = 0 THEN 1 ELSE 0 END)          AS grid_zero_largou_dos_boxes,
       MIN(pos) AS posicao_min, MAX(pos) AS posicao_max,
       MIN(pontos) AS pontos_min, MAX(pontos) AS pontos_max,
       MIN(voltas) AS voltas_min, MAX(voltas) AS voltas_max,
       ROUND(MIN(idade), 1) AS idade_min, ROUND(MAX(idade), 1) AS idade_max,
       SUM(CASE WHEN idade < 16 OR idade > 60 THEN 1 ELSE 0 END) AS idade_implausivel
FROM r
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Outliers nos pit stops
# MAGIC Um pit stop normal fica entre 20 e 30 segundos contando o tempo no pit lane. Mas tem parada de vários minutos (bandeira vermelha, conserto do carro) e isso estraga qualquer média.
# MAGIC Usei Q3 + 3*IQR calculado por temporada, porque o tempo médio mudou bastante ao longo dos anos.

# COMMAND ----------

display(spark.sql(f"""
WITH p AS (
  SELECT ra.year, TRY_CAST(ps.milliseconds AS BIGINT) / 1000.0 AS segundos
  FROM {B}.pit_stops ps JOIN {B}.races ra ON ps.raceId = ra.raceId
),
lim AS (
  SELECT year, PERCENTILE_APPROX(segundos, 0.25) AS q1, PERCENTILE_APPROX(segundos, 0.5) AS mediana,
         PERCENTILE_APPROX(segundos, 0.75) AS q3
  FROM p GROUP BY year
)
SELECT p.year, COUNT(*) AS paradas, ROUND(l.mediana, 2) AS mediana_s, ROUND(MAX(p.segundos), 1) AS max_s,
       SUM(CASE WHEN p.segundos > l.q3 + 3 * (l.q3 - l.q1) THEN 1 ELSE 0 END) AS outliers
FROM p JOIN lim l USING (year)
GROUP BY p.year, l.mediana
ORDER BY p.year
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que vou tratar na Silver
# MAGIC
# MAGIC Juntando tudo que encontrei:
# MAGIC
# MAGIC - `\N` e texto vazio viram NULL em todas as colunas
# MAGIC - `position`, `time` e `milliseconds` ficam nulos para quem não terminou a corrida. Mantive assim e uso `positionOrder` (sempre preenchido) quando preciso de posição
# MAGIC - qualifying e pit stops só existem nas temporadas mais recentes, então as perguntas que dependem deles ficam limitadas a esse período
# MAGIC - tudo é texto: converto com `try_cast` para não quebrar em valor estranho
# MAGIC - tempos de classificação vêm como "1:23.456", converto para milissegundos
# MAGIC - país do circuito aparece como USA e United States (e UK, UAE). Padronizei com o nome completo
# MAGIC - nacionalidade tem espaço sobrando e sinônimos (Argentinian / Argentine), resolvo com trim e um de-para
# MAGIC - os 130+ status viram uma coluna de categoria (terminou, acidente, falha mecânica etc.)
# MAGIC - resultados duplicados por carro dividido nos anos 50: mantidos
# MAGIC - `grid = 0` não é pole, é quem largou dos boxes ou não largou. Viro NULL e crio uma flag
# MAGIC - paradas muito longas ganham uma flag e ficam de fora das médias de pit stop
