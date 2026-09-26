# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Perfil de Qualidade dos Dados (Bronze)
# MAGIC
# MAGIC Antes de transformar, medimos a qualidade do dado bruto:
# MAGIC
# MAGIC | Dimensão | Pergunta | Seção |
# MAGIC |---|---|---|
# MAGIC | Completude | Quantos `\N` / vazios existem por coluna? | 1 |
# MAGIC | Unicidade | As chaves primárias são únicas? | 2 |
# MAGIC | Integridade referencial | Toda chave estrangeira aponta para um registro existente? | 3 |
# MAGIC | Consistência | Datas, tempos e números seguem o formato? Há grafias diferentes para o mesmo valor? | 4 |
# MAGIC | Acurácia | Os valores fazem sentido (idade do piloto, grid, pontos, voltas)? | 5 |
# MAGIC | Outliers | Há valores extremos (ex.: paradas nos boxes de vários minutos)? | 6 |
# MAGIC
# MAGIC Cada achado vira uma regra no notebook `03_silver_transformacao` (resumo no fim).

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
# MAGIC No Ergast o nulo é o texto `\N`. Contamos `\N`, vazio e `NULL` por coluna.

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
# MAGIC ## 2. Unicidade das chaves

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
# MAGIC Se houver duplicados na chave de negócio de `results`, vale inspecionar: no início da F1 era permitido **dividir
# MAGIC o carro** (dois pilotos no mesmo carro), o que gera casos legítimos.

# COMMAND ----------

display(spark.sql(f"""
SELECT r.raceId, ra.year, ra.name, r.driverId, COUNT(*) AS n
FROM {B}.results r JOIN {B}.races ra ON r.raceId = ra.raceId
GROUP BY ALL HAVING COUNT(*) > 1 ORDER BY ra.year LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Integridade referencial (registros órfãos)

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
# MAGIC ## 4. Consistência (formatos e grafias)

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
# MAGIC ### Domínios: grafias diferentes para a mesma coisa?

# COMMAND ----------

display(spark.table(f"{B}.circuits").groupBy("country").count().orderBy("country"))

# COMMAND ----------

display(spark.table(f"{B}.drivers").groupBy("nationality").count().orderBy("nationality"))

# COMMAND ----------

# MAGIC %md
# MAGIC `status` tem mais de 130 valores (ex.: `Engine`, `Gearbox`, `+1 Lap`, `Accident`). Para analisar confiabilidade eles
# MAGIC precisam ser **agrupados em categorias**, o que é feito na Silver.

# COMMAND ----------

display(spark.sql(f"""
SELECT s.status, COUNT(*) AS resultados
FROM {B}.results r JOIN {B}.status s ON r.statusId = s.statusId
GROUP BY s.status ORDER BY resultados DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia (valores plausíveis)

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
# MAGIC ## 6. Outliers: duração das paradas nos boxes
# MAGIC Uma parada normal dura de ~20 a ~30 s (tempo total no pit lane). Paradas de vários minutos acontecem em
# MAGIC **bandeira vermelha** ou em **reparos**, e distorcem médias. Usamos a regra Q3 + 3·IQR por temporada.

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
# MAGIC ## Resumo: problema → tratamento na Silver
# MAGIC
# MAGIC > **Preencha os números** com os resultados das células acima antes de copiar para o README.
# MAGIC
# MAGIC | # | Dimensão | Problema | Tratamento na Silver |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Completude | Nulo representado pelo texto `\N` | `\N` e vazio → `NULL` em todas as colunas |
# MAGIC | 2 | Completude | `results.position`, `time`, `milliseconds` nulos para quem não terminou | Mantidos `NULL`; `position_order` (sempre preenchido) usado nas análises |
# MAGIC | 3 | Completude | `qualifying` e `pit_stops` só existem nas temporadas mais recentes | Cobertura documentada; análises dessas perguntas limitadas ao período com dado |
# MAGIC | 4 | Consistência | Tudo como texto | Cast para `INT`, `DOUBLE`, `DATE` com `try_cast` |
# MAGIC | 5 | Consistência | Tempos como texto `m:ss.sss` (Q1/Q2/Q3) | Convertidos para milissegundos (`BIGINT`) |
# MAGIC | 6 | Consistência | País do circuito com 2 grafias (`USA` e `United States`) | Padronizado para `United States`; `UK` → `United Kingdom`; `UAE` → `United Arab Emirates` |
# MAGIC | 7 | Consistência | Nacionalidade com espaço sobrando e sinônimos (`Argentinian ` × `Argentine`) | `TRIM` + padronização |
# MAGIC | 8 | Consistência | 130+ status distintos | Nova coluna `categoria_status` (FINALIZOU, ACIDENTE, FALHA_MECANICA, …) |
# MAGIC | 9 | Unicidade | Piloto com 2 resultados na mesma corrida (carro dividido, anos 1950) | Mantidos (fato histórico), documentados |
# MAGIC | 10 | Acurácia | `grid = 0` significa largada dos boxes / não largou, não "pole" | `grid` 0 → `NULL` + `flag_largou_boxes` |
# MAGIC | 11 | Outliers | Paradas de minutos (bandeira vermelha/reparo) | `flag_parada_atipica`; excluídas das médias de pit stop |
