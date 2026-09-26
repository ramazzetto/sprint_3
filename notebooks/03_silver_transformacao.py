# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver
# MAGIC
# MAGIC Aqui aplico os tratamentos que levantei no notebook 02. Cada entidade vira uma tabela limpa, tipada e com nome em português.
# MAGIC
# MAGIC Tabelas que gerei: circuitos, equipes, pilotos, corridas, status, resultados, classificacao_grid, pit_stops, campeonato_pilotos e campeonato_equipes.
# MAGIC
# MAGIC Deixei de fora da Silver (continuam na Bronze):
# MAGIC - lap_times: quase 600 mil linhas e nenhuma das minhas perguntas usa tempo volta a volta
# MAGIC - sprint_results: sprint só existe desde 2021, são poucas corridas
# MAGIC - constructor_results: a informação já está em constructor_standings
# MAGIC - seasons: só tem o ano e um link da Wikipedia
# MAGIC
# MAGIC Funções auxiliares da célula abaixo: `nul` troca `\N` e vazio por NULL, `tipo` faz o cast com try_cast (se não converter vira NULL em vez de quebrar) e `tempo_ms` converte "1:23.456" em milissegundos.

# COMMAND ----------

CATALOGO = "mvp_f1"
B, S = f"{CATALOGO}.bronze", f"{CATALOGO}.silver"

from pyspark.sql import functions as F, Window

def nul(c):
    v = F.trim(F.col(c))
    return F.when(v.isNull() | v.isin("\\N", ""), None).otherwise(v)

def nul_sql(c):
    return f"nullif(nullif(trim(`{c}`), '\\\\N'), '')"

def tipo(c, t):
    return F.expr(f"try_cast({nul_sql(c)} AS {t})")

def tempo_ms(c):
    # aceita "1:23.456" e "23.456"
    v = nul_sql(c)
    return F.expr(f"""
      CAST(ROUND(CASE
        WHEN {v} LIKE '%:%' THEN try_cast(split({v}, ':')[0] AS INT) * 60000
                               + try_cast(split({v}, ':')[1] AS DOUBLE) * 1000
        ELSE try_cast({v} AS DOUBLE) * 1000 END) AS BIGINT)""")

def gravar(df, nome, descricao, comentarios):
    # grava e já coloca as descrições no catálogo
    tabela = f"{S}.{nome}"
    df = df.withColumn("_data_processamento", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tabela)
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{descricao}'")
    comentarios = {**comentarios, "_data_processamento": "Momento do processamento na Silver."}
    for col, txt in comentarios.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {col} COMMENT '{txt}'")
    print(f"{tabela:<40} {spark.table(tabela).count():>9,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Circuitos
# MAGIC Padronizando o país (USA, UK e UAE para o nome completo). Korea também, porque o circuito é na Coreia do Sul.

# COMMAND ----------

PAIS_PADRAO = {"USA": "United States", "UK": "United Kingdom", "UAE": "United Arab Emirates", "Korea": "South Korea"}
mapa_pais = F.create_map([F.lit(x) for kv in PAIS_PADRAO.items() for x in kv])

circuitos = spark.table(f"{B}.circuits").select(
    tipo("circuitId", "INT").alias("circuit_id"),
    nul("circuitRef").alias("circuit_ref"),
    nul("name").alias("nome"),
    nul("location").alias("cidade"),
    F.coalesce(mapa_pais[nul("country")], nul("country")).alias("pais"),
    tipo("lat", "DOUBLE").alias("latitude"),
    tipo("lng", "DOUBLE").alias("longitude"),
    tipo("alt", "INT").alias("altitude_m"),
)
gravar(circuitos, "circuitos", "Silver: circuitos onde houve GP. Linhagem: bronze.circuits.", {
    "circuit_id": "Id do circuito no Ergast. Chave primária. Origem: circuits.circuitId.",
    "circuit_ref": "Apelido técnico do circuito (ex.: monza). Origem: circuits.circuitRef.",
    "nome": "Nome do circuito. Origem: circuits.name.",
    "cidade": "Cidade/localidade. Origem: circuits.location.",
    "pais": "País padronizado (USA/United States -> United States; UK -> United Kingdom; UAE -> United Arab Emirates). Origem: circuits.country.",
    "latitude": "Latitude (-90 a 90). Origem: circuits.lat.",
    "longitude": "Longitude (-180 a 180). Origem: circuits.lng.",
    "altitude_m": "Altitude em metros; pode ser NULL. Origem: circuits.alt.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Equipes

# COMMAND ----------

equipes = spark.table(f"{B}.constructors").select(
    tipo("constructorId", "INT").alias("constructor_id"),
    nul("constructorRef").alias("constructor_ref"),
    nul("name").alias("nome"),
    nul("nationality").alias("nacionalidade"),
)
gravar(equipes, "equipes", "Silver: equipes (construtores). Linhagem: bronze.constructors.", {
    "constructor_id": "Id da equipe no Ergast. Chave primária. Origem: constructors.constructorId.",
    "constructor_ref": "Apelido técnico da equipe. Origem: constructors.constructorRef.",
    "nome": "Nome da equipe (ex.: Ferrari, McLaren). Origem: constructors.name.",
    "nacionalidade": "Nacionalidade da equipe (ex.: Italian, British). Origem: constructors.nationality.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pilotos
# MAGIC Para a pergunta 5 (piloto corre melhor em casa?) precisei ligar o piloto ao país do circuito. Só que o piloto tem nacionalidade ("Brazilian") e o circuito tem país ("Brazil").
# MAGIC Montei um de-para na mão. Coloquei um assert logo depois para avisar se aparecer alguma nacionalidade nova que eu não mapeei.

# COMMAND ----------

NACIONALIDADE_PADRAO = {"Argentinian": "Argentine"}
NACIONALIDADE_PAIS = {
    "American": "United States", "American-Italian": "United States", "Argentine": "Argentina",
    "Argentine-Italian": "Argentina", "Australian": "Australia", "Austrian": "Austria", "Belgian": "Belgium",
    "Brazilian": "Brazil", "British": "United Kingdom", "Canadian": "Canada", "Chilean": "Chile",
    "Chinese": "China", "Colombian": "Colombia", "Czech": "Czech Republic", "Danish": "Denmark",
    "Dutch": "Netherlands", "East German": "Germany", "Finnish": "Finland", "French": "France",
    "German": "Germany", "Hungarian": "Hungary", "Indian": "India", "Indonesian": "Indonesia",
    "Irish": "Ireland", "Italian": "Italy", "Japanese": "Japan", "Liechtensteiner": "Liechtenstein",
    "Malaysian": "Malaysia", "Mexican": "Mexico", "Monegasque": "Monaco", "New Zealander": "New Zealand",
    "Polish": "Poland", "Portuguese": "Portugal", "Rhodesian": "Zimbabwe", "Russian": "Russia",
    "South African": "South Africa", "Spanish": "Spain", "Swedish": "Sweden", "Swiss": "Switzerland",
    "Thai": "Thailand", "Uruguayan": "Uruguay", "Venezuelan": "Venezuela",
}
mapa_nac = F.create_map([F.lit(x) for kv in NACIONALIDADE_PADRAO.items() for x in kv])
mapa_nac_pais = F.create_map([F.lit(x) for kv in NACIONALIDADE_PAIS.items() for x in kv])

nac = F.coalesce(mapa_nac[nul("nationality")], nul("nationality"))
pilotos = spark.table(f"{B}.drivers").select(
    tipo("driverId", "INT").alias("driver_id"),
    nul("driverRef").alias("driver_ref"),
    tipo("number", "INT").alias("numero_permanente"),
    nul("code").alias("codigo"),
    nul("forename").alias("nome"),
    nul("surname").alias("sobrenome"),
    F.concat_ws(" ", nul("forename"), nul("surname")).alias("nome_completo"),
    tipo("dob", "DATE").alias("data_nascimento"),
    nac.alias("nacionalidade"),
    mapa_nac_pais[nac].alias("pais"),
)
sem_pais = pilotos.filter("pais IS NULL AND nacionalidade IS NOT NULL").select("nacionalidade").distinct().collect()
assert not sem_pais, f"Nacionalidades sem de-para para país: {sem_pais}"

gravar(pilotos, "pilotos", "Silver: pilotos. Linhagem: bronze.drivers + de-para nacionalidade->país (enriquecimento manual).", {
    "driver_id": "Id do piloto no Ergast. Chave primária. Origem: drivers.driverId.",
    "driver_ref": "Apelido técnico (ex.: hamilton). Origem: drivers.driverRef.",
    "numero_permanente": "Número permanente do carro (existe desde 2014; NULL antes). Origem: drivers.number.",
    "codigo": "Código de 3 letras (ex.: HAM); NULL para pilotos antigos. Origem: drivers.code.",
    "nome": "Primeiro nome. Origem: drivers.forename.",
    "sobrenome": "Sobrenome. Origem: drivers.surname.",
    "nome_completo": "Nome + sobrenome. Derivado.",
    "data_nascimento": "Data de nascimento. Origem: drivers.dob (yyyy-MM-dd).",
    "nacionalidade": "Nacionalidade padronizada (TRIM; Argentinian -> Argentine). Origem: drivers.nationality.",
    "pais": "País correspondente à nacionalidade, no mesmo padrão de silver.circuitos.pais. Derivado via de-para.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Corridas

# COMMAND ----------

corridas = spark.table(f"{B}.races").select(
    tipo("raceId", "INT").alias("race_id"),
    tipo("year", "INT").alias("ano"),
    tipo("round", "INT").alias("rodada"),
    tipo("circuitId", "INT").alias("circuit_id"),
    nul("name").alias("nome_gp"),
    tipo("date", "DATE").alias("data"),
    (nul("sprint_date").isNotNull()).alias("fim_de_semana_sprint"),
)
gravar(corridas, "corridas", "Silver: corridas (Grandes Prêmios). Linhagem: bronze.races.", {
    "race_id": "Id da corrida no Ergast. Chave primária. Origem: races.raceId.",
    "ano": "Temporada (1950-2024). Origem: races.year.",
    "rodada": "Número da etapa na temporada (>= 1). Origem: races.round.",
    "circuit_id": "FK para silver.circuitos. Origem: races.circuitId.",
    "nome_gp": "Nome do GP (ex.: Brazilian Grand Prix). Origem: races.name.",
    "data": "Data da corrida. Origem: races.date.",
    "fim_de_semana_sprint": "TRUE se o fim de semana teve corrida sprint. Derivado de races.sprint_date não nulo.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Status
# MAGIC Agrupei os mais de 130 status em poucas categorias. O que não é acidente, problema do piloto ou desclassificação acabou caindo em falha mecânica (Engine, Gearbox, Hydraulics etc.).

# COMMAND ----------

ACIDENTE = ["Accident", "Collision", "Collision damage", "Spun off", "Fatal accident", "Damage", "Debris"]
NAO_LARGOU_DESCLASSIFICADO = ["Did not qualify", "Did not prequalify", "107% Rule", "Withdrew", "Disqualified",
                              "Excluded", "Underweight", "Not restarted", "Not classified", "Safety concerns"]
PILOTO = ["Injured", "Injury", "Physical", "Driver unwell", "Illness", "Eye injury"]
OUTROS = ["Retired", "Safety", "Stalled"]

s = nul("status")
categoria = (F.when((s == "Finished") | s.rlike(r"^\+[0-9]+ Laps?$"), "FINALIZOU")
              .when(s.isin(ACIDENTE), "ACIDENTE")
              .when(s.isin(NAO_LARGOU_DESCLASSIFICADO), "NAO_LARGOU_OU_DESCLASSIFICADO")
              .when(s.isin(PILOTO), "PILOTO")
              .when(s.isin(OUTROS), "OUTROS")
              .otherwise("FALHA_MECANICA"))

status = spark.table(f"{B}.status").select(
    tipo("statusId", "INT").alias("status_id"),
    s.alias("status"),
    categoria.alias("categoria_status"),
)
gravar(status, "status", "Silver: situação final do piloto na corrida, com categoria. Linhagem: bronze.status + regra de categorização.", {
    "status_id": "Id do status. Chave primária. Origem: status.statusId.",
    "status": "Status original do Ergast (ex.: Finished, Engine, +1 Lap). Origem: status.status.",
    "categoria_status": "FINALIZOU (Finished ou +n voltas), ACIDENTE, FALHA_MECANICA (demais falhas técnicas), PILOTO (saúde), NAO_LARGOU_OU_DESCLASSIFICADO, OUTROS. Derivado.",
})
display(spark.table(f"{S}.status").groupBy("categoria_status").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultados
# MAGIC grid = 0 vira NULL (não é posição de largada) e ganha uma flag.

# COMMAND ----------

grid = tipo("grid", "INT")
resultados = spark.table(f"{B}.results").select(
    tipo("resultId", "INT").alias("result_id"),
    tipo("raceId", "INT").alias("race_id"),
    tipo("driverId", "INT").alias("driver_id"),
    tipo("constructorId", "INT").alias("constructor_id"),
    tipo("number", "INT").alias("numero_carro"),
    F.when(grid > 0, grid).alias("grid"),
    (grid == 0).alias("flag_largou_boxes_ou_sem_grid"),
    tipo("position", "INT").alias("posicao_oficial"),
    nul("positionText").alias("posicao_texto"),
    tipo("positionOrder", "INT").alias("posicao_final"),
    tipo("points", "DOUBLE").alias("pontos"),
    tipo("laps", "INT").alias("voltas"),
    tipo("milliseconds", "BIGINT").alias("tempo_total_ms"),
    tipo("fastestLap", "INT").alias("volta_mais_rapida"),
    tipo("rank", "INT").alias("ranking_volta_rapida"),
    tempo_ms("fastestLapTime").alias("tempo_volta_rapida_ms"),
    tipo("fastestLapSpeed", "DOUBLE").alias("velocidade_volta_rapida_kmh"),
    tipo("statusId", "INT").alias("status_id"),
)
gravar(resultados, "resultados", "Silver: resultado de cada piloto em cada corrida. Linhagem: bronze.results.", {
    "result_id": "Id do resultado. Chave primária. Origem: results.resultId.",
    "race_id": "FK para silver.corridas. Origem: results.raceId.",
    "driver_id": "FK para silver.pilotos. Origem: results.driverId.",
    "constructor_id": "FK para silver.equipes. Origem: results.constructorId.",
    "numero_carro": "Número do carro na corrida. Origem: results.number.",
    "grid": "Posição de largada (>= 1). NULL quando o original era 0 (largou dos boxes/sem posição). Origem: results.grid.",
    "flag_largou_boxes_ou_sem_grid": "TRUE se results.grid = 0.",
    "posicao_oficial": "Posição oficial; NULL se não classificado. Origem: results.position.",
    "posicao_texto": "Posição como texto (R = abandonou, D = desclassificado, W = não largou, N = não classificado...). Origem: results.positionText.",
    "posicao_final": "Ordem final de chegada, sempre preenchida (>= 1). Usada nas análises. Origem: results.positionOrder.",
    "pontos": "Pontos no campeonato (>= 0; o sistema de pontos mudou ao longo das eras). Origem: results.points.",
    "voltas": "Voltas completadas (>= 0). Origem: results.laps.",
    "tempo_total_ms": "Tempo total de prova em ms; NULL para quem não terminou na mesma volta. Origem: results.milliseconds.",
    "volta_mais_rapida": "Número da volta mais rápida do piloto. Origem: results.fastestLap (disponível desde 2004).",
    "ranking_volta_rapida": "Posição do piloto no ranking de voltas mais rápidas. Origem: results.rank.",
    "tempo_volta_rapida_ms": "Tempo da volta mais rápida em ms. Origem: results.fastestLapTime (m:ss.sss -> ms).",
    "velocidade_volta_rapida_kmh": "Velocidade média da volta mais rápida (km/h). Origem: results.fastestLapSpeed.",
    "status_id": "FK para silver.status. Origem: results.statusId.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classificação (qualifying)

# COMMAND ----------

q = spark.table(f"{B}.qualifying").select(
    tipo("qualifyId", "INT").alias("qualify_id"),
    tipo("raceId", "INT").alias("race_id"),
    tipo("driverId", "INT").alias("driver_id"),
    tipo("constructorId", "INT").alias("constructor_id"),
    tipo("position", "INT").alias("posicao_classificacao"),
    tempo_ms("q1").alias("q1_ms"),
    tempo_ms("q2").alias("q2_ms"),
    tempo_ms("q3").alias("q3_ms"),
)
q = q.withColumn("melhor_tempo_ms", F.least("q1_ms", "q2_ms", "q3_ms"))
gravar(q, "classificacao_grid", "Silver: resultado da classificação (qualifying). Linhagem: bronze.qualifying.", {
    "qualify_id": "Id do registro. Chave primária. Origem: qualifying.qualifyId.",
    "race_id": "FK para silver.corridas. Origem: qualifying.raceId.",
    "driver_id": "FK para silver.pilotos. Origem: qualifying.driverId.",
    "constructor_id": "FK para silver.equipes. Origem: qualifying.constructorId.",
    "posicao_classificacao": "Posição obtida na classificação (>= 1). Origem: qualifying.position.",
    "q1_ms": "Tempo no Q1 em ms. Origem: qualifying.q1 (m:ss.sss -> ms).",
    "q2_ms": "Tempo no Q2 em ms; NULL se eliminado no Q1. Origem: qualifying.q2.",
    "q3_ms": "Tempo no Q3 em ms; NULL se eliminado antes. Origem: qualifying.q3.",
    "melhor_tempo_ms": "Menor entre q1_ms, q2_ms e q3_ms. Derivado.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pit stops
# MAGIC Usei a coluna `milliseconds` em vez de `duration`, porque a `duration` muda de formato quando passa de 60s (vira "16:44.718").
# MAGIC Parada atípica = acima de Q3 + 3*IQR da mesma temporada.

# COMMAND ----------

ps = spark.table(f"{B}.pit_stops").select(
    tipo("raceId", "INT").alias("race_id"),
    tipo("driverId", "INT").alias("driver_id"),
    tipo("stop", "INT").alias("numero_parada"),
    tipo("lap", "INT").alias("volta"),
    tipo("milliseconds", "BIGINT").alias("duracao_ms"),
)
ps = ps.join(spark.table(f"{S}.corridas").select("race_id", "ano"), "race_id", "left")
lim = ps.groupBy("ano").agg(F.percentile_approx("duracao_ms", 0.25).alias("q1"),
                            F.percentile_approx("duracao_ms", 0.75).alias("q3"))
ps = (ps.join(lim, "ano", "left")
        .withColumn("flag_parada_atipica", (F.col("duracao_ms") > F.col("q3") + 3 * (F.col("q3") - F.col("q1"))) | (F.col("duracao_ms") <= 0))
        .select("race_id", "driver_id", "numero_parada", "volta", "duracao_ms", "flag_parada_atipica"))
gravar(ps, "pit_stops", "Silver: paradas nos boxes (cobertura só nas temporadas recentes). Linhagem: bronze.pit_stops.", {
    "race_id": "FK para silver.corridas. Origem: pit_stops.raceId.",
    "driver_id": "FK para silver.pilotos. Origem: pit_stops.driverId.",
    "numero_parada": "Ordem da parada do piloto na corrida (1, 2, 3...). Origem: pit_stops.stop.",
    "volta": "Volta em que parou. Origem: pit_stops.lap.",
    "duracao_ms": "Tempo total no pit lane em ms (> 0). Origem: pit_stops.milliseconds.",
    "flag_parada_atipica": "TRUE se duração > Q3 + 3*IQR da temporada (bandeira vermelha/reparo). Excluída das médias.",
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Campeonatos (standings)

# COMMAND ----------

for bronze, silver, chave, fk in [("driver_standings", "campeonato_pilotos", "driverStandingsId", ("driverId", "driver_id")),
                                  ("constructor_standings", "campeonato_equipes", "constructorStandingsId", ("constructorId", "constructor_id"))]:
    df = spark.table(f"{B}.{bronze}").select(
        tipo(chave, "INT").alias("standing_id"),
        tipo("raceId", "INT").alias("race_id"),
        tipo(fk[0], "INT").alias(fk[1]),
        tipo("points", "DOUBLE").alias("pontos_acumulados"),
        tipo("position", "INT").alias("posicao_campeonato"),
        tipo("wins", "INT").alias("vitorias_acumuladas"),
    )
    alvo = "piloto" if fk[1] == "driver_id" else "equipe"
    gravar(df, silver, f"Silver: classificação do campeonato por {alvo} após cada corrida. Linhagem: bronze.{bronze}.", {
        "standing_id": f"Id do registro. Chave primária. Origem: {bronze}.{chave}.",
        "race_id": "Corrida após a qual a classificação foi calculada. FK para silver.corridas.",
        fk[1]: f"FK para silver.{'pilotos' if alvo == 'piloto' else 'equipes'}. Origem: {bronze}.{fk[0]}.",
        "pontos_acumulados": "Pontos acumulados na temporada até a corrida (>= 0).",
        "posicao_campeonato": "Posição no campeonato após a corrida (>= 1).",
        "vitorias_acumuladas": "Vitórias acumuladas na temporada até a corrida (>= 0).",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência final
# MAGIC Se alguma dessas contagens não for zero o notebook para com erro.

# COMMAND ----------

chk = spark.sql(f"""
SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT result_id) FROM {S}.resultados)                        AS pk_resultados_dup,
  (SELECT COUNT(*) - COUNT(DISTINCT race_id)   FROM {S}.corridas)                          AS pk_corridas_dup,
  (SELECT COUNT(*) FROM {S}.resultados WHERE posicao_final IS NULL OR posicao_final < 1)   AS posicao_final_invalida,
  (SELECT COUNT(*) FROM {S}.resultados WHERE pontos < 0)                                   AS pontos_negativos,
  (SELECT COUNT(*) FROM {S}.corridas WHERE data IS NULL)                                   AS corridas_sem_data,
  (SELECT COUNT(*) FROM {S}.circuitos WHERE pais IN ('USA', 'UK', 'UAE'))                  AS pais_nao_padronizado
""").collect()[0]
print(chk.asDict())
assert all(v == 0 for v in chk.asDict().values()), chk
print("Silver ok")
