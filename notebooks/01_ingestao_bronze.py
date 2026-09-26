# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta via API do Kaggle → Bronze
# MAGIC
# MAGIC **Fonte:** Kaggle, dataset [`rohanrao/formula-1-world-championship-1950-2020`](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)
# MAGIC ("Formula 1 World Championship (1950 - 2024)", licença **CC0: Public Domain**). É uma cópia do banco relacional
# MAGIC **Ergast Motor Racing Database**, com 14 arquivos CSV.
# MAGIC
# MAGIC **Fluxo deste notebook**
# MAGIC 1. Autentica na API do Kaggle. A chave vem de um *secret scope* ou do arquivo `kaggle.json` guardado no volume
# MAGIC    `config.credenciais`, **nunca escrita no código**.
# MAGIC 2. Baixa e descompacta o dataset no volume `landing.arquivos`.
# MAGIC 3. Grava cada CSV como uma tabela Delta na Bronze, **sem alterar valores**. Todas as colunas ficam como texto e o
# MAGIC    marcador de nulo do Ergast (`\N`) é mantido como veio.
# MAGIC 4. Registra a carga na tabela de controle `bronze.controle_ingestao`.

# COMMAND ----------

# MAGIC %pip install kaggle==1.6.17 --quiet

# COMMAND ----------

CATALOGO = "mvp_f1"
DATASET_KAGGLE = "rohanrao/formula-1-world-championship-1950-2020"
PASTA_LANDING = f"/Volumes/{CATALOGO}/landing/arquivos"
ARQUIVO_CREDENCIAL = f"/Volumes/{CATALOGO}/config/credenciais/kaggle.json"
FORCAR_DOWNLOAD = False   # True = baixa de novo mesmo que os arquivos já estejam no volume

ARQUIVOS_ESPERADOS = [
    "circuits", "constructor_results", "constructor_standings", "constructors",
    "driver_standings", "drivers", "lap_times", "pit_stops", "qualifying",
    "races", "results", "seasons", "sprint_results", "status",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1–2. Autenticação e download
# MAGIC **Como configurar a credencial (uma vez):** no Kaggle, *Settings → API → Create New Token* baixa o `kaggle.json`.
# MAGIC Depois, escolha uma opção:
# MAGIC - **Opção A (recomendada):** secret scope, pela CLI do Databricks:
# MAGIC   `databricks secrets create-scope kaggle`, `databricks secrets put-secret kaggle username` e `... put-secret kaggle key`.
# MAGIC - **Opção B:** enviar o `kaggle.json` pela UI para *Catalog → mvp_f1 → config → credenciais*.

# COMMAND ----------

import os, json

def carregar_credenciais_kaggle():
    """Define KAGGLE_USERNAME / KAGGLE_KEY a partir do secret scope ou do kaggle.json no volume."""
    try:
        os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get("kaggle", "username")
        os.environ["KAGGLE_KEY"] = dbutils.secrets.get("kaggle", "key")
        return "secret scope 'kaggle'"
    except Exception:
        with open(ARQUIVO_CREDENCIAL) as f:
            cred = json.load(f)
        os.environ["KAGGLE_USERNAME"] = cred["username"]
        os.environ["KAGGLE_KEY"] = cred["key"]
        return ARQUIVO_CREDENCIAL

def arquivos_presentes():
    return {f[:-4] for f in os.listdir(PASTA_LANDING) if f.endswith(".csv")}

if FORCAR_DOWNLOAD or not set(ARQUIVOS_ESPERADOS) <= arquivos_presentes():
    origem = carregar_credenciais_kaggle()
    from kaggle.api.kaggle_api_extended import KaggleApi   # importar só depois de definir as variáveis
    api = KaggleApi()
    api.authenticate()
    print(f"Autenticado no Kaggle via {origem}. Baixando {DATASET_KAGGLE} ...")
    api.dataset_download_files(DATASET_KAGGLE, path=PASTA_LANDING, unzip=True, quiet=False)
else:
    print("Arquivos já presentes no volume: download pulado (use FORCAR_DOWNLOAD = True para baixar de novo).")

faltando = set(ARQUIVOS_ESPERADOS) - arquivos_presentes()
assert not faltando, f"Arquivos esperados não encontrados: {faltando}"
display(dbutils.fs.ls(PASTA_LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspeção do arquivo bruto
# MAGIC Antes de ler com Spark, olhamos o início de um arquivo para confirmar separador (`,`), aspas e o marcador de nulo (`\N`).

# COMMAND ----------

with open(f"{PASTA_LANDING}/results.csv", "rb") as f:
    print(f.read(400).decode("utf-8", errors="replace"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Gravação na Bronze
# MAGIC Uma tabela por arquivo, com o mesmo nome (`bronze.results`, `bronze.drivers`, …).
# MAGIC Colunas mantêm o nome original do Ergast (camelCase) e todas são `STRING`.

# COMMAND ----------

from pyspark.sql import functions as F

controle = []
for nome in ARQUIVOS_ESPERADOS:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)     # tudo texto: tipagem é responsabilidade da Silver
          .option("quote", '"')
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .csv(f"{PASTA_LANDING}/{nome}.csv"))
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])   # remove BOM, se houver
    df = (df.withColumn("_arquivo_origem", F.lit(f"{nome}.csv"))
            .withColumn("_fonte", F.lit(f"kaggle:{DATASET_KAGGLE}"))
            .withColumn("_data_ingestao", F.current_timestamp()))
    tabela = f"{CATALOGO}.bronze.{nome}"
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tabela))
    linhas = spark.table(tabela).count()
    controle.append((nome, f"{nome}.csv", len(df.columns) - 3, linhas))
    print(f"{tabela:<40} {linhas:>9,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Tabela de controle e descrições

# COMMAND ----------

descricoes = {
    "circuits": "Circuitos (autódromos) onde houve GP",
    "constructor_results": "Pontos de cada equipe em cada corrida",
    "constructor_standings": "Classificação do campeonato de construtores após cada corrida",
    "constructors": "Equipes (construtores)",
    "driver_standings": "Classificação do campeonato de pilotos após cada corrida",
    "drivers": "Pilotos",
    "lap_times": "Tempo de cada volta de cada piloto em cada corrida",
    "pit_stops": "Cada parada nos boxes",
    "qualifying": "Resultado da classificação (Q1/Q2/Q3)",
    "races": "Corridas (Grandes Prêmios) por temporada",
    "results": "Resultado de cada piloto em cada corrida",
    "seasons": "Temporadas",
    "sprint_results": "Resultado das corridas sprint",
    "status": "Situação final do piloto na corrida (terminou, acidente, motor, …)",
}
for nome, desc in descricoes.items():
    spark.sql(f"COMMENT ON TABLE {CATALOGO}.bronze.{nome} IS 'Bronze (como veio do Kaggle/Ergast, tudo texto, nulo = \\\\N): {desc}.'")

spark.createDataFrame(controle, "tabela string, arquivo string, colunas int, linhas long") \
     .withColumn("fonte", F.lit(f"kaggle:{DATASET_KAGGLE}")) \
     .withColumn("data_ingestao", F.current_timestamp()) \
     .write.mode("append").saveAsTable(f"{CATALOGO}.bronze.controle_ingestao")

display(spark.sql(f"""
  SELECT * FROM {CATALOGO}.bronze.controle_ingestao
  WHERE data_ingestao = (SELECT MAX(data_ingestao) FROM {CATALOGO}.bronze.controle_ingestao)
  ORDER BY tabela
"""))
