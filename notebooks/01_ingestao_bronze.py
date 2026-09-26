# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta via API do Kaggle → Bronze
# MAGIC
# MAGIC **Fonte:** Kaggle, dataset [`rohanrao/formula-1-world-championship-1950-2020`](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)
# MAGIC ("Formula 1 World Championship (1950 - 2024)", licença **CC0: Public Domain**). É uma cópia do banco relacional
# MAGIC **Ergast Motor Racing Database**, com 14 arquivos CSV.
# MAGIC
# MAGIC **Fluxo deste notebook**
# MAGIC 1. Autentica na API do Kaggle com um **API Token** (padrão atual do Kaggle). O token vem de um *secret scope*
# MAGIC    ou de um arquivo guardado no volume `config.credenciais`, e **nunca é escrito no código**.
# MAGIC 2. Baixa e descompacta o dataset no volume `landing.arquivos`.
# MAGIC 3. Grava cada CSV como uma tabela Delta na Bronze, **sem alterar valores**. Todas as colunas ficam como texto e o
# MAGIC    marcador de nulo do Ergast (`\N`) é mantido como veio.
# MAGIC 4. Registra a carga na tabela de controle `bronze.controle_ingestao`.

# COMMAND ----------

# MAGIC %pip install "kaggle>=1.8.0" --quiet

# COMMAND ----------

CATALOGO = "mvp_f1"
DATASET_KAGGLE = "rohanrao/formula-1-world-championship-1950-2020"
PASTA_LANDING = f"/Volumes/{CATALOGO}/landing/arquivos"
PASTA_CREDENCIAIS = f"/Volumes/{CATALOGO}/config/credenciais"
ARQUIVO_TOKEN = f"{PASTA_CREDENCIAIS}/kaggle_token.txt"   # API Token (recomendado pelo Kaggle)
ARQUIVO_LEGADO = f"{PASTA_CREDENCIAIS}/kaggle.json"       # Legacy API Credentials (alternativa)
FORCAR_DOWNLOAD = False   # True = baixa de novo mesmo que os arquivos já estejam no volume

ARQUIVOS_ESPERADOS = [
    "circuits", "constructor_results", "constructor_standings", "constructors",
    "driver_standings", "drivers", "lap_times", "pit_stops", "qualifying",
    "races", "results", "seasons", "sprint_results", "status",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1–2. Autenticação e download
# MAGIC **Como configurar a credencial (uma vez):** no Kaggle, *Settings → API Tokens → Generate New Token* e copie o token.
# MAGIC Depois, escolha uma opção (a primeira encontrada é usada):
# MAGIC - **Opção A:** secret scope, pela CLI do Databricks: `databricks secrets create-scope kaggle` e
# MAGIC   `databricks secrets put-secret kaggle token`.
# MAGIC - **Opção B:** salvar o token (só o texto do token, uma linha) num arquivo `kaggle_token.txt` e enviar pela UI para
# MAGIC   *Catalog → mvp_f1 → config → credenciais*.
# MAGIC - **Opção C (legado):** enviar o `kaggle.json` gerado em *Legacy API Credentials* para o mesmo volume.
# MAGIC
# MAGIC A biblioteca `kaggle` se autentica sozinha **no momento do import**, lendo as variáveis de ambiente
# MAGIC (`KAGGLE_API_TOKEN` ou `KAGGLE_USERNAME`/`KAGGLE_KEY`). Por isso as variáveis são definidas antes do `import kaggle`,
# MAGIC e usamos o objeto já autenticado `kaggle.api`.

# COMMAND ----------

import os, json

def carregar_credenciais_kaggle():
    """Define as variáveis de ambiente de autenticação do Kaggle. Retorna de onde a credencial veio."""
    try:
        os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get("kaggle", "token")
        return "secret scope 'kaggle' (API Token)"
    except Exception:
        pass
    if os.path.exists(ARQUIVO_TOKEN):
        with open(ARQUIVO_TOKEN) as f:
            os.environ["KAGGLE_API_TOKEN"] = f.read().strip()
        return f"{ARQUIVO_TOKEN} (API Token)"
    if os.path.exists(ARQUIVO_LEGADO):
        with open(ARQUIVO_LEGADO) as f:
            cred = json.load(f)
        os.environ["KAGGLE_USERNAME"], os.environ["KAGGLE_KEY"] = cred["username"], cred["key"]
        return f"{ARQUIVO_LEGADO} (credencial legada)"
    raise FileNotFoundError(
        f"Nenhuma credencial do Kaggle encontrada. Envie kaggle_token.txt (ou kaggle.json) para {PASTA_CREDENCIAIS}.")

def arquivos_presentes():
    return {f[:-4] for f in os.listdir(PASTA_LANDING) if f.endswith(".csv")}

if FORCAR_DOWNLOAD or not set(ARQUIVOS_ESPERADOS) <= arquivos_presentes():
    origem = carregar_credenciais_kaggle()
    import kaggle            # autentica no import, usando as variáveis definidas acima
    api = kaggle.api
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
