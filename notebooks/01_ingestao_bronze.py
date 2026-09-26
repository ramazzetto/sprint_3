# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingestão (Kaggle -> Bronze)
# MAGIC
# MAGIC Fonte: dataset [Formula 1 World Championship (1950 - 2024)](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) do Kaggle, licença CC0.
# MAGIC Ele é uma cópia do Ergast (banco relacional com histórico da F1) e vem em 14 CSVs.
# MAGIC
# MAGIC O que o notebook faz:
# MAGIC 1. autentica na API do Kaggle com o token guardado no volume de credenciais
# MAGIC 2. baixa e descompacta os CSVs no volume `landing.arquivos`
# MAGIC 3. grava cada CSV como tabela Delta na Bronze, sem mexer em nada (tudo como texto, inclusive o `\N`)
# MAGIC 4. registra a carga numa tabela de controle

# COMMAND ----------

# MAGIC %pip install "kaggle>=1.8.0" --quiet

# COMMAND ----------

CATALOGO = "mvp_f1"
DATASET_KAGGLE = "rohanrao/formula-1-world-championship-1950-2020"
PASTA_LANDING = f"/Volumes/{CATALOGO}/landing/arquivos"
PASTA_CREDENCIAIS = f"/Volumes/{CATALOGO}/config/credenciais"
ARQUIVO_TOKEN = f"{PASTA_CREDENCIAIS}/kaggle_token.txt"
ARQUIVO_LEGADO = f"{PASTA_CREDENCIAIS}/kaggle.json"
FORCAR_DOWNLOAD = False

ARQUIVOS_ESPERADOS = [
    "circuits", "constructor_results", "constructor_standings", "constructors",
    "driver_standings", "drivers", "lap_times", "pit_stops", "qualifying",
    "races", "results", "seasons", "sprint_results", "status",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autenticação e download
# MAGIC Aqui tive um problema: a primeira versão usava o `kaggle.json` (usuário + chave), mas o Kaggle mudou e hoje gera um API Token, que é um texto só.
# MAGIC Ajustei para ler o token de um arquivo `kaggle_token.txt` no volume `config.credenciais`. Deixei o `kaggle.json` como alternativa e também a opção de secret scope.
# MAGIC
# MAGIC Detalhe que me custou um tempo: a biblioteca se autentica sozinha no `import kaggle`, então a variável de ambiente tem que ser definida antes do import.
# MAGIC
# MAGIC Se os arquivos já estiverem no volume, o download é pulado (dá pra forçar com `FORCAR_DOWNLOAD = True`).

# COMMAND ----------

import os, json

def carregar_credenciais_kaggle():
    # tenta secret scope, depois o txt, depois o kaggle.json antigo
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
    import kaggle            # autentica aqui, no import
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
# MAGIC Dando uma olhada no começo de um arquivo antes de ler com Spark (separador, aspas e o tal do `\N`):

# COMMAND ----------

with open(f"{PASTA_LANDING}/results.csv", "rb") as f:
    print(f.read(400).decode("utf-8", errors="replace"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravando na Bronze
# MAGIC Uma tabela por arquivo, mesmo nome do CSV. Mantive os nomes de coluna originais (camelCase) e tudo como string, a tipagem fica pra Silver.

# COMMAND ----------

from pyspark.sql import functions as F

controle = []
for nome in ARQUIVOS_ESPERADOS:
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .option("quote", '"')
          .option("escape", '"')
          .option("encoding", "UTF-8")
          .csv(f"{PASTA_LANDING}/{nome}.csv"))
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])   # tira o BOM se tiver
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
# MAGIC ### Descrições das tabelas e controle de carga

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
    "status": "Situação final do piloto na corrida (terminou, acidente, motor etc.)",
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
