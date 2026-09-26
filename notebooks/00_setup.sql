-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup do ambiente
-- MAGIC
-- MAGIC Cria a estrutura do Lakehouse no Unity Catalog seguindo a **Arquitetura Medalhão**:
-- MAGIC
-- MAGIC | Objeto | Papel |
-- MAGIC |---|---|
-- MAGIC | `mvp_f1` (catálogo) | Agrupa todo o projeto |
-- MAGIC | `mvp_f1.landing.arquivos` (volume) | Arquivos CSV exatamente como baixados do Kaggle |
-- MAGIC | `mvp_f1.config.credenciais` (volume) | `kaggle.json` com a chave da API (**nunca vai para o Git**) |
-- MAGIC | `mvp_f1.bronze` | Dado como veio, em Delta, com metadados de ingestão |
-- MAGIC | `mvp_f1.silver` | Dado limpo, tipado e padronizado (uma tabela por entidade) |
-- MAGIC | `mvp_f1.gold` | Esquema estrela + agregados para responder às perguntas |
-- MAGIC
-- MAGIC > Se o Free Edition não permitir criar um catálogo novo, use o catálogo padrão do workspace
-- MAGIC > (ex.: `workspace`) e troque `mvp_f1` por ele em todos os notebooks.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS mvp_f1
COMMENT 'MVP pós-graduação: pipeline de dados da Fórmula 1 (1950-2024) - Rodrigo';

-- COMMAND ----------

USE CATALOG mvp_f1;

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Arquivos brutos baixados do Kaggle, sem nenhuma alteração';
CREATE SCHEMA IF NOT EXISTS config  COMMENT 'Configurações e credenciais (acesso restrito)';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Camada Bronze: dado como veio da fonte + metadados de ingestão';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Camada Silver: dado limpo, tipado e padronizado';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Camada Gold: esquema estrela e agregados para análise';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS landing.arquivos
COMMENT 'CSVs do dataset Kaggle rohanrao/formula-1-world-championship-1950-2020 (fonte original: Ergast)';

CREATE VOLUME IF NOT EXISTS config.credenciais
COMMENT 'Guarda o kaggle.json usado pela ingestão. Não versionar.';

-- COMMAND ----------

SHOW SCHEMAS IN mvp_f1;
