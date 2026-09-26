-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Setup
-- MAGIC
-- MAGIC Cria o catálogo, os schemas e os volumes do projeto. Rodar uma vez só (se rodar de novo não dá problema, está tudo com IF NOT EXISTS).
-- MAGIC
-- MAGIC Organização que escolhi:
-- MAGIC - `landing.arquivos`: volume onde ficam os CSVs do jeito que vêm do Kaggle
-- MAGIC - `config.credenciais`: volume para o token do Kaggle (não vai pro GitHub de jeito nenhum)
-- MAGIC - `bronze`, `silver` e `gold`: as três camadas da arquitetura medalhão
-- MAGIC
-- MAGIC Obs: no Free Edition consegui criar o catálogo `mvp_f1` normalmente. Se der erro de permissão, dá pra usar o catálogo `workspace` e trocar o nome nos notebooks.

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
COMMENT 'Token da API do Kaggle usado na ingestão. Não versionar.';

-- COMMAND ----------

-- conferindo se criou tudo
SHOW SCHEMAS IN mvp_f1;
