-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/Files/formula1/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/Files/formula1/presentation"


-- COMMAND ----------

DESCRIBE DATABASE f1_presentation

-- COMMAND ----------


