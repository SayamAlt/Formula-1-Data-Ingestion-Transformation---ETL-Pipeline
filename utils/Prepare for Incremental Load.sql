-- Databricks notebook source
DROP DATABASE IF EXISTS f1_race_results_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formula1datalake11.dfs.core.windows.net/"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_race_results_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@formula1datalake11.dfs.core.windows.net/"