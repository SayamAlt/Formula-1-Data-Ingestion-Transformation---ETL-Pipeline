-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_race_results_presentation
LOCATION "abfss://presentation@formula1datalake11.dfs.core.windows.net/"

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SHOW TABLES IN  f1_race_results_presentation;

-- COMMAND ----------

DESC DATABASE EXTENDED f1_race_results_presentation;