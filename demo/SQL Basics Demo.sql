-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE EXTENDED f1_race_results_processed;

-- COMMAND ----------

USE f1_race_results_processed;

-- COMMAND ----------

SHOW CURRENT DATABASE;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM f1_race_results_processed.drivers;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * FROM drivers
WHERE nationality = 'British' AND date_of_birth >= '1980-01-01';

-- COMMAND ----------

SELECT name, date_of_birth FROM drivers
WHERE nationality = 'British' AND date_of_birth >= '1980-01-01'
ORDER BY date_of_birth DESC;

-- COMMAND ----------

SELECT * FROM drivers
ORDER BY nationality, date_of_birth DESC;

-- COMMAND ----------

SELECT name, nationality, date_of_birth FROM drivers
WHERE (nationality = 'French' AND date_of_birth >= '1975-10-01') OR nationality = 'Indian';