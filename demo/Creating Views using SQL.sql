-- Databricks notebook source
SELECT current_database();

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW CURRENT DATABASE;

-- COMMAND ----------

SELECT * FROM race_results
WHERE race_year = 2008;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW race_results_view AS
SELECT * FROM race_results
WHERE race_year = 2015;

-- COMMAND ----------

SELECT * FROM race_results_view;

-- COMMAND ----------

SHOW VIEWS;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW race_results_global_view AS 
SELECT * FROM race_results
WHERE race_year = 2012;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.race_results_global_view;

-- COMMAND ----------

USE default;

-- COMMAND ----------

CREATE OR REPLACE VIEW race_results_permanent_view AS
SELECT * FROM demo.race_results
WHERE race_year = 2014;

-- COMMAND ----------

SELECT * FROM race_results_permanent_view;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;