-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").saveAsTable("demo.race_results")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW CURRENT DATABASE;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

select * from race_results;

-- COMMAND ----------

DESCRIBE EXTENDED race_results;

-- COMMAND ----------

select * from race_results
where race_year = 2016;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * FROM demo.race_results
WHERE race_year = 2016;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE race_results_sql;