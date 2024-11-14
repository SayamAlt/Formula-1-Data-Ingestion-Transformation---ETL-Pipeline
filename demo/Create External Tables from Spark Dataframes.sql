-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_external").saveAsTable("race_results_external")

-- COMMAND ----------

DESC EXTENDED race_results_external;

-- COMMAND ----------

CREATE TABLE demo.race_results_external_sql (
    race_year INT,
    race_name STRING,
    race_date TIMESTAMP,
    circuit_location STRING,
    driver_name STRING,
    driver_number INT,
    driver_nationality STRING,
    team STRING,
    grid INT,
    fastest_lap INT,
    race_time STRING,
    points FLOAT,
    position INT,
    created_date TIMESTAMP
)
USING PARQUET
LOCATION "abfss://presentation@formula1datalake11.dfs.core.windows.net/race_results_external_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_external_sql
SELECT * FROM demo.race_results WHERE race_year = 2016;

-- COMMAND ----------

SELECT COUNT(*) FROM demo.race_results_external_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_external_sql;

-- COMMAND ----------

SHOW TABLES IN demo;