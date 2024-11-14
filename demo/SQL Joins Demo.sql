-- Databricks notebook source
USE f1_race_results_presentation;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM driver_standings;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_standings_temp_2016
AS
SELECT * FROM driver_standings
WHERE race_year = 2016;

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_standings_temp_2019
AS
SELECT * FROM driver_standings
WHERE race_year = 2019;

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2019;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inner Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 JOIN driver_standings_temp_2019 as d_2019
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Left Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 LEFT JOIN driver_standings_temp_2019 as d_2019 
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Right Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 RIGHT JOIN driver_standings_temp_2019 as d_2019 
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Full Outer Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 FULL JOIN driver_standings_temp_2019 as d_2019 
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Semi Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 SEMI JOIN driver_standings_temp_2019 as d_2019 
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 ANTI JOIN driver_standings_temp_2019 as d_2019 
ON d_2016.driver_name = d_2019.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cross Join

-- COMMAND ----------

SELECT * FROM driver_standings_temp_2016 as d_2016 CROSS JOIN driver_standings_temp_2019 as d_2019;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 22*26