-- Databricks notebook source
USE f1_race_results_processed;

-- COMMAND ----------

SELECT *, CONCAT(driver_ref,'-',code) AS new_driver_ref FROM drivers;

-- COMMAND ----------

SELECT *, SPLIT(name,' ')[0] AS forename, SPLIT(name,' ')[1] AS surname FROM drivers;

-- COMMAND ----------

SELECT *, CURRENT_TIMESTAMP() AS ts FROM drivers;

-- COMMAND ----------

SELECT *, DATE_FORMAT(date_of_birth,'dd-MM-yyyy') as formatted_date FROM drivers;

-- COMMAND ----------

SELECT *, DATE_ADD(date_of_birth,10) as added_date FROM drivers;

-- COMMAND ----------

SELECT * FROM drivers
WHERE MONTH(date_of_birth) = 1;

-- COMMAND ----------

SELECT COUNT(*) from DRIVERS;

-- COMMAND ----------

SELECT * FROM drivers
WHERE date_of_birth IN (SELECT MAX(date_of_birth) FROM drivers);

-- COMMAND ----------

SELECT COUNT(*) FROM drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality, COUNT(*) as count FROM drivers
GROUP BY nationality
ORDER BY count DESC
LIMIT 20;

-- COMMAND ----------

SELECT nationality, COUNT(*) as count FROM drivers
GROUP BY nationality
HAVING count > 100;

-- COMMAND ----------

select nationality, name, date_of_birth, RANK() OVER(PARTITION BY nationality ORDER BY date_of_birth DESC) as age_rank
from drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------

select nationality, name, date_of_birth, RANK() OVER (PARTITION BY nationality ORDER BY date_of_birth) as age_rank
from drivers
ORDER by nationality, age_rank;