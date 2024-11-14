-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS formula_1_race_results;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.circuits;
CREATE TABLE IF NOT EXISTS formula_1_race_results.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat STRING,
  lng STRING,
  alt STRING,
  url STRING
)
USING csv
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/circuits.csv",header True)

-- COMMAND ----------

SELECT * FROM formula_1_race_results.circuits;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.races;
CREATE TABLE IF NOT EXISTS formula_1_race_results.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/races.csv",header True)

-- COMMAND ----------

SELECT * FROM formula_1_race_results.races;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.drivers;
CREATE TABLE IF NOT EXISTS formula_1_race_results.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

SELECT * FROM formula_1_race_results.drivers;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.constructors;
CREATE TABLE IF NOT EXISTS formula_1_race_results.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/constructors.json")

-- COMMAND ----------

SELECT * FROM formula_1_race_results.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.results;
CREATE TABLE IF NOT EXISTS formula_1_race_results.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING JSON
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/results.json")

-- COMMAND ----------

SELECT * FROM formula_1_race_results.results;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.pit_stops;
CREATE TABLE IF NOT EXISTS formula_1_race_results.pit_stops (
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration DOUBLE,
  milliseconds INT
)
USING JSON
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM formula_1_race_results.pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.lap_times;

CREATE TABLE IF NOT EXISTS formula_1_race_results.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/lap_times")

-- COMMAND ----------

SELECT * FROM formula_1_race_results.lap_times;

-- COMMAND ----------

DROP TABLE IF EXISTS formula_1_race_results.qualifying;

CREATE TABLE IF NOT EXISTS formula_1_race_results.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (path "abfss://raw@formula1datalake11.dfs.core.windows.net/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM formula_1_race_results.qualifying;

-- COMMAND ----------

DESCRIBE EXTENDED formula_1_race_results.qualifying;