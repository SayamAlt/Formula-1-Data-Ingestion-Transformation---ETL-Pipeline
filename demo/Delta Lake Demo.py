# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to Delta Lake (Managed table).
# MAGIC 2. Write data to Delta Lake (External table).
# MAGIC 3. Read data from Delta Lake (Table).
# MAGIC 4. Read data from Delta Lake (File).

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo 
# MAGIC LOCATION "abfss://demo@formula1datalake11.dfs.core.windows.net/"

# COMMAND ----------

results = spark.read.option("inferSchema",True).json(f"{raw_folder_path}/2021-03-28/results.json")
display(results)

# COMMAND ----------

results.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

results.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_demo;

# COMMAND ----------

# MAGIC %sql  -- Create an external table
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@formula1datalake11.dfs.core.windows.net/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

results_external = spark.read.format("delta").load(f"{demo_folder_path}/results_external")
display(results_external)

# COMMAND ----------

results.write.format("delta").mode("overwrite").partitionBy("constructorId").save(f"{demo_folder_path}/results_partitioned")

# COMMAND ----------

spark.read.format("delta").load(f"{demo_folder_path}/results_partitioned").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_partitioned
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://demo@formula1datalake11.dfs.core.windows.net/results_partitioned"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

deltatable = DeltaTable.forPath(spark,f"{demo_folder_path}/results_managed")
deltatable.update("position<=10",{"points": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

deltatable = DeltaTable.forPath(spark,f"{demo_folder_path}/results_managed")
deltatable.delete("points=0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using Merge

# COMMAND ----------

drivers_day_1_df = spark.read.format("delta").json(f"{raw_folder_path}/2021-03-28/drivers.json").filter("driverId <= 10") \
  .select("driverId","dob","name.forename","name.surname")
display(drivers_day_1_df)

# COMMAND ----------

drivers_day_1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day_2_df = spark.read.format("delta").json(f"{raw_folder_path}/2021-03-28/drivers.json") \
  .filter("driverId BETWEEN 6 AND 15") \
  .select("driverId","dob","name.forename","name.surname")
display(drivers_day_2_df)

# COMMAND ----------

drivers_day_2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day_3_df = spark.read.format("delta").json(f"{raw_folder_path}/2021-03-28/drivers.json") \
  .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
  .select("driverId","dob","name.forename","name.surname")
display(drivers_day_3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merged (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING, 
# MAGIC   surname STRING,
# MAGIC   createdDate DATE, -- Create a new record
# MAGIC   updatedDate DATE -- Update a column with new values
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merged dm
# MAGIC USING drivers_day1 dd
# MAGIC ON dm.driverId = dd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET dm.dob = dd.dob,
# MAGIC            dm.forename = dd.forename,
# MAGIC            dm.surname = dd.surname,
# MAGIC            dm.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (driverId,dob,forename,surname,createdDate) VALUES (dd.driverId,dd.dob,dd.forename,dd.surname,current_timestamp) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merged dm
# MAGIC USING drivers_day2 dd
# MAGIC ON dm.driverId = dd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET dm.dob = dd.dob,
# MAGIC            dm.forename = dd.forename,
# MAGIC            dm.surname = dd.surname,
# MAGIC            dm.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (driverId,dob,forename,surname,createdDate) VALUES (dd.driverId,dd.dob,dd.forename,dd.surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

deltatable = DeltaTable.forPath(spark,f"{demo_folder_path}/drivers_merged")

deltatable.alias("dlt").merge(
  drivers_day_3_df.alias("src"),
  "dlt.driverId = src.driverId") \
  .whenMatchedUpdate(set={"dob": "src.dob", "forename": "src.forename", "surname": "src.surname", "updatedDate": "current_timestamp()"}) \
  .whenNotMatchedInsert(values={
    "driverId": "src.driverId",
    "dob": "src.dob",
    "forename": "src.forename",
    "surname": "src.surname",
    "createdDate": "current_timestamp()"
  }) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History
# MAGIC 2. Time Travel
# MAGIC 3. Vacuum

# COMMAND ----------

spark.sql("DESC HISTORY f1_demo.drivers_merged").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC TIMESTAMP AS OF '2024-11-10T02:50:19.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf","2024-11-10T02:50:19.000+00:00").load(f"{demo_folder_path}/drivers_merged")
display(df)

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf",4).load(f"{demo_folder_path}/drivers_merged")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merged RETAIN 1 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merged WHERE driverId = 11;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merged dm
# MAGIC USING f1_demo.drivers_merged VERSION AS OF 4 dd
# MAGIC ON dm.driverId = dd.driverId
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC INSERT * -- Rolling back the changes

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC WHERE driverId IN (1,5);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merged
# MAGIC WHERE driverId IN (2,3,4);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;

# COMMAND ----------

for driver_id in range(3,20):
  spark.sql(f"""
            INSERT INTO f1_demo.drivers_txn
            SELECT * FROM f1_demo.drivers_merged
            WHERE driverId = {driver_id}
            """)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merged;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn;