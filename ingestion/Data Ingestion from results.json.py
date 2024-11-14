# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

# spark.read.json("abfss://raw@formula1datalake11.dfs.core.windows.net/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# %sql
# SELECT raceId, COUNT(*) as cnt FROM results_cutover
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

# spark.read.json("abfss://raw@formula1datalake11.dfs.core.windows.net/2021-03-28/results.json").createOrReplaceTempView("results_delta_w1")

# COMMAND ----------

# %sql
# SELECT raceId, COUNT(*) as cnt FROM results_delta_w1
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

# spark.read.json("abfss://raw@formula1datalake11.dfs.core.windows.net/2021-04-18/results.json").createOrReplaceTempView("results_delta_w2")

# COMMAND ----------

# %sql
# SELECT raceId, COUNT(*) as cnt FROM results_delta_w2
# GROUP BY raceId
# ORDER BY raceId DESC;

# COMMAND ----------

dbutils.widgets.text("p_data_source","Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/common functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_schema = StructType(fields=[
    StructField('resultId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('grid',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('positionText',StringType(),True),
    StructField('positionOrder',IntegerType(),True),
    StructField('points',FloatType(),True),
    StructField('laps',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True),
    StructField('fastestLap',IntegerType(),True),
    StructField('rank',IntegerType(),True),
    StructField('fastestLapTime',StringType(),True),
    StructField('fastestLapSpeed',StringType(),True),
    StructField('statusId',IntegerType(),True)
])
results_schema

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json",schema=results_schema)
display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

results_df.count()

# COMMAND ----------

results_df.describe().show()

# COMMAND ----------

results_df = results_df.drop('statusId')
results_df.show(5)

# COMMAND ----------

results_df = results_df.withColumnRenamed('resultId', 'result_id') \
                       .withColumnRenamed('raceId', 'race_id') \
                       .withColumnRenamed('driverId', 'driver_id') \
                       .withColumnRenamed('constructorId', 'constructor_id') \
                       .withColumnRenamed('positionText', 'position_text') \
                       .withColumnRenamed('positionOrder', 'position_order') \
                       .withColumnRenamed('fastestLap', 'fastest_lap') \
                       .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                       .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                       .withColumn('data_source', lit(v_data_source)) \
                       .withColumn('file_date', lit(v_file_date))
display(results_df)

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Removing duplicate records from results table

# COMMAND ----------

results_df = results_df.dropDuplicates(subset=['race_id','driver_id'])
results_df.show(5)

# COMMAND ----------

# Method 1 for Incremental Load
# for race_id_list in results_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2 (More Efficient)

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") # Overrides the default behavior of "update" for partitioned tables by changing the default behavior of "update" to "append" i.e. "static" to "dynamic"

# COMMAND ----------

# results_df = results_df.select('result_id',
#  'driver_id',
#  'constructor_id',
#  'number',
#  'grid',
#  'position',
#  'position_text',
#  'position_order',
#  'points',
#  'laps',
#  'time',
#  'milliseconds',
#  'fastest_lap',
#  'rank',
#  'fastest_lap_time',
#  'fastest_lap_speed',
#  'data_source',
#  'file_date',
#  'ingestion_date',
#  'race_id')

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_processed.results")

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#     results_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_delta_table(results_df,'f1_processed','results','race_id',"tgt.result_id = src.result_id AND tgt.race_id = src.race_id")

# COMMAND ----------

# overwrite_partition(results_df,'f1_processed','results','race_id')

# COMMAND ----------

# results_df.write.parquet(f'{processed_folder_path}/results',mode='overwrite',partitionBy='race_id')
# results_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) as cnt FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(*) as cnt FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING cnt > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;