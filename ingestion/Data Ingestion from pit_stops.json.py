# Databricks notebook source
from pyspark.sql.functions import col

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('stop', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('time', StringType(), False),
    StructField('duration', DoubleType(), False),
    StructField('milliseconds', IntegerType(), False)
])
pitstops_schema

# COMMAND ----------

pitstops_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json',schema=pitstops_schema,multiLine=True)
display(pitstops_df)

# COMMAND ----------

pitstops_df.count()

# COMMAND ----------

pitstops_df.printSchema()

# COMMAND ----------

pitstops_df.describe().show()

# COMMAND ----------

pitstops_df = pitstops_df.withColumnRenamed('raceId','race_id') \
                         .withColumnRenamed('driverId','driver_id') \
                         .withColumn('data_source', lit(v_data_source)) \
                         .withColumn('file_date', lit(v_file_date))
display(pitstops_df)

# COMMAND ----------

pitstops_df = add_ingestion_date(pitstops_df)
pitstops_df.show(5)

# COMMAND ----------

# # pitstops_df.write.parquet(f'{processed_folder_path}/pit_stops',mode='overwrite')
# pitstops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_processed.pit_stops")

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_processed.pit_stops")

# COMMAND ----------

# overwrite_partition(pitstops_df,'f1_processed','pit_stops','race_id')
merge_delta_table(pitstops_df,'f1_processed','pit_stops','race_id','tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop')

# COMMAND ----------


display(spark.read.format("delta").load(f'{processed_folder_path}/pit_stops'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) as cnt FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;