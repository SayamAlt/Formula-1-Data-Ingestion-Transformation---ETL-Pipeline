# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest csv files from lap_times folder

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

laptimes_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('position', IntegerType(), False),
    StructField('time', StringType(), False),
    StructField('milliseconds', IntegerType(), False)
])
laptimes_schema

# COMMAND ----------

laptimes_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv',schema=laptimes_schema) # Wildcard path to access a particular file type
display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

laptimes_df.printSchema()

# COMMAND ----------

laptimes_df.describe().show()

# COMMAND ----------

laptimes_df = laptimes_df.withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('driverId','driver_id') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))
display(laptimes_df)

# COMMAND ----------

laptimes_df = add_ingestion_date(laptimes_df)
laptimes_df.show(5)

# COMMAND ----------

# laptimes_df.write.parquet(f'{processed_folder_path}/lap_times',mode='overwrite')
# laptimes_df.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_processed.lap_times")

# COMMAND ----------

# overwrite_partition(laptimes_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_processed.lap_times")

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_processed.lap_times")

# COMMAND ----------

merge_delta_table(laptimes_df,'f1_processed','lap_times','race_id','tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap')

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/lap_times'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) as cnt FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;