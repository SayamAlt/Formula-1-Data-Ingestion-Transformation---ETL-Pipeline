# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest multiple multi-line JSON files from qualifying folder

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])
qualifying_schema

# COMMAND ----------

qualifying_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json',schema=qualifying_schema,multiLine=True)
display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

qualifying_df.describe().show()

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
    .withColumnRenamed('raceId','race_id') \
    .withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('constructorId','constructor_id') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))
qualifying_df.show(5)

# COMMAND ----------

qualifying_df = add_ingestion_date(qualifying_df)
qualifying_df.show(5)

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_processed.qualifying")

# COMMAND ----------

# qualifying_df.write.parquet(f'{processed_folder_path}/qualifying',mode='overwrite')
# qualifying_df.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_processed.qualifying")

# COMMAND ----------

# overwrite_partition(qualifying_df,'f1_processed','qualifying','race_id')
merge_delta_table(qualifying_df,'f1_processed','qualifying','race_id','tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id')

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/qualifying'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) as cnt FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;