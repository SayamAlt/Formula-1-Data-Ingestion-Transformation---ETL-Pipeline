# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

name_schema = StructType([
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])
name_schema

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema, True),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])
drivers_schema

# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json',schema=drivers_schema)
display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('driverId','driver_id') \
    .withColumnRenamed('driverRef','driver_ref') \
    .withColumnRenamed('dob','date_of_birth') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date)) \
    .withColumn('name', concat(col('name.forename'),lit(' '),col('name.surname')))
display(drivers_df)

# COMMAND ----------

drivers_df = add_ingestion_date(drivers_df)
drivers_df.show(5)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df = drivers_df.drop('url')
drivers_df.show(5)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS f1_processed.drivers")

# COMMAND ----------

# drivers_df.write.parquet(f'{processed_folder_path}/drivers',mode='overwrite')
drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/drivers'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")