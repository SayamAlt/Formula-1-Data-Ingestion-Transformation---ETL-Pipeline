# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

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

raw_folder_path

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])
circuits_schema

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv",header=True,schema=circuits_schema,inferSchema=False)
circuits_df.printSchema()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df = circuits_df.select(["circuitId", "circuitRef", "name", "location", "country", "lat", "lng","alt"])
circuits_df.show(5)

# COMMAND ----------

circuits_df.select(col('name')).show()

# COMMAND ----------

circuits_df = circuits_df.select(col('circuitId').alias('circuit_id'),col('circuitRef').alias('circuit_ref'),col('name'),col('location'),col('country'),col('lat').alias('latitude'),col('lng').alias('longitude'),col('alt').alias('altitude'))
circuits_df.show(5)

# COMMAND ----------

circuits_df = circuits_df.withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
circuits_df.show(5)

# COMMAND ----------

circuits_df = add_ingestion_date(circuits_df)
display(circuits_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS f1_processed.circuits")

# COMMAND ----------

# circuits_df.write.parquet(f"{processed_folder_path}/circuits",mode='overwrite')
circuits_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")