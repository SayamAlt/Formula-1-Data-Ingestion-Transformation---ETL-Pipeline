# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

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

from pyspark.sql.functions import col, to_timestamp, concat, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])
races_schema

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=races_schema)
races_df.printSchema()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.show(5)

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

races_df = races_df.drop('url')
races_df.show(5)

# COMMAND ----------

races_df = races_df.withColumnRenamed('raceId','race_id') \
                   .withColumnRenamed('year','race_year') \
                   .withColumnRenamed('circuitId','circuit_id')
races_df.show(5)

# COMMAND ----------

races_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))
races_df.show(5)

# COMMAND ----------

races_df = add_ingestion_date(races_df)

# COMMAND ----------

races_df = races_df.drop('date', 'time')
races_df.show()

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS f1_processed.races")

# COMMAND ----------

# races_df.write.parquet(f"{processed_folder_path}/races",mode='overwrite',partitionBy='race_year')
races_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/races'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")