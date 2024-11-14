# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","Ergast API")


# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_file_date

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/common functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING" # DDL formatted string
constructors_schema

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json",schema=constructors_schema)
constructors_df.show(5)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructors_df.count()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df = constructors_df.drop('url')
constructors_df.show(5)

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed('constructorId','constructor_id') \
    .withColumnRenamed('constructorRef','constructor_ref') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))
constructors_df.show(5)

# COMMAND ----------

constructors_df = add_ingestion_date(constructors_df)
display(constructors_df)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS f1_processed.constructors")

# COMMAND ----------

# constructors_df.write.parquet('abfss://processed@formula1datalake11.dfs.core.windows.net/constructors',mode='overwrite')
constructors_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.format("delta").load(f'{processed_folder_path}/constructors'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")