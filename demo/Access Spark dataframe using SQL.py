# Databricks notebook source
# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results)

# COMMAND ----------

race_results.count()

# COMMAND ----------

race_results.createOrReplaceTempView("race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from race_results
# MAGIC where race_year = 2018;

# COMMAND ----------

race_results_2015_df = spark.sql("select * from race_results where race_year = 2015")
display(race_results_2015_df)

# COMMAND ----------

race_results.createOrReplaceGlobalTempView("race_results_global")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.race_results_global;