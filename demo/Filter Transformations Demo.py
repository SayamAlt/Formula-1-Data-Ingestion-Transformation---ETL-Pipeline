# Databricks notebook source
# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

display(df.filter("race_year=2019"))

# COMMAND ----------

display(df.filter(df.race_year==2019))

# COMMAND ----------

display(df.filter(df['race_year']==2019))

# COMMAND ----------

display(df.filter((df['race_year'] == 2019) & (df['round'] <= 10)))

# COMMAND ----------

display(df.where((df['race_year'] == 2019) & (df['round'] <= 10)))

# COMMAND ----------

display(df.filter("race_year=2019 and round <= 10"))