# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(df)

# COMMAND ----------

race_results = df.filter("race_year == 2020")
display(race_results)

# COMMAND ----------

race_results.select(count('*')).show()

# COMMAND ----------

race_results.select(count('race_name')).show()

# COMMAND ----------

race_results.select(countDistinct('race_name').alias('num_races')).show()

# COMMAND ----------

race_results.select(sum('points').alias('total_points')).show()

# COMMAND ----------

race_results.filter("driver_name == 'Lewis Hamilton'").select(sum('points').alias('total_points'),countDistinct('race_name').alias('num_races')).show()

# COMMAND ----------

race_results.groupBy('driver_name').agg(sum('points').alias('total_points'),countDistinct('race_name').alias('num_races')) \
    .sort('total_points',ascending=False).show()

# COMMAND ----------

filtered_df = df.filter("race_year between 2010 and 2020")
filtered_df.count()

# COMMAND ----------

driver_standings_df = filtered_df.groupBy('race_year','driver_name').agg(sum('points').alias('total_points'),countDistinct('race_name').alias('num_races')).sort('total_points',ascending=False)
display(driver_standings_df)

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'))
display(driver_standings_df.withColumn("rank",rank().over(driver_rank_spec)).sort('race_year',ascending=False))

# COMMAND ----------

