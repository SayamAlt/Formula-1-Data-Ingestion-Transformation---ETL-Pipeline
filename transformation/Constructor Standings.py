# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/common functions"

# COMMAND ----------

from pyspark.sql.functions import col, count, when, rank, desc, sum
from pyspark.sql.window import Window

# COMMAND ----------

# Find race years for which data is to be reprocessed
race_years_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date='{v_file_date}'").select("race_year").distinct().collect()
race_years_list

# COMMAND ----------

race_years = []

for race_year in race_years_list:
    race_years.append(race_year['race_year'])

print(race_years)

# COMMAND ----------

race_results = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_years))
race_results.count()

# COMMAND ----------

display(race_results)

# COMMAND ----------

race_results.printSchema()

# COMMAND ----------

constructor_standings = race_results.groupBy('race_year','team').agg(sum('points').alias('total_points'),count(when(col('position') == 1,1)).alias('wins'))
display(constructor_standings)

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))

constructor_standings = constructor_standings.withColumn('rank',rank().over(constructor_rank_spec))
display(constructor_standings)

# COMMAND ----------

display(constructor_standings.filter('race_year == 2012'))

# COMMAND ----------

display(constructor_standings.filter('race_year == 2020'))

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_presentation.constructor_standings")

# COMMAND ----------

# constructor_standings.write.parquet(f"{presentation_folder_path}/constructor_standings",mode='overwrite')
# constructor_standings.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_presentation.constructor_standings")
# overwrite_partition(constructor_standings,"f1_presentation","constructor_standings","race_year")

# COMMAND ----------

merge_delta_table(constructor_standings,'f1_presentation','constructor_standings','race_year','tgt.race_year = src.race_year AND tgt.team = src.team')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;

# COMMAND ----------

dbutils.notebook.exit("Success")