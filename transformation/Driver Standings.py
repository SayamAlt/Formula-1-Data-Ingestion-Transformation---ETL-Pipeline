# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, count_if, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/common functions"

# COMMAND ----------

race_years_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date='{v_file_date}'") \
    .select('race_year') \
    .distinct() \
    .collect()
race_years_list

# COMMAND ----------

race_years= []

for race_year in race_years_list:
    race_years.append(race_year['race_year'])

print(race_years)

# COMMAND ----------

race_results = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col('race_year').isin(race_years))
display(race_results)

# COMMAND ----------

driver_standings = race_results.groupBy('race_year','driver_name','driver_nationality').agg(sum('points').alias('total_points'))
display(driver_standings.filter('race_year == 2016'))

# COMMAND ----------

driver_standings = race_results.groupBy('race_year','driver_name','driver_nationality') \
    .agg(sum('points').alias('total_points'), \
        count_if(col('position') == 1).alias('wins'))
display(driver_standings)

# COMMAND ----------

display(driver_standings.filter("race_year == 2020").orderBy(desc('total_points')))

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df = driver_standings.withColumn("rank", rank().over(driver_rank_spec))
display(final_df)

# COMMAND ----------

display(final_df.filter('race_year == 2018'))

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_presentation.driver_standings")

# COMMAND ----------

# final_df.write.parquet(f"{presentation_folder_path}/driver_standings",mode='overwrite')
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_presentation.driver_standings")
# overwrite_partition(final_df,"f1_presentation","driver_standings","race_year")

# COMMAND ----------

merge_delta_table(final_df,'f1_presentation','driver_standings','race_year','tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings;

# COMMAND ----------

dbutils.notebook.exit("Success")