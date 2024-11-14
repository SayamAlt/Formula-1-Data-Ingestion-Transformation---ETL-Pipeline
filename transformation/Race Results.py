# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/common functions"

# COMMAND ----------

circuits = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
drivers = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
constructors = spark.read.format("delta").load(f"{processed_folder_path}/constructors")
races = spark.read.format("delta").load(f"{processed_folder_path}/races")
results = spark.read.format("delta").load(f"{processed_folder_path}/results")

# COMMAND ----------

races.printSchema()

# COMMAND ----------

races = races.withColumnRenamed('name','race_name') \
             .withColumnRenamed('race_timestamp','race_date')
races.show(5)

# COMMAND ----------

circuits.printSchema()

# COMMAND ----------

circuits = circuits.withColumnRenamed('location','circuit_location')

# COMMAND ----------

drivers.printSchema()

# COMMAND ----------

drivers = drivers.withColumnRenamed('number','driver_number') \
                 .withColumnRenamed('name','driver_name') \
                 .withColumnRenamed('nationality','driver_nationality')
drivers.show(5)

# COMMAND ----------

constructors.printSchema()

# COMMAND ----------

constructors.show(5)

# COMMAND ----------

constructors = constructors.withColumnRenamed('name','team')

# COMMAND ----------

results.printSchema()

# COMMAND ----------

results = results.withColumnRenamed('time','race_time') \
    .withColumnRenamed('race_id','results_race_id') \
    .withColumnRenamed('driver_id','results_driver_id') \
    .filter(f"file_date='{v_file_date}'") \
    .withColumnRenamed("file_date","results_file_date")
results.show(5)

# COMMAND ----------

races_circuits_df = races.join(circuits,races.circuit_id==circuits.circuit_id,how='inner')
races_circuits_df.printSchema()

# COMMAND ----------

final_df = results.join(races_circuits_df,results.results_race_id==races_circuits_df.race_id) \
                     .join(drivers,results.results_driver_id==drivers.driver_id) \
                     .join(constructors,results.constructor_id==constructors.constructor_id) 
final_df.printSchema()       

# COMMAND ----------

final_df = final_df.select('race_id','driver_id','race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','fastest_lap','race_time','points','position','results_file_date')
display(final_df)

# COMMAND ----------

final_df.count()

# COMMAND ----------

final_df = final_df.withColumn('created_date',current_timestamp()).withColumnRenamed("results_file_date","file_date")
final_df.printSchema()

# COMMAND ----------

final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy('points',ascending=False).show(5)

# COMMAND ----------

# final_df.write.parquet(f"{presentation_folder_path}/race_results",mode='overwrite')
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_race_results_presentation.race_results")
# overwrite_partition(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS f1_presentation.race_results")

# COMMAND ----------

merge_delta_table(final_df,'f1_presentation','race_results','race_id','tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(*) as cnt
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id  
# MAGIC ORDER BY race_id DESC;