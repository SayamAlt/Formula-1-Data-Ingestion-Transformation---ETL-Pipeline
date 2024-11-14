# Databricks notebook source
# MAGIC %run "/Workspace/Users/n01606417@humber.ca/formula1/includes/configuration"

# COMMAND ----------

races = spark.read.parquet(f"{processed_folder_path}/races")
circuits = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races.count(), circuits.count()

# COMMAND ----------

races = races.filter("race_year=2019").withColumnRenamed('name','race_name')
races.count()

# COMMAND ----------

circuits = circuits.withColumnRenamed('name','circuit_name').filter("circuit_id < 70")
circuits.count()

# COMMAND ----------

races.printSchema()

# COMMAND ----------

circuits.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='inner')
race_circuits_df.printSchema()

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = race_circuits_df.select(races.race_id,races.race_name,circuits.circuit_id,circuits.circuit_name,circuits.location,circuits.country,races.round)
race_circuits_df.printSchema()

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outer Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Outer Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='left')
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Outer Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='right')
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='full')
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi/Left Semi Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='semi') # Everything in the left dataframe that is part of the right dataframe
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits.join(races,races.circuit_id == circuits.circuit_id,how='semi')
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti/Left Anti Join

# COMMAND ----------

race_circuits_df = races.join(circuits,races.circuit_id == circuits.circuit_id,how='anti') # Everything in the left dataframe that is not part of the right dataframe
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits.join(races,races.circuit_id == circuits.circuit_id,how='anti') # All the circuits where races were not held
display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Join

# COMMAND ----------

race_circuits_df = races.crossJoin(circuits)
display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

races.count() * circuits.count()