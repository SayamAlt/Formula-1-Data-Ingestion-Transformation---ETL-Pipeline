# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from circuits.csv',timeout_seconds=0,arguments={"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from constructors.json',timeout_seconds=0,arguments={"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from drivers.json',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from lap_times folder',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from pit_stops.json',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from qualifying folder',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from races.csv',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output

# COMMAND ----------

v_output = dbutils.notebook.run(path='/Workspace/Users/n01606417@humber.ca/formula1/ingestion/Data Ingestion from results.json',timeout_seconds=0,arguments={'p_data_source': 'Ergast API', "p_file_date": "2021-04-18"})

# COMMAND ----------

v_output