# Databricks notebook source
# MAGIC %md
# MAGIC ## Access ADLS using SAS Token
# MAGIC
# MAGIC <ol>
# MAGIC   <li>Set the Spark config for SAS Token</li>
# MAGIC   <li>List the files in a demo container</li>
# MAGIC   <li>Read data from circuits.csv file</li>
# MAGIC </ol>

# COMMAND ----------

sas_token = dbutils.secrets.get(scope='formula1-dl-secret-scope',key='formula1-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1datalake11.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1datalake11.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1datalake11.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake11.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake11.dfs.core.windows.net/circuits.csv",inferSchema=True))