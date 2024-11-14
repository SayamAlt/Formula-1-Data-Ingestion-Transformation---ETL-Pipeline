# Databricks notebook source
# MAGIC %md
# MAGIC ## Access ADLS using Access Keys
# MAGIC
# MAGIC <ol>
# MAGIC   <li>Set the Spark config fs.azure.account.key</li>
# MAGIC   <li>List files from demo container</li>
# MAGIC   <li>Read data from circuits.csv file</li>
# MAGIC </ol>

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope='formula1-dl-secret-scope',key='formula1-dl-access-key')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1datalake11.dfs.core.windows.net",dbutils.secrets.get(scope='formula1-dl-secret-scope',
                                                                                                  key='formula1-dl-access-key'))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake11.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake11.dfs.core.windows.net/",inferSchema=True))