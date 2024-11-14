# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

def move_partition_col_at_end(input_df,partitionByCol):
    cols = input_df.columns
    cols.remove(partitionByCol)
    cols.append(partitionByCol)
    return input_df.select(cols)

def overwrite_partition(input_df, db_name, table_name, partition_col):
    output_df = move_partition_col_at_end(input_df, partition_col)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

from delta.tables import DeltaTable
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

def merge_delta_table(input_df,db_name,table_name,partition_col,merge_condition):
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltatable = DeltaTable.forName(spark,f"{db_name}.{table_name}")
        deltatable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition,
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format("delta").saveAsTable(f"{db_name}.{table_name}")