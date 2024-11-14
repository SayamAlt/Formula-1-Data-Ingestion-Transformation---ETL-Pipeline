# Databricks notebook source
dbutils.widgets.text("p_file_date",'2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# %sql
# USE f1_race_results_processed;

# COMMAND ----------

# %sql
# SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results (
# MAGIC   race_year INT,
# MAGIC   team_name STRING,
# MAGIC   nationality STRING,
# MAGIC   driver_id INT,
# MAGIC   driver_name STRING,
# MAGIC   race_id INT,
# MAGIC   position INT,
# MAGIC   points INT,
# MAGIC   calculated_points INT,
# MAGIC   created_date TIMESTAMP,
# MAGIC   updated_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW race_results_processed 
          AS
          SELECT rc.race_year, c.name as team_name, d.nationality, d.driver_id, d.name as driver_name, rc.race_id, re.position, re.points, 11-re.position as calculated_points FROM f1_processed.results re 
          JOIN f1_processed.drivers d ON re.driver_id = d.driver_id
          JOIN f1_processed.constructors c ON re.constructor_id = c.constructor_id
          JOIN f1_processed.races rc ON re.race_id = rc.race_id
          WHERE re.position <= 10 AND re.file_date = '{v_file_date}'
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_processed
# MAGIC WHERE race_year = 2021;

# COMMAND ----------

spark.sql(f"""
    MERGE INTO f1_presentation.calculated_race_results tgt
    USING race_results_processed src
    ON (tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id)
    WHEN MATCHED THEN
      UPDATE SET tgt.position = src.position,
                tgt.points = src.points,
                tgt.calculated_points = src.calculated_points,
                tgt.updated_date = current_timestamp
    WHEN NOT MATCHED THEN
      INSERT (race_year, team_name, nationality, driver_id, driver_name, race_id, position, points, calculated_points, created_date) VALUES(src.race_year,src.team_name,src.nationality,src.driver_id,src.driver_name,src.race_id,src.position,src.points,src.calculated_points,current_timestamp)       
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM race_results_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_presentation.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW TABLES IN f1_race_results_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_race_results_presentation.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_presentation.calculated_race_results;