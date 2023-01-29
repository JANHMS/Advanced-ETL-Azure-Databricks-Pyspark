# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read all the data as required

# COMMAND ----------

# MAGIC %run "../../Includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races
# MAGIC 
# MAGIC Pretty simple just an innter join on the id

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes
# MAGIC 
# MAGIC several joins at the same time. 
# MAGIC - default is a left join, so if we do not pass any parameter here we are doing a left join. 

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------


final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using filter 
# MAGIC 1. Using filter with Pyspark SQL syntax
# MAGIC 2. Using filter with Pyspark Python syntax

# COMMAND ----------

#display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))


display(final_df.filter((final_df['race_year'] == 2020) & (final_df['race_name'] == 'Abu Dhabi Grand Prix')).orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/formula1/presentation

# COMMAND ----------

# MAGIC %md
# MAGIC We can now load the the file form the mount

# COMMAND ----------

df_read_races = spark.read.parquet('dbfs:/mnt/Files/formula1/presentation/race_results/')
display(df_read_races)

# COMMAND ----------


