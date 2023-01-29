# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

# MAGIC %run "../../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# we want to find the points and again the wins per race per team

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy('race_year', 'team') \
.agg(sum("points").alias('total_points'),
    count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

# Now we want to create a windo and create a rank per race year
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

rank_window = Window.partitionBy('race_year').orderBy(col('total_points').desc())
final_df = constructor_standings_df.withColumn('rank', rank().over(rank_window))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Exploring a bit with TempViews

# COMMAND ----------

final_df.createOrReplaceTempView('final_df_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM final_df_v

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) FROM final_df_v

# COMMAND ----------

sql_table = spark.sql("SELECT * FROM final_df_v WHERE race_year = 2020")
sql_table.show()

# COMMAND ----------

# MAGIC %md
# MAGIC As we know (basic Databricks) the Temp view will not be available in any other notebook as well when we detach the cluster from the notebook it will become unavailable. 
# MAGIC 
# MAGIC -> Therefore we have Global views 

# COMMAND ----------

final_df.createOrReplaceGlobalTempView('final_df_glob_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.final_df_glob_v;

# COMMAND ----------

global_final_df = spark.sql("SELECT * FROM global_temp.final_df_glob_v;")
global_final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC We are able to access this table in other notebook, since it is a global temp view. 

# COMMAND ----------

final_df_t = spark.sql("SELECT * FROM global_temp.final_df_glob_v)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small directory exploration

# COMMAND ----------

#We could write the data to the Databricks storage and then create a Delta Table from it. 
final_df.write.format("delta").mode("overwrite").save("/mnt/Files/formula1/delta")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/formula1/delta

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/formula1/presentation/

# COMMAND ----------


