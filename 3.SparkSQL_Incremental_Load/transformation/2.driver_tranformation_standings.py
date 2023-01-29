# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings
# MAGIC - Doing some transformations
# MAGIC - Applying window funtion to wondow_spec
# MAGIC 
# MAGIC First we are loading again the configured parameters form our other notebook.

# COMMAND ----------

# MAGIC %run "../../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import count

print(race_results_df[race_results_df['position'] == 1].count())
display(race_results_df[race_results_df['position'] == 1])

# COMMAND ----------

from pyspark.sql.functions import col, count, when, sum 
test_count_df = race_results_df.agg(count(when(col('position')==1, True)).alias('wins'))
display(test_count_df)

# COMMAND ----------

# finding out who has most wins per person per team 
from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum('points').alias('total_points'),
    count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

display(driver_standings_df.orderBy(col('wins').desc()))

# COMMAND ----------

# finding out, which team is perfroming the best in total over all years
best_performing_team_df = race_results_df \
.groupBy("team") \
.agg(sum('points').alias('total_points'),
     count(when(col('position') == 1, True)).alias('wins'))


display(best_performing_team_df.orderBy(col('wins').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that in total Ferrari is the best team

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

display(driver_standings_df.orderBy(col('wins').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC We can use a Window from pyspark and apply to this window then window functions. 
# MAGIC e.g. rank is a window funtion 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))
final_df.show()

# COMMAND ----------

driver_rank_spec_different = Window.partitionBy('race_year').orderBy(col("total_points").desc(),col('wins').desc())
final_df_different = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec_different))
final_df_different.show()

# COMMAND ----------

join_df = final_df_different.join(final_df, (final_df.race_year == final_df_different.race_year) & (final_df.driver_name == final_df_different.driver_name), 'anti')
join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC With the anti join we can see rows, that are in one df, but not in the other. 
# MAGIC 
# MAGIC In this case it is empty, which means, that both ways result in the same df.

# COMMAND ----------

# MAGIC %md 

# COMMAND ----------

driver_rank_spec

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------


