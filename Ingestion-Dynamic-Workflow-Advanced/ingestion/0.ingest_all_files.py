# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("parameter_test", "")
value_test = dbutils.widgets.get('parameter_test')

# COMMAND ----------

value_test

# COMMAND ----------

# MAGIC %md 
# MAGIC we can create widgets and they are integraged in the notebook and I can use that widget to pass down parameters. 
# MAGIC 
# MAGIC ### Note
# MAGIC We are doing this to pass down parameters from our main notebook to our child notebooks. 

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Jan Testing"})


# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": "Jan Testing"})

# COMMAND ----------

v_result
