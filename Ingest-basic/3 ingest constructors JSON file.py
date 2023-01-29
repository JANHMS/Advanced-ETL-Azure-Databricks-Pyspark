# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

#We can either create the schema via DDL 
# constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# or via Pyspark methods my preferred method 
from pyspark.sql.functions import *
from pyspark.sql.types import *

constructor_schema = StructType(fields=[
  StructField('constructorId', IntegerType(), False),
  StructField('constructorRef', StringType(), True),
  StructField('nationality', StringType(), True),
  StructField('url', StringType(), True),
])

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json("/mnt/Files/formula1/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/Files/formula1/processed/constructors")

# COMMAND ----------

constructor_final_df.show(5)

# COMMAND ----------


