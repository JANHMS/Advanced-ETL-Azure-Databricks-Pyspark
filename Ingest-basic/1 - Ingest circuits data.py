# Databricks notebook source
# MAGIC %run 
# MAGIC ./Includes/configuration

# COMMAND ----------

# MAGIC %run
# MAGIC ./Includes/common_functions

# COMMAND ----------

#With secret/createScope
# replace the aldsAccountName with your storage name. The storage name must be unique across all Azure. 
aldsAccountName="formula1project2023"
adlsContainerName="raw"
mountPoint="/mnt/Files/formula1/raw/"

# COMMAND ----------

clientSecret=dbutils.secrets.get(scope="formula1project", key="clientsecret")
appId=dbutils.secrets.get(scope="formula1project",key="appId")
tenantId=dbutils.secrets.get(scope="formula1project",key="tenantId")
endpoint="https://login.microsoftonline.com/"+tenantId+"/oauth2/token",

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": appId,
       "fs.azure.account.oauth2.client.secret": clientSecret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/bac959ee-2787-4fc3-9798-7c830e543973/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"
        }

# COMMAND ----------

dbutils.fs.mount(
source = "abfss://raw@formula1project2023.dfs.core.windows.net/raw",
mount_point = mountPoint,
extra_configs = configs)

# COMMAND ----------


# If we already have inrrectly something mounted we can unmount
# dbutils.fs.unmount("/mnt/Files/formula1/raw")

# ATTENTION: and remove the file from blob 
#dbutils.fs.rm("/mnt/Files/formula1/raw", True)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/formula1/raw/

# COMMAND ----------

df_circuits = spark.read.option('header', True)\
                        .option('interferSchema', True)\
                        .csv("dbfs:/mnt/Files/formula1/raw/circuits.csv")

df_circuits.show(5)


# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_circuits_final = spark.read\
.option('head', True)\
.schema(circuits_schema)\
.csv("dbfs:/mnt/Files/formula1/raw/circuits.csv")


# COMMAND ----------

# DBTITLE 1,select only specific cols
circuits_selected_df = df_circuits_final.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
 circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1/processed/circuits")

# COMMAND ----------

# MAGIC %md 
# MAGIC Now we loaded all the circut dataframe to the databricks mount storage

# COMMAND ----------


display(dbutils.fs.mounts())

# COMMAND ----------


