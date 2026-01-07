# Databricks notebook source
# MAGIC %md
# MAGIC # **AUTOLOADER QUERY**

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists dba.bronze.autovol

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format","json")\
          .option("cloudFiles.schemaLocation","/Volumes/dba/bronze/autovol/destination/checkpoint/")\
          .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
          .load("/Volumes/dba/bronze/autovol/raw/")
          

# COMMAND ----------

# df = spark.readStream.format("cloudFiles")\
#           .option("cloudFiles.format","csv")\
#           .option("cloudFiles.schemaLocation","/Volumes/dba/bronze/autovol/destination/checkpoint/")\
#           .option("cloudFiles.schemaEvolutionMode","rescue")\
#           .load("/Volumes/dba/bronze/autovol/raw/")
          

# COMMAND ----------

df.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation","/Volumes/dba/bronze/autovol/destination/checkpoint/")\
        .trigger(availableNow=True)\
        .option("mergeSchema",True)\
        .start("/Volumes/dba/bronze/autovol/destination/data/")


# COMMAND ----------

df = spark.read.format("delta")\
            .load("/Volumes/dba/bronze/autovol/destination/data")
display(df)

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

rescued_schema = StructType() \
    .add("discount", StringType()) \
    .add("payment_method", StringType())

# Parse the stringified JSON column
df = df.withColumn("rescued_struct", from_json(col("_rescued_data"), rescued_schema))

# Extract individual fields
df = df.withColumn("rescued_discount", col("rescued_struct.discount")) \
       .withColumn("rescued_payment_method", col("rescued_struct.payment_method"))

# COMMAND ----------

display(df)

# COMMAND ----------


