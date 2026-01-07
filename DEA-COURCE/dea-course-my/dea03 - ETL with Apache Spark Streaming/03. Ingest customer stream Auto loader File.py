# Databricks notebook source
# MAGIC %md
# MAGIC #### streaming customer data from cloud files to Delta Lake using Auto loader
# MAGIC - Read files using DataStreamReader API
# MAGIC - Transform the dataframe
# MAGIC >     File Path: The cloud file path where the data is stored.
# MAGIC >     Ingestion Date: The current timestamp when the data is ingested.
# MAGIC - Write the transformed data stream to Delta Lake Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read files using DataStreamReader API: 

# COMMAND ----------

# MAGIC %md
# MAGIC `pathGlobFilter: only ingest mentioned file data`

# COMMAND ----------

customers_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "created_timestamp timestamp, date_of_birth date, member_since date")
    .option("cloudFiles.schemaLocation", "/Volumes/gizmobox-new/landing/operational_data/customers_autoloader/_schema")
    .option("pathGlobFilter", "customers_2024_*.json")
    .load("/Volumes/gizmobox-new/landing/operational_data/customers_autoloader/")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 02. Transform the dataframe: Modify the dataframe by adding two additional columns:
# MAGIC - File Path
# MAGIC - Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

customers_transformed_df = (
    customers_df.withColumn("file_path", col("_metadata.file_path"))
                .withColumn("ingestion_date", current_timestamp())
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 03. Write the transformed data stream to Delta Lake Table

# COMMAND ----------

streaming_query = (
    customers_transformed_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/gizmobox-new/landing/operational_data/customers_autoloader/_checkpoint_stream")
    .toTable("`gizmobox-new`.bronze.customers_autoloader")
)


# COMMAND ----------

# stop streaming_query
streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.customers_autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists `gizmobox-new`.bronze.customers_autoloader
