# Databricks notebook source
# MAGIC %md
# MAGIC #### streaming customer data from cloud files to Delta Lake
# MAGIC - Read files using DataStreamReader API: Use the DataStreamReader API to read data from cloud storage.
# MAGIC - Transform the dataframe: Modify the dataframe by adding two additional columns:
# MAGIC >     File Path: The cloud file path where the data is stored.
# MAGIC >     Ingestion Date: The current timestamp when the data is ingested.
# MAGIC - Write the transformed data stream to Delta Lake Table: After transformation, the data stream is written to a Delta Lake table.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read files using DataStreamReader API: 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

customers_schema = StructType(fields=[
    StructField("customer_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("date_of_birth", DateType()),
    StructField("telephone", StringType()),
    StructField("email", StringType()),
    StructField("member_since", DateType()),
    StructField("created_timestamp", TimestampType())
])


# COMMAND ----------

customers_df = (
                    spark.readStream
                        .format("json")
                        .schema(customers_schema)
                        .load("/Volumes/gizmobox-new/landing/operational_data/customers_stream/")
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

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `gizmobox-new`.bronze.customers_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 03. Write the transformed data stream to Delta Lake Table

# COMMAND ----------

streaming_query = (
    customers_transformed_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/gizmobox-new/landing/operational_data/customers_stream/_checkpoint_stream")
    .toTable("`gizmobox-new`.bronze.customers_stream")
)


# COMMAND ----------

# stop streaming_query
streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.customers_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED `gizmobox-new`.bronze.customers_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY `gizmobox-new`.bronze.customers_stream

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.customers_stream
# MAGIC version as of 2
