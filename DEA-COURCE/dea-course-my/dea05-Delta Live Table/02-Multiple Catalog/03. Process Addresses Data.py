# Databricks notebook source
# MAGIC %md
# MAGIC #### Process Addresses Data
# MAGIC 1. Ingest the data into the data lakehouse - bronze_addresses
# MAGIC 2. Perform data quality checks and transform the data as required - silver_addresses_clean
# MAGIC 3. Apply changes to the Addresses data (SCD Type 2) - silver_addresses

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Ingest the data into the data lakehouse - bronze_addresses
# MAGIC

# COMMAND ----------

@dlt.table(
    name = "bronze.addresses",
    table_properties = {'quality' : 'bronze'},
    comment = "Raw addresses data ingested from the source system"
)
def create_bronze_addresses():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/circuitbox-new/landing/operational_data/addresses/")
            .select(
                "*",
                F.col("_metadata.file_path").alias("input_file_path"),
                F.current_timestamp().alias("ingest_timestamp")
            )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Perform data quality checks and transform the data as required - silver_addresses_clean
# MAGIC

# COMMAND ----------

@dlt.table(
    name = "silver.addresses_clean",
    comment = "Cleaned addresses data",
    table_properties = {'quality': 'silver'}
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_address", "address_line_1 IS NOT NULL")
@dlt.expect("valid_postcode", "LENGTH(postcode) = 5")
def create_silver_addresses_clean():
    return (
        spark.readStream.table("LIVE.bronze.addresses")
        .select(
            "customer_id",
            "address_line_1",
            "city",
            "state",
            "postcode",
            F.col("created_date").cast("date")
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Apply changes to the Addresses data (SCD Type 2) - silver_addresses

# COMMAND ----------

dlt.create_streaming_table(
    name = "silver.addresses",
    comment = "SCD Type 2 addresses data",
    table_properties = {'quality': 'silver'}
)


# COMMAND ----------

dlt.apply_changes(
    target = "silver.addresses",
    source = "silver.addresses_clean",
    keys = ["customer_id"],
    sequence_by = "created_date",
    stored_as_scd_type = 2
)
