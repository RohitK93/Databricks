# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Refunds Data
# MAGIC   1. Extract specific portion of the string from refund_reason using the split function.
# MAGIC   2. Extract specific portion of the string from refund_reason using the regexp_extract function.
# MAGIC   3. Extract date and time from the refund_timestamp.
# MAGIC   4. Write transformed data to the Silver schema in Hive metastore.

# COMMAND ----------

df_refunds = spark.read.table("`gizmobox-new`.bronze.py_refunds")

display(df_refunds)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT   refund_id,
# MAGIC          payment_id,
# MAGIC          refund_timestamp,
# MAGIC          refund_amount,
# MAGIC          refund_reason
# MAGIC FROM `gizmobox-new`.bronze.py_refunds;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Extract specific portion of the string from refund_reason using the split function.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
df_split_refunds = (
                df_refunds.select(
                    "payment_id",
                    "refund_id",
                    "refund_timestamp",
                    "refund_amount",
                    F.split("refund_reason", ":")[0].alias("refund_reason"),
                    F.split("refund_reason", ":")[1].alias("refund_source"),
                )
)

display(df_split_refunds)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT   refund_id,
# MAGIC          payment_id,
# MAGIC          refund_timestamp
# MAGIC          refund_amount,
# MAGIC          SPLIT(refund_reason, ':') as split_refund_reason,
# MAGIC          SPLIT(refund_reason, ':')[0] as refund_reason,
# MAGIC          SPLIT(refund_reason, ':')[1] as refund_source
# MAGIC FROM hive_metastore.gizmobox_new_bronze.refunds;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Extract specific portion of the string from refund_reason using regexp_extract function
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
df_transformed_refunds = (
                df_refunds.select(
                    "payment_id",
                    "refund_id",
                    F.date_format("refund_timestamp", "yyyy-MM-dd").alias("refund_date"),
                    F.date_format("refund_timestamp", "HH.mm.ss").alias("refund_time"),
                    "refund_amount",
                    F.regexp_extract("refund_reason", "^([^:]+):", 1).alias("refund_reason"),
                    F.regexp_extract("refund_reason", "^[^:]+:(.*)$", 1).alias("refund_source"),
                )
)

display(df_transformed_refunds)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  refund_id,
# MAGIC         payment_id,
# MAGIC         refund_timestamp
# MAGIC         refund_amount,
# MAGIC         regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
# MAGIC         regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
# MAGIC FROM hive_metastore.gizmobox_new_bronze.refunds;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 3. Extract date and time from the refund_timestamp
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  refund_id,
# MAGIC         payment_id,
# MAGIC         cast(date_format(refund_timestamp, 'yyyy-MM-dd') as DATE ) as refund_date, 
# MAGIC         date_format(refund_timestamp, 'HH.mm.ss') as refund_time, 
# MAGIC         refund_amount,
# MAGIC         regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
# MAGIC         regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
# MAGIC FROM hive_metastore.gizmobox_new_bronze.refunds;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 4. Write transformed data to the Silver schema in hive metastore

# COMMAND ----------

df_transformed_refunds.writeTo("`gizmobox-new`.silver.py_refunds").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.silver.py_refunds

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended hive_metastore.gizmobox_new_silver.refunds
