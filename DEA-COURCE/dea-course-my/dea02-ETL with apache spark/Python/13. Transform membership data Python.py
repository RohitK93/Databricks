# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Memberships Data
# MAGIC   1. Extract customer_id from the file path.
# MAGIC   2. Write transformed data to the Silver schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_memberships

# COMMAND ----------

df_membership = spark.table("`gizmobox-new`.bronze.py_memberships")
display(df_membership)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Extract customer_id from the file path.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

df_extracted_membership = (
                df_membership.select(
                    F.regexp_extract("path", '.*/([0-9]+)\\.png$', 1).alias("customer_id"),
                    F.col("content").alias("membership_card")
                )
)

display(df_extracted_membership)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Write transformed data to the Silver schema.

# COMMAND ----------

df_extracted_membership.writeTo("`gizmobox-new`.silver.py_membership").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.silver.py_membership

# COMMAND ----------


