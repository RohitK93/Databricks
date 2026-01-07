# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Addresses Data
# MAGIC   1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address
# MAGIC   2. Write transformed data to the Silver schema

# COMMAND ----------

df_addresses = spark.table("`gizmobox-new`.bronze.py_addresses")
display(df_addresses)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
df_pivoted_addresses = (
    df_addresses
        .groupBy('customer_id')
        .pivot('address_type', ['shipping', 'billing'])
        .agg(
            F.max('address_line_1').alias('address_line_1'),
            F.max('city').alias('city'),
            F.max('state').alias('state'),
            F.max('postcode').alias('postcode')
        )
)

display(df_pivoted_addresses)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (SELECT customer_id,
# MAGIC              address_type,
# MAGIC              address_line_1,
# MAGIC              city,
# MAGIC              state,
# MAGIC              postcode
# MAGIC       FROM `gizmobox-new`.bronze.v_addresses)
# MAGIC PIVOT (MAX(address_line_1) AS address_line_1,
# MAGIC        MAX(city) AS city,
# MAGIC        MAX(state) AS state,
# MAGIC        MAX(postcode) AS postcode
# MAGIC        FOR address_type IN ('shipping', 'billing'));
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Write transformed data to the Silver schema
# MAGIC

# COMMAND ----------

df_pivoted_addresses.writeTo("`gizmobox-new`.silver.py_addresses").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.silver.py_addresses
