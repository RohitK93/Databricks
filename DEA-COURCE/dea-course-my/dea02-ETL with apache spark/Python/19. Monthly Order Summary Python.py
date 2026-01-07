# Databricks notebook source
# MAGIC %md
# MAGIC #### Monthly Order Summary
# MAGIC For each of the customer, produce the following summary per month:
# MAGIC   1. total orders
# MAGIC   2. total items bought
# MAGIC   3. total amount spent
# MAGIC
# MAGIC

# COMMAND ----------

df_orders = spark.table("`gizmobox-new`.silver.py_orders")
display(df_orders)


# COMMAND ----------

# MAGIC %md
# MAGIC #### For each of customer, produce the summary per month
# MAGIC ##### 1. Total Orders
# MAGIC ##### 2. Total Items Bought
# MAGIC ##### 3. Total Amount Spent

# COMMAND ----------

from pyspark.sql import functions as F
df_orders_summary = (
        df_orders
        .withColumn("order_month", F.date_format("transaction_timestamp", 'yyyy-MM'))
        .groupBy("order_month", "customer_id") \
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_items_bought"),
            F.sum(F.col("price") * F.col("quantity")).alias("total_amount")
        )
)

display(df_orders_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the transformed data to the gold schema

# COMMAND ----------

df_orders_summary.writeTo("`gizmobox-new`.gold.py_order_summary_monthly").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.gold.py_order_summary_monthly
