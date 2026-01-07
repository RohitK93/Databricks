# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Orders JSON File
# MAGIC   1. Query Orders File using JSON Format
# MAGIC   2. Query Orders File using TEXT Format
# MAGIC   3. Create Orders View in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Query Orders File using JSON Format**

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox-new/landing/operational_data/orders")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Query Orders File using TEXT Format**

# COMMAND ----------

df = spark.read.format("text").load("/Volumes/gizmobox-new/landing/operational_data/orders")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Create Orders View in Bronze Schema**

# COMMAND ----------

df.writeTo("`gizmobox-new`.bronze.py_orders").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_orders
