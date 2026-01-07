# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Memberships - Image Files
# MAGIC   1. Query Memberships File using binaryFile Format
# MAGIC   2. Create Memberships View in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Query Memberships File using binaryFile Format**
# MAGIC

# COMMAND ----------

# MAGIC %fs ls '/Volumes/gizmobox-new/landing/operational_data/memberships/'

# COMMAND ----------

df = spark.read.format("binaryFile").load("/Volumes/gizmobox-new/landing/operational_data/memberships/*/*.png")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Create Memberships View in Bronze Schema**

# COMMAND ----------

df.writeTo("`gizmobox-new`.bronze.py_memberships").createOrReplace()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_memberships 
