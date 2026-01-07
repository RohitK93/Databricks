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

# MAGIC %sql
# MAGIC select * from json.`/Volumes/gizmobox-new/landing/operational_data/orders`

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Query Orders File using TEXT Format**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`/Volumes/gizmobox-new/landing/operational_data/orders`

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Create Orders View in Bronze Schema**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW `gizmobox-new`.bronze.v_orders AS
# MAGIC select * from text.`/Volumes/gizmobox-new/landing/operational_data/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.v_orders
