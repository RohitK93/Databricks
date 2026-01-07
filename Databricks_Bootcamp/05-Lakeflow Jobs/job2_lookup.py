# Databricks notebook source
parameter = 'Toys'

# COMMAND ----------

# MAGIC %md
# MAGIC **SET VALUE**

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'lookup_output', value = parameter)

# COMMAND ----------


