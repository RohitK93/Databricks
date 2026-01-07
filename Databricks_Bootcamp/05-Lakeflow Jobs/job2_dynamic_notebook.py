# Databricks notebook source
dbutils.widgets.text("para1","")

# COMMAND ----------

para1_value = dbutils.widgets.get("para1")
para1_value

# COMMAND ----------

# MAGIC %md
# MAGIC ### **GET VALUE**

# COMMAND ----------

var2 = dbutils.jobs.taskValues.get('lookup', 'lookup_output')
var2
