# Databricks notebook source
dbutils.widgets.text("sql_row","")
dbutils.widgets.text("sql_col","")

# COMMAND ----------

var1 = dbutils.widgets.get("sql_row")
var2 = dbutils.widgets.get("sql_col")

print(var1)

# COMMAND ----------

print(var2)

# COMMAND ----------


