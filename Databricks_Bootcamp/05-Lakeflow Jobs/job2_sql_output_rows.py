# Databricks notebook source
dbutils.widgets.text("sql_output","")

# COMMAND ----------

sql_output_text = dbutils.widgets.get("sql_output")
sql_output_text

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

sql_output_list = eval(sql_output_text)
sql_output_list
