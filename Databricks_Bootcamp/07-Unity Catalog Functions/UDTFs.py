# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dba.bronze.udtf(p_para INT)
# MAGIC RETURNS TABLE 
# MAGIC RETURN 
# MAGIC (
# MAGIC   SELECT * FROM dba.SILVER.sales_enr WHERE total_amount > p_para
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.bronze.udtf(100)

# COMMAND ----------


