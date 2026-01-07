# Databricks notebook source
# MAGIC %md
# MAGIC ### **CTAS**

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history dba.silver.sales_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS dba.silver.ctas
# MAGIC AS 
# MAGIC SELECT * FROM dba.silver.sales_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.ctas

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history dba.silver.ctas

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DEEP CLONE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS dba.silver.deepclonenew
# MAGIC CLONE dba.silver.sales_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dba.silver.deepclonenew

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.deepclonenew

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SHALLOW CLONE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dba.silver.shallow
# MAGIC SHALLOW CLONE dba.silver.sales_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history dba.silver.shallow

# COMMAND ----------


