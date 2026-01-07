# Databricks notebook source
# MAGIC %md
# MAGIC ### **SCALAR FUNCTIONS**

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dba.bronze.sql_scalar_func(p_par INT)
# MAGIC RETURNS INT   
# MAGIC LANGUAGE SQL 
# MAGIC RETURN p_par*10 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT total_amount, dba.bronze.sql_scalar_func(total_amount) FROM dba.silver.sales_enr

# COMMAND ----------

# MAGIC %md
# MAGIC **PYTHON**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dba.bronze.python_scalar_func(p_par INT)
# MAGIC RETURNS INT 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS 
# MAGIC $$
# MAGIC     return p_par*10
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT total_amount, dba.bronze.python_scalar_func(total_amount) FROM dba.silver.sales_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION  dba.bronze.python_scalar_func_cust(p_par INT)
# MAGIC RETURNS STRING 
# MAGIC LANGUAGE PYTHON 
# MAGIC AS 
# MAGIC $$
# MAGIC     import requests
# MAGIC     response = requests.get(f".v,f.,sl,s,lvsv,dv{p_par}").json()
# MAGIC     response_str = str(response)
# MAGIC     return response_str
# MAGIC $$
