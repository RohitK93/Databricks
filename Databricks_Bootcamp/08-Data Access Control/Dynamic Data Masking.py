# Databricks notebook source
# MAGIC %md
# MAGIC ### **CREATE THE MASK FUNCTIONS**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dba.silver.dynamic_mask(p_email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('developers') THEN p_email ELSE '***' END;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **APPLYING MASK**

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dba.silver.customers_enr
# MAGIC ALTER COLUMN email SET MASK dba.silver.dynamic_mask

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add user in developers group
# MAGIC
# MAGIC SELECT * FROM dba.silver.customers_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC -- user is added in developers group
# MAGIC
# MAGIC SELECT * FROM dba.silver.customers_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.stores_enr

# COMMAND ----------


