# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.stores_enr

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Mapping Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists dba.silver.rls_mapping;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dba.silver.rls_mapping
# MAGIC (
# MAGIC   user_id STRING,
# MAGIC   region STRING 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dba.silver.rls_mapping 
# MAGIC VALUES
# MAGIC ("rohit.t.kadamm@gmail.com","east")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.rls_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT EXISTS
# MAGIC (SELECT * FROM dba.silver.rls_mapping
# MAGIC WHERE 
# MAGIC user_id = current_user() AND
# MAGIC region = 'east')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CREATE RLS FUNCTION**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION dba.silver.rls_func(p_region STRING)
# MAGIC RETURNS BOOLEAN 
# MAGIC RETURN 
# MAGIC EXISTS
# MAGIC (SELECT * FROM dba.silver.rls_mapping
# MAGIC WHERE 
# MAGIC user_id = current_user() AND
# MAGIC region = lower(p_region))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **APPLYING FUNCTION ON REGION COLUMN**

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dba.silver.stores_enr
# MAGIC SET ROW FILTER dba.silver.rls_func ON (region)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.stores_enr

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dba.silver.rls_mapping 
# MAGIC VALUES
# MAGIC ("rohit.t.kadamm@gmail.com","west"),
# MAGIC ("rohit.t.kadamm@gmail.com",'south'),
# MAGIC ('rohit.t.kadamm@gmail.com','north')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dba.silver.stores_enr
