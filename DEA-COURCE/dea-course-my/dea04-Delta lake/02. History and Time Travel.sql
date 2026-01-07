-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### History and Time Travel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Query Delta Lake table history
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Query previous versions of the data
-- MAGIC

-- COMMAND ----------

select * from demo.delta_lake.companies
version as of 1

-- COMMAND ----------

select * from demo.delta_lake.companies
version as of 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Query data from a specific time
-- MAGIC

-- COMMAND ----------

select * from demo.delta_lake.companies
timestamp as of '2025-12-10T17:03:40.000+00:00'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Restore data to a specific version

-- COMMAND ----------

RESTORE TABLE demo.delta_lake.companies
VERSION AS OF 1

-- COMMAND ----------

select * from demo.delta_lake.companies

-- COMMAND ----------

DESCRIBE HISTORY demo.delta_lake.companies
