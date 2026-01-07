-- Databricks notebook source
-- MAGIC %md
-- MAGIC # **Volumes**

-- COMMAND ----------

create catalog if not exists dba;

-- COMMAND ----------

use catalog dba;
create schema if not exists  bronze;

-- COMMAND ----------

use catalog dba;
use schema bronze;
create volume if not exists autovol;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS dba.bronze.bronze_volume

-- COMMAND ----------

SELECT * FROM csv.`/Volumes/dba/bronze/bronze_volume/sales/fact_sales.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # **DBUTILS**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/Volumes/dba/bronze/bronze_volume/sales/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/dba/bronze/bronze_volume/customers")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/Volumes/dba/bronze/bronze_volume/sales/","/Volumes/dba/bronze/bronze_volume/customers/",True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/Volumes/dba/bronze/bronze_volume/customers/fact_sales.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.put("/Volumes/dba/bronze/bronze_volume/customers/test.txt","True")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.put("/Volumes/dba/bronze/bronze_volume/customers/test.py","print(Hello Ansh Lamba)",True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC all_items = dbutils.fs.ls("/Volumes/dba/bronze/bronze_volume/customers/")
-- MAGIC all_items

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_names = [i.name for i in all_items]
-- MAGIC file_names

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for i in file_names:
-- MAGIC     dbutils.fs.rm(f"/Volumes/dba/bronze/bronze_volume/customers/{i}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("par1","")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC par1_value = dbutils.widgets.get("par1")
-- MAGIC par1_value

-- COMMAND ----------


