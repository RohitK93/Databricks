-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### VACUUM Command:
-- MAGIC - The VACUUM command is used to remove old, unused files from Delta Lake to free up storage. It permanently deletes files that are no longer referenced in the transaction log and are older than the retention threshold (default is 7 days).
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 1: Check the Table History
-- MAGIC

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

-- by default file will be deleted after 7 days
VACUUM demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- VACUUM demo.delta_lake.optimize_stock_prices RETAIN 0 HOURS;

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

select * from demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

--  failed because files are deleted
select * from demo.delta_lake.optimize_stock_prices 
  version as of 1;
