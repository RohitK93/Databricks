-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Compaction using OPTIMIZE and Z-Ordering
-- MAGIC ###### OPTIMIZE Command
-- MAGIC - `The` OPTIMIZE command is used to merge multiple small files into fewer, larger files. This improves performance in Delta Lake.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Table demo.delta_lake.optimize_stock_prices and insert data

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.optimize_stock_prices;
CREATE TABLE IF NOT EXISTS demo.delta_lake.optimize_stock_prices
(
    stock_id STRING,
    price DOUBLE,
    trading_date DATE
);


-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('AAPL', 227.65, '2025-02-10');


-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('GOOGL', 2805, '2025-02-10');


-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('MSFT', 325, '2025-02-10');


-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('AMZN', 3520, '2025-02-12');


-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

-- data get it from 4 diffrent file, check numFile
DESC DETAIL demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

select * from demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

optimize demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

-- data get it from 1 diffrent file, check numFile
DESC DETAIL demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### zorder by

-- COMMAND ----------

optimize demo.delta_lake.optimize_stock_prices
zorder by stock_id;
