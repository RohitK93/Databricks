-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### COPY INTO Command
-- MAGIC - Incrementally loads data into Delta Lake tables from Cloud Storage.
-- MAGIC - Supports schema evolution.
-- MAGIC - Supports a wide range of file formats (CSV, JSON, Parquet, Delta).
-- MAGIC - Alternative to Auto Loader for batch ingestion.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Creating the table to copy the data into.

-- COMMAND ----------

create table if not exists demo.delta_lake.raw_stock_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Incrementally loading new files into the table.

-- COMMAND ----------

delete from demo.delta_lake.raw_stock_prices;

copy into demo.delta_lake.raw_stock_prices
from 'abfss://demo@deacourseextdl3.dfs.core.windows.net/landing/stock_prices/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');



-- COMMAND ----------

select * from demo.delta_lake.raw_stock_prices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### MERGE Statement
-- MAGIC - Used for upserts (Insert/Update/Delete operations in a single statement).
-- MAGIC - Allows merging new data into a target table based on a matching condition.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create the table to merge data into:

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.stock_prices (
    stock_id STRING,
    price DOUBLE,
    trading_date DATE
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Merge the source data into the target table:
-- MAGIC - Insert new stocks that have been received.
-- MAGIC - Update stock price and trading date if updates have been received.
-- MAGIC - Delete stocks that are de-listed from the exchange (when the status is 'DELISTED').

-- COMMAND ----------

MERGE INTO demo.delta_lake.stock_prices AS target
USING demo.delta_lake.raw_stock_prices AS source
ON target.stock_id = source.stock_id
WHEN MATCHED AND source.status = 'ACTIVE' THEN
    UPDATE SET target.price = source.price, target.trading_date = source.trading_date
WHEN MATCHED AND source.status = 'DELISTED' THEN
    DELETE
WHEN NOT MATCHED AND source.status = 'ACTIVE' THEN
    INSERT (stock_id, price, trading_date) 
    VALUES (source.stock_id, source.price, source.trading_date);


-- COMMAND ----------

select * from demo.delta_lake.stock_prices
