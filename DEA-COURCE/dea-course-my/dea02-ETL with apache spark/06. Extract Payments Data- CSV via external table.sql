-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Extract Data From the Payments Files
-- MAGIC   1. List the files from Payment folder
-- MAGIC   2. Create External Table
-- MAGIC   3. Demonstrate the effect of Adding/Updating/Deleting files
-- MAGIC   4. Demonstrate the effect of Dropping the Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. list the files from external payment folder

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/landing/external_data/payments/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. create external table

-- COMMAND ----------

create table if not exists `gizmobox-new`.bronze.payments
(
    payment_id Integer, order_id Integer, payment_timestamp Timestamp, payment_status Integer, payment_method String
)
using csv
options (
  header = "true",
  delimiter = ","
)
location "abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/landing/external_data/payments/"

-- COMMAND ----------

select * from `gizmobox-new`.bronze.payments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### get the external table detail

-- COMMAND ----------

describe extended `gizmobox-new`.bronze.payments

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Demonstate effect of adding/updating/deleteing files

-- COMMAND ----------

select * from `gizmobox-new`.bronze.payments

-- COMMAND ----------

REFRESH TABLE `gizmobox-new`.bronze.payments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Demonstate effect of dropping the table
-- MAGIC

-- COMMAND ----------

-- DROP TABLE `gizmobox-new`.bronze.payments;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC So whenever you are using external tables to read the data, if you are creating the external table
-- MAGIC upfront and then you just execute in the select statements every day, make sure to refresh the table
-- MAGIC so that it's picking up all the new data that's available.
