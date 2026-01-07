-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Refunds Data
-- MAGIC   1. Extract specific portion of the string from refund_reason using the split function.
-- MAGIC   2. Extract specific portion of the string from refund_reason using the regexp_extract function.
-- MAGIC   3. Extract date and time from the refund_timestamp.
-- MAGIC   4. Write transformed data to the Silver schema in Hive metastore.

-- COMMAND ----------

SELECT   refund_id,
         payment_id,
         refund_timestamp,
         refund_amount,
         refund_reason
FROM hive_metastore.gizmobox_new_bronze.refunds;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Extract specific portion of the string from refund_reason using the split function.
-- MAGIC

-- COMMAND ----------

SELECT   refund_id,
         payment_id,
         refund_timestamp
         refund_amount,
         SPLIT(refund_reason, ':') as split_refund_reason,
         SPLIT(refund_reason, ':')[0] as refund_reason,
         SPLIT(refund_reason, ':')[1] as refund_source
FROM hive_metastore.gizmobox_new_bronze.refunds;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Extract specific portion of the string from refund_reason using regexp_extract function
-- MAGIC

-- COMMAND ----------

SELECT  refund_id,
        payment_id,
        refund_timestamp
        refund_amount,
        regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
        regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
FROM hive_metastore.gizmobox_new_bronze.refunds;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 3. Extract date and time from the refund_timestamp
-- MAGIC

-- COMMAND ----------

SELECT  refund_id,
        payment_id,
        cast(date_format(refund_timestamp, 'yyyy-MM-dd') as DATE ) as refund_date, 
        date_format(refund_timestamp, 'HH.mm.ss') as refund_time, 
        refund_amount,
        regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
        regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
FROM hive_metastore.gizmobox_new_bronze.refunds;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### 4. Write transformed data to the Silver schema in hive metastore

-- COMMAND ----------

create schema hive_metastore.gizmobox_new_silver;

-- COMMAND ----------

create or replace table hive_metastore.gizmobox_new_silver.refunds
as
SELECT  refund_id,
        payment_id,
        cast(date_format(refund_timestamp, 'yyyy-MM-dd') as DATE ) as refund_date, 
        date_format(refund_timestamp, 'HH.mm.ss') as refund_time, 
        refund_amount,
        regexp_extract(refund_reason, '^([^:]+):', 1) AS refund_reason,
        regexp_extract(refund_reason, '^[^:]+:(.*)$', 1) AS refund_source
FROM hive_metastore.gizmobox_new_bronze.refunds;


-- COMMAND ----------

select * from hive_metastore.gizmobox_new_silver.refunds

-- COMMAND ----------

desc extended hive_metastore.gizmobox_new_silver.refunds
