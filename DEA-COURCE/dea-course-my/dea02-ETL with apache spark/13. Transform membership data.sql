-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Memberships Data
-- MAGIC   1. Extract customer_id from the file path.
-- MAGIC   2. Write transformed data to the Silver schema.

-- COMMAND ----------

select * from `gizmobox-new`.bronze.v_memberships

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Extract customer_id from the file path.
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
  content as membership_card
FROM `gizmobox-new`.bronze.v_memberships;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Write transformed data to the Silver schema.

-- COMMAND ----------

CREATE OR REPLACE TABLE `gizmobox-new`.silver.membership
as 
SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
  content as membership_card
FROM `gizmobox-new`.bronze.v_memberships;


-- COMMAND ----------

select * from `gizmobox-new`.silver.membership

-- COMMAND ----------


