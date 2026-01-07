-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Orders Data - String to JSON
-- MAGIC   1. Pre-process the JSON String to fix the Data Quality Issues
-- MAGIC   2. Transform JSON String to JSON Object
-- MAGIC   3. Write transformed data to the silver schema

-- COMMAND ----------

SELECT * 
FROM `gizmobox-new`.bronze.v_orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Pre-process the JSON String to fix the Data Quality Issues

-- COMMAND ----------

SELECT value,
       regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value
FROM `gizmobox-new`.bronze.v_orders;


-- COMMAND ----------

create or replace temporary view tv_orders_fixed
as 
SELECT value,
       regexp_replace(value, '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "\$1"') AS fixed_value
FROM `gizmobox-new`.bronze.v_orders;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Transform the JSON String into a JSON Object

-- COMMAND ----------

select schema_of_json(fixed_value) as schema,
    fixed_value
  from tv_orders_fixed
limit 1

-- COMMAND ----------

select from_json(fixed_value,
                  'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') as json_value,
    fixed_value
  from tv_orders_fixed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Write Transformed Data to the Silver Schema

-- COMMAND ----------

create or replace table `gizmobox-new`.silver.orders_json
as
select from_json(fixed_value,
                  'STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>') as json_value
  from tv_orders_fixed

-- COMMAND ----------

select * from `gizmobox-new`.silver.orders_json
