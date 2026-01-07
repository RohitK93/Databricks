-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Orders Data - Explode Arrays
-- MAGIC   1. Access elements from the JSON object
-- MAGIC   2. Deduplicate Array Elements
-- MAGIC   3. Explode Arrays
-- MAGIC   4. Write the Transformed Data to Silver Schema

-- COMMAND ----------

SELECT *
FROM `gizmobox-new`.silver.orders_json;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Access elements from the JSON object 

-- COMMAND ----------

SELECT 
    json_value.order_id,
    json_value.order_status,
    json_value.payment_method,
    json_value.total_amount,
    json_value.transaction_timestamp,
    json_value.customer_id,
    json_value.items
FROM 
    `gizmobox-new`.silver.orders_json;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Deduplicate array elements 

-- COMMAND ----------

SELECT 
    json_value.order_id,
    json_value.order_status,
    json_value.payment_method,
    json_value.total_amount,
    json_value.transaction_timestamp,
    json_value.customer_id,
    array_distinct(json_value.items) as items
FROM 
    `gizmobox-new`.silver.orders_json;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Explode arrays — Flatten the arrays into separate rows for easier analysis and processing.

-- COMMAND ----------

SELECT 
    json_value.order_id,
    json_value.order_status,
    json_value.payment_method,
    json_value.total_amount,
    json_value.transaction_timestamp,
    json_value.customer_id,
    explode(array_distinct(json_value.items)) as item
FROM 
    `gizmobox-new`.silver.orders_json;


-- COMMAND ----------

create or replace temporary view tv_orders_exploded
as 
SELECT 
    json_value.order_id,
    json_value.order_status,
    json_value.payment_method,
    json_value.total_amount,
    json_value.transaction_timestamp,
    json_value.customer_id,
    explode(array_distinct(json_value.items)) as item
FROM 
    `gizmobox-new`.silver.orders_json;


-- COMMAND ----------

select * from tv_orders_exploded

-- COMMAND ----------

SELECT 
    order_id,
    order_status,
    payment_method,
    total_amount,
    transaction_timestamp,
    customer_id,
    item.item_id,
    item.name,
    item.price,
    item.quantity,
    item.category,
    item.details.brand,
    item.details.color
FROM 
    tv_orders_exploded;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Write the transformed data to the Silver schema

-- COMMAND ----------

create or replace table `gizmobox-new`.silver.orders
as
SELECT 
    order_id,
    order_status,
    payment_method,
    total_amount,
    transaction_timestamp,
    customer_id,
    item.item_id,
    item.name,
    item.price,
    item.quantity,
    item.category,
    item.details.brand,
    item.details.color
FROM 
    tv_orders_exploded;


-- COMMAND ----------

select * from `gizmobox-new`.silver.orders
