-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Addresses Data
-- MAGIC   1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address
-- MAGIC   2. Write transformed data to the Silver schema

-- COMMAND ----------

SELECT  customer_id,
        address_type,
        address_line_1,
        city,
        state,
        postcode
FROM `gizmobox-new`.bronze.v_addresses;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##### 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT *
FROM (SELECT customer_id,
             address_type,
             address_line_1,
             city,
             state,
             postcode
      FROM `gizmobox-new`.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing'));


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Write transformed data to the Silver schema
-- MAGIC

-- COMMAND ----------

create or replace table `gizmobox-new`.silver.addresses
as
SELECT *
FROM (SELECT customer_id,
             address_type,
             address_line_1,
             city,
             state,
             postcode
      FROM `gizmobox-new`.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing'));


-- COMMAND ----------

select * from `gizmobox-new`.silver.addresses
