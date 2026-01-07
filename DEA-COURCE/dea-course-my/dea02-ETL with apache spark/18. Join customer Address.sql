-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Join Customer and Address
-- MAGIC Join customer data with address data to create a customer_address table which contains the address of each customer on the same record.

-- COMMAND ----------

select * from `gizmobox-new`.silver.addresses

-- COMMAND ----------

select * from `gizmobox-new`.silver.customers

-- COMMAND ----------

select c.customer_id,
      c.customer_name,
      c.email,
      c.date_of_birth,
      c.member_since,
      c.telephone,
      a.shipping_address_line_1,
      a.shipping_city,
      a.shipping_state,
      a.shipping_postcode,
      a.billing_address_line_1,
      a.billing_city,
      a.billing_state,
      a.billing_postcode
  from `gizmobox-new`.silver.customers c
  inner join `gizmobox-new`.silver.addresses a
  on c.customer_id = a.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Write the transformed data to the gold schema

-- COMMAND ----------

create or replace table `gizmobox-new`.gold.customer_address
as
select c.customer_id,
      c.customer_name,
      c.email,
      c.date_of_birth,
      c.member_since,
      c.telephone,
      a.shipping_address_line_1,
      a.shipping_city,
      a.shipping_state,
      a.shipping_postcode,
      a.billing_address_line_1,
      a.billing_city,
      a.billing_state,
      a.billing_postcode
  from `gizmobox-new`.silver.customers c
  inner join `gizmobox-new`.silver.addresses a
  on c.customer_id = a.customer_id

-- COMMAND ----------

select * from `gizmobox-new`.gold.customer_address

-- COMMAND ----------


