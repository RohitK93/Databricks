-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Monthly Order Summary
-- MAGIC For each of the customer, produce the following summary per month:
-- MAGIC   1. total orders
-- MAGIC   2. total items bought
-- MAGIC   3. total amount spent
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from `gizmobox-new`.silver.orders
order by customer_id, order_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### For each of customer, produce the summary per month
-- MAGIC ##### 1. Total Orders
-- MAGIC ##### 2. Total Items Bought
-- MAGIC ##### 3. Total Amount Spent

-- COMMAND ----------

select customer_id, 
      date_format(transaction_timestamp, 'yyyy-MM') as order_month, 
      count(distinct order_id) as total_orders,
      sum(quantity) as total_items_bought,
      sum(price * quantity) as total_amount
from `gizmobox-new`.silver.orders
group by order_month, customer_id
order by customer_id asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Write the transformed data to the gold schema

-- COMMAND ----------

create or replace table `gizmobox-new`.gold.order_summary_monthly
as
select customer_id, 
      date_format(transaction_timestamp, 'yyyy-MM') as order_month, 
      count(distinct order_id) as total_orders,
      sum(quantity) as total_items_bought,
      sum(price * quantity) as total_amount
from `gizmobox-new`.silver.orders
group by order_month, customer_id
order by customer_id asc


-- COMMAND ----------

select * from `gizmobox-new`.gold.order_summary_monthly
