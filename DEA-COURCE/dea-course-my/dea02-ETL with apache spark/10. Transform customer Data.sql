-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Customer Data
-- MAGIC   1. Remove records with NULL customer_id
-- MAGIC   2. Remove exact duplicate records
-- MAGIC   3. Remove duplicate records based on created_timestamp
-- MAGIC   4. CAST the columns to the correct Data Type
-- MAGIC   5. Write transformed data to the Silver schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Remove records with NULL customer_id
-- MAGIC

-- COMMAND ----------

select *
from `gizmobox-new`.bronze.v_customers
where customer_id is not null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Remove exact duplicate records
-- MAGIC

-- COMMAND ----------

select *
from `gizmobox-new`.bronze.v_customers
where customer_id is not null
order by customer_id;

-- COMMAND ----------

select distinct *
from `gizmobox-new`.bronze.v_customers
where customer_id is not null
order by customer_id;

-- COMMAND ----------

select customer_id, 
    max(created_timestamp),
    max(customer_name),
    max(date_of_birth),
    max(email),
    max(member_since),
    max(telephone)
from `gizmobox-new`.bronze.v_customers
where customer_id is not null
group by customer_id
order by customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Remove duplicate records based on created_timestamp
-- MAGIC

-- COMMAND ----------

create or replace temporary view v_customer_distinct
as
select distinct *
  from `gizmobox-new`.bronze.v_customers
where customer_id is not null
order by customer_id;

-- COMMAND ----------

select customer_id, 
    max(created_timestamp) as max_created_timestamp
from v_customer_distinct
group by customer_id

-- COMMAND ----------

with cte_max as (
select customer_id, 
    max(created_timestamp) as max_created_timestamp
from v_customer_distinct
group by customer_id
)
select * 
  from v_customer_distinct t
  join cte_max m
    on t.customer_id = m.customer_id
    and t.created_timestamp = m.max_created_timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. CAST the columns to the correct Data Type
-- MAGIC

-- COMMAND ----------

with cte_max as (
select customer_id, 
    max(created_timestamp) as max_created_timestamp
from v_customer_distinct
group by customer_id
)
select cast(t.created_timestamp as timestamp) As created_timestamp,
            t.customer_id,
            t.customer_name,
            CAST(t.date_of_birth as date) as date_of_birth,
            t.email,
            cast(t.member_since as date) as member_since, 
            t.telephone
  from v_customer_distinct t
  join cte_max m
    on t.customer_id = m.customer_id
    and t.created_timestamp = m.max_created_timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Write transform data to silver schema
-- MAGIC

-- COMMAND ----------

create table `gizmobox-new`.silver.customers
as
with cte_max as (
select customer_id, 
    max(created_timestamp) as max_created_timestamp
from v_customer_distinct
group by customer_id
)
select cast(t.created_timestamp as timestamp) As created_timestamp,
            t.customer_id,
            t.customer_name,
            CAST(t.date_of_birth as date) as date_of_birth,
            t.email,
            cast(t.member_since as date) as member_since, 
            t.telephone
  from v_customer_distinct t
  join cte_max m
    on t.customer_id = m.customer_id
    and t.created_timestamp = m.max_created_timestamp



-- COMMAND ----------

select * from `gizmobox-new`.silver.customers

-- COMMAND ----------

DESCRIBE EXTENDED `gizmobox-new`.silver.customers

-- COMMAND ----------


