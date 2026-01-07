-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Transform Payments Data
-- MAGIC   1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time.
-- MAGIC   2. Map payment_status to contain descriptive values: (1-Success, 2-Pending, 3-Cancelled, 4-Failed).
-- MAGIC   3. Write transformed data to the Silver schema.

-- COMMAND ----------

select  payment_id, 
        order_id, 
        payment_timestamp, 
        payment_status, 
        payment_method
  from `gizmobox-new`.bronze.payments
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time.**

-- COMMAND ----------

select  payment_id, 
        order_id, 
        cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
        date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
        payment_status, 
        payment_method
  from `gizmobox-new`.bronze.payments
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2. Map payment_status to contain descriptive values: (1-Success, 2-Pending, 3-Cancelled, 4-Failed).**
-- MAGIC

-- COMMAND ----------

select  payment_id, 
        order_id, 
        cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
        date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
        case payment_status
          when 1 then 'SUCCESS'
          when 2 then 'PENDING'
          when 3 then 'CANCELLED'
          when 4 then 'FAILED'
        end as payment_status, 
        payment_method
  from `gizmobox-new`.bronze.payments
  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **3. Write transformed data to the Silver schema.**

-- COMMAND ----------

create table `gizmobox-new`.silver.payments
as
select  payment_id, 
        order_id, 
        cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
        date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
        case payment_status
          when 1 then 'SUCCESS'
          when 2 then 'PENDING'
          when 3 then 'CANCELLED'
          when 4 then 'FAILED'
        end as payment_status, 
        payment_method
  from `gizmobox-new`.bronze.payments
  

-- COMMAND ----------

select * from gizmobox.silver.payments
