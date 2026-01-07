-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Extract Data From the Memberships - Image Files
-- MAGIC   1. Query Memberships File using binaryFile Format
-- MAGIC   2. Create Memberships View in Bronze Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. Query Memberships File using binaryFile Format**
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/gizmobox-new/landing/operational_data/memberships/'

-- COMMAND ----------

select * from binaryFile.`/Volumes/gizmobox-new/landing/operational_data/memberships/*/*.png` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2. Create Memberships View in Bronze Schema**

-- COMMAND ----------

create or replace view `gizmobox-new`.bronze.v_memberships 
as
select * from binaryFile.`/Volumes/gizmobox-new/landing/operational_data/memberships/*/*.png`

-- COMMAND ----------

select * from `gizmobox-new`.bronze.v_memberships
