-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Extract Data From the Returns SQL Table
-- MAGIC   1. Create Bronze Schema in Hive Metastore
-- MAGIC   2. Create External Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **1. Create Bronze schema in Hive Metastaore**
-- MAGIC - Unity catalog doensnt allow to use JDBC to connect to external database. You need to use Hive Metastaore
-- MAGIC - if you want to work with Unity catalog you have to use LakeHouse Federation
-- MAGIC

-- COMMAND ----------

create schema if not exists hive_metastore.gizmobox_new_bronze;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **2. Create External Table**

-- COMMAND ----------

create table if not exists hive_metastore.gizmobox_new_bronze.refunds
using jdbc
options (
  url "jdbc:sqlserver://gizmobox-srv-rk.database.windows.net:1433;database=gizmobox-db",
  dbtable "refunds",
  user "gizmoboxadmin",
  password "R 3"
);

-- COMMAND ----------

select * from hive_metastore.bronze.refunds

-- COMMAND ----------

DESC EXTENDED hive_metastore.bronze.refunds

-- COMMAND ----------


