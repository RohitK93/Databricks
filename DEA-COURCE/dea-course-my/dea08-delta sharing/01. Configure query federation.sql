-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Configure Access to Azure SQL Database via Lakehouse Federation
-- MAGIC   1. Create Connection - asql_gizmobox_db_conn
-- MAGIC   2. Create Foreign Catalog - asql_gizmobox_db_catalog

-- COMMAND ----------

CREATE CONNECTION asql_gizmobox_db_conn_sql TYPE sqlserver
OPTIONS (
  host 'gizmobox-srv-rk.database.windows.net',
  port '1433',
  user 'gizmoboxadmin',
  password 'Rohit@7273'
);

-- COMMAND ----------

CREATE FOREIGN CATALOG IF NOT EXISTS asql_gizmobox_db_catalog_sql USING CONNECTION asql_gizmobox_db_conn_sql
OPTIONS (database 'gizmobox-db');

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

use catalog asql_gizmobox_db_catalog_sql ;
show schemas;
use schema dbo;
show tables;
select * from refunds;

-- COMMAND ----------


