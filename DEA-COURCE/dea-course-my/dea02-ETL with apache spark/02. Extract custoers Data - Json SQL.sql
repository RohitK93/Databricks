-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Extract Data From the Customers JSON File
-- MAGIC     1. Query Single File
-- MAGIC     2. Query List of Files using wildcard Characters
-- MAGIC     3. Query all the files in a Folder
-- MAGIC     4. Select File Metadata
-- MAGIC     5. Register Files in Unity Catalog using Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 1. Query Single File
-- MAGIC     

-- COMMAND ----------

select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_10.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 2. Query List of Files using wildcard Characters
-- MAGIC     

-- COMMAND ----------

select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_*.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 3. Query all the files in a Folder"

-- COMMAND ----------

select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Select File Metadata

-- COMMAND ----------

SELECT
  _metadata.file_path AS file_path, 
  _metadata,
  * 
FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 5. Register Files in Unity Catalog using Views

-- COMMAND ----------

CREATE OR REPLACE VIEW `gizmobox-new`.bronze.v_customers AS
SELECT 
  _metadata.file_path AS file_path, 
  _metadata,
  * 
FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

-- COMMAND ----------

select * from `gizmobox-new`.bronze.v_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. create temporary View:
-- MAGIC - available only when duration of spark session

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW tv_customers AS
SELECT 
  _metadata.file_path AS file_path, 
  _metadata,
  * 
FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

-- COMMAND ----------

select * from tv_customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. create global temporary View:
-- MAGIC - available only when duration of spark application until cluster has terminated

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers AS
SELECT 
  _metadata.file_path AS file_path, 
  _metadata,
  * 
FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

-- COMMAND ----------

select * from global_temp.gtv_customers
