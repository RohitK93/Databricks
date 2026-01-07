-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Set-up the project environment for GizmoBox Data Lakehouse
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. create external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_course_ext_dl_gizmobox_new
      URL 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/'
      WITH (STORAGE CREDENTIAL rkcred)
    COMMENT 'External location for gizmobox-new';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Access the container gizmobox

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. create catalog

-- COMMAND ----------


CREATE CATALOG IF NOT EXISTS `gizmobox-new`  
    MANAGED LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/'  
    COMMENT 'This is gizmobox-new catalog';



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. create schema
-- MAGIC   1. bronze
-- MAGIC   2. silver
-- MAGIC   3. gold
-- MAGIC   4. landing

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

use catalog `gizmobox-new`;

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing
      MANAGED LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/landing' ;
CREATE SCHEMA IF NOT EXISTS bronze
      MANAGED LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS silver
      MANAGED LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS gold
      MANAGED LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/gold';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 5. create volume

-- COMMAND ----------

USE CATALOG `gizmobox-new`;
USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://gizmobox-new@deacourseextdl3.dfs.core.windows.net/landing/operational_data/';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/gizmobox-new/landing/operational_data
