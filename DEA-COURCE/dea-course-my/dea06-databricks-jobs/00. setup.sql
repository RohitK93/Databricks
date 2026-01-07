-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### 1. Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_course_ext_dl_demo1
      URL 'abfss://demo1@deacourseextdl3.dfs.core.windows.net/'
      WITH (STORAGE CREDENTIAL rkcred)
    COMMENT 'External location for demo1';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Create Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo1
  MANAGED LOCATION 'abfss://demo1@deacourseextdl3.dfs.core.windows.net/'
  COMMENT 'This is demo1 catalog';
