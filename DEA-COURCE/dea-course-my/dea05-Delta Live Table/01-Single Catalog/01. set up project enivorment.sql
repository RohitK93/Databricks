-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Set-up the project environment for CircuitBox Data Lakehouse
-- MAGIC 1. Create external location - dea_course_ext_dl_circuitbox
-- MAGIC 2. Create Catalog - circuitbox
-- MAGIC 3. Create Schemas
-- MAGIC     - landing
-- MAGIC     - lakehouse
-- MAGIC 4. Create Volume - operational_data
-- MAGIC
-- MAGIC [](url)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Create External Location
-- MAGIC
-- MAGIC - External Location Name: dea_course_ext_dl_circuitbox
-- MAGIC - ADLS Path: abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/
-- MAGIC - Storage Credential: rkcred
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_course_ext_dl_circuitbox
      URL 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/'
      WITH (STORAGE CREDENTIAL rkcred)
    COMMENT 'External location for circuitbox';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Create Catalog
-- MAGIC
-- MAGIC - Catalog Name: circuitbox
-- MAGIC - Managed Location: abfs://circuitbox@deacourseextdl3.dfs.core.windows.net/

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS circuitbox
  MANAGED LOCATION 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/'
  COMMENT 'This is circuitbox catalog';

-- COMMAND ----------

show catalogs;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Create Schemas
-- MAGIC 1. Schema Name: landing
-- MAGIC     Managed Location: abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/landing
-- MAGIC 2. Schema Name: lakehouse
-- MAGIC     Managed Location: abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/lakehouse

-- COMMAND ----------

use catalog circuitbox;

CREATE SCHEMA IF NOT EXISTS landing
      MANAGED LOCATION 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/landing' ;


CREATE SCHEMA IF NOT EXISTS lakehouse
      MANAGED LOCATION 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/lakehouse' ;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Create Volume
-- MAGIC 1. Volume Name: operational_data
-- MAGIC     ADLS Path: abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/landing/operational_data/

-- COMMAND ----------

USE CATALOG circuitbox;
USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://circuitbox@deacourseextdl3.dfs.core.windows.net/landing/operational_data/';


-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/circuitbox/landing/operational_data/'
