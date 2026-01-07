-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Set-up the project environment for CircuitBox Data Lakehouse
-- MAGIC 1. Create external location - dea_course_ext_dl_circuitbox_new
-- MAGIC 2. Create Catalog - circuitbox-new
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
-- MAGIC - External Location Name: dea_course_ext_dl_circuitbox_new
-- MAGIC - ADLS Path: abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/
-- MAGIC - Storage Credential: rkcred
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_course_ext_dl_circuitbox_new
      URL 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/'
      WITH (STORAGE CREDENTIAL rkcred)
    COMMENT 'External location for circuitbox-new';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Create Catalog
-- MAGIC
-- MAGIC - Catalog Name: circuitbox-new
-- MAGIC - Managed Location: abfs://circuitbox-new@deacourseextdl3.dfs.core.windows.net/

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS `circuitbox-new`
  MANAGED LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/'
  COMMENT 'This is circuitbox-new catalog';

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Create Schemas
-- MAGIC 1. Schema Name: landing
-- MAGIC     Managed Location: abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/landing
-- MAGIC 2. Schema Name: lakehouse
-- MAGIC     Managed Location: abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/lakehouse

-- COMMAND ----------

use catalog `circuitbox-new`;

CREATE SCHEMA IF NOT EXISTS landing
      MANAGED LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/landing' ;


CREATE SCHEMA IF NOT EXISTS bronze
      MANAGED LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS silver
      MANAGED LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS gold
      MANAGED LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/gold';


-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 4. Create Volume
-- MAGIC 1. Volume Name: operational_data
-- MAGIC     ADLS Path: abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/landing/operational_data/

-- COMMAND ----------

USE CATALOG `circuitbox-new`;
USE SCHEMA landing;

CREATE EXTERNAL VOLUME IF NOT EXISTS operational_data
LOCATION 'abfss://circuitbox-new@deacourseextdl3.dfs.core.windows.net/landing/operational_data/';


-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/circuitbox-new/landing/operational_data/'
