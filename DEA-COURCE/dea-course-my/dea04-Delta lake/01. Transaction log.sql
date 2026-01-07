-- Databricks notebook source
-- MAGIC %fs ls 'abfss://demo@deacourseextdl3.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demo_course_ext_dl
      URL 'abfss://demo@deacourseextdl3.dfs.core.windows.net/'
      WITH (STORAGE CREDENTIAL rkcred)
    COMMENT 'External location for demo';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo
  MANAGED LOCATION 'abfss://demo@deacourseextdl3.dfs.core.windows.net/catalogs/'
  COMMENT 'This is demo catalog';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 1. Create a new schema under the demo catalog for this section of the course (delta_lake)
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
      MANAGED LOCATION 'abfss://demo@deacourseextdl3.dfs.core.windows.net/delta_lake' ;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. create delta lake table

-- COMMAND ----------

drop table if exists demo.delta_lake.companies

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
(
    company_name STRING,
    founded_date DATE,
    country STRING
);


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. insert data

-- COMMAND ----------

insert into demo.delta_lake.companies
values ('Apple', '1976-04-01', 'USA')

-- COMMAND ----------

select * from demo.delta_lake.companies

-- COMMAND ----------

insert into demo.delta_lake.companies
values ('GOOGLE', '1977-05-02', 'USA'),
        ('MICROSOFT', '1978-06-03', 'USA'),
        ('RELIANCE', '1978-07-04', 'INDIA')


-- COMMAND ----------

insert into demo.delta_lake.companies
values ('TATA', '1900-01-01', 'INDIA')
