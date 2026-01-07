-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create Table - Table & Column Properties
-- MAGIC
-- MAGIC ##### 1. Table Properties
-- MAGIC - 1.1. COMMENT : allows you to document the purpose of the table.
-- MAGIC - 1.2. TBLPROPERTIES : used to specify table-level metadata or configuration settings.
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE or REPLACE TABLE demo.delta_lake.companies
  (company_name STRING,
   founded_date DATE,
   country STRING)
COMMENT 'This table contains information about some of the successful tech companies'
TBLPROPERTIES('sensitivity' = 'true');


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. Column Properties
-- MAGIC - 2.1 NOT NULL Constraints : enforces data integrity and quality by ensuring that a specific column cannot contain NULL values.
-- MAGIC - 2.2 COMMENT : documents the purpose or context of individual columns in a table.
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE or REPLACE TABLE demo.delta_lake.companies
  (company_name STRING NOT NULL,
   founded_date DATE COMMENT 'THE Date company was founded',
   country STRING)
COMMENT 'This table contains information about some of the successful tech companies'
TBLPROPERTIES('sensitivity' = 'true');


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 3. Generated Columns – derived or computed columns, whose values are computed at the time of inserting a new record.
-- MAGIC - 3.1 Generated Identity Columns – used to generate an identity, for example, a primary key value.
-- MAGIC - 3.2 Generated Computed Columns – automatically calculate and store derived values based on other columns in the same table.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 3.1. Generated Identity Columns:
-- MAGIC `GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ]`
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE or REPLACE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1), 
   company_name STRING NOT NULL,
   founded_date DATE COMMENT 'THE Date company was founded',
   country STRING)
COMMENT 'This table contains information about some of the successful tech companies'
TBLPROPERTIES('sensitivity' = 'true');


-- COMMAND ----------

insert into demo.delta_lake.companies
(company_name, founded_date, country)
values  ('Apple', '1976-04-01', 'USA'),
        ('GOOGLE', '1977-05-02', 'USA'),
        ('MICROSOFT', '1978-06-03', 'USA'),
        ('RELIANCE', '1978-07-04', 'INDIA'),
        ('TATA', '1900-01-01', 'INDIA')


-- COMMAND ----------

select * from demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 3.2. Generated Computed Columns
-- MAGIC GENERATED ALWAYS AS ( `expr` )
-- MAGIC
-- MAGIC `expr` may be composed of literals, column identifiers within the table, and deterministic, built-in SQL functions or operators except:
-- MAGIC - Aggregate functions
-- MAGIC - Analytic window functions
-- MAGIC - Ranking window functions
-- MAGIC - Table-valued generator functions
-- MAGIC
-- MAGIC Also, `expr` must not contain any subquery.

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE or REPLACE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1), 
   company_name STRING NOT NULL,
   founded_date DATE COMMENT 'THE Date company was founded',
   founded_year INT GENERATED ALWAYS AS (year(founded_date)),
   country STRING)
COMMENT 'This table contains information about some of the successful tech companies'
TBLPROPERTIES('sensitivity' = 'true');


-- COMMAND ----------

insert into demo.delta_lake.companies
(company_name, founded_date, country)
values  ('Apple', '1976-04-01', 'USA'),
        ('GOOGLE', '1977-05-02', 'USA'),
        ('MICROSOFT', '1978-06-03', 'USA'),
        ('RELIANCE', '1978-07-04', 'INDIA'),
        ('TATA', '1900-01-01', 'INDIA')


-- COMMAND ----------

select * from demo.delta_lake.companies
