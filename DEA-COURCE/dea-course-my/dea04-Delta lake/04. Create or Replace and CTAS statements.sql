-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### 1. Difference between Create or Replace and Drop and Create Table Statements:
-- MAGIC ###### 1.1. behavior of the DROP and CREATE statements.
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

CREATE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1), 
   company_name STRING NOT NULL,
   founded_date DATE,
   country STRING
  );


insert into demo.delta_lake.companies
(company_name, founded_date, country)
values  ('Apple', '1976-04-01', 'USA'),
        ('GOOGLE', '1977-05-02', 'USA'),
        ('MICROSOFT', '1978-06-03', 'USA'),
        ('RELIANCE', '1978-07-04', 'INDIA'),
        ('TATA', '1900-01-01', 'INDIA')


-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 1.2. behavior of the REPLACE and CREATE statements.

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.companies;

-- COMMAND ----------

CREATE or REPLACE TABLE demo.delta_lake.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1), 
   company_name STRING NOT NULL,
   founded_date DATE,
   country STRING
  );


insert into demo.delta_lake.companies
(company_name, founded_date, country)
values  ('Apple', '1976-04-01', 'USA'),
        ('GOOGLE', '1977-05-02', 'USA'),
        ('MICROSOFT', '1978-06-03', 'USA'),
        ('RELIANCE', '1978-07-04', 'INDIA'),
        ('TATA', '1900-01-01', 'INDIA')


-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### 2. CTAS (Create Table As Select) statement

-- COMMAND ----------

drop table if exists demo.delta_lake.companies_india;

CREATE TABLE demo.delta_lake.companies_india AS
SELECT *
FROM demo.delta_lake.companies
WHERE country = 'INDIA';


-- COMMAND ----------

select * from demo.delta_lake.companies_india

-- COMMAND ----------

DESC HISTORY demo.delta_lake.companies_india

-- COMMAND ----------

DESC demo.delta_lake.companies_india

-- COMMAND ----------

DROP TABLE demo.delta_lake.companies_india;

CREATE TABLE demo.delta_lake.companies_india AS
SELECT
    CAST(company_id AS INT) AS company_id,  -- Casting the company_id to an integer type
    company_name,
    founded_date,
    country
FROM demo.delta_lake.companies
WHERE country = 'INDIA';


-- COMMAND ----------

DESC demo.delta_lake.companies_india

-- COMMAND ----------

ALTER TABLE demo.delta_lake.companies_india
  ALTER COLUMN founded_date COMMENT 'Date the company was founded';


-- COMMAND ----------

DESC demo.delta_lake.companies_india

-- COMMAND ----------

ALTER TABLE demo.delta_lake.companies_india
  ALTER COLUMN company_id SET NOT NULL ;
