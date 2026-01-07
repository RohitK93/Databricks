-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Insert Overwrite
-- MAGIC - Replace all the data in a table
-- MAGIC - Replace all the data from a specific partition
-- MAGIC - How to handle schema changes
-- MAGIC
-- MAGIC `INSERT OVERWRITE - Overwrites the existing data in a table or a specific partition with the new data.`.   
-- MAGIC `INSERT INTO - Appends new data.`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 1. Replace all the data in a table

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.gold_companies;

CREATE TABLE demo.delta_lake.gold_companies
(company_name STRING,
 founded_date DATE,
 country STRING);

INSERT INTO demo.delta_lake.gold_companies
(company_name, founded_date, country)
VALUES 
  ('Apple', '1976-04-01', 'USA'),
  ('TATA', '1900-01-01', 'INDIA');

SELECT * FROM demo.delta_lake.gold_companies;


-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.bronze_companies;

CREATE TABLE demo.delta_lake.bronze_companies
(company_name STRING,
 founded_date DATE,
 country STRING);

INSERT INTO demo.delta_lake.bronze_companies
(company_name, founded_date, country)
VALUES 
  ('Apple', '1976-04-01', 'USA'),
  ('GOOGLE', '1977-05-02', 'USA'),
  ('MICROSOFT', '1978-06-03', 'USA'),
  ('RELIANCE', '1978-07-04', 'INDIA'),
  ('TATA', '1900-01-01', 'INDIA');

SELECT * FROM demo.delta_lake.bronze_companies;


-- COMMAND ----------

insert overwrite table demo.delta_lake.gold_companies
  select * 
    from demo.delta_lake.bronze_companies

-- COMMAND ----------

DESC HISTORY demo.delta_lake.GOLD_COMPANIES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 2. Replace all the data from a specific partition

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.gold_companies_PARTITIONED;

CREATE TABLE demo.delta_lake.gold_companies_PARTITIONED
(company_name STRING,
 founded_date DATE,
 country STRING)
 PARTITIONED BY (country);

INSERT INTO demo.delta_lake.gold_companies_PARTITIONED
(company_name, founded_date, country)
VALUES 
  ('Apple', '1976-04-01', 'USA'),
  ('TATA', '1900-01-01', 'INDIA');

SELECT * FROM demo.delta_lake.gold_companies_PARTITIONED;


-- COMMAND ----------

DESC EXTENDED demo.delta_lake.gold_companies_PARTITIONED

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.bronze_companies_usa;

CREATE TABLE demo.delta_lake.bronze_companies_usa
(company_name STRING,
 founded_date DATE,
 country STRING);

INSERT INTO demo.delta_lake.bronze_companies_usa
(company_name, founded_date, country)
VALUES 
  ('Apple', '1976-04-01', 'USA'),
  ('GOOGLE', '1977-05-02', 'USA'),
  ('MICROSOFT', '1978-06-03', 'USA');

SELECT * FROM demo.delta_lake.bronze_companies_usa;


-- COMMAND ----------

insert overwrite table demo.delta_lake.gold_companies_PARTITIONED
partition (country='USA')
  SELECT  company_name, 
          founded_date
    FROM demo.delta_lake.bronze_companies_usa;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.gold_companies_PARTITIONED;

-- COMMAND ----------

desc history demo.delta_lake.gold_companies_PARTITIONED

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### 3. How to handle schema changes
-- MAGIC - Insert Overwrite: This method is used to overwrite the data in a table or partition when there are no schema changes.
-- MAGIC - Create or Replace Table: This method is used when there are schema changes

-- COMMAND ----------


