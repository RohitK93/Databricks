-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### User Defined Functions (UDF)
-- MAGIC > - User Defined Functions (UDF) in Spark are custom functions created by users to extend the capabilities of Spark SQL and PySpark.
-- MAGIC > - UDFs allow us to perform calculations or transformations to apply business logic that are not possible with built-in functions.
-- MAGIC > - You define the function once and use it across multiple queries. stored in hive metastore, unity catalog metastore 
-- MAGIC > - SQL UDFs are recommended over Python UDFs due to better optimization.
-- MAGIC
-- MAGIC ##### Syntax
-- MAGIC
-- MAGIC > - CREATE FUNCTION catalog_name.schema_name.udf_name(param_name data_type)
-- MAGIC > - RETURNS return_type
-- MAGIC > - RETURN expression;
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### creating a function that concatenates a first name and surname to output the full name

-- COMMAND ----------

CREATE OR REPLACE FUNCTION `gizmobox-new`.default.get_fullname(firstname STRING, surname STRING)
RETURNS STRING
RETURN CONCAT(initcap(firstname), ' ', initcap(surname));


-- COMMAND ----------

select `gizmobox-new`.default.get_fullname('Rohit', 'Kadam')

-- COMMAND ----------

DESC FUNCTION EXTENDED `gizmobox-new`.default.get_fullname;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION `gizmobox-new`.default.get_payment_status(payment_status INT)
RETURNS STRING
RETURN case payment_status
          when 1 then 'SUCCESS'
          when 2 then 'PENDING'
          when 3 then 'CANCELLED'
          when 4 then 'FAILED'
        end;


-- COMMAND ----------

select  payment_id, 
        order_id, 
        cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
        date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
        `gizmobox-new`.default.get_payment_status(payment_status) as payment_status, 
        payment_method
  from `gizmobox-new`.bronze.payments
  
