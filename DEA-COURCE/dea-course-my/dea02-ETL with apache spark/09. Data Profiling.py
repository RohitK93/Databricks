# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Profiling in Databricks
# MAGIC   1. Profile Data using UI
# MAGIC   2. Profile Data using DBUTILS Package (dbutils.data.summarize method)
# MAGIC   3. Profile Data Manually
# MAGIC -         COUNT
# MAGIC -         COUNT_IF
# MAGIC -         MIN
# MAGIC -         MAX
# MAGIC -         WHERE Clause

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 1. Profile  data using user interface

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Profile Data using DBUTILS Package (dbutils.data.summarize method)

# COMMAND ----------

df = spark.table('`gizmobox-new`.bronze.v_customers')
dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Profile data manually
# MAGIC ##### 3.1. count function

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), count(customer_id), count(email), count(email)
# MAGIC from `gizmobox-new`.bronze.v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2. count if function

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), count_if(customer_id is null), count_if(email is null), count_if(email is null)
# MAGIC from `gizmobox-new`.bronze.v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3. Where
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from `gizmobox-new`.bronze.v_customers
# MAGIC where customer_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4. distinct keyword

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_number_of_records,
# MAGIC         count(Distinct customer_id) as total_number_of_distinct_customers
# MAGIC from `gizmobox-new`.bronze.v_customers
# MAGIC where customer_id is not null

# COMMAND ----------


