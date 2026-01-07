# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data - Using PySpark
# MAGIC   1. Run SQL Commands using Python - spark.sql Function
# MAGIC   2. Spark DataFrame Reader API
# MAGIC   3. Read Tables using spark.table Function

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Run SQL Commands using Python - spark.sql Function**

# COMMAND ----------

df = spark.sql('select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/`')
display(df)

# COMMAND ----------

spark.sql('''create or replace  temp view tv_customers
               as
               select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/`''')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tv_customers

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Spark DataFrame Reader API**

# COMMAND ----------

df = spark.read.format('json') \
    .load('/Volumes/gizmobox-new/landing/operational_data/customers/')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Read Tables using spark.table Function**

# COMMAND ----------

df = spark.table('`gizmobox-new`.bronze.v_addresses')
display(df)

# COMMAND ----------


