# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Returns SQL Table
# MAGIC 1. Create Bronze Schema in Hive Metastore
# MAGIC 1. Create External Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Bronze Schema in Hive Metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hive_metastore.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create External Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.bronze.refunds
# MAGIC USING JDBC
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://gizmobox-srv.database.windows.net:1433;database=gizmobox-db',
# MAGIC   dbtable 'refunds',
# MAGIC   user 'gizmoboxadm',
# MAGIC   password 'Gizmobox@Adm'
# MAGIC );  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.bronze.refunds;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED hive_metastore.bronze.refunds;
