# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Returns SQL Table
# MAGIC   1. Read Return data via JDBC
# MAGIC   2. Create Return table in Bronze schema

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Read Return data via JDBC**
# MAGIC   

# COMMAND ----------

df = (
        spark.read.format("jdbc")
            .option("url", "jdbc:sqlserver://gizmobox-srv-rk.database.windows.net:1433;database=gizmobox-db")
            .option("dbtable", "refunds")
            .option("user", "gizmoboxadmin")
            .option("password", "Rohit@7273")
            .load()
    )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Create Return table in Bronze schema**

# COMMAND ----------

df.writeTo("`gizmobox-new`.bronze.py_refunds").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_refunds

# COMMAND ----------


