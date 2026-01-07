# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Address Files
# MAGIC   1. Demonstrate Limitations of CSV file format in SELECT statement
# MAGIC   2. Read all address file
# MAGIC   3. Create Addresses view in the Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Demonstrate Limitations of CSV file format in SELECT statement**

# COMMAND ----------

df = (
        spark.read.format("csv").load("/Volumes/gizmobox-new/landing/operational_data/addresses/"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Read all address file**
# MAGIC   

# COMMAND ----------

df = (
        spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", "\t")
            .load("/Volumes/gizmobox-new/landing/operational_data/addresses/")
    )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Create Addresses view in the Bronze Layer**

# COMMAND ----------

df.writeTo("`gizmobox-new`.bronze.py_addresses").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_addresses
