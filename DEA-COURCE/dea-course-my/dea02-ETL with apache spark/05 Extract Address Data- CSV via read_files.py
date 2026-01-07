# Databricks notebook source
# MAGIC %md
# MAGIC #### Extract Data From the Address Files
# MAGIC   1. Demonstrate Limitations of CSV file format in SELECT statement
# MAGIC   2. Use read_files function to overcome the limitations
# MAGIC   3. Create Addresses view in the Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Demonstrate Limitations of CSV file format in SELECT statement**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/gizmobox-new/landing/operational_data/addresses/`

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Use read_files function to overcome the limitations**
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from read_files('/Volumes/gizmobox-new/landing/operational_data/addresses/',
# MAGIC     format=> 'csv',
# MAGIC     delimiter=> '\t',
# MAGIC     header=> True)

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Create Addresses view in the Bronze Layer**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view `gizmobox-new`.bronze.v_addresses as
# MAGIC select * 
# MAGIC from read_files('/Volumes/gizmobox-new/landing/operational_data/addresses/',
# MAGIC     format=> 'csv',
# MAGIC     delimiter=> '\t',
# MAGIC     header=> True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gizmobox.bronze.v_addresses
