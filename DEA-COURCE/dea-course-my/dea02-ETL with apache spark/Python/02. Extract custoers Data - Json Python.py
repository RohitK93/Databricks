# Databricks notebook source
# MAGIC %md
# MAGIC ##### Extract Data From the Customers JSON File
# MAGIC     1. Query Single File
# MAGIC     2. Query List of Files using wildcard Characters
# MAGIC     3. Query all the files in a Folder
# MAGIC     4. Select File Metadata
# MAGIC     5. Register Files in Unity Catalog using Views

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1. Query Single File
# MAGIC     

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_10.json")
display(df)

# COMMAND ----------

df = spark.read.json("/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_10.json")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_10.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2. Query List of Files using wildcard Characters
# MAGIC     

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_*.json")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/customers_2024_*.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 3. Query all the files in a Folder"

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/gizmobox-new/landing/operational_data/customers/")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/gizmobox-new/landing/operational_data/customers/`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Select File Metadata

# COMMAND ----------

df_metadata = df.select("_metadata.file_path", "*")
display(df_metadata)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   _metadata.file_path AS file_path, 
# MAGIC   _metadata,
# MAGIC   * 
# MAGIC FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. create table in branz table

# COMMAND ----------

df_metadata.write.format("delta").mode("overwrite").saveAsTable("`gizmobox-new`.bronze.py_customers")

# COMMAND ----------

df_metadata.writeTo("gizmobox-new.bronze.py_customers").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.py_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW `gizmobox-new`.bronze.v_customers AS
# MAGIC SELECT 
# MAGIC   _metadata.file_path AS file_path, 
# MAGIC   _metadata,
# MAGIC   * 
# MAGIC FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.bronze.v_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. create temporary View:
# MAGIC - available only when duration of spark session

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tv_customers AS
# MAGIC SELECT 
# MAGIC   _metadata.file_path AS file_path, 
# MAGIC   _metadata,
# MAGIC   * 
# MAGIC FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tv_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. create global temporary View:
# MAGIC - available only when duration of spark application until cluster has terminated

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW gtv_customers AS
# MAGIC SELECT 
# MAGIC   _metadata.file_path AS file_path, 
# MAGIC   _metadata,
# MAGIC   * 
# MAGIC FROM json.`/Volumes/gizmobox-new/landing/operational_data/customers`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gtv_customers
