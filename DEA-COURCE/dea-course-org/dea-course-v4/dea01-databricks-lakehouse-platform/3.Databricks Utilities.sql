-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Databricks Utilities
-- MAGIC - **File system utilities** 
-- MAGIC - **Secrets Utilities**
-- MAGIC - **Widget Utilities**
-- MAGIC - **Notebook Workflow Utilities**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### File system utilities

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/'))

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/databricks-datasets/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC items = dbutils.fs.ls('/databricks-datasets/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Count folders and files using list comprehensions 
-- MAGIC folder_count = len([item for item in items if item.name.endswith("/")]) 
-- MAGIC file_count = len([item for item in items if not item.name.endswith("/")]) 
-- MAGIC # Display the results 
-- MAGIC print(f"Total Folders: {folder_count}") 
-- MAGIC print(f"Total Files: {file_count}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help('cp')

-- COMMAND ----------


