# Databricks notebook source
myarray = [
    {
        "parent_folder_name" : "raw",
        "folder_name" : "customers"
    },
    {
        "parent_folder_name" : "raw",
        "folder_name" : "sales"
    },
    {
        "parent_folder_name" : "raw",
        "folder_name" : "products"
    },
    {
        "parent_folder_name" : "raw",
        "folder_name" : "stores"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("myarray", myarray)

# COMMAND ----------

length_array = len(myarray)

# COMMAND ----------

dbutils.jobs.taskValues.set("length_array", length_array)

# COMMAND ----------


