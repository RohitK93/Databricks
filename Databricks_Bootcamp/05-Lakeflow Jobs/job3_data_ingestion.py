# Databricks notebook source
# MAGIC %sql
# MAGIC create volume if not exists dba.bronze.jobvolume

# COMMAND ----------

dbutils.widgets.text("folder_name","")
dbutils.widgets.text("parent_folder_name","")

# COMMAND ----------

folder_name = dbutils.widgets.get("folder_name")
parent_folder_name = dbutils.widgets.get("parent_folder_name")

# COMMAND ----------

df = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(f"/Volumes/dba/bronze/bronze_volume/{folder_name}/")

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save(f"/Volumes/dba/bronze/jobvolume/{parent_folder_name}/{folder_name}/")
