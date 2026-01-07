# Databricks notebook source
data = [(1,100),(2,200),(3,300)]
schema = "id INT, salary INT"

# COMMAND ----------

df = spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta")\
            .mode("append")\
            .save("/Volumes/dba/bronze/bronze_volume/deltaopt/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/` ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS dba.bronze.liquidtable2
# MAGIC (
# MAGIC   id INT,
# MAGIC   salary INT 
# MAGIC )
# MAGIC CLUSTER BY AUTO

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dba.bronze.liquidtable2
# MAGIC CLUSTER BY none

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/` TO VERSION AS OF 5 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC
# MAGIC VACUUM delta.`/Volumes/dba/bronze/bronze_volume/deltaopt/` RETAIN 0 HOURS DRY RUN;
