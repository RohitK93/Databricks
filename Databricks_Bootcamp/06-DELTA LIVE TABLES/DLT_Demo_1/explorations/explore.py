# Databricks notebook source
df = spark.read.table("dba.silver.sales_enr")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricksansh.bronze.source
# MAGIC (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   processDate TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricksansh.bronze.source 
# MAGIC VALUES
# MAGIC (1,'honey',current_timestamp()),
# MAGIC (2,'milk',current_timestamp()),
# MAGIC (3,'wholeWheatBread',current_timestamp()),
# MAGIC (4,'mint',current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricksansh.dlt_schema.scd1_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricksansh.dlt_schema.scd3_table

# COMMAND ----------


