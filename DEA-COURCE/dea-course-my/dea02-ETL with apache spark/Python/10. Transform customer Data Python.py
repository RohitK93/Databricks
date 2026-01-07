# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Customer Data
# MAGIC   1. Remove records with NULL customer_id
# MAGIC   2. Remove exact duplicate records
# MAGIC   3. Remove duplicate records based on created_timestamp
# MAGIC   4. CAST the columns to the correct Data Type
# MAGIC   5. Write transformed data to the Silver schema

# COMMAND ----------

df = spark.sql("""
        select *
        from `gizmobox-new`.bronze.py_customers
""")
display(df)

# COMMAND ----------

df = spark.table(" `gizmobox-new`.bronze.py_customers")
display(df)

# COMMAND ----------

df = spark.read.table("`gizmobox-new`.bronze.py_customers")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Remove records with NULL customer_id
# MAGIC

# COMMAND ----------

df = (
    spark.read
        .table("`gizmobox-new`.bronze.py_customers")
        .filter("customer_id is not null")
)
display(df)

# COMMAND ----------

df = spark.read.table("`gizmobox-new`.bronze.py_customers")
df_filter = df.filter(df.customer_id.isNotNull())
display(df_filter)

# COMMAND ----------

df = spark.read.table("`gizmobox-new`.bronze.py_customers")
df_filter = df.where(df.customer_id.isNotNull())
display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Remove exact duplicate records
# MAGIC

# COMMAND ----------

df_distinct = df_filter.distinct()
display(df_distinct)

# COMMAND ----------

df_distinct = df_filter.dropDuplicates()
display(df_distinct)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Remove duplicate records based on created_timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_max_ts = (
           df_distinct.groupBy("customer_id")
                    .agg(F.max("created_timestamp").alias('max_created_timestamp'))
)
display(df_max_ts)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view v_customer_distinct
# MAGIC as
# MAGIC select distinct *
# MAGIC   from `gizmobox-new`.bronze.v_customers
# MAGIC where customer_id is not null
# MAGIC order by customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, 
# MAGIC     max(created_timestamp) as max_created_timestamp
# MAGIC from v_customer_distinct
# MAGIC group by customer_id

# COMMAND ----------

df_distinct_customer = (
                        df_distinct
                            .join(df_max_ts,    (df_distinct.customer_id == df_max_ts.customer_id) & 
                                                (df_distinct.created_timestamp == df_max_ts.max_created_timestamp),
                                                "inner"
                            )
                            .select(df_distinct["*"])
)
display(df_distinct_customer)


# COMMAND ----------

df_distinct_customer_demo = (
                        df_distinct
                            .join(df_max_ts, on=["customer_id"], how="inner")
)
display(df_distinct_customer_demo)


# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_max as (
# MAGIC select customer_id, 
# MAGIC     max(created_timestamp) as max_created_timestamp
# MAGIC from v_customer_distinct
# MAGIC group by customer_id
# MAGIC )
# MAGIC select * 
# MAGIC   from v_customer_distinct t
# MAGIC   join cte_max m
# MAGIC     on t.customer_id = m.customer_id
# MAGIC     and t.created_timestamp = m.max_created_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. CAST the columns to the correct Data Type
# MAGIC

# COMMAND ----------

df_casted_customer = (
    df_distinct_customer.select(
        F.col("file_path").alias("file_path"),
        F.col("created_timestamp").alias("created_timestamp"),
        F.col("customer_id").alias("customer_id"),
        F.col("customer_name").alias("customer_name"),
        F.col("date_of_birth").cast("date").alias("date_of_birth"),
        F.col("email").alias("email"),
        F.col("member_since").cast("date").alias("member_since"),
        F.col("telephone").alias("telephone")
    )
)
display(df_casted_customer)

# COMMAND ----------

df_casted_customer = (
    df_distinct_customer.select(
        df_distinct_customer.created_timestamp.cast("timestamp"),
        df_distinct_customer.customer_id,
        df_distinct_customer.customer_name,
        df_distinct_customer.date_of_birth.cast("date"),
        df_distinct_customer.email,
        df_distinct_customer.member_since.cast("date"),
        df_distinct_customer.telephone
    )
)
display(df_casted_customer)

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_max as (
# MAGIC select customer_id, 
# MAGIC     max(created_timestamp) as max_created_timestamp
# MAGIC from v_customer_distinct
# MAGIC group by customer_id
# MAGIC )
# MAGIC select cast(t.created_timestamp as timestamp) As created_timestamp,
# MAGIC             t.customer_id,
# MAGIC             t.customer_name,
# MAGIC             CAST(t.date_of_birth as date) as date_of_birth,
# MAGIC             t.email,
# MAGIC             cast(t.member_since as date) as member_since, 
# MAGIC             t.telephone
# MAGIC   from v_customer_distinct t
# MAGIC   join cte_max m
# MAGIC     on t.customer_id = m.customer_id
# MAGIC     and t.created_timestamp = m.max_created_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write transform data to silver schema
# MAGIC

# COMMAND ----------

df_casted_customer.writeTo("`gizmobox-new`.silver.py_customers").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.silver.py_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED `gizmobox-new`.silver.py_customers

# COMMAND ----------


