# Databricks notebook source
# MAGIC %md
# MAGIC # **PYSPARK**

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dba;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **CUSTOMERS**

# COMMAND ----------

df = spark.read.format("csv")\
          .option("inferSchema",True)\
          .option("header",True)\
          .load("/Volumes/dba/bronze/bronze_volume/customers/")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("name",upper(col("name")))
display(df)

# COMMAND ----------

df = df.withColumn("domain",split(col("email"),"@")[1])
display(df)

# COMMAND ----------

display(
    df.groupBy("domain")
    .agg(count(col("customer_id")).alias("total_customers"))
    .sort(col("total_customers").desc())
)

# COMMAND ----------

df = df.withColumn("processDate",current_timestamp())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPSERT 

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("dba.silver.customers_enr"):

    dlt_obj = DeltaTable.forName(spark, "dba.silver.customers_enr")

    dlt_obj.alias("trg").merge(df.alias("src"), "trg.customer_id == src.customer_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:

    df.write.format("delta")\
            .mode("append")\
            .saveAsTable("dba.silver.customers_enr")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PRODUCTS**

# COMMAND ----------

df_prod = spark.read.format("csv")\
            .option("inferSchema",True)\
            .option("header",True)\
            .load("/Volumes/dba/bronze/bronze_volume/products/")
display(df_prod)

# COMMAND ----------

df_prod = df_prod.withColumn("processDate",current_timestamp())
display(df_prod)

# COMMAND ----------

display(df_prod.groupBy("category").agg(avg("price").alias("avg_price")))


# COMMAND ----------

if spark.catalog.tableExists("dba.silver.products_enr"):

    dlt_obj = DeltaTable.forName(spark, "dba.silver.products_enr")

    dlt_obj.alias("trg").merge(df_prod.alias("src"), "trg.product_id == src.product_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:

    df_prod.write.format("delta")\
            .mode("append")\
            .saveAsTable("dba.silver.products_enr")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dba.silver.products_enr

# COMMAND ----------

# MAGIC %md
# MAGIC ### **STORES**

# COMMAND ----------

df_str = spark.read.format("csv")\
              .option("inferSchema",True)\
              .option("header",True)\
              .load("/Volumes/dba/bronze/bronze_volume/stores/")
display(df_str)

# COMMAND ----------

df_str = df_str.withColumn("store_name",regexp_replace(col("store_name"),"_",""))
display(df_str)

# COMMAND ----------

df_str = df_str.withColumn("processDate",current_timestamp())
display(df_str)


# COMMAND ----------

if spark.catalog.tableExists("dba.silver.stores_enr"):

    dlt_obj = DeltaTable.forName(spark, "dba.silver.stores_enr")

    dlt_obj.alias("trg").merge(df_str.alias("src"), "trg.store_id == src.store_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:

    df_str.write.format("delta")\
            .mode("append")\
            .saveAsTable("dba.silver.stores_enr")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SALES**

# COMMAND ----------

df_sales = spark.read.format("csv")\
              .option("inferSchema",True)\
              .option("header",True)\
              .load("/Volumes/dba/bronze/bronze_volume/sales/")
display(df_sales)

# COMMAND ----------

df_sales = df_sales.withColumn("pricePerSale",round(col("total_amount")/col("quantity"),2))
df_sales = df_sales.withColumn("processDate",current_timestamp())

display(df_sales)


# COMMAND ----------

if spark.catalog.tableExists("dba.silver.sales_enr"):

    dlt_obj = DeltaTable.forName(spark, "dba.silver.sales_enr")

    dlt_obj.alias("trg").merge(df_sales.alias("src"), "trg.sales_id == src.sales_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:

    df_sales.write.format("delta")\
            .mode("append")\
            .saveAsTable("dba.silver.sales_enr")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SPARK SQL**

# COMMAND ----------

df = spark.sql("SELECT * FROM dba.silver.products_enr")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp_products")

# COMMAND ----------

df = spark.sql("""
            SELECT *,
                    CASE 
                    WHEN category = 'Toys' THEN 'YES' ELSE 'NO' END AS flag
                    FROM temp_products
            """)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PySpark UDF**

# COMMAND ----------

def greet(p_input):
  return "Hello"+str(p_input)

# COMMAND ----------

udf_greet = udf(greet)

# COMMAND ----------

df = df.withColumn("greet",udf_greet(col("flag")))
display(df)
