# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform Payments Data
# MAGIC   1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time.
# MAGIC   2. Map payment_status to contain descriptive values: (1-Success, 2-Pending, 3-Cancelled, 4-Failed).
# MAGIC   3. Write transformed data to the Silver schema.

# COMMAND ----------

df = spark.table(" `gizmobox-new`.bronze.py_payments")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Extract Date and Time from payment_timestamp and create new columns payment_date and payment_time.**

# COMMAND ----------

from pyspark.sql import functions as F

df_extracted_payments = (
                            df.select(
                                "payment_id",
                                "order_id",
                                F.date_format("payment_timestamp", 'yyyy-MM-dd').cast("date")
                                            .alias("payment_date"),
                                F.date_format("payment_timestamp", 'HH.mm.ss').alias("payment_time"),
                                "payment_status",
                                "payment_method"
                            )
)
display(df_extracted_payments)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  payment_id, 
# MAGIC         order_id, 
# MAGIC         cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
# MAGIC         date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
# MAGIC         payment_status, 
# MAGIC         payment_method
# MAGIC   from `gizmobox-new`.bronze.payments
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Map payment_status to contain descriptive values: (1-Success, 2-Pending, 3-Cancelled, 4-Failed).**
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

df_mapped_payments = (
                        df_extracted_payments
                        .select(
                            "payment_id",
                            "order_id",
                            "payment_date",
                            "payment_time",
                            F.when(F.col("payment_status") == 1, "Success")
                            .when(F.col("payment_status") == 2, "Pending")
                            .when(F.col("payment_status") == 3, "Cancelled")
                            .when(F.col("payment_status") == 4, "Failed")
                            .alias("payment_status"),
                            "payment_method"
                        )
)
display(df_mapped_payments)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  payment_id, 
# MAGIC         order_id, 
# MAGIC         cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
# MAGIC         date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
# MAGIC         case payment_status
# MAGIC           when 1 then 'SUCCESS'
# MAGIC           when 2 then 'PENDING'
# MAGIC           when 3 then 'CANCELLED'
# MAGIC           when 4 then 'FAILED'
# MAGIC         end as payment_status, 
# MAGIC         payment_method
# MAGIC   from `gizmobox-new`.bronze.payments
# MAGIC   

# COMMAND ----------

# MAGIC %md 
# MAGIC **3. Write transformed data to the Silver schema.**

# COMMAND ----------

df_mapped_payments.writeTo("`gizmobox-new`.silver.py_payments").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table `gizmobox-new`.silver.payments
# MAGIC as
# MAGIC select  payment_id, 
# MAGIC         order_id, 
# MAGIC         cast(date_format(payment_timestamp, 'yyyy-MM-dd') as DATE ) as payment_date, 
# MAGIC         date_format(payment_timestamp, 'HH.mm.ss') as payment_time, 
# MAGIC         case payment_status
# MAGIC           when 1 then 'SUCCESS'
# MAGIC           when 2 then 'PENDING'
# MAGIC           when 3 then 'CANCELLED'
# MAGIC           when 4 then 'FAILED'
# MAGIC         end as payment_status, 
# MAGIC         payment_method
# MAGIC   from `gizmobox-new`.bronze.payments
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.silver.py_payments
