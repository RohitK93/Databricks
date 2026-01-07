# Databricks notebook source
# MAGIC %md
# MAGIC #### Join Customer and Address
# MAGIC Join customer data with address data to create a customer_address table which contains the address of each customer on the same record.

# COMMAND ----------

df_addresses = spark.table("`gizmobox-new`.silver.py_addresses")
display(df_addresses)

# COMMAND ----------

df_customers = spark.table("`gizmobox-new`.silver.py_customers")
display(df_customers)

# COMMAND ----------

df_customer_addresses = (
            df_customers
            .join(df_addresses, on='customer_id', how='inner')
            .select(
                "customer_id",
                "customer_name",
                "email",
                "date_of_birth",
                "member_since",
                "telephone",
                "shipping_address_line_1",
                "shipping_city",
                "shipping_state",
                "shipping_postcode",
                "billing_address_line_1",
                "billing_city",
                "billing_state",
                "billing_postcode"
            )

)

display(df_customer_addresses)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the transformed data to the gold schema

# COMMAND ----------

df_customer_addresses.writeTo("`gizmobox-new`.gold.py_customer_address").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `gizmobox-new`.gold.py_customer_address

# COMMAND ----------


