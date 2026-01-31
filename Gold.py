# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC # Read from Silver Table/s

# COMMAND ----------

# MAGIC %md
# MAGIC # Business Trandformation and Modelling

# COMMAND ----------

query = """
select 
row_number() over (order by ci.customer_id) As customer_key,
ci.customer_id,
ci.firstname,
ci.lastname

from
silver.crm_customers ci
"""
df = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write it to Gold layer's table

# COMMAND ----------

(
df.write
.mode("overwrite")
.format("delta")
.saveAsTable("workspace.gold.dim_customers")

)

# COMMAND ----------

df. display()