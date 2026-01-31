# Databricks notebook source
# MAGIC %md
# MAGIC #Init

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import trim, col

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from Bronze Table

# COMMAND ----------

df = spark.table("workspace.bronze.crm_cust_info")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##Trimming

# COMMAND ----------

for field in df.schema.fields:
  if isinstance(field.dataType, StringType):
    df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

RENAME_MAP={
    "cst_id": "customer_id",
    "cst_key": "customer_key",
    "cst_firstname": "firstname",
    "cst_lastname": "lastname",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalization

# COMMAND ----------

df = (
    df
    .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
        .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
                  .otherwise("n/a")
        )
        .withColumn(
            "cst_gndr",
            F.when(F.upper(F.col("cst_gndr")) == "M", "MALE")
            .when(F.upper(F.col("cst_gndr")) == "F", "Female")
            .otherwise("n/a")
                      )
        )
    

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming the columns

# COMMAND ----------

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name) 

# COMMAND ----------

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Write into Silver table

# COMMAND ----------

(
df.write
.mode("overwrite")
.format("delta")
.saveAsTable("silver.crm_customers")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.crm_customers