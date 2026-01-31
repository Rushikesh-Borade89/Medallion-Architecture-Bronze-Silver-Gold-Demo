# Databricks notebook source
# MAGIC %md
# MAGIC #Reading from CSV

# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_crm/sales_details.csv")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write it to Bronze Layer

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_Sales_details")

# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_crm/prd_info.csv")
)
display(df)

df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_prd_info")


# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_crm/cust_info.csv")
)
display(df)

df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_cust_info")

# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_erp/CUST_AZ12.csv")
)
display(df)

df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_CUST_AZ12")

# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_erp/LOC_A101.csv")
)
display(df)

df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_LOC_A101")

# COMMAND ----------

df = (spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/Volumes/workspace/bronze/source_system/source_erp/PX_CAT_G1V2.csv")
)
display(df)

df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_PX_CAT_G1V2")
