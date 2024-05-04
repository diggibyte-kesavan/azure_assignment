# Databricks notebook source
# MAGIC %md
# MAGIC run the utils notebook in customer notebook

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC read the sales_store csv file from dbfs

# COMMAND ----------

raw_sales_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240106_sales_data.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC change the column name into snakecase

# COMMAND ----------

renamed_sales_df = toSnakeCase(raw_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write based on upsert [table_name: customer_sales] (in silver layer path is silver/sales_view/tablename/{delta pearquet}

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/sales'
write_delta_upsert(renamed_sales_df, writeTo)

# COMMAND ----------

    