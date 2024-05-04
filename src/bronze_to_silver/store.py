# Databricks notebook source
# MAGIC %md
# MAGIC run the utils notebook in customer notebook

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC import necessary functions

# COMMAND ----------

from pyspark.sql.functions import split, to_date

# COMMAND ----------

# MAGIC %md
# MAGIC read the sales_store csv file from dbfs

# COMMAND ----------

raw_store_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/store/20240105_sales_store.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC change the column name into snakecase

# COMMAND ----------

renamed_store_df = toSnakeCase(raw_store_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a store category columns and the value is exatracted from email Eg: "electromart" from johndoe@electromart.com

# COMMAND ----------

store_category_df = renamed_store_df.withColumn("domain", split('email_address', '@')[1]).withColumn("store_category", split('domain', '\.')[0]).drop('domain')
store_category_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC created_at, updated_at date as yyyy-MM-dd format

# COMMAND ----------

formated_date_df = store_category_df.withColumn('created_at', to_date('created_at', 'dd-MM-yyyy')).withColumn('updated_at', to_date('updated_at', 'dd-MM-yyyy'))
formated_date_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write based on upsert [table_name: store] (in silver layer path is silver/sales_view/tablename/{delta pearquet}

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/store'
write_delta_upsert(formated_date_df, writeTo)