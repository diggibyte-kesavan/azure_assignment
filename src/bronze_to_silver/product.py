# Databricks notebook source
# MAGIC %md
# MAGIC run the utils notebook in customer notebook

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC import necessary functions

# COMMAND ----------

from pyspark.sql.functions import when,col

# COMMAND ----------

# MAGIC %md
# MAGIC read the sales_product csv file from dbfs

# COMMAND ----------

raw_producet_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/product/20240105_sales_product.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC change the column name into snakecase

# COMMAND ----------

renamed_product_df = toSnakeCase(raw_producet_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a column sub_category (Use Category columns id category_id=1, "phone"; 2, "laptop"; 3,"playstation"; 4,"e-device"

# COMMAND ----------

sub_category_df = renamed_product_df.withColumn("sub_category", when(col('category_id') == 1, "phone")
        .when(col('category_id') == 2 , "laptop")
        .when(col('category_id') == 3, "playstation")
        .when(col('category_id') == 4, "e-device"))

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/product'
write_delta_upsert(sub_category_df, writeTo)

# COMMAND ----------

