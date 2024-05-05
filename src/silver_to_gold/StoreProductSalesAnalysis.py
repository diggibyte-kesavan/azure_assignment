# Databricks notebook source
# MAGIC %md
# MAGIC run the bronze_to_silver utils notebook in StoreProductSalesAnalysis

# COMMAND ----------

# MAGIC %run ../bronze_to_silver/utils

# COMMAND ----------

# MAGIC %md
# MAGIC using product and store table get the data

# COMMAND ----------

product_path = 'dbfs:/mnt/silver/sales_view/product'
store_path = 'dbfs:/mnt/silver/sales_view/store'

# COMMAND ----------

# MAGIC %md
# MAGIC assign variable to the udf and path

# COMMAND ----------

product_df = read_delta_file(product_path)
store_df = read_delta_file(store_path)

# COMMAND ----------

# MAGIC %md
# MAGIC using product and store table get the below data
# MAGIC store_id,store_name,location,manager_name,product_name,product_code,description,category_id,price,stock_quantity,supplier_id,product_created_at,product_updated_at,image_url,weight,expiry_date,is_active,tax_rate.
# MAGIC

# COMMAND ----------

merged_product_store_df = product_df.join(store_df, product_df.store_id == store_df.store_id, "inner")
product_store_df = merged_product_store_df.select(store_df.store_id,"store_name","location","manager_name","product_id","product_name","product_code","description","category_id","price","stock_quantity","supplier_id",product_df.created_at.alias("product_created_at"),product_df.updated_at.alias("product_updated_at"),"image_url","weight","expiry_date","is_active","tax_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC Read the delta table (using UDF functions)

# COMMAND ----------

customer_sales_path = "dbfs:/mnt/silver/sales_view/customer_sales"
customer_sales_df = read_delta_file(customer_sales_path)
customer_sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Using the above data & customer_sales and get the below data
# MAGIC OrderDate,Category,City,CustomerID,OrderID,Product ID,Profit,Region,Sales,Segment,ShipDate,ShipMode,latitude,longitude,store_name,location,manager_name,product_name,price,stock_quantity,image_url 
# MAGIC

# COMMAND ----------

merged_prodcust_custsale_df = product_store_df.join(customer_sales_df, product_store_df.product_id == customer_sales_df.product_id, "inner")
final_df = merged_prodcust_custsale_df.select("OrderDate","Category","City","CustomerID","OrderID",product_df.product_id.alias('ProductID'),"Profit","Region","Sales","Segment","ShipDate","ShipMode","latitude","longitude","store_name","location","manager_name","product_name","price","stock_quantity","image_url")

# COMMAND ----------

# MAGIC %md
# MAGIC Write based on overwrite (table_name : StoreProductSalesAnalysis )(in gold layer path is gold/sales_view/tablename/{delta pearquet}

# COMMAND ----------

writeTo = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis"
write_delta_upsert(final_df,writeTo)