# Databricks notebook source
# MAGIC %md
# MAGIC run the utils notebook in customer notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Users/kesavan.k@diggibyte.com/bronze_to_silver/utils"

# COMMAND ----------

# MAGIC %md
# MAGIC import necessary functions

# COMMAND ----------

from pyspark.sql.functions import split,when,col,to_date

# COMMAND ----------

# MAGIC %md
# MAGIC read the dbfs customer csv file in customer notebook

# COMMAND ----------

raw_customer_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/customer/20240106_sales_custmer.csv', header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC change the column name into snakecase

# COMMAND ----------

renamed_customer_df = toSnakeCase(raw_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC By using the "Name" column split by " " and create two columns first_name and last_name

# COMMAND ----------

splited_name_df = renamed_customer_df.withColumn('first_name', split(renamed_customer_df.name, " ")[0]).withColumn('last_name', split(renamed_customer_df.name, " ")[1]).drop(renamed_customer_df.name)

# COMMAND ----------

# MAGIC %md
# MAGIC Create column domain and extract from email columns Ex: Email = "josephrice131@slingacademy.com" domain="slingacademy"

# COMMAND ----------

extract_domain_df = splited_name_df.withColumn("tempdomain", split(splited_name_df.email_id, "@")[1]).drop(splited_name_df.email_id)
extract_domain_df = extract_domain_df.withColumn('domain', split(extract_domain_df.tempdomain, '\.')[0]).drop(extract_domain_df.tempdomain)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a column gender where male = "M" and Female="F"

# COMMAND ----------

converted_gender_df = extract_domain_df.withColumn('gender', when(col('gender') == 'male', 'M').otherwise('F'))
# converted_gender_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC From Joining date create two colums date and time by splitting based on " " delimiter.

# COMMAND ----------

splited_join_date_df = converted_gender_df.withColumn('date', split(col('joining_date'), " ")[0]).withColumn('time', split(col('joining_date'), ' ')[1]).drop('joining_date')
splited_join_date_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC •	 Date column should be on "yyyy-MM-dd" format.

# COMMAND ----------

converted_date_df = splited_join_date_df.withColumn('date', to_date(col('date'), 'dd-MM-yyyy'))
converted_date_df.display()

# COMMAND ----------

expenditure_df = converted_date_df.withColumn('expenditure-status', when(col('spent') < 200, 'MINIMUM').otherwise('MAXIMUM'))
expenditure_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write based on upsert [table_name: customer] (in silver layer path is silver/sales_view/tablename/{delta pearquet}

# COMMAND ----------

writeTo = f'dbfs:/mnt/Silver/sales_view/customer'
expenditure_df.write.format('delta').save(writeTo)

# COMMAND ----------

