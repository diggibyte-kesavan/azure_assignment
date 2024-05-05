# Databricks notebook source
# MAGIC %md
# MAGIC create a function for mounting adls and databricks notebooks

# COMMAND ----------

def mount_blob_storage(storage_account_name, container_name, mount_point, access_key):
    dbutils.fs.mount(
        source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
        mount_point=mount_point,
        extra_configs={
            f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key
        }
    )

# COMMAND ----------

# MAGIC %md
# MAGIC create the text widgets and pass the dynamic parameters

# COMMAND ----------

dbutils.widgets.text("storage_account_name","storageassignmentkesavan","Storage Account Name")
dbutils.widgets.text("container_name","silver","Container Name")
dbutils.widgets.text("mount_point", "/mnt/silver", "Mount Point")
dbutils.widgets.text("access_key", "kcfzgrlAXcVj0H0Qvcn08mNBKGO05SQ+vuxeKLf90wpBbb07wBLUSBH14NtlBBfYXRfya/x7oZ1q+ASttT8f1w==", "Access Key")

# COMMAND ----------

# MAGIC %md
# MAGIC Get and store the widgets into variables

# COMMAND ----------

storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")
mount_point = dbutils.widgets.get("mount_point")
access_key = dbutils.widgets.get("access_key")

# COMMAND ----------

# MAGIC %md
# MAGIC Call the funtion and pass the variable

# COMMAND ----------

mount_blob_storage(storage_account_name, container_name, mount_point, access_key)

# COMMAND ----------

from pyspark.sql.functions import udf
def toSnakeCase(df):
    for column in df.columns:
        snake_case_col = ''
        for char in column:
            if char ==' ':
                snake_case_col += '_'
            else:
                snake_case_col += char.lower()
        df = df.withColumnRenamed(column, snake_case_col)
    return df

udf(toSnakeCase)

# COMMAND ----------

def write_delta_upsert(df, delta_path):
    df.write.format("delta").mode("overwrite").save(delta_path)

# COMMAND ----------

def read_delta_file(delta_path):
    df = spark.read.format("delta").load(delta_path)
    return df
udf(read_delta_file)