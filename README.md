# ADF ASSIGNMENT

### 1.Create a container in ADLS with the project name as sales_view_devtst
* Create a folder for customer, product, store, and sales - Upload the files in sequence to make it work in real-time.

### 2.Create an ADF Pipeline to get the latest modified files the folders of the ADLS 
* Parameterize the Pipeline to work dynamically.
* Values need to be passed through parameters and it should work for all day's file

### 3.Create a Bronze/sales_view/  (Bronze – container)
* Subfolders->  customer, product, store, sales, and store the raw data copied from the ADF pipeline

### 4.Customer file
* All the column header should be in snake case in lower case (By using UDF function dynamically works for all camel case to snake case)
* By using the "Name" column split by " " and create two columns first_name and last_name
* Create column domain and extract from email columns Ex: Email = "josephrice131@slingacademy.com" domain="slingacademy"
* Create a column gender where male = "M" and Female="F"
* From Joining date create two colums date and time by splitting based on " " delimiter.
* Date column should be on "yyyy-MM-dd" format.
* Create a column expenditure-status, based on spent column is spent below 200 column value is "MINIMUM" else "MAXIMUM"
* Write based on upsert [table_name: customer] (in silver layer path is silver/sales_view/tablename/{delta pearquet}

### 5.Product File
* All the column header shlould be in snake case in lower case (use same UDF Function)
* Create a column sub_category (Use Category columns id category_id=1, "phone"; 2, "laptop"; 3,"playstation"; 4,"e-device"
* Write based on upsert [table_name: product](in silver layer path is silver/sales_view/tablename/{delta pearquet}

### 6.store
* Read the data make sure header shlould be in snake case in lower case (use same UDF Function)
* Create a store category columns and the value is exatracted from email Eg: "electromart" from johndoe@electromart.com
* created_at, updated_at date as yyyy-MM-dd format
* Write based on upsert [table_name: store] (in silver layer path is silver/sales_view/tablename/{delta pearquet}

### 7.Sales
* Read the data make sure header shlould be in snake case in lower case (use same UDF Function)
* Write based on upsert [table_name: customer_sales] (in silver layer path is silver/sales_view/tablename/{delta pearquet}
* Note: all date needs to be manintained in yyyy-MM-dd format only

### 8.gold layer
* using product and store table get the below data
* store_id,store_name,location,manager_name,product_name,product_code,description,category_id,price,stock_quantity,supplier_id,product_created_at,product_updated_at,image_url,weight,expiry_date,is_active,tax_rate.
* Read the delta table (using UDF functions)
* Using the above data & customer_sales and get the below data
* OrderDate,Category,City,CustomerID,OrderID,Product ID,Profit,Region,Sales,Segment,ShipDate,ShipMode,latitude,longitude,store_name,location,manager_name,product_name,price,stock_quantity,image_url 
* Write based on overwrite (table_name : StoreProductSalesAnalysis )(in gold layer path is gold/sales_view/tablename/{delta pearquet}