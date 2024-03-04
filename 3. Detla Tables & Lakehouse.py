# Databricks notebook source
df_pp = spark.read.parquet("dbfs:/mnt/Demo/silver/parquet/Product")
df_so = spark.read.parquet("dbfs:/mnt/Demo/silver/parquet/SalesOrders")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Delta Tables
# MAGIC ### Tabele utworzone z określoną LOKALIZACJĄ są uważane za niezarządzane przez magazyn metadanych. W przeciwieństwie do tabeli zarządzanej, w której nie określono ścieżki, pliki tabeli niezarządzanej nie są usuwane po USUNIĘCIU tabeli. Jednakże zmiany w zarejestrowanej tabeli lub plikach zostaną odzwierciedlone w obu lokalizacjach.
# MAGIC
# MAGIC Najlepsze praktyki Tabele zarządzane wymagają, aby dane tabeli były przechowywane w systemie DBFS. Tabele niezarządzane przechowują metadane tylko w systemie DBFS.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE ubb_dc6_demo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ubb_dc6_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS salesOrders;
# MAGIC
# MAGIC CREATE TABLE salesOrders
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/mnt/Demo/silver/delta/SalesOrders';
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from ubb_dc6_demo.salesOrders

# COMMAND ----------

df_del_pp = spark.read.load("dbfs:/mnt/Demo/silver/delta/Product") 
df_del_pp.write.saveAsTable("product")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM salesorders s
# MAGIC JOIN product p ON s.ProductID = p.ProductID

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT SalesOrderID,Name as ProductName,SUM((UnitPrice * OrderQty)) as salesPrice FROM salesorders s
# MAGIC JOIN product p ON s.ProductID = p.ProductID
# MAGIC group by SalesOrderID,ProductName

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW salesPrice
# MAGIC AS
# MAGIC SELECT SalesOrderID,Name as ProductName,SUM((UnitPrice * OrderQty)) as salesPrice FROM salesorders s
# MAGIC JOIN product p ON s.ProductID = p.ProductID
# MAGIC group by SalesOrderID,ProductName

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS localSalesPrice

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS localSalesPrice
# MAGIC AS
# MAGIC select * from salesPrice

# COMMAND ----------

# MAGIC %md Zapisanie zagregowanej tabeli do warstwy gold

# COMMAND ----------

storageAccount="mdsndcbbdatalake"
storageKey ="zIUiYAIz8w1lrT2e+OGnYQyVeCyJgGioCR7xf+BiB9qWsbwv0wrJXIWEd7Ax/pIJ0836wKreFRjb+ASt+1poBg=="
mountpoint = "/mnt/Demo/gold"
storageEndpoint =   "wasbs://gold@{}.blob.core.windows.net".format(storageAccount)
storageConnSting = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)

try:
  dbutils.fs.mount(
    source = storageEndpoint,
    mount_point = mountpoint,
    extra_configs = {storageConnSting:storageKey})
except:
  print("Already mounted")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS aggSalesPrice

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE aggSalesPrice
# MAGIC USING delta
# MAGIC LOCATION 'dbfs:/mnt/Demo/gold/delta/salesPrice'
# MAGIC AS SELECT *
# MAGIC FROM salesPrice

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aggSalesPrice