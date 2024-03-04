# Databricks notebook source
# MAGIC %md 
# MAGIC ## Przetwarzanie danych z zewnętrzengo Data Lake
# MAGIC ### Aby przetwarzać dane z innego storage account musimy go powiązac z naszym worksapce - robimy to za pomocą mount'a 

# COMMAND ----------

storageAccount="mdsndcbbdatalake"
storageKey ="zIUiYAIz8w1lrT2e+OGnYQyVeCyJgGioCR7xf+BiB9qWsbwv0wrJXIWEd7Ax/pIJ0836wKreFRjb+ASt+1poBg=="
mountpoint = "/mnt/Demo/bronze"
storageEndpoint =   "wasbs://bronze@{}.blob.core.windows.net".format(storageAccount)
storageConnSting = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)

try:
  dbutils.fs.mount(
    source = storageEndpoint,
    mount_point = mountpoint,
    extra_configs = {storageConnSting:storageKey})
except:
  print("Already mounted")


# COMMAND ----------

# MAGIC %md ### Przetwaranie danych w DataFrame za pomocą PySpark

# COMMAND ----------

df_so = spark.read.load("dbfs:/mnt/Demo/bronze/raw/SalesOrderDetail",  
    format='csv',
    header=True,
    sep = ";")

display(df_so)

# COMMAND ----------

# MAGIC %md Transformacja danych tabeli SalesOrders

# COMMAND ----------

df_so = df_so.select("SalesOrderDetailID","SalesOrderID","ProductID","OrderQty","UnitPrice")

display(df_so)

# COMMAND ----------

from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.sql.functions import col,cast

df_so = df_so.select(
    col("SalesOrderDetailID").cast("int").alias("SalesOrderDetailID"),
    col("SalesOrderID").cast('int').alias("SalesOrderID"),
    col("ProductID").cast('int').alias("ProductID"),
    col("UnitPrice").cast('decimal(8,2)').alias("UnitPrice"),
    col("OrderQty").cast('int').alias("OrderQty"),
    )

display(df_so)

# COMMAND ----------

df_so.dtypes

# COMMAND ----------

df_so.count()

# COMMAND ----------

df_so = df_so.where(df_so.OrderQty > 1)

display(df_so)

# COMMAND ----------

df_so.count()

# COMMAND ----------

# MAGIC %md Product table - transformacje

# COMMAND ----------

df_p = spark.read.load("dbfs:/mnt/Demo/bronze/raw/Product.csv",  
    format='csv',
    header=True,
    sep = ";")

display(df_p)

# COMMAND ----------

df_p = df_p.select("ProductID","Name","ProductNumber")
display(df_p)

# COMMAND ----------

# MAGIC %md ###Przetwarzanie danych w DataFrame za pomoca SQL

# COMMAND ----------

# MAGIC %md SalesOrders

# COMMAND ----------

df_so_sql = spark.read.load("dbfs:/mnt/Demo/bronze/raw/SalesOrderDetail",  
    format='csv',
    header=True,
    sep = ";")
display(df_so_sql)

# COMMAND ----------

# MAGIC %md Utworzenie widoku tymczasowego

# COMMAND ----------


df_so_sql.createOrReplaceTempView("SalesOrderDetail")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from SalesOrderDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC CAST(SalesOrderID as int) as SalesOrderID,
# MAGIC CAST(SalesOrderDetailID as int) as SalesOrderDetailID,
# MAGIC CAST(ProductId as int) as SalesOrderDetailID,
# MAGIC CAST(UnitPrice as decimal(8,2)) as UnitPrice,
# MAGIC CAST(OrderQty as int) as OrderQty
# MAGIC FROM SalesOrderDetail
# MAGIC where OrderQty > 1

# COMMAND ----------

# MAGIC %md Product

# COMMAND ----------

df_p_sql = spark.read.load("dbfs:/mnt/Demo/bronze/raw/Product.csv",  
    format='csv',
    header=True,
    sep = ";")

display(df_p_sql)

# COMMAND ----------

# MAGIC %md Utworzenie widoku tymaczasowego

# COMMAND ----------

df_p_sql.createOrReplaceTempView("Product")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ProductId,Name,ProductNumber from Product

# COMMAND ----------

# MAGIC %md ### Utowrzenie plików parquet na data lake

# COMMAND ----------

storageAccount="mdsndcbbdatalake"
storageKey ="zIUiYAIz8w1lrT2e+OGnYQyVeCyJgGioCR7xf+BiB9qWsbwv0wrJXIWEd7Ax/pIJ0836wKreFRjb+ASt+1poBg=="
mountpoint = "/mnt/Demo/silver"
storageEndpoint =   "wasbs://silver@{}.blob.core.windows.net".format(storageAccount)
storageConnSting = "fs.azure.account.key.{}.blob.core.windows.net".format(storageAccount)


try:
    dbutils.fs.mount(
    source = storageEndpoint,
    mount_point = mountpoint,
    extra_configs = {storageConnSting:storageKey})
except:
    print("Already mounted")
    


# COMMAND ----------

df_p.write.mode("overwrite").parquet("dbfs:/mnt/Demo/silver/parquet/Product")
df_so.write.mode("overwrite").parquet("dbfs:/mnt/Demo/silver/parquet/SalesOrders")

# COMMAND ----------

df_pso = spark.read.parquet("dbfs:/mnt/Demo/silver/parquet/SalesOrders")

display(df_pso)

# COMMAND ----------

df_pp = spark.read.parquet("dbfs:/mnt/Demo/silver/parquet/Product")

display(df_pp)