# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Komendy wywołujace dany język programowania
# MAGIC
# MAGIC * **&percnt;python**
# MAGIC * **&percnt;scala**
# MAGIC * **&percnt;sql**
# MAGIC * **&percnt;r**

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC println("Pisze w scala")

# COMMAND ----------


print ("Pisze w python")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select 'Pisze w SQL'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dodanie plików do DBFS
# MAGIC ### Dodanie plików csv i json do wbudowanego storage
# MAGIC
# MAGIC

# COMMAND ----------

# Wrzucic pliki do DBFS

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Otworzenie plików csv oraz json za pomocą pyspark, spark i sql

# COMMAND ----------


df = spark.read.load('dbfs:/FileStore/raw/Product.csv',
    format='csv',
    header=True,
   # sep = ";"
)
display(df.limit(10))

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.format("csv").option("header", "true").option("sep", ";").load("dbfs:/FileStore/raw/Product.csv")
# MAGIC display(df.limit(10))

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from products

# COMMAND ----------



SELECT * FROM parquet.`examples/src/main/resources/users.parquet`

# COMMAND ----------

file_path_part1 = "dbfs:/FileStore/raw/salesOrders/SalesOrderDetail_Part1.csv"
file_path = "dbfs:/FileStore/raw/salesOrders/*"

# COMMAND ----------

# MAGIC %md Sprawdzenie ilości wierzszy w DataFrame

# COMMAND ----------


dfso1 = spark.read.load(file_path_part1,
    format='csv',
    header=True,
    sep = ";"
)
dfso1.count()

# COMMAND ----------


dfso = spark.read.load(file_path,
    format='csv',
    header=True,
    sep = ";"
)
dfso.count()

# COMMAND ----------

# MAGIC %md  Sprawdzenie schematu DataFrame

# COMMAND ----------

dfso.schema

# COMMAND ----------

# MAGIC %md Sprawdzenie czy DataFrame jest pusty

# COMMAND ----------

dfso.isEmpty()