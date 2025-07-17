# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# Lire un fichier CSV
csvDF = spark.read.format("csv").option("header", "true").load("file:/Workspace/Users/diopous1@gmail.com/Exercices/data /customers-1000.csv")
csvDF.show(5)


# COMMAND ----------

# Lire un fichier Parquet
parquetDF = spark.read.format("parquet").load("file:/Workspace/Users/diopous1@gmail.com/Exercices/data /MT cars.parquet")
parquetDF.show(5)
