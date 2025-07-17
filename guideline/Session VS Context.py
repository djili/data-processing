# Databricks notebook source
# MAGIC %md
# MAGIC # SparkContext

# COMMAND ----------

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("My Spark App").setMaster("local")
sc = SparkContext(conf=conf).getOrCreate()
print(sc)
print("SparkContext created successfully!")
sc.stop()

# COMMAND ----------

from pyspark.sql import SparkSession

session = SparkSession.builder \
  .appName("My Spark Application") \
  .config("spark.master", "local") \
  .getOrCreate()
print("SparkSession created successfully!")
session.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # SparkSession