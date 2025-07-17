# Databricks notebook source
# MAGIC %md
# MAGIC # Structure de données 
# MAGIC nous allons créer les structure RDD et Dataframe

# COMMAND ----------

# Importation 
from pyspark import SparkConf, SparkContext

# COMMAND ----------

# conf
conf = SparkConf().setMaster("local").setAppName("Datastructure")
sc = SparkContext.getOrCreate(conf = conf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation d'un RDD

# COMMAND ----------

liste = (1,2,5,6)
rdd = sc.parallelize(liste)
display(rdd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creation d'un dataframe

# COMMAND ----------

# Création d'un dataframe
data = [("John Doe", "30"), ("Jane Doe", "25")]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, schema=columns)
display(df)

# COMMAND ----------

# Création d'un dataframe avec les colonnes directement
df = spark.createDataFrame([("John Doe", 30), ("Jane Doe", 25)], ["Name", "Age"])
display(df)

# COMMAND ----------

csdfvDF = spark.read.format("csv").option("header", "true").load("file:/Workspace/Users/diopous1@gmail.com/Exercices/data /customers-1000.csv")
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creation d'un Dataset

# COMMAND ----------

# Uniquement disponible en java et scala 

# COMMAND ----------

# MAGIC %md
# MAGIC # API Pandas

# COMMAND ----------

import pyspark.pandas as pd

pandasDataframe = pd.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])
pandasDataframe.describe()
type(pandasDataframe)