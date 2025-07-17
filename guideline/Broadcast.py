# Databricks notebook source
from pyspark.sql import SparkSession

# Initialisation
spark = SparkSession.builder \
    .appName("BroadcastVariableExample") \
    .master("local[*]") \
    .getOrCreate()

# Créer une variable de diffusion avec un dictionnaire d'ID et de noms
dataMap = {1: "Alice", 2: "Bob", 3: "Charlie", 4: "David"}
broadcastData = spark.sparkContext.broadcast(dataMap)

# Fonction utilisant la variable de diffusion pour récupérer des noms en fonction des identifiants
def getNameById(id):
    map = broadcastData.value
    return map.get(id, "Unknown")

# Exemple de RDD d'identifiants pour récupérer des noms à l'aide de la variable de diffusion
idRDD = spark.sparkContext.parallelize([1, 3, 2, 5, 4])
resultRDD = idRDD.map(lambda id: (id, getNameById(id)))

# Collecte tu résultat 
resultList = resultRDD.collect()

# Affichage des differents elements
for item in resultList:
    print(item)