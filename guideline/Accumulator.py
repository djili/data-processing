# Databricks notebook source
# MAGIC %md
# MAGIC # Accumulateur Simple

# COMMAND ----------

from pyspark.sql import SparkSession

# Spark Context et données
sc = spark.sparkContext
data = [1, 2, 3, 4, 5, 6, 8]

# creation d'un accumulateur
accumulator = sc.accumulator(0) 

# Creation d'un RDD
numbers = sc.parallelize(data)

# Fonction d'incrementation de l'accumulateur 
# il verifie le nombre paire 
def increment_acc(num):
    global accumulator
    if num % 2 == 0:
        accumulator += 1

numbers.foreach(increment_acc)

print("------------ Output --------------")
#affichage de la valeur de l'accumulateur 
print("Nombre d'occurence des nombres paire dans le RDD: " + str(accumulator.value))

# COMMAND ----------

# MAGIC %md
# MAGIC # Accumulateur spécialiser 

# COMMAND ----------

# MAGIC %md
# MAGIC Nous allons créer un accumulateur personnalisé qui regroupe les chaînes et renvoie une chaîne concaténée de toutes les valeurs accumulées :

# COMMAND ----------

from pyspark.accumulators import AccumulatorParam

class StringConcatenationAccumulator(AccumulatorParam):
    def zero(self, initialValue=""):
        return ""
    
    def addInPlace(self, v1, v2):
        return v1 + v2

customAccumulator = sc.accumulator("", StringConcatenationAccumulator())
data = ["Hello, ", "this ", "is ", "a ", "custom ", "accumulator."]
strings = sc.parallelize(data)

strings.foreach(lambda x: customAccumulator.add(x))
print("------------ Output --------------")
print("Concatenated string: " + customAccumulator.value)