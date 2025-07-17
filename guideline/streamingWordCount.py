# Databricks notebook source
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Créer un StreamingContext avec un intervalle de batch de 1 seconde
ssc = StreamingContext(sc, 1)

# COMMAND ----------

# Connexion au socket TCP sur le port 9999
# nc -lk 9999
lines = ssc.socketTextStream("localhost", 9999)

# COMMAND ----------

# Diviser chaque ligne en mots
words = lines.flatMap(lambda line: line.split(" "))

# COMMAND ----------

# Compter chaque mot dans chaque batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# COMMAND ----------

# Imprimer les résultats
wordCounts.pprint()

# COMMAND ----------

# Commencer le calcul
ssc.start()

# COMMAND ----------

# Attendre que le calcul soit terminé
ssc.awaitTermination()
