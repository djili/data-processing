# Databricks notebook source
# MAGIC %md
# MAGIC Les actions RDD sont des opérations qui déclenchent l'exécution du traitement des données décrit par les transformations RDD et renvoient les résultats au programme pilote Spark. Ce processus est souvent appelé un travail. Contrairement aux transformations qui définissent les opérations à effectuer, les actions collectent ou génèrent les résultats des calculs RDD. Ci-dessous, nous examinerons différents types d'actions fréquemment utilisées dans les applications Spark.

# COMMAND ----------

from pyspark import SparkConf, SparkContext

# COMMAND ----------

# conf
conf = SparkConf().setMaster("local").setAppName("Action Notebook")
sc = SparkContext.getOrCreate(conf = conf)

# COMMAND ----------

numbersRDD = sc.parallelize((1, 2, 3, 4, 5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## collect
# MAGIC collect() est l'une des actions les plus courantes, qui récupère l'intégralité des données RDD vers le programme pilote. Puisqu’il récupère toutes les données, il doit être utilisé avec prudence, en particulier avec de grands ensembles de données.

# COMMAND ----------


collectedNumbers = numbersRDD.collect()
print(", ".join(map(str, collectedNumbers)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## count
# MAGIC count() renvoie le nombre d'éléments dans le RDD.

# COMMAND ----------

totalCount = numbersRDD.count()
print(totalCount)


# COMMAND ----------

# MAGIC %md
# MAGIC ## take
# MAGIC take(n) renvoie les n premiers éléments du RDD.

# COMMAND ----------

firstThree = numbersRDD.take(3)
print(", ".join(map(str, firstThree)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## first
# MAGIC first() est similaire à take(1)mais elle est plus lisible et renvoie le premier élément du RDD.

# COMMAND ----------

firstElement = numbersRDD.first()
print(firstElement)


# COMMAND ----------

# MAGIC %md
# MAGIC ## reduce
# MAGIC reduce() effectue une opération binaire associative et commutative spécifiée sur les éléments RDD et renvoie le résultat.

# COMMAND ----------

sum = numbersRDD.reduce(lambda a,b : a+b)
print(sum)


# COMMAND ----------

# MAGIC %md
# MAGIC ## fold
# MAGIC fold() est similaire à reduce(), mais prend une « valeur zéro » supplémentaire à utiliser pour l'appel initial sur chaque partition. La « valeur zéro » doit être l'élément d'identité de l'opération.
# MAGIC
# MAGIC

# COMMAND ----------


foldedResult = numbersRDD.fold(0, lambda a, b: a + b)
print(foldedResult)

# COMMAND ----------

# MAGIC %md
# MAGIC ## aggregate
# MAGIC aggregate() est encore plus générale que fold()et reduce(). Il faut une « valeur zéro » initiale, une fonction pour combiner les éléments du RDD avec l'accumulateur, et une autre fonction pour fusionner deux accumulateurs.

# COMMAND ----------

aggregateResult = numbersRDD.aggregate(
    (0, 0),  # zeroValue
    lambda acc, value: (acc[0] + value, acc[1] + 1),  # seqOp
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # combOp
)
print(aggregateResult)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mise en cache et persistance
# MAGIC Lorsque des actions RDD sont appelées, Spark calcule le RDD et sa lignée. Si un RDD est utilisé plusieurs fois, il est efficace de le conserver en mémoire en appelant la méthode persist()ou cache(). De cette façon, Spark peut réutiliser le RDD pour des actions ultérieures sans recalculer l'intégralité du lignage.

# COMMAND ----------

numbersRDD.cache() # Persist the RDD in memory
sumCached = numbersRDD.reduce(lambda a, b: a + b) # Uses the cached data
print(sumCached)