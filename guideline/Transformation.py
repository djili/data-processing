# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation de base
# MAGIC Les transformations sont au cœur de la capacité de traitement des données de Spark. Lorsque vous appliquez une transformation à un RDD, vous créez un nouveau RDD. Les transformations sont paresseuses, ce qui signifie qu’elles ne calculent pas leurs résultats immédiatement. Au lieu de cela, ils se souviennent simplement des transformations appliquées à un ensemble de données de base (par exemple, un fichier). Les transformations ne sont calculées que lorsqu'une action nécessite qu'un résultat soit renvoyé au programme pilote.

# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Transformation Notebook")
sc = SparkContext.getOrCreate(conf = conf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## map
# MAGIC La transformation map applique une fonction à chaque élément du RDD et renvoie un nouveau RDD représentant les résultats

# COMMAND ----------

data = sc.parallelize((1, 2, 3, 4))
squaredData = data.map(lambda x: x * x)
squaredData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## filter
# MAGIC La filter permet de sélectionner des éléments du RDD qui répondent à certains critères.

# COMMAND ----------

data = sc.parallelize((1, 2, 3, 4))
evenData = data.filter(lambda x : x % 2 == 0)
evenData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## flatMap
# MAGIC Tout en map une fonction à chaque élément individuellement, flatMap peut produire plusieurs éléments de sortie pour chaque élément d'entrée

# COMMAND ----------

wordsList = sc.parallelize(("hello world", "how are you"))
words = wordsList.flatMap(lambda x : x.split(" "))
words.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## union
# MAGIC La union fusionne deux RDD
# MAGIC

# COMMAND ----------

rdd1 = sc.parallelize((1, 2, 3))
rdd2 = sc.parallelize((4, 5, 6))
commonRDD = rdd1.union(rdd2)
commonRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## intersection
# MAGIC La intersection renvoie un nouveau RDD qui contient uniquement les éléments trouvés dans les deux RDD sources.

# COMMAND ----------

rdd1 = sc.parallelize((1, 2, 3))
rdd2 = sc.parallelize((3, 4, 5))
commonRDD = rdd1.intersection(rdd2)
commonRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## distinct
# MAGIC distinct supprime les doublons dans un RDD

# COMMAND ----------

rdd = sc.parallelize((1, 2, 3, 2, 3))
distinctRDD = rdd.distinct()
distinctRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## substract
# MAGIC subtract renvoie un RDD avec des éléments d'un RDD qui ne sont pas trouvés dans un autre. 

# COMMAND ----------

rdd1 = sc.parallelize((1, 2, 3, 4))
rdd2 = sc.parallelize((3, 4, 5, 6))
resultRDD = rdd1.subtract(rdd2)
resultRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## cartesian
# MAGIC cartesian renvoie toutes les paires possibles de (a, b) où a est dans un RDD et b est dans l'autre.

# COMMAND ----------

rdd1 = sc.parallelize((1, 2))
rdd2 = sc.parallelize(("a", "b"))
cartesianRDD = rdd1.cartesian(rdd2)
cartesianRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations cle-valeur
# MAGIC Lorsque vous travaillez avec des RDD de paires clé-valeur, il existe des transformations supplémentaires qui permettent des agrégations complexes et d'autres opérations sur des paires en fonction de leur clé.

# COMMAND ----------

# MAGIC %md
# MAGIC ## reduceByKey
# MAGIC reduceByKey agrège les valeurs de chaque clé, à l'aide d'une fonction de réduction associative. 

# COMMAND ----------

data = sc.parallelize([("apple", 2), ("orange", 3), ("apple", 1)])
reducedData = data.reduceByKey(lambda a, b: a+b)
reducedData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupByKey
# MAGIC groupByKey regroupe les valeurs de chaque clé du RDD en une seule séquence. Notez qu’il reduceByKeys’agit souvent d’un meilleur choix car il présente de meilleures caractéristiques de performance. 

# COMMAND ----------

data = sc.parallelize([("apple", 2), ("orange", 3), ("apple", 1)])
groupedData = data.groupByKey()
groupedData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## sortByKey
# MAGIC sortByKey trie le RDD par clé.

# COMMAND ----------

data = sc.parallelize([("apple", 2), ("orange", 3), ("banana", 1)])
sortedData = data.sortByKey()
sortedData.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## join
# MAGIC join rejoint deux RDD par clé

# COMMAND ----------

rdd1 = sc.parallelize([("apple", 5), ("orange", 3)])
rdd2 = sc.parallelize([("apple", 2), ("orange", 4)])
joinedRDD = rdd1.join(rdd2)
joinedRDD.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## cogroup
# MAGIC cogroup regroupe les valeurs de chaque clé dans deux RDD. Cela peut être utile pour les jointures complexes sur plusieurs RDD

# COMMAND ----------

rdd1 = sc.parallelize([("apple", 2), ("orange", 3)])
rdd2 = sc.parallelize([("apple", 4), ("orange", 5)])
cogroupedRDD = rdd1.cogroup(rdd2)
cogroupedRDD.collect()