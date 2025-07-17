# Databricks notebook source
# MAGIC %md
# MAGIC # Filtrage
# MAGIC Le filtrage est une opération de transformation disponible dans Spark qui renvoie un nouveau RDD composé uniquement des éléments qui répondent à une condition spécifique. Il prend une fonction qui évalue une valeur booléenne et applique cette fonction à tous les éléments du RDD pour renvoyer uniquement les éléments pour lesquels la fonction produit true.

# COMMAND ----------

from pyspark import SparkContext, SparkConf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtre simple

# COMMAND ----------

conf = SparkConf().setMaster("*").setAppName("Filtrage notebook")
sc = SparkContext.getOrCreate()
numbersRDD = sc.parallelize((1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
evenNumbersRDD = numbersRDD.filter(lambda n: n % 2 == 0)
evenNumbers = evenNumbersRDD.collect()
for number in evenNumbers:
    print(number)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtre complexe
# MAGIC Le filtrage peut également être appliqué à des conditions plus complexes. Supposons que vous ayez affaire à un ensemble de données de personnes et que vous souhaitiez filtrer toutes les personnes de moins de 18 ans et portant un nom spécifique. Tout d’abord, vous devez créer une classe de cas qui représente une personne.

# COMMAND ----------

from pyspark.sql import Row

# Creation d'une liste
peopleList = [
    Row(name="Alice", age=29), Row(name="Bob", age=20), 
    Row(name="Charlie", age=17), Row(name="David", age=15), 
    Row(name="Eve", age=38), Row(name="Frank", age=13)
]

# Parallelize pour creer le RDD
peopleRDD = sc.parallelize(peopleList)

# Filtrage
adultsNamedAliceRDD = peopleRDD.filter(lambda person: person.age >= 18 and person.name == "Alice")

# collect du RDD et affichage 
display(adultsNamedAliceRDD.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtre enchaîner
# MAGIC Les filtres peuvent être chaînés pour appliquer une séquence de conditions. Chaque appel de filtre entraînera un nouveau RDD, et les conditions pourront être appliquées les unes après les autres.****

# COMMAND ----------

#adultsRDD = peopleRDD.filter(lambda age : age >= 18)
#adultsNamedAliceRDD = adultsRDD.filter(lambda name : name =="Alice")
adultsNamedAliceRDD = peopleRDD.filter(lambda person : person.age >= 18).filter(lambda person : person.name =="Alice")
display(adultsNamedAliceRDD.collect())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrage à l'aide de fonctions externes
# MAGIC Les conditions de filtre peuvent être extraites vers des fonctions externes pour une meilleure lisibilité, en particulier lorsqu'il s'agit de conditions complexes ou lorsque la même logique est réutilisée dans différentes parties de l'application.

# COMMAND ----------


def isAdult(person): 
    return person.age >= 18
def isNamedAlice(person): 
    return person.name == "Alice"

adultsRDD = peopleRDD.filter(isAdult)
adultsNamedAliceRDD = adultsRDD.filter(isNamedAlice)
display(adultsNamedAliceRDD.collect())
