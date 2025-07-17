# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# Créer une SparkSession
spark = SparkSession.builder \
    .appName("DiabetesClassification") \
    .getOrCreate()

# COMMAND ----------

# Charger les données à partir d'un fichier CSV
data = spark.read.csv("file:/Workspace/Users/diopous1@gmail.com/Exercices/data /diabetes.csv", header=True, inferSchema=True)
data.printSchema()
data.show(5)

# COMMAND ----------

# Assembler les colonnes de caractéristiques
feature_columns = [col for col in data.columns if col != 'Diabetic']
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_prepared = assembler.transform(data)


# COMMAND ----------

# Diviser les données en ensembles d'entraînement et de test
train_data, test_data = data_prepared.randomSplit([0.8, 0.2])

# COMMAND ----------

# Entraîner un modèle de régression logistique
lr = LogisticRegression(featuresCol="features", labelCol="Diabetic")
lr_model = lr.fit(train_data)

# COMMAND ----------

# Faire des prédictions sur les données de test
predictions = lr_model.transform(test_data)
predictions.select("features", "Diabetic", "prediction").show(5)

# COMMAND ----------

# Correct the label column name to "Diabetic"
evaluator = BinaryClassificationEvaluator(labelCol="Diabetic", rawPredictionCol="prediction", metricName="areaUnderROC")
areaUnderROC = evaluator.evaluate(predictions)
print(f"ROC: {areaUnderROC}")
