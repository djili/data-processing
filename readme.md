# Projet de Traitement de Données avec Apache Spark

Ce projet contient plusieurs notebooks et scripts pour le traitement de données avec Apache Spark. Voici un aperçu des différentes parties du projet :

## Notebooks Principaux

### 1. Machine Learning
- **DiabeteDetection.ipynb** : Détection du diabète en utilisant l'apprentissage automatique avec Spark ML.

### 2. Analyse de Données
- **movies.ipynb** : Analyse des données de films, y compris le filtrage par genres et l'analyse des notes.
- **log.ipynb** : Traitement et analyse de fichiers journaux (logs).

### 3. Traitement de Texte
- **wordcount.ipynb** : Comptage de mots simple à partir d'un texte.


### 3. Données en Temps Réel
- **WeatherStreaming.ipynb** : Analyse des données météorologiques en temps réel.
- **StreamingWordCount.ipynb** : Comptage de mots en temps réel avec Spark Streaming.

### 4. Connexion aux Bases de Données et transformation des données
- **ConnectDatabase.ipynb** : Exemples de connexion à différentes bases de données avec Spark.
- **Join.ipynb** : Exemples d'opérations de jointure entre différents jeux de données.

## Structure des Données

Le dossier `data/` contient les fichiers de données utilisés dans les notebooks :
- `diabetes.csv` : Données sur le diabète
- `discours.txt` : Fichier texte pour l'analyse de texte
- `log.txt` : Fichier de logs pour l'analyse
- `movies_metadata.csv` : Métadonnées des films

## Configuration Requise

- Apache Spark
- Python 3.9
- Bibliothèques Python : pyspark

## Installation

1. Installer Apache Spark depuis [le site officiel](https://spark.apache.org/downloads.html)

## Utilisation

Chaque notebook peut être exécuté indépendamment. Assurez-vous d'avoir démarré une session Spark avant d'exécuter les notebooks.

## Auteur

Made with ❤️ by Abdoiu Khadre DIOP