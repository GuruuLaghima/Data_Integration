
---

# **Population Survey Project**
Ce projet utilise Kafka et Spark pour traiter, enrichir et analyser les données de population. Les résultats finaux sont stockés dans MongoDB.

---

## **Prérequis**
Avant de commencer, assurez-vous d'avoir installé les outils suivants :
- Docker 
- Apache Spark
- HDFS
- MongoDB
- Python avec les librairies nécessaires (`pyspark`, `pymongo`)

---

## **Étapes du projet**

### **1. Lancer HDFS**

#### **Démarrer HDFS**
1. Assurez-vous que Hadoop est installé sur votre machine.
2. Lancez le Namenode et le Datanode :
   ```bash
   start-dfs.sh
   ```

---

### **2. Démarrage des services avec Docker Compose**

#### **Démarrer Kafka et Zookeeper**
Dans le répertoire contenant le fichier `docker-compose.yml`, exécutez :
```bash
docker-compose up -d
```

Cela démarre :
- **Zookeeper** pour la gestion de Kafka
- **Kafka** comme système de messagerie

---

### **3. Préparer les données**

#### **Créer les répertoires dans HDFS**
```bash
hdfs dfs -mkdir -p /data/raw
hdfs dfs -mkdir -p /data/processed/enriched_data
```

#### **Uploader les fichiers CSV dans HDFS**
```bash
hdfs dfs -put data/raw/health-insurance.csv /data/raw/
hdfs dfs -put data/raw/household-income.csv /data/raw/
hdfs dfs -put data/raw/self-employment-income.csv /data/raw/
hdfs dfs -put data/raw/processed_population_data.csv /data/raw/
```

---

### **4. Publier les données avec Kafka**

#### **Exécuter le producteur Kafka**
Depuis votre environnement local :
```bash
python scripts/producer.py
```

---

### **5. Traitement des données avec Spark**

#### **Exécuter l'intégration Kafka-Spark**
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 scripts/kafka_spark_integration.py
```

#### **Vérifier les données traitées dans HDFS**
Lister les fichiers :
```bash
hdfs dfs -ls /data/processed/enriched_data
```

Afficher un aperçu :
```bash
hdfs dfs -cat /data/processed/enriched_data/part-*.csv | head -n 10
```

---

### **6. Calculer les métriques**

#### **Exécuter le script de calcul des métriques**
```bash
python scripts/metrics.py
```

---

### **7. Stocker les résultats dans MongoDB**

#### **Exécuter le script d'insertion dans MongoDB**
```bash
python scripts/mongodb_insert.py
```

#### **Vérifier les données dans MongoDB**
1. Connectez-vous à MongoDB :
   ```bash
   mongosh
   ```
2. Utilisez la base de données :
   ```javascript
   use population_data;
   ```
3. Lister les documents insérés :
   ```javascript
   db.enriched_metrics.find().pretty();
   ```
---
