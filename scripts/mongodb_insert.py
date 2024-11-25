from pyspark.sql import SparkSession
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .getOrCreate()


hdfs_path = "hdfs://localhost:9000/data/processed/metrics_summary/part-*.csv"

metrics_df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Conversion du DataFrame Spark en DataFrame Pandas pour insertion dans MongoDB
metrics_data = metrics_df.toPandas()

MONGO_URI = "mongodb://localhost:27017"
DATABASE = "population_data"
COLLECTION = "metrics_summary"

client = MongoClient(MONGO_URI)
db = client[DATABASE]
collection = db[COLLECTION]

# Conversion du DataFrame Pandas en une liste de dictionnaires pour l'insertion
metrics_data_dict = metrics_data.to_dict("records")
collection.insert_many(metrics_data_dict)

print(f"--> {len(metrics_data_dict)} -->'{COLLECTION}'.")

client.close()

spark.stop()
