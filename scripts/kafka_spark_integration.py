from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.sql.streaming.checkpointLocation", "/data/processed/checkpoint") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

# Définition schéma pour messages Kafka 
schema = StructType([
    StructField("Neighborhood", StringType(), True),
    StructField("Id", StringType(), True),
    StructField("Estimate; Total", DoubleType(), True),
    StructField("Margin of Error; Total", DoubleType(), True)
])


income_df = spark.read.csv("hdfs://localhost:9000/data/raw/household-income.csv", header=True, inferSchema=True) \
    .select(
        col("Neighborhood"),
        col("Id").cast("string"),
        col("Estimate; Aggregate household income in the past 12 months (in 2015 Inflation-adjusted dollars)").alias("Income_Estimate"),
        col("Margin of Error; Aggregate household income in the past 12 months (in 2015 Inflation-adjusted dollars)").alias("Income_Margin")
    )

health_df = spark.read.csv("hdfs://localhost:9000/data/raw/health-insurance.csv", header=True, inferSchema=True) \
    .select(
        col("Neighborhood"),
        col("Id").cast("string"),
        col("Estimate; Total:").alias("Health_Total_Estimate"),
        col("Margin of Error; Total:").alias("Health_Margin")
    )

employment_df = spark.read.csv("hdfs://localhost:9000/data/raw/self-employment-income.csv", header=True, inferSchema=True) \
    .select(
        col("Neighborhood"),
        col("Id").cast("string"),
        col("Estimate; Total: - With self-employment income").alias("Employment_Estimate"),
        col("Margin of Error; Total: - With self-employment income").alias("Employment_Margin")
    )

population_df = spark.read.csv("hdfs://localhost:9000/data/raw/processed_population_data.csv", header=True, inferSchema=True) \
    .select(
        col("Neighborhood"),
        col("Id").cast("string"),
        col("Estimate; Total").alias("Population_Estimate"),
        col("Margin of Error; Total").alias("Population_Margin")
    )


enriched_df = population_df \
    .join(income_df, on=["Neighborhood", "Id"], how="left") \
    .join(health_df, on=["Neighborhood", "Id"], how="left") \
    .join(employment_df, on=["Neighborhood", "Id"], how="left")

enriched_df.write.csv("hdfs://localhost:9000/data/processed/enriched_data", mode="overwrite", header=True)
