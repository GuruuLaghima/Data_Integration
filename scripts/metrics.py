from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min

spark = SparkSession.builder \
    .appName("Metrics Calculation") \
    .getOrCreate()

enriched_file = "hdfs://localhost:9000/data/processed/enriched_data"

enriched_df = spark.read.csv(enriched_file, header=True, inferSchema=True)

enriched_df.printSchema()

# Metriques
metrics_df = enriched_df.select(
    col("Neighborhood"),
    col("Population_Estimate").alias("Population"),
    col("Income_Estimate").alias("Income"),
    col("Health_Total_Estimate").alias("Health"),
    col("Employment_Estimate").alias("Employment")
).groupBy("Neighborhood").agg(
    avg("Population").alias("Avg_Population"),
    min("Income").alias("Min_Income"),
    max("Income").alias("Max_Income"),
    avg("Health").alias("Avg_Health"),
    avg("Employment").alias("Avg_Employment")
)

metrics_df.show()

output_path = "hdfs://localhost:9000/data/processed/metrics_summary"
metrics_df.write.csv(output_path, mode="overwrite", header=True)
