from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, percentile_approx

spark = SparkSession.builder \
    .appName("Metrics Calculation") \
    .getOrCreate()

enriched_file = "hdfs://localhost:9000/data/processed/enriched_data"

enriched_df = spark.read.csv(enriched_file, header=True, inferSchema=True)

enriched_df.printSchema()

# Metriques
# Metriques
from pyspark.sql.functions import sum, col

metrics_df = enriched_df.select(
    col("Neighborhood"),
    col("Population_Estimate").alias("Population"),
    col("Income_Estimate").alias("Income"),
    col("Health_Total_Estimate").alias("Health"),
    col("Employment_Estimate").alias("Employment")
).groupBy("Neighborhood").agg(
    sum("Population").alias("Total_Population"),
    (sum("Income") / sum("Population")).alias("Income_Per_Capita"),
    (sum("Employment") / sum("Population")).alias("Employment_Rate"),
    (sum("Health") / sum("Population")).alias("Health_Coverage_Rate"),
)



metrics_df.show()

output_path = "hdfs://localhost:9000/data/processed/metrics_summary"
metrics_df.write.csv(output_path, mode="overwrite", header=True)
