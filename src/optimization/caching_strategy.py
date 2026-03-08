from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
import os

spark = get_spark(config_file="configs/spark/optimized.conf")

input_dir = "data/raw/"
output_dir = "data/benchmark_results/cached_pipeline/"
os.makedirs(output_dir, exist_ok=True)

df = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Cache the DataFrame to avoid recomputation
df.cache()

# Example transformations
agg1 = df.groupBy("match_id").count()
agg2 = df.groupBy("player_id").count()

agg1.write.mode("overwrite").parquet(os.path.join(output_dir, "agg1"))
agg2.write.mode("overwrite").parquet(os.path.join(output_dir, "agg2"))

# Unpersist cache
df.unpersist()