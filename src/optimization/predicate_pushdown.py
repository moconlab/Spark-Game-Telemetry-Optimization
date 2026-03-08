from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
import os

spark = get_spark(config_file="configs/spark/optimized.conf")

input_dir = "data/raw/"
output_dir = "data/benchmark_results/predicate_pushdown/"
os.makedirs(output_dir, exist_ok=True)

df = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Filter early: only NA region events
filtered = df.filter(df.region == "NA")

# Aggregation after filter
agg = filtered.groupBy("match_id").count()

agg.write.mode("overwrite").parquet(os.path.join(output_dir, "filtered_agg"))