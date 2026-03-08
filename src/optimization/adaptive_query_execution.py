from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from src.transformations.event_aggregations import aggregate_events
import os

spark = get_spark(config_file="configs/spark/optimized.conf")

# Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", True)

input_dir = "data/raw/"
output_dir = "data/benchmark_results/adaptive_query/"
os.makedirs(output_dir, exist_ok=True)

df = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Aggregate events
agg = aggregate_events(df)

agg.write.mode("overwrite").parquet(os.path.join(output_dir, "agg_adaptive"))