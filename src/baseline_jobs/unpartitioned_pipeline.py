from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from src.transformations.event_aggregations import aggregate_events
from src.transformations.player_metrics import player_metrics
import os

spark = get_spark(config_file="configs/spark/baseline.conf")

input_dir = "data/raw/"
output_dir = "data/benchmark_results/baseline/"
os.makedirs(output_dir, exist_ok=True)

# Read all CSV batches
df = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Baseline pipeline: unoptimized
events_agg = aggregate_events(df)
player_stats = player_metrics(df)

# Write results
events_agg.write.mode("overwrite").parquet(os.path.join(output_dir, "events_agg"))
player_stats.write.mode("overwrite").parquet(os.path.join(output_dir, "player_stats"))