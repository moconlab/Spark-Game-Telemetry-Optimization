from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from src.transformations.event_aggregations import aggregate_events
from src.transformations.player_metrics import player_metrics
from pyspark.sql.functions import col
import os

spark = get_spark(config_file="configs/spark/optimized.conf")

input_dir = "data/raw/"
output_dir = "data/benchmark_results/optimized/"
os.makedirs(output_dir, exist_ok=True)

# Read all CSV batches
df = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Optimization: repartition by match_id for better shuffle performance
df = df.repartition("match_id")

events_agg = aggregate_events(df)
player_stats = player_metrics(df)

events_agg.write.mode("overwrite").parquet(os.path.join(output_dir, "events_agg"))
player_stats.write.mode("overwrite").parquet(os.path.join(output_dir, "player_stats"))