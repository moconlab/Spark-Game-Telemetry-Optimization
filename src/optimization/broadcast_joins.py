from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from pyspark.sql.functions import broadcast
import os

spark = get_spark(config_file="configs/spark/optimized.conf")

input_dir = "data/raw/"
output_dir = "data/benchmark_results/optimized_broadcast/"
os.makedirs(output_dir, exist_ok=True)

# Load main telemetry events
events = spark.read.option("header", True).csv(os.path.join(input_dir, "*.csv"))

# Simulate small reference table (player regions)
players = events.select("player_id", "region").dropDuplicates()

# Broadcast join for optimization
joined = events.join(broadcast(players), on="player_id", how="inner")

joined.write.mode("overwrite").parquet(os.path.join(output_dir, "events_joined"))