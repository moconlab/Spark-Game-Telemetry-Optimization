from pyspark.sql import functions as F
from spark_jobs.utils.spark_session import get_spark
from spark_jobs.utils.metrics_logger import MetricsLogger

spark = get_spark("daily-metrics-optimized")
logger = MetricsLogger(spark, "daily-metrics-optimized")
logger.start()

# Enable AQE explicitly
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# GOOD: Use partitioned Parquet
df = spark.read.parquet("data/partitioned/")

# GOOD: Early filter (predicate pushdown)
df = df.filter("event_type IS NOT NULL")

# GOOD: Cache reused dataset
df.cache()

daily = (
    df.groupBy("region", "event_type")
      .agg(
          F.count("*").alias("event_count"),
          F.sum("event_value").alias("total_value")
      )
)

logger.log_execution_plan(daily)

daily.write.mode("overwrite").parquet(
    "data/output/daily_metrics_optimized"
)

logger.stop()
spark.stop()