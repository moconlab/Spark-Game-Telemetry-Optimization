from pyspark.sql import functions as F
from spark_jobs.utils.spark_session import get_spark
from spark_jobs.utils.metrics_logger import MetricsLogger

spark = get_spark("daily-metrics-naive")
logger = MetricsLogger(spark, "daily-metrics-naive")
logger.start()

# BAD: Read raw JSON (no predicate pushdown, no partition pruning)
df = spark.read.json("data/raw/events.json")

# BAD: No filtering early
# BAD: Wide shuffle with default 200 partitions
daily = (
    df.groupBy("region", "event_type")
      .agg(
          F.count("*").alias("event_count"),
          F.sum("event_value").alias("total_value")
      )
)

# BAD: Blind repartition (extra shuffle)
daily = daily.repartition(200)

logger.log_execution_plan(daily)

daily.write.mode("overwrite").json("data/output/daily_metrics_naive")

logger.stop()
spark.stop()