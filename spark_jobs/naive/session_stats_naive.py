from pyspark.sql import functions as F
from spark_jobs.utils.spark_session import get_spark
from spark_jobs.utils.metrics_logger import MetricsLogger

spark = get_spark("session-stats-naive")
logger = MetricsLogger(spark, "session-stats-naive")
logger.start()

df = spark.read.json("data/raw/events.json")

# BAD: Self join (huge shuffle)
joined = df.join(df, on="player_id")

# BAD: No broadcast hint even if dimension small
session_stats = (
    joined.groupBy("player_id")
          .agg(F.count("*").alias("interaction_count"))
)

logger.log_execution_plan(session_stats)

session_stats.write.mode("overwrite").json(
    "data/output/session_stats_naive"
)

logger.stop()
spark.stop()