from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from spark_jobs.utils.spark_session import get_spark
from spark_jobs.utils.metrics_logger import MetricsLogger

spark = get_spark("session-stats-optimized")
logger = MetricsLogger(spark, "session-stats-optimized")
logger.start()

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "64")

df = spark.read.parquet("data/partitioned/")

# GOOD: Create small dimension table
player_dim = df.select("player_id").distinct()

# GOOD: Broadcast small table
joined = df.join(broadcast(player_dim), on="player_id")

session_stats = (
    joined.groupBy("player_id")
          .agg(F.count("*").alias("interaction_count"))
)

logger.log_execution_plan(session_stats)

session_stats.write.mode("overwrite").parquet(
    "data/output/session_stats_optimized"
)

logger.stop()
spark.stop()