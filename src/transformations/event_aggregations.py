from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg

def aggregate_events(df: DataFrame) -> DataFrame:
    """
    Aggregates telemetry events by match and event_type.
    """
    return df.groupBy("match_id", "event_type") \
             .agg(
                 count("*").alias("event_count"),
                 avg("latency_ms").alias("avg_latency_ms")
             )