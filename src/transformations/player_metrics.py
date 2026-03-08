from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct

def player_metrics(df: DataFrame) -> DataFrame:
    """
    Calculates player-level metrics.
    """
    return df.groupBy("player_id") \
             .agg(
                 countDistinct("match_id").alias("matches_played"),
                 countDistinct("event_type").alias("unique_events")
             )