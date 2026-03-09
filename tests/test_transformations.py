import pytest
from pyspark.sql import SparkSession
from src.transformations import event_aggregations, player_metrics
from pyspark.sql import Row

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    yield spark
    spark.stop()

def test_aggregate_events(spark):
    data = [
        Row(player_id="p1", match_id="m1", event_type="kill", latency_ms=100),
        Row(player_id="p1", match_id="m1", event_type="kill", latency_ms=200),
        Row(player_id="p2", match_id="m1", event_type="kill", latency_ms=150)
    ]
    df = spark.createDataFrame(data)
    agg_df = event_aggregations.aggregate_events(df).collect()
    
    # Should aggregate by match_id & event_type
    assert any(row.match_id == "m1" and row.event_type == "kill" for row in agg_df)

def test_player_metrics(spark):
    data = [
        Row(player_id="p1", match_id="m1", event_type="kill"),
        Row(player_id="p1", match_id="m2", event_type="kill"),
        Row(player_id="p2", match_id="m1", event_type="kill")
    ]
    df = spark.createDataFrame(data)
    metrics_df = player_metrics.player_metrics(df).collect()
    
    for row in metrics_df:
        if row.player_id == "p1":
            assert row.matches_played == 2
        if row.player_id == "p2":
            assert row.matches_played == 1