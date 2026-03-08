import pytest
import os
from pyspark.sql import SparkSession
from src.utils.spark_session import get_spark
from src.baseline_jobs import unpartitioned_pipeline

@pytest.fixture(scope="module")
def spark():
    spark = get_spark(config_file="configs/spark/baseline.conf")
    yield spark
    spark.stop()

def test_baseline_pipeline_runs(spark, tmp_path):
    # Redirect outputs to tmp_path to avoid writing to repo
    output_dir = tmp_path / "benchmark"
    os.makedirs(output_dir, exist_ok=True)
    
    # Monkey patch output directory inside pipeline if needed
    # For simplicity, just test if function executes
    # Normally, you could refactor pipelines to accept output_dir param
    try:
        # Only testing run, not data correctness
        # You could import a pipeline function and call it here
        pass  # placeholder for pipeline run
    except Exception as e:
        pytest.fail(f"Baseline pipeline failed: {e}")