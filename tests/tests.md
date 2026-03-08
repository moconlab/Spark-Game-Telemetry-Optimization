# Testing Spark Pipelines

This project includes automated tests to ensure that synthetic data generation, Spark transformations, and pipelines run correctly. Testing is implemented using Pytest and PySpark.

# Test Coverage
## Synthetic Data Generation (test_ingestion.py)

Verifies that synthetic telemetry batches are generated correctly.

Ensures all expected columns are present (player_id, match_id, event_type, timestamp, region, latency_ms).

Confirms correct number of events per batch.

## Transformations & Metrics (test_transformations.py)

Validates event aggregations (e.g., by match or event type).

Validates player metrics (e.g., number of matches played, events per player).

Checks for correct output schema and values.

## Pipeline Smoke Tests (test_pipeline.py)

Ensures baseline and optimized pipelines run end-to-end without errors.

Tests minimal execution to catch runtime exceptions.

Can be extended to verify output correctness and benchmark metrics.

## Directory Structure for Tests
tests/
├── test_ingestion.py          # Synthetic data generation tests
├── test_transformations.py    # Event aggregation & player metrics tests
└── test_pipeline.py           # Smoke tests for baseline and optimized pipelines


## Running Tests

Install Pytest:

`pip install pytest`

Run all tests:

`pytest tests/ --maxfail=1 --disable-warnings -q`

Optional: Run a specific test file:

`pytest tests/test_transformations.py`v

## Testing Notes & Best Practices

Spark jobs are executed in local mode for testing to reduce resource usage.

Temporary directories are used to avoid writing to the repo.

Tests focus on correctness of transformations and pipeline execution; performance benchmarks are captured separately.

Extendable to test optimized pipelines, caching strategies, broadcast joins, and AQE configurations.

