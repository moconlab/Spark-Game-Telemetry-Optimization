#!/bin/bash

# Generate synthetic telemetry
python3 src/ingestion/generate_events.py

# Run benchmark pipelines
python3 benchmarks/run_benchmarks.py