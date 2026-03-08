# Spark-Game-Telemetry-Optimization
Game Telemetry Optimization: From Raw Events to Player Insights

## Overview

Modern multiplayer games generate billions of telemetry events per day.
This project simulates a production-scale game analytics pipeline and focuses on optimizing Spark jobs, not just building them.

The goal is to demonstrate deep understanding of Spark internals by intentionally introducing performance bottlenecks and then eliminating them through targeted optimizations.

## Problem Statement

A multiplayer game emits high-volume events:

- Player movement

- Combat interactions

- Match outcomes

- Inventory changes

Raw telemetry is noisy, skewed, and expensive to process.
Naive Spark implementations lead to:

- Excessive shuffles

- Long job runtimes

- Skewed tasks

- Inefficient joins

## Dataset

Synthetic but realistic telemetry data

- events (fact table, very large)

  - event_id

  - player_id

  - match_id

  - event_type

  - event_timestamp

- players (dimension)

- matches (dimension)

Stored in Parquet to enable:

- Predicate pushdown

- Column pruning

- Efficient IO

## Experiments & Optimizations

### 1. Join Strategy Optimization

Baseline

- Shuffle-heavy joins

- Broadcast disabled

Optimized

- Broadcast joins for small dimension tables

- Join reordering

Measured Improvements

- Reduced shuffle size

- Fewer stages

### 2. Partitioning & Data Skew

Baseline

- Random repartitioning

- Severe skew on popular matches

Optimized

- Partitioning by match_id

- Shuffle partition tuning

Skew-aware strategies

### 3. Caching Strategy

Baseline

- Over-caching unused DataFrames

Optimized

- Cache only reused intermediate datasets

- Appropriate storage levels
  
### 4. Column Pruning & Predicate Pushdown

Baseline

- Full scans of wide event schema

Optimized

- Selective column reads

- Early filtering

## Performance Measurement

Metrics collected for every experiment:

- Job runtime

- Shuffle read/write

- Number of stages

- Task skew

Measured using:

- Spark UI

- Programmatic timers

- Lower task latency

## Results (Example)

| Optimization | Before | After | Improvement |
| ------------ | ------ | ----- | ----------- |
| Join Runtime | 180s   | 35s   | 5.1×        |
| Shuffle Size | 9.8GB  | 750MB | 13×         |

## Tech Stack

- PySpark

- Apache Spark

- Databricks or Local Spark

- Parquet

## Repo Structure

sspark-game-telemetry-optimization/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── configs/
│   ├── pipeline.yaml
│   └── spark/
│       ├── baseline.conf
│       └── optimized.conf
│
├── data/
│   ├── raw/
│   └── benchmark_results/
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── src/
│   ├── ingestion/
│   │   └── generate_events.py
│   │
│   ├── transformations/
│   │   ├── event_aggregations.py
│   │   └── player_metrics.py
│   │
│   ├── baseline_jobs/
│   │   └── unpartitioned_pipeline.py
│   │
│   ├── optimized_jobs/
│   │   └── partitioned_pipeline.py
│   │
│   └── utils/
│       ├── spark_session.py
│       └── logger.py
│
├── benchmarks/
│   └── run_benchmarks.py
│
└── scripts/
    └── run_pipeline.sh