# Spark-Game-Telemetry-Optimization
Game Telemetry Optimization: From Raw Events to Player Insights

## Overview

Modern multiplayer games generate billions of telemetry events per day.
This project simulates a production-scale game analytics pipeline and focuses on optimizing Spark jobs, not just building them.

The goal is to demonstrate deep understanding of Spark internals by intentionally introducing performance bottlenecks and then eliminating them through targeted optimizations.

Goals

1. Simulate **production-scale game telemetry** using synthetic data.  
2. Build **baseline Spark pipelines** with intentional bottlenecks.  
3. Implement **optimized pipelines** using advanced Spark techniques:
   - Partitioning & shuffle optimization  
   - Broadcast joins  
   - Caching & persistence  
   - Predicate pushdown  
   - Adaptive Query Execution (AQE)  
4. Benchmark pipelines and compare performance metrics.

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

spark-game-telemetry-optimization/
│
├── data/                     # Synthetic telemetry & benchmark results
├── src/
│   ├── ingestion/            # Generate synthetic telemetry events
│   ├── transformations/      # Reusable Spark transformations
│   ├── baseline_jobs/        # Slow, unoptimized pipelines
│   ├── optimized_jobs/       # Runnable optimized pipelines
│   ├── optimization/         # Modular experiments (broadcast join, caching, AQE)
│   └── utils/                # Spark session & logging helpers
├── benchmarks/               # Scripts to benchmark all pipelines
├── configs/                  # Pipeline & Spark configuration files
├── scripts/                  # Helper scripts to run pipelines
└── docker/                   # Docker & docker-compose setup


## Data & Pipeline Flow

[ Synthetic Telemetry Generator ]
              │
              ▼
       data/raw/*.csv
              │
    ┌─────────┴─────────┐
    │                   │
[ baseline_jobs ]   [ optimized_jobs ]
(unoptimized)       (optimized pipelines)
    │                   │
    ▼                   ▼
baseline_results     optimized_results
    │                   │
    └─────────┐─────────┘
              ▼
       benchmarks/run_benchmarks.py
              │
              ▼
   Performance metrics & comparison