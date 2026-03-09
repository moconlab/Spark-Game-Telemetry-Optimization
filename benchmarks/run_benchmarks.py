import time
import subprocess

pipelines = [
    ("Baseline", "src/baseline_jobs/unpartitioned_pipeline.py"),
    ("Optimized Partitioned", "src/optimized_jobs/partitioned_pipeline.py"),
    ("Broadcast Join", "src/optimization/broadcast_joins.py"),
    ("Caching Strategy", "src/optimization/caching_strategy.py"),
    ("Predicate Pushdown", "src/optimization/predicate_pushdown.py"),
    ("Adaptive Query", "src/optimization/adaptive_query_execution.py")
]

for name, script in pipelines:
    start = time.time()
    subprocess.run(["spark-submit", script])
    end = time.time()
    print(f"{name} pipeline completed in {end - start:.2f} seconds")