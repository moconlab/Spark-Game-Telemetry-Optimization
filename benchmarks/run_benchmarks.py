import time
import subprocess

pipelines = [
    ("Baseline", "src/baseline_jobs/unpartitioned_pipeline.py"),
    ("Optimized", "src/optimized_jobs/partitioned_pipeline.py")
]

for name, script in pipelines:
    start = time.time()
    subprocess.run(["spark-submit", script])
    end = time.time()
    print(f"{name} pipeline completed in {end - start:.2f} seconds")