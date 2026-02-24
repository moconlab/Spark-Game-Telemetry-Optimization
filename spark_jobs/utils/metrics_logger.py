import time


class MetricsLogger:

    def __init__(self, spark, job_name: str):
        self.spark = spark
        self.job_name = job_name
        self.start_time = None

    def start(self):
        print(f"\n🚀 Starting job: {self.job_name}")
        self.start_time = time.time()

    def log_execution_plan(self, df):
        print("\n📊 Execution Plan:")
        df.explain(mode="formatted")

    def stop(self):
        duration = time.time() - self.start_time
        print(f"\n⏱ Job '{self.job_name}' completed in {duration:.2f} seconds\n")
        return duration