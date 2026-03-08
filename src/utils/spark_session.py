from pyspark.sql import SparkSession
import yaml

def get_spark(app_name="TelemetryPipeline", config_file=None):
    builder = SparkSession.builder.appName(app_name)
    
    if config_file:
        with open(config_file) as f:
            confs = f.read().splitlines()
            for line in confs:
                if line.strip() and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    builder = builder.config(key.strip(), value.strip())
                    
    return builder.getOrCreate()