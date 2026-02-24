from pyspark.sql import SparkSession
from spark_jobs.utils.config_loader import load_config


def get_spark(app_name: str) -> SparkSession:
    config = load_config()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.serializer", config["spark"]["serialization"]["serializer"])
        .config("spark.sql.shuffle.partitions",
                config["spark"]["sql"]["shuffle_partitions"])
        .config("spark.sql.autoBroadcastJoinThreshold",
                config["spark"]["sql"]["auto_broadcast_join_threshold"])
        .config("spark.sql.adaptive.enabled",
                config["spark"]["sql"]["adaptive_enabled"])
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark