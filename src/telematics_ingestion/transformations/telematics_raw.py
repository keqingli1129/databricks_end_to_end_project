from pyspark import pipelines as dp
from pyspark.sql import functions as F
from databricks.sdk.runtime import dbutils

from databricks_end_to_end_project.telematics import kafka_config

SOURCE = spark.conf.get("telematics.source")  # "files" or "kafka"
TOPIC = spark.conf.get("telematics.topic")


def _read_kafka():
    servers, key, secret = kafka_config.read_credentials(dbutils, spark.conf.get("telematics.secret_scope"))
    options = kafka_config.spark_kafka_options(servers, key, secret, TOPIC)
    return spark.readStream.format("kafka").options(**options).load()


def _read_landing_files():
    """Read the simulator's JSON files with Auto Loader, shaped like Kafka rows so parsing works unchanged."""
    lines = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(spark.conf.get("telematics.landing_path"))
    )
    return lines.select(
        F.lit(None).cast("binary").alias("key"),
        F.col("value").cast("binary").alias("value"),
        F.lit(TOPIC).alias("topic"),
        F.lit(None).cast("int").alias("partition"),
        F.lit(None).cast("long").alias("offset"),
        F.col("_metadata.file_modification_time").alias("timestamp"),
    )


@dp.table(comment=f"Raw telematics records (source: {SOURCE}); value is still bytes.")
def telematics_raw():
    return _read_kafka() if SOURCE == "kafka" else _read_landing_files()
