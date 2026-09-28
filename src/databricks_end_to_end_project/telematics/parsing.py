"""Turn raw Kafka records (bytes) into telematics columns."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

# Like the transcript: parse every field as a string for now; typing comes in silver.
PAYLOAD_SCHEMA = MapType(StringType(), StringType())
EVENT_FIELDS = ("chassis_number", "speed", "latitude", "longitude", "event_timestamp")


def parse_telematics(raw_df: DataFrame) -> DataFrame:
    """Decode the Kafka value to JSON text and split it into one column per event field."""
    decoded = raw_df.withColumn("raw_json", F.col("value").cast("string")).withColumn(
        "payload", F.from_json("raw_json", PAYLOAD_SCHEMA)
    )
    return decoded.select(
        *[F.col("payload").getItem(field).alias(field) for field in EVENT_FIELDS],
        F.struct("topic", "partition", "offset", "timestamp").alias("stream_metadata"),
        "raw_json",
    )
