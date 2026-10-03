"""Silver -> gold: aggregates and pre-joined materialized views for BI and apps (see docs/transcript_4.txt)."""

from geopy.distance import geodesic
from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD = spark.conf.get("transformations.gold_schema")  # e.g. e2e_dev.dev_keqingli1129_gold
GOLD_PROPERTIES = {"quality": "gold"}
HOUSTON_CENTER = (29.7604, -95.3698)


@F.udf("double")
def distance_from_city_center_km(latitude, longitude):
    """Great-circle distance (geopy, offline) from a point to downtown Houston."""
    if latitude is None or longitude is None:
        return None
    return float(geodesic((latitude, longitude), HOUSTON_CENTER).km)


@dp.materialized_view(
    name=f"{GOLD}.aggregated_telematics",
    comment="Telematics per car: speed statistics, average location and its distance to the city center.",
    table_properties=GOLD_PROPERTIES,
)
def aggregated_telematics():
    return (
        spark.read.table("telematics")
        .groupBy("chassis_number")
        .agg(
            F.avg("speed").alias("avg_speed"),
            F.max("speed").alias("max_speed"),
            F.avg("latitude").alias("avg_latitude"),
            F.avg("longitude").alias("avg_longitude"),
            F.count("*").alias("event_count"),
            F.min("event_timestamp").alias("first_event_at"),
            F.max("event_timestamp").alias("last_event_at"),
        )
        .withColumn("distance_from_city_center_km", distance_from_city_center_km("avg_latitude", "avg_longitude"))
    )


@dp.materialized_view(
    name=f"{GOLD}.customer_claim_policy",
    comment="Each claim with its policy and customer.",
    table_properties=GOLD_PROPERTIES,
)
def customer_claim_policy():
    claims = spark.read.table("claim")
    policies = spark.read.table("policy")
    customers = spark.read.table("customer")
    return claims.join(policies, "policy_no").join(customers, "customer_id")


@dp.materialized_view(
    name=f"{GOLD}.customer_claim_policy_telematics",
    comment="Claims with policy, customer and (where available) the car's aggregated telematics.",
    table_properties=GOLD_PROPERTIES,
)
def customer_claim_policy_telematics():
    return spark.read.table(f"{GOLD}.customer_claim_policy").join(
        spark.read.table(f"{GOLD}.aggregated_telematics"), "chassis_number", "left"
    )
