"""Silver -> gold: aggregates and pre-joined materialized views for BI and apps (see docs/transcript_4.txt)."""

from geopy.distance import geodesic
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from claims_app.claim_rules import CHECKS, COVERAGE_LIMITS, EXPECTED_DAMAGE, MAX_SPEED_KMH

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


# --- Claim checks (part 6): the business rules used by the dashboard, Genie and the app. ---
# The rule constants live in src/claims_app/claim_rules.py, the single copy shared with the app (part 6b).


def _as_map(mapping):
    return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


CLAIM_CHECKS_SCHEMA = """
    claim_no STRING COMMENT 'Claim number (key), e.g. CLM00000001',
    policy_no STRING COMMENT 'Policy the claim was made on',
    customer_id STRING COMMENT 'Customer who owns the policy',
    full_name STRING COMMENT 'Customer full name',
    incident_date DATE COMMENT 'Date of the accident',
    incident_type STRING COMMENT 'COLLISION, THEFT, WEATHER, VANDALISM or GLASS',
    incident_severity STRING COMMENT 'Severity claimed by the customer: Trivial Damage < Minor Damage < Major Damage < Total Loss',
    claim_amount DECIMAL(12,2) COMMENT 'Amount claimed, in USD',
    coverage STRING COMMENT 'Policy coverage: COMPREHENSIVE, COLLISION or LIABILITY',
    coverage_limit INT COMMENT 'Maximum claim amount for the coverage, in USD',
    start_date DATE COMMENT 'Policy start date',
    end_date DATE COMMENT 'Policy end date',
    chassis_number STRING COMMENT 'Car chassis number (links to telematics)',
    max_speed DOUBLE COMMENT 'Highest speed recorded by the car telematics, km/h; NULL if the car has no telematics',
    image_name STRING COMMENT 'Photo the customer uploaded for the claim',
    expected_damage STRING COMMENT 'Claimed severity mapped to the model labels ok/minor/major',
    predicted_damage STRING COMMENT 'Damage predicted from the photo by the ML model: ok, minor or major',
    severity_match BOOLEAN COMMENT 'TRUE if the predicted damage equals the expected damage; NULL if no prediction',
    amount_within_limit BOOLEAN COMMENT 'TRUE if claim_amount <= coverage_limit',
    policy_valid BOOLEAN COMMENT 'TRUE if incident_date is within the policy start and end date',
    speed_ok BOOLEAN COMMENT 'TRUE if max_speed <= 150 km/h; NULL if the car has no telematics',
    failed_checks ARRAY<STRING> COMMENT 'Names of the checks that failed',
    claim_status STRING COMMENT 'auto_approved if no check failed, otherwise needs_review'
"""


@dp.materialized_view(
    name=f"{GOLD}.claim_checks",
    comment="One row per claim with the automatic claim checks and the resulting status (auto_approved / needs_review).",
    # External metadata lets readers get this MV's change feed: the app's continuous Lakebase sync (part 6b).
    table_properties={**GOLD_PROPERTIES, "pipelines.externalMetadata.enabled": "true"},
    schema=CLAIM_CHECKS_SCHEMA,
)
def claim_checks():
    claims = spark.read.table(f"{GOLD}.customer_claim_policy_telematics")
    photos = spark.read.table("claim_images_metadata").groupBy("claim_no").agg(F.first("image_name").alias("image_name"))
    predictions = spark.read.table(f"{GOLD}.claim_image_predictions").select(
        "image_name", F.col("damage_prediction").alias("predicted_damage")
    )
    checked = (
        claims.join(photos, "claim_no", "left")
        .join(predictions, "image_name", "left")
        .withColumn("expected_damage", _as_map(EXPECTED_DAMAGE)[F.col("incident_severity")])
        .withColumn("coverage_limit", _as_map(COVERAGE_LIMITS)[F.col("coverage")].cast("int"))
        .withColumn("severity_match", F.col("expected_damage") == F.col("predicted_damage"))
        .withColumn("amount_within_limit", F.col("claim_amount") <= F.col("coverage_limit"))
        .withColumn("policy_valid", F.col("incident_date").between(F.col("start_date"), F.col("end_date")))
        .withColumn("speed_ok", F.col("max_speed") <= MAX_SPEED_KMH)
        .withColumn(
            "failed_checks",
            F.filter(F.array(*[F.when(~F.col(c), F.lit(c)) for c in CHECKS]), lambda name: name.isNotNull()),
        )
        .withColumn("claim_status", F.when(F.size("failed_checks") == 0, "auto_approved").otherwise("needs_review"))
    )
    return checked.select(
        "claim_no",
        "policy_no",
        "customer_id",
        "full_name",
        "incident_date",
        "incident_type",
        "incident_severity",
        F.col("claim_amount").cast("decimal(12,2)").alias("claim_amount"),
        "coverage",
        "coverage_limit",
        "start_date",
        "end_date",
        "chassis_number",
        "max_speed",
        "image_name",
        "expected_damage",
        "predicted_damage",
        *CHECKS,
        "failed_checks",
        "claim_status",
    )


# --- Policy lookup (part 6b): what the app needs to check a new claim against its policy. ---

POLICY_LOOKUP_SCHEMA = """
    policy_no STRING COMMENT 'Policy number (key), e.g. POL0000001',
    customer_id STRING COMMENT 'Customer who owns the policy',
    full_name STRING COMMENT 'Customer full name',
    coverage STRING COMMENT 'Policy coverage: COMPREHENSIVE, COLLISION or LIABILITY',
    coverage_limit INT COMMENT 'Maximum claim amount for the coverage, in USD',
    start_date DATE COMMENT 'Policy start date',
    end_date DATE COMMENT 'Policy end date',
    chassis_number STRING COMMENT 'Car chassis number (links to telematics)',
    max_speed DOUBLE COMMENT 'Highest speed recorded by the car telematics, km/h; NULL if the car has no telematics'
"""


@dp.materialized_view(
    name=f"{GOLD}.policy_lookup",
    comment="One row per policy with its coverage limit, dates and the car's top speed, for checking new claims.",
    # External metadata lets the app's continuous Lakebase sync read this MV's change feed.
    table_properties={**GOLD_PROPERTIES, "pipelines.externalMetadata.enabled": "true"},
    schema=POLICY_LOOKUP_SCHEMA,
)
def policy_lookup():
    policies = spark.read.table("policy")
    customers = spark.read.table("customer").select("customer_id", "full_name")
    speeds = spark.read.table(f"{GOLD}.aggregated_telematics").select("chassis_number", "max_speed")
    return (
        policies.join(customers, "customer_id")  # inner, like customer_claim_policy: deleted customers drop out
        .join(speeds, "chassis_number", "left")
        .select(
            "policy_no",
            "customer_id",
            "full_name",
            "coverage",
            _as_map(COVERAGE_LIMITS)[F.col("coverage")].cast("int").alias("coverage_limit"),
            "start_date",
            "end_date",
            "chassis_number",
            "max_speed",
        )
    )
