"""Bronze -> silver: clean types and apply data quality checks (see docs/transcript_4.txt)."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = spark.conf.get("transformations.bronze_schema")  # e.g. e2e_dev.dev_keqingli1129_bronze
SILVER_PROPERTIES = {"quality": "silver"}

# --- From the CDC tables (part 2). They receive updates/deletes, so read them in batch: materialized views. ---

SEVERITY_LEVEL = (
    F.when(F.col("incident_severity") == "Trivial Damage", 1)
    .when(F.col("incident_severity") == "Minor Damage", 2)
    .when(F.col("incident_severity") == "Major Damage", 3)
    .when(F.col("incident_severity") == "Total Loss", 4)
)


@dp.materialized_view(name="claim", comment="Cleaned claims with a numeric severity level.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_claim_no": "claim_no IS NOT NULL", "valid_claim_amount": "claim_amount >= 0"})
def claim():
    return spark.read.table(f"{BRONZE}.claim").withColumn("severity_level", SEVERITY_LEVEL)


@dp.materialized_view(name="policy", comment="Cleaned policies; premium is never negative.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_policy_no": "policy_no IS NOT NULL", "valid_policy_period": "end_date > start_date"})
def policy():
    return spark.read.table(f"{BRONZE}.policy").withColumn("premium", F.abs("premium"))


@dp.materialized_view(name="customer", comment="Cleaned customers with full name and age.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_customer_id": "customer_id IS NOT NULL", "valid_email": "email LIKE '%@%'"})
def customer():
    return (
        spark.read.table(f"{BRONZE}.customer")
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("age", F.floor(F.months_between(F.current_date(), "date_of_birth") / 12).cast("int"))
    )


# --- From append-only bronze tables (parts 1 and 3): streaming tables, each run processes only new rows. ---


@dp.table(name="telematics", comment="Typed telematics events.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_chassis_number": "chassis_number IS NOT NULL", "valid_speed": "speed BETWEEN 0 AND 250"})
def telematics():
    return spark.readStream.table(f"{BRONZE}.telematics").select(
        "chassis_number",
        F.col("speed").cast("double").alias("speed"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.col("stream_metadata.timestamp").alias("ingested_at"),
    )


@dp.table(name="training_images", comment="Training images with their label (ok/minor/major).", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_label": "label IN ('ok', 'minor', 'major')", "non_empty_image": "length > 0"})
def training_images():
    return (
        spark.readStream.table(f"{BRONZE}.training_images")
        .withColumn("file_name", F.regexp_extract("path", r"[^/]+$", 0))
        .withColumn("label", F.regexp_extract("file_name", r"-(ok|minor|major)", 1))
    )


@dp.table(name="claim_images", comment="Customer-uploaded claim photos with their file name.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_image_name": "image_name IS NOT NULL AND image_name != ''", "non_empty_image": "length > 0"})
def claim_images():
    return spark.readStream.table(f"{BRONZE}.claim_images").withColumn(
        "image_name", F.regexp_extract("path", r"[^/]+$", 0)
    )


@dp.table(name="claim_images_metadata", comment="Which image belongs to which claim and car.", table_properties=SILVER_PROPERTIES)
@dp.expect_all_or_drop({"valid_claim_no": "claim_no IS NOT NULL", "valid_image_id": "image_id IS NOT NULL"})
def claim_images_metadata():
    return (
        spark.readStream.table(f"{BRONZE}.claim_images_metadata")
        .withColumn("image_id", F.col("image_id").cast("int"))
        .drop("_rescued_data", "new_column_1")
    )
