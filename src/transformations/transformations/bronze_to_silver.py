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
