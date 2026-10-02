# Databricks notebook source
# MAGIC %md
# MAGIC # Claim images with Auto Loader + cleanSource (plain PySpark)
# MAGIC Reads customer-uploaded claim photos from the `claims` volume into `bronze.claim_images`, then
# MAGIC **moves** processed files to `claims/archive/`. Outside a pipeline, the checkpoint and schema
# MAGIC locations must be set by hand. See docs/transcript_3.txt and docs/part3_object_storage_plan.md (Task 6).

# COMMAND ----------

dbutils.widgets.text("claims_volume_path", "/Volumes/e2e_dev/dev_keqingli1129_landing/claims")
dbutils.widgets.text("target_table", "e2e_dev.dev_keqingli1129_bronze.claim_images")

CLAIMS_VOLUME = dbutils.widgets.get("claims_volume_path")
TARGET_TABLE = dbutils.widgets.get("target_table")

SOURCE_PATH = f"{CLAIMS_VOLUME}/images"
ARCHIVE_PATH = f"{CLAIMS_VOLUME}/archive"  # same volume, NOT inside images/ (or archived files are re-read)
METADATA_PATH = f"{CLAIMS_VOLUME}/_autoloader/claim_images"  # checkpoint + schema location

# COMMAND ----------

# cleanSource: after a file is processed (and retentionDuration has passed), MOVE it to the archive.
clean_source_options = {
    "cloudFiles.cleanSource": "MOVE",
    "cloudFiles.cleanSource.retentionDuration": "1 minute",
    "cloudFiles.cleanSource.moveDestination": ARCHIVE_PATH,
}

# COMMAND ----------

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", METADATA_PATH)
    .options(**clean_source_options)
    .load(SOURCE_PATH)
    .writeStream.option("checkpointLocation", f"{METADATA_PATH}/checkpoint")
    .trigger(availableNow=True)  # process what's there now, then stop (batch-style run)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------

print("rows in target:", spark.table(TARGET_TABLE).count())
print("files still in images/:", len(dbutils.fs.ls(SOURCE_PATH)))
print("files in archive/:", len(dbutils.fs.ls(ARCHIVE_PATH)))
