# Databricks notebook source
# MAGIC %md
# MAGIC # Claim images with Auto Loader + cleanSource (plain PySpark)
# MAGIC Reads customer-uploaded claim photos from the `claims` volume into `bronze.claim_images`, then
# MAGIC **moves** processed files to `claims/archive/`. Outside a pipeline, the checkpoint and schema
# MAGIC locations must be set by hand. See docs/transcript_3.txt and docs/part3_object_storage_plan.md (Task 6).

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))  # this notebook is in src/object_storage/, so ".." is src/ (shared package)

from databricks_end_to_end_project.logging_utils import enable_table_logging, flush_logs, get_logger

# COMMAND ----------

dbutils.widgets.text("claims_volume_path", "/Volumes/e2e_dev/dev_keqingli1129_landing/claims")
dbutils.widgets.text("target_table", "e2e_dev.dev_keqingli1129_bronze.claim_images")
dbutils.widgets.text("log_table", "e2e_dev.dev_keqingli1129_ops.app_logs")
dbutils.widgets.text("job_id", "")  # filled by the job ({{job.id}}); empty when run by hand
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_key", "")

CLAIMS_VOLUME = dbutils.widgets.get("claims_volume_path")
TARGET_TABLE = dbutils.widgets.get("target_table")

log = get_logger("object_storage.claim_images")
enable_table_logging(
    dbutils.widgets.get("log_table"),
    job_id=dbutils.widgets.get("job_id"),
    job_run_id=dbutils.widgets.get("job_run_id"),
    task_key=dbutils.widgets.get("task_key"),
)

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

log.info("ingesting %s into %s", SOURCE_PATH, TARGET_TABLE)
try:
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
except Exception:
    log.exception("claim image ingestion failed")  # ERROR, so it is written to the log table at once
    raise

# COMMAND ----------

log.info("rows in target: %d", spark.table(TARGET_TABLE).count())
log.info("files still in images/: %d", len(dbutils.fs.ls(SOURCE_PATH)))
log.info("files in archive/: %d", len(dbutils.fs.ls(ARCHIVE_PATH)))
flush_logs()  # a notebook's process doesn't exit, so write the buffered rows now
