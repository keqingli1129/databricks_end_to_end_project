# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Prepare training images
# MAGIC Resizes the labelled training images to 224×224 (the size ResNet was pre-trained on) and stores them
# MAGIC as small JPEGs in a new table. Also checks which download sites serverless can reach.

# COMMAND ----------

dbutils.widgets.text("source_table", "e2e_dev.dev_keqingli1129_silver.training_images")
dbutils.widgets.text("target_table", "e2e_dev.dev_keqingli1129_silver.training_images_resized")

SOURCE_TABLE = dbutils.widgets.get("source_table")
TARGET_TABLE = dbutils.widgets.get("target_table")
IMAGE_SIZE = 224

# COMMAND ----------

import io

from PIL import Image
from pyspark.sql import functions as F


@F.udf("binary")
def resize_image(content):
    """Decode an image, resize it to IMAGE_SIZE×IMAGE_SIZE RGB, return JPEG bytes."""
    if content is None:
        return None
    image = Image.open(io.BytesIO(content)).convert("RGB").resize((IMAGE_SIZE, IMAGE_SIZE))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


resized = spark.read.table(SOURCE_TABLE).select("path", "file_name", "label", resize_image("content").alias("content"))
resized.write.mode("overwrite").saveAsTable(TARGET_TABLE)

# COMMAND ----------

display(spark.table(TARGET_TABLE).select("file_name", "label", F.length("content").alias("jpeg_bytes")))

# COMMAND ----------

# Which download sites can serverless reach? (Free Edition restricts outbound internet.)
import json
import urllib.request

checks = {
    "huggingface": "https://huggingface.co/microsoft/resnet-18/resolve/main/config.json",
    "pytorch_cpu_wheels": "https://download.pytorch.org/whl/cpu/",
}
reachable = {}
for name, url in checks.items():
    try:
        urllib.request.urlopen(url, timeout=15)
        reachable[name] = "reachable"
    except Exception as error:  # noqa: BLE001 - report any connection failure
        reachable[name] = f"NOT reachable: {type(error).__name__}: {error}"

result = {"rows": spark.table(TARGET_TABLE).count(), **reachable}
print(result)
dbutils.notebook.exit(json.dumps(result))
