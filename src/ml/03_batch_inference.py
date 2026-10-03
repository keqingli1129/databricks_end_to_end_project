# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Batch inference with the registered model
# MAGIC Loads `claims_damage_level@prod` as a Spark UDF, scores the training images (with a confusion matrix)
# MAGIC and the customer claim photos, and writes both to gold.

# COMMAND ----------

# MAGIC %pip install torch==2.5.1 transformers==4.46.3 mlflow==3.8.1 --extra-index-url https://download.pytorch.org/whl/cpu

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("model_name", "e2e_dev.dev_keqingli1129_gold.dev_keqingli1129_claims_damage_level")
dbutils.widgets.text("training_table", "e2e_dev.dev_keqingli1129_silver.training_images_resized")
dbutils.widgets.text("claim_images_table", "e2e_dev.dev_keqingli1129_silver.claim_images")
dbutils.widgets.text("gold_schema", "e2e_dev.dev_keqingli1129_gold")

MODEL_NAME = dbutils.widgets.get("model_name")
TRAINING_TABLE = dbutils.widgets.get("training_table")
CLAIM_IMAGES_TABLE = dbutils.widgets.get("claim_images_table")
GOLD = dbutils.widgets.get("gold_schema")

# COMMAND ----------

import json

import mlflow
import pandas as pd

mlflow.set_registry_uri("databricks-uc")
predict_damage = mlflow.pyfunc.spark_udf(spark, f"models:/{MODEL_NAME}@prod", result_type="string", env_manager="local")

# Training images: the transcript's damage_predictions table + a confusion matrix.
damage_predictions = spark.table(TRAINING_TABLE).withColumn("damage_prediction", predict_damage("content"))
damage_predictions.write.mode("overwrite").saveAsTable(f"{GOLD}.damage_predictions")

results = spark.table(f"{GOLD}.damage_predictions").select("label", "damage_prediction").toPandas()
confusion = pd.crosstab(results.label, results.damage_prediction, rownames=["actual"], colnames=["predicted"])
display(confusion.reset_index())

# COMMAND ----------

# The customer claim photos: what part 6 compares with the customer's own severity claim.
claim_predictions = spark.table(CLAIM_IMAGES_TABLE).select(
    "image_name", "path", predict_damage("content").alias("damage_prediction")
)
claim_predictions.write.mode("overwrite").saveAsTable(f"{GOLD}.claim_image_predictions")
display(spark.table(f"{GOLD}.claim_image_predictions"))

# COMMAND ----------

result = {
    "accuracy_on_training_images": float((results.label == results.damage_prediction).mean()),
    "confusion": confusion.to_dict(),
    "claim_images_scored": spark.table(f"{GOLD}.claim_image_predictions").count(),
}
print(result)
dbutils.notebook.exit(json.dumps(result))
