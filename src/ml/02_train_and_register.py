# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Fine-tune ResNet-18, track with MLflow, register in Unity Catalog
# MAGIC Trains on CPU (Free Edition has no GPU), logs everything to the MLflow experiment, registers the model
# MAGIC as a new version of the Unity Catalog model and points the "prod" alias at it.

# COMMAND ----------

# MAGIC %pip install torch==2.5.1 transformers==4.46.3 mlflow==3.8.1 --extra-index-url https://download.pytorch.org/whl/cpu

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("training_table", "e2e_dev.dev_keqingli1129_silver.training_images_resized")
dbutils.widgets.text("experiment_name", "/Users/keqingli1129@gmail.com/[dev keqingli1129] e2e_damage_classifier")
dbutils.widgets.text("model_name", "e2e_dev.dev_keqingli1129_gold.dev_keqingli1129_claims_damage_level")
dbutils.widgets.text("base_model", "microsoft/resnet-18")
dbutils.widgets.text("epochs", "5")

TRAINING_TABLE = dbutils.widgets.get("training_table")
EXPERIMENT_NAME = dbutils.widgets.get("experiment_name")
MODEL_NAME = dbutils.widgets.get("model_name")
BASE_MODEL = dbutils.widgets.get("base_model")
EPOCHS = int(dbutils.widgets.get("epochs"))
LEARNING_RATE = 5e-5
BATCH_SIZE = 8
LABELS = ["ok", "minor", "major"]
LABEL2ID = {label: i for i, label in enumerate(LABELS)}

# COMMAND ----------

import io

import mlflow
import pandas as pd
import torch
from mlflow.models import infer_signature
from PIL import Image
from transformers import AutoImageProcessor, AutoModelForImageClassification

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)
torch.manual_seed(42)

data = spark.table(TRAINING_TABLE).select("file_name", "label", "content").toPandas()
train_df = data.sample(frac=0.8, random_state=42)
test_df = data.drop(train_df.index)
print(f"train={len(train_df)} test={len(test_df)}", train_df.label.value_counts().to_dict())

processor = AutoImageProcessor.from_pretrained(BASE_MODEL)


def to_pixels(contents):
    """JPEG bytes -> normalized tensor batch, as ResNet expects."""
    images = [Image.open(io.BytesIO(c)).convert("RGB") for c in contents]
    return processor(images=images, return_tensors="pt")["pixel_values"]


x_train, y_train = to_pixels(train_df.content), torch.tensor(train_df.label.map(LABEL2ID).values)
x_test, y_test = to_pixels(test_df.content), torch.tensor(test_df.label.map(LABEL2ID).values)

model = AutoModelForImageClassification.from_pretrained(
    BASE_MODEL,
    num_labels=len(LABELS),
    id2label=dict(enumerate(LABELS)),
    label2id=LABEL2ID,
    ignore_mismatched_sizes=True,  # replace the 1000-class ImageNet head with a 3-class head
)

# COMMAND ----------


class DamageClassifier(mlflow.pyfunc.PythonModel):
    """Image bytes (or base64 strings from serving) -> 'ok' / 'minor' / 'major'."""

    def load_context(self, context):
        from transformers import AutoImageProcessor, AutoModelForImageClassification

        self.processor = AutoImageProcessor.from_pretrained(context.artifacts["model_dir"])
        self.model = AutoModelForImageClassification.from_pretrained(context.artifacts["model_dir"]).eval()

    def predict(self, context, model_input, params=None):
        import base64
        import io

        import torch
        from PIL import Image

        images = [
            Image.open(io.BytesIO(c if isinstance(c, (bytes, bytearray)) else base64.b64decode(c))).convert("RGB")
            for c in model_input["content"]
        ]
        with torch.no_grad():
            logits = self.model(**self.processor(images=images, return_tensors="pt")).logits
        return [self.model.config.id2label[i] for i in logits.argmax(-1).tolist()]


# COMMAND ----------

optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE)

with mlflow.start_run(run_name="resnet18-finetune") as run:
    mlflow.log_params(
        {
            "base_model": BASE_MODEL,
            "epochs": EPOCHS,
            "learning_rate": LEARNING_RATE,
            "batch_size": BATCH_SIZE,
            "train_images": len(train_df),
            "test_images": len(test_df),
            "training_table": TRAINING_TABLE,
        }
    )
    for epoch in range(EPOCHS):
        model.train()
        order = torch.randperm(len(x_train))
        epoch_loss = 0.0
        for start in range(0, len(order), BATCH_SIZE):
            batch = order[start : start + BATCH_SIZE]
            loss = model(pixel_values=x_train[batch], labels=y_train[batch]).loss
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
            epoch_loss += loss.item() * len(batch)
        model.eval()
        with torch.no_grad():
            accuracy = (model(pixel_values=x_test).logits.argmax(-1) == y_test).float().mean().item()
        mlflow.log_metrics({"train_loss": epoch_loss / len(x_train), "test_accuracy": accuracy}, step=epoch)
        print(f"epoch {epoch}: loss={epoch_loss / len(x_train):.3f} test_accuracy={accuracy:.2f}")

    model_dir = "/tmp/damage_classifier"
    model.save_pretrained(model_dir)
    processor.save_pretrained(model_dir)
    sample = test_df.head(1)[["content"]]
    model_info = mlflow.pyfunc.log_model(
        name="model",
        python_model=DamageClassifier(),
        artifacts={"model_dir": model_dir},
        signature=infer_signature(sample, pd.Series(["ok"])),
        pip_requirements=[
            "--extra-index-url https://download.pytorch.org/whl/cpu",
            "torch==2.5.1",
            "transformers==4.46.3",
            "pillow",
        ],
    )

# COMMAND ----------

import json

from mlflow import MlflowClient

version = mlflow.register_model(model_info.model_uri, MODEL_NAME).version
MlflowClient().set_registered_model_alias(MODEL_NAME, "prod", version)
result = {
    "run_id": run.info.run_id,
    "model": MODEL_NAME,
    "version": version,
    "alias": "prod",
    "final_test_accuracy": accuracy,
}
print(result)
dbutils.notebook.exit(json.dumps(result))
