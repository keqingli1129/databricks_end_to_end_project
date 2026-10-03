# Part 5: damage-severity classifier with MLflow (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. Steps use checkboxes (`- [ ]`). **No automated tests.**

**Goal:**

- **Train:** fine-tune `microsoft/resnet-18` on `silver.training_images` (ok/minor/major) in Databricks serverless notebooks.
- **Track and register:** track the run in MLflow, and register `gold.claims_damage_level` with alias `prod`.
- **Infer:** batch-score into gold, and serve the model from a CPU endpoint, as in [transcript_5.txt](transcript_5.txt).

**Design:** [part5_ml_design.md](part5_ml_design.md).

**Tech stack:**

- Databricks serverless notebooks (`%pip`), PyTorch (CPU), Hugging Face `transformers` and Pillow
- MLflow: experiments, pyfunc, the Unity Catalog registry and `spark_udf`
- Model Serving
- Declarative Automation Bundles: experiment, registered model, job and serving endpoint resources

## Global constraints

- **Library versions,** pinned in every `%pip` cell and in the model's requirements: `torch==2.5.1`, `transformers==4.46.3`, `mlflow==3.8.1`. The CPU wheels come from `https://download.pytorch.org/whl/cpu`, an *extra* index, so PyPI still works if that site is blocked.
- **Profile `DEFAULT`, target `dev`,** and run local commands as `env -u PYTHONPATH …`.
- **Run one task at a time:** `databricks bundle run damage_classifier --only <task_key> --profile DEFAULT`, so earlier tasks, especially training, aren't repeated.
- **No tests** and no Claude commits.

---

### Task 1: ML resources and image preparation

**Files:**
- Create: `resources/damage_classifier.yml`
- Create: `src/ml/01_prepare_images.py`

- [x] **Step 1.1: Create the ML resources file**

  **Do:** create `resources/damage_classifier.yml`:

  ```yaml
  # Part 5 (see docs/transcript_5.txt): car-damage severity classifier.
  # Experiment + Unity Catalog registered model + training/inference job (+ serving endpoint, Task 5).

  resources:
    experiments:
      # Keys must be unique across ALL resource types, hence the suffix (the job is also "damage_classifier").
      damage_classifier_experiment:
        name: /Users/${workspace.current_user.userName}/e2e_damage_classifier

    registered_models:
      claims_damage_level:
        catalog_name: ${var.catalog}
        schema_name: ${resources.schemas.gold.name}
        name: claims_damage_level
        comment: ResNet-18 fine-tuned to classify car damage in a photo as ok / minor / major.
        # No "aliases" here: the "prod" alias is set by the training notebook.

    jobs:
      damage_classifier:
        name: damage_classifier
        tasks:
          - task_key: prepare_images
            notebook_task:
              notebook_path: ../src/ml/01_prepare_images.py
              base_parameters:
                source_table: ${var.catalog}.${resources.schemas.silver.name}.training_images
                target_table: ${var.catalog}.${resources.schemas.silver.name}.training_images_resized
  ```

  **Why:**
  - **The experiment** is the transcript's "create MLflow experiment", defined here instead of in the UI.
  - **The registered model** is the Unity Catalog entry that every trained version goes into. Because the bundle owns it, it's created in **gold** with a comment, and it's removed with `bundle destroy`.
  - **The job** gets one task now, and more in Tasks 3–4.

- [x] **Step 1.2: Create the image-preparation notebook**

  **Do:** create `src/ml/01_prepare_images.py`:

  ```python
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
      except Exception as error:  # noqa: BLE001 - report any failure
          reachable[name] = f"NOT reachable: {type(error).__name__}: {error}"

  result = {"rows": spark.table(TARGET_TABLE).count(), **reachable}
  print(result)
  dbutils.notebook.exit(json.dumps(result))
  ```

  **Why:**
  - **The resize:** this is the transcript's "resize image UDF". ResNet was trained on 224×224 images, and the files shrink from about 1.6 MB to about 15 KB each.
  - **The reachability check** answers the biggest unknown **before** any training: can serverless download the model, and the fast CPU version of torch?
  - **`dbutils.notebook.exit(...)`** returns the result to the job run, so it can be read from the CLI.

- [x] **Step 1.3: Validate, deploy and run**

  **Do:** validate, deploy, then `env -u PYTHONPATH databricks bundle run damage_classifier --only prepare_images --profile DEFAULT`.

  *(2026-10-03: the first validate failed with `multiple resources … have been defined with the same key: damage_classifier`, because the experiment and the job shared a key. Resource keys must be unique across all types, so the experiment is now `damage_classifier_experiment`, and the Task 5 endpoint is `claims_damage_level_endpoint`.)*

  **Check:**
  - **Created:** the experiment, the registered model and the job.
  - **The run succeeds,** and its output reports `rows: 56`, plus whether Hugging Face and pytorch.org are reachable.
  - **In the Catalog:** `silver.training_images_resized` exists. Gold shows the (empty) model `claims_damage_level`, and the experiment appears under **Experiments**.

- [x] **Step 1.4: Check the files** with `git status --short`. The resources file and `src/ml/` should be new.

---

### Task 2 (only if Hugging Face is NOT reachable): Upload the base model to a volume

*Not needed: on 2026-10-03, step 1.3 reported huggingface.co and download.pytorch.org both **reachable** from serverless.*

- [ ] **Step 2.1: Download ResNet-18 on your laptop and upload it**

  **Do:**

  ```bash
  mkdir -p /tmp/resnet-18 && cd /tmp/resnet-18
  for f in config.json preprocessor_config.json model.safetensors; do
    curl -fsSLO "https://huggingface.co/microsoft/resnet-18/resolve/main/$f"
  done
  databricks fs mkdir dbfs:/Volumes/e2e_dev/dev_keqingli1129_landing/files/models/resnet-18 --profile DEFAULT
  databricks fs cp -r /tmp/resnet-18 dbfs:/Volumes/e2e_dev/dev_keqingli1129_landing/files/models/resnet-18 --profile DEFAULT
  ```

  Then, in Task 3's job task, set `base_model: /Volumes/e2e_dev/dev_keqingli1129_landing/files/models/resnet-18`.

  **Why:** `from_pretrained` accepts a **folder** as well as a Hugging Face name, so nothing else changes.

---

### Task 3: Train, track and register

**Files:**
- Create: `src/ml/02_train_and_register.py`
- Modify: `resources/damage_classifier.yml` (add a task)

- [ ] **Step 3.1: Create the training notebook**

  **Do:** create `src/ml/02_train_and_register.py`:

  ```python
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
  dbutils.widgets.text("model_name", "e2e_dev.dev_keqingli1129_gold.claims_damage_level")
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
          {"base_model": BASE_MODEL, "epochs": EPOCHS, "learning_rate": LEARNING_RATE, "batch_size": BATCH_SIZE,
           "train_images": len(train_df), "test_images": len(test_df), "training_table": TRAINING_TABLE}
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
  result = {"run_id": run.info.run_id, "model": MODEL_NAME, "version": version, "alias": "prod",
            "final_test_accuracy": accuracy}
  print(result)
  dbutils.notebook.exit(json.dumps(result))
  ```

  **Why:**
  - **`%pip install` + `restartPython()`:** the serverless stand-in for the transcript's ML cluster libraries.
  - **The processor:** it resizes and normalises images into tensors, the transcript's "neural networks work with tensors … the model expects normalized input".
  - **`ignore_mismatched_sizes`:** this swaps the 1,000-class ImageNet head for our 3 classes, which is the **fine-tuning**.
  - **`mlflow.start_run`:** logs the parameters and per-epoch metrics, the transcript's "everything is tracked".
  - **The pyfunc wrapper:** the transcript's "wrap the model so we can register it and do inference". It accepts raw bytes for batch scoring, and base64 strings for serving.
  - **`register_model` plus the alias:** a new UC model version, labelled `prod`.

- [ ] **Step 3.2: Add the training task to the job**

  **Do:** in `resources/damage_classifier.yml`, append under `tasks:`:

  ```yaml
          - task_key: train_and_register
            depends_on:
              - task_key: prepare_images
            notebook_task:
              notebook_path: ../src/ml/02_train_and_register.py
              base_parameters:
                training_table: ${var.catalog}.${resources.schemas.silver.name}.training_images_resized
                experiment_name: ${resources.experiments.damage_classifier_experiment.name}
                model_name: ${var.catalog}.${resources.schemas.gold.name}.${resources.registered_models.claims_damage_level.name}
                base_model: microsoft/resnet-18   # or the volume folder from Task 2
                epochs: "5"
  ```

- [ ] **Step 3.3: Deploy and run the training task only**

  **Do:** deploy, then `env -u PYTHONPATH databricks bundle run damage_classifier --only train_and_register --profile DEFAULT`.

  **Check:** `SUCCESS`, and the output shows `version: 1`, `alias: prod` and a `final_test_accuracy`. Expect about 5–10 minutes, mostly installing torch.

- [ ] **Step 3.4: Look at the experiment and the model**

  **Do, in the UI:**
  - **Experiments** → `e2e_damage_classifier` → the run `resnet18-finetune`. Look at its **parameters**, its **metrics charts** (train_loss falling, test_accuracy) and the **model** artifact.
  - **Catalog** → gold → **Models** → `claims_damage_level`: **version 1**, alias **prod**, linked to the run.

- [ ] **Step 3.5: Check the files** with `git status --short`.

---

### Task 4: Batch inference and the confusion matrix

**Files:**
- Create: `src/ml/03_batch_inference.py`
- Modify: `resources/damage_classifier.yml` (add a task)

- [ ] **Step 4.1: Create the batch-inference notebook**

  **Do:** create `src/ml/03_batch_inference.py`:

  ```python
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

  dbutils.widgets.text("model_name", "e2e_dev.dev_keqingli1129_gold.claims_damage_level")
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
  ```

  **Why:** `mlflow.pyfunc.spark_udf` turns the registered model into a Spark column function. That's the transcript's "register a Spark UDF … batch inference". `@prod` always picks the version the alias points to. `pandas.crosstab` builds the confusion matrix, with actual labels as rows and predictions as columns.

- [ ] **Step 4.2: Add the inference task to the job**

  **Do:** append under `tasks:`:

  ```yaml
          - task_key: batch_inference
            depends_on:
              - task_key: train_and_register
            notebook_task:
              notebook_path: ../src/ml/03_batch_inference.py
              base_parameters:
                model_name: ${var.catalog}.${resources.schemas.gold.name}.${resources.registered_models.claims_damage_level.name}
                training_table: ${var.catalog}.${resources.schemas.silver.name}.training_images_resized
                claim_images_table: ${var.catalog}.${resources.schemas.silver.name}.claim_images
                gold_schema: ${var.catalog}.${resources.schemas.gold.name}
  ```

- [ ] **Step 4.3: Deploy and run the inference task only**

  **Do:** deploy, then `env -u PYTHONPATH databricks bundle run damage_classifier --only batch_inference --profile DEFAULT`.

  **Check:**
  - **The run:** `SUCCESS`.
  - **The output:** the confusion matrix (actual × predicted for ok/minor/major), the accuracy on the training images, and `claim_images_scored: 17`.
  - **The tables:** `gold.damage_predictions` (56 rows) and `gold.claim_image_predictions` (17 rows) exist.

  **If the UDF fails because the workers can't find `torch`:** change the notebook to score on the driver. Use `mlflow.pyfunc.load_model(...)`, apply it to `toPandas()`, then write the result with `spark.createDataFrame(...)`.

- [ ] **Step 4.4: Check the files** with `git status --short`.

---

### Task 5: Real-time serving endpoint

**Files:** Modify `resources/damage_classifier.yml` (add an endpoint).

- [ ] **Step 5.1: Add the endpoint resource**

  **Do:** append to `resources/damage_classifier.yml`:

  ```yaml

    model_serving_endpoints:
      claims_damage_level_endpoint:
        name: e2e-claims-damage-level
        config:
          served_entities:
            - entity_name: ${var.catalog}.${resources.schemas.gold.name}.${resources.registered_models.claims_damage_level.name}
              entity_version: "1"
              workload_size: Small
              scale_to_zero_enabled: true
  ```

  **Why:**
  - **The resource** is the transcript's "serve this model in a real-time endpoint … specify the hardware". Here it's **CPU, Small**, because Free Edition has no GPU serving.
  - **Scale-to-zero:** it costs nothing while idle.
  - **A fixed version:** endpoints need a version number, not an alias.

- [ ] **Step 5.2: Deploy and wait until the endpoint is ready**

  **Do:** deploy. Then poll with `databricks serving-endpoints get e2e-claims-damage-level --profile DEFAULT` (in dev, the name may carry a `dev-…` prefix; `bundle summary` shows it) until `state.ready` is `READY`. The first container build takes about 10–20 minutes.

- [ ] **Step 5.3: Send it one photo**

  **Do:** base64-encode one claim photo and query the endpoint:

  ```bash
  B64=$(base64 -w0 data/object_storage/claims/images/1_High.jpg)
  echo "{\"dataframe_records\": [{\"content\": \"$B64\"}]}" > /tmp/request.json
  databricks serving-endpoints query <endpoint-name> --json @/tmp/request.json --profile DEFAULT
  ```

  **Check:** the answer is `{"predictions": ["ok" | "minor" | "major"]}`. That's real-time inference, the input part 6's app will use. If the first call times out while the endpoint wakes from zero, repeat it.

- [ ] **Step 5.4: Check the files** with `git status --short`.

---

### Task 6: Update CLAUDE.md

- [ ] **Step 6.1: Document part 5**

  **Do:**
  1. Add to the Commands block:

     ```bash
     databricks bundle run damage_classifier --only <prepare_images|train_and_register|batch_inference> --profile <p>   # ML steps
     ```

  2. Add an Architecture bullet covering:
     - the experiment, the registered model `gold.claims_damage_level` (alias `prod` set by code) and the job's 3 notebook tasks in `src/ml/`
     - the pinned library versions and the CPU wheel index
     - the pyfunc input (a binary `content` column, or base64 strings when served)
     - that each training run creates a new version, and the endpoint is pinned in YAML
     - the Free Edition limits that shaped the design: no GPU, restricted internet, CPU serving only

  **Check:** `git status --short` shows `CLAUDE.md` modified.

---

### Task 7 (later): Option B, train on your laptop

To be planned when Tasks 1–6 are done. The idea:

- **Same code, different machine:** reuse the training logic from `02_train_and_register.py` as a local script, run with `uv run`. It reads `silver.training_images_resized` through Databricks Connect, and trains on your laptop's CPU, which gives full internet access and uses no Databricks quota.
- **Same destination:** it logs to the **same** MLflow experiment, and registers a new version of the **same** UC model, using `mlflow.set_tracking_uri("databricks")`.
- **Local packages:** torch and transformers would go into a separate, optional `ml` dependency group, so the normal `uv sync` stays small.
