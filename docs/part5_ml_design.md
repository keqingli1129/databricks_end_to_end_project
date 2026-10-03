# Part 5: damage-severity classifier with MLflow (design)

This design follows [transcript_5.txt](transcript_5.txt):

- **Train:** fine-tune a pre-trained ResNet on the labelled car-damage photos.
- **Track:** record the run in an **MLflow experiment**.
- **Register:** store the model in the **Unity Catalog model registry** (gold), with alias `prod`.
- **Batch inference:** run it with a Spark UDF and draw a confusion matrix.
- **Real-time inference:** serve it from a **Model Serving endpoint**.

## Free Edition constraints and decisions

| Topic | Free Edition | Decision |
|---|---|---|
| ML runtime cluster | no custom compute | **serverless notebooks**, with `%pip install` for torch, transformers and mlflow |
| GPU | none (limited serverless GPU only after LinkedIn verification) | train on **CPU**; small model, few epochs |
| internet from serverless | "restricted to a limited set of trusted domains" | `01_prepare_images` **checks** huggingface.co and download.pytorch.org. If Hugging Face is blocked, the model files are downloaded on the laptop and uploaded to a volume, and the training notebook takes the model location as a parameter. |
| model serving | CPU custom models allowed; limited number of active endpoints; no GPU | one **CPU, Small, scale-to-zero** endpoint |
| compute quota | exceeding it shuts compute down for the day | 5 epochs, ResNet-18 |

**Your choices:**

- **Training location:** in Databricks first (A); on your laptop later (B, a follow-up task).
- **Base model:** **`microsoft/resnet-18`**.
- **Serving endpoint:** **create it now**, as a bundle resource.
- **No automated tests,** and no Claude commits. Profile `DEFAULT`, target `dev`.

## Architecture

```text
silver.training_images (56: ok 17 / minor 21 / major 18)
  └─ job damage_classifier (serverless notebook tasks, src/ml/)
       ├─ prepare_images      01_prepare_images.py      → silver.training_images_resized (224×224 JPEG)
       │                                                   + prints reachability of huggingface.co / download.pytorch.org
       ├─ train_and_register  02_train_and_register.py  → MLflow run in the experiment (params, loss/accuracy per epoch, pyfunc model)
       │                                                   → UC model <catalog>.<gold>.claims_damage_level, new version, alias "prod"
       └─ batch_inference     03_batch_inference.py     → gold.damage_predictions (training images + prediction) + confusion matrix
                                                           → gold.claim_image_predictions (the 17 customer claim photos)
endpoint (bundle resource): serves claims_damage_level version 1 on CPU, Small, scale-to-zero
```

## The model

- **Base:** `transformers.AutoModelForImageClassification.from_pretrained(base_model, num_labels=3, id2label, label2id, ignore_mismatched_sizes=True)`. A new 3-class head replaces the 1,000-class ImageNet head.
- **Data:** `silver.training_images_resized`, loaded into pandas (56 small images). Random **80/20** split, seed 42, so about 45 train and 11 test images. Images go through `AutoImageProcessor` (resize, tensor, ImageNet normalisation), which is the transcript's "tensors … normalized".
- **Training:** a plain PyTorch loop: AdamW, learning rate 5e-5, batch 8, 5 epochs. No `Trainer`, so there are fewer dependencies.
- **MLflow logging:** parameters (base model, epochs, learning rate, batch size, image counts), per-epoch `train_loss` and `test_accuracy`, and the model.
- **Model format:** an **MLflow pyfunc** class `DamageClassifier`. It takes a DataFrame with a **binary** column `content` (image bytes), or base64 strings when called through serving, and returns the label `ok`, `minor` or `major`. The fine-tuned weights and processor are logged as artifacts. Its pip requirements are `torch`, `transformers` and `pillow`.
- **Registration:** `mlflow.set_registry_uri("databricks-uc")`, `mlflow.register_model(...)` into the bundle-defined registered model, then `MlflowClient().set_registered_model_alias(name, "prod", version)`.

## Units

| File | Responsibility |
|---|---|
| `resources/damage_classifier.yml` | the **experiment** (`/Users/<you>/e2e_damage_classifier`), the **registered model** `claims_damage_level` in gold (no `aliases` in YAML; the alias is set by code), the **job** `damage_classifier` (notebook tasks added one per task), and later the **serving endpoint** |
| `src/ml/01_prepare_images.py` | widgets `source_table` and `target_table`; a Pillow UDF to resize to 224×224 JPEG; overwrites the target table; reachability check |
| `src/ml/02_train_and_register.py` | `%pip install`; widgets `training_table`, `experiment_name`, `model_name`, `base_model`, `epochs`; trains, logs, registers, sets the alias |
| `src/ml/03_batch_inference.py` | `%pip install`; widgets `model_name`, `training_table`, `claim_images_table`, `gold_schema`; `mlflow.pyfunc.spark_udf(..., env_manager="local")`; writes 2 gold tables; prints a confusion matrix (`pandas.crosstab`) |

## Risks and expected behaviour

- **Every run of `train_and_register` creates a new model version** and moves `prod` to it. The endpoint is pinned to **version 1** in YAML. To serve a newer version, change `entity_version` and redeploy.
- **The first endpoint deployment builds a container** with torch, taking about 10–20 minutes. A scale-to-zero endpoint takes about a minute to answer the first request after being idle.
- **Accuracy:** with 56 images, expect roughly the transcript's "it did okay", with some confusion between minor and ok. `damage_predictions` scores the **training** images, as the transcript does, so it's an optimistic view. The 11 held-out test images give the honest `test_accuracy` in MLflow.
- **`env_manager="local"`** means the UDF uses the notebook's installed libraries. If serverless workers don't see the `%pip` libraries, the fallback is to score with the driver: `toPandas()`, call the model directly, then `createDataFrame`.
