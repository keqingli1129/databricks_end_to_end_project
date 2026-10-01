# Part 3: object-storage ingestion with Auto Loader (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. Steps use checkboxes (`- [ ]`). **No automated tests**, as in part 2.

**Goal:** ingest images and CSV files from landing volumes into bronze with Auto Loader. Show schema evolution (`addNewColumns` → `rescue`) and archiving with `cleanSource`, as in [transcript_3.txt](transcript_3.txt).

**Design:** [part3_object_storage_design.md](part3_object_storage_design.md).

**Tech stack:**

- Declarative Automation Bundles
- Unity Catalog managed volumes
- Lakeflow Declarative Pipelines with Auto Loader (`cloudFiles`)
- a serverless notebook job using plain PySpark Structured Streaming

## Global constraints

- **No new catalogs:** add only volumes, in landing. dev landing: `e2e_dev.dev_keqingli1129_landing`.
- **Keep the images out of git:** `data/` is gitignored.
- **Profile `DEFAULT`, target `dev`,** and run local commands as `env -u PYTHONPATH …`.
- **No tests** and no Claude commits.

**Paths used below (dev):**

| Name | Path |
|---|---|
| training volume | `/Volumes/e2e_dev/dev_keqingli1129_landing/training_images` |
| claims volume | `/Volumes/e2e_dev/dev_keqingli1129_landing/claims` |
| CLI form (needs the `dbfs:` prefix) | `dbfs:/Volumes/e2e_dev/dev_keqingli1129_landing/…` |

---

### Task 1: Prepare the files locally

**Files:**
- Modify: `.gitignore`
- Create: `src/object_storage/prepare_files.py`

- [x] **Step 1.1: Keep `data/` out of git** *(done by you before the plan started: `.gitignore` already has `data`)*

  **Do:** add one line to the end of `.gitignore`:

  ```text
  data/
  ```

  **Check:** `git status --short` no longer lists `data/`.

  **Why:** 112 MB of images would stay in the git history for good, even if deleted later.

- [x] **Step 1.2: Write the prepare script**

  **Do:** create `src/object_storage/prepare_files.py`:

  ```python
  """Prepare part 3 upload files from data/object_storage/ (run locally, stdlib only).

  1. Rewrites image_metadata.csv so claim_no / chassis_no match part 2's source data:
     row n -> claim CLM%08d(n); chassis = CHS%06d of that claim's policy, POL = (n * 7919) % 12000 + 1.
  2. Writes two one-row CSVs for the schema-evolution demo (new_column_1, then new_column_2).
  The original CSV is not modified.
  """

  import csv
  from pathlib import Path

  DATA = Path(__file__).resolve().parents[2] / "data" / "object_storage"
  SOURCE_CSV = DATA / "claims" / "metadata" / "image_metadata.csv"
  PREPARED = DATA / "prepared"

  N_CLAIMS = 13_000  # part 2: claims CLM00000001..CLM00013000
  N_POLICIES = 12_000  # part 2: policies POL0000001..POL0012000
  COLUMNS = ["image_name", "image_id", "claim_no", "chassis_no"]


  def policy_number(claim_n: int) -> int:
      """Same formula as src/source_database/seed.sql: which policy claim n belongs to."""
      return (claim_n * 7919) % N_POLICIES + 1


  def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
      path.parent.mkdir(parents=True, exist_ok=True)
      with path.open("w", newline="") as f:
          writer = csv.DictWriter(f, fieldnames=columns)
          writer.writeheader()
          writer.writerows(rows)
      print(f"wrote {len(rows):>6} rows -> {path.relative_to(DATA.parent.parent)}")


  def main() -> None:
      with SOURCE_CSV.open(newline="") as f:
          original = list(csv.DictReader(f))

      rows = [
          {
              "image_name": row["image_name"],
              "image_id": row["image_id"],
              "claim_no": f"CLM{n:08d}",
              "chassis_no": f"CHS{policy_number(n):06d}",
          }
          for n, row in enumerate(original[:N_CLAIMS], start=1)
      ]
      write_csv(PREPARED / "claims_metadata" / "image_metadata.csv", COLUMNS, rows)

      # Schema-evolution demo: same shape as the real rows, plus extra columns.
      write_csv(
          PREPARED / "schema_evolution" / "image_metadata_new_column_1.csv",
          COLUMNS + ["new_column_1"],
          [{**rows[0], "new_column_1": "new column value 1"}],
      )
      write_csv(
          PREPARED / "schema_evolution" / "image_metadata_new_column_2.csv",
          COLUMNS + ["new_column_1", "new_column_2"],
          [{**rows[1], "new_column_1": "new column value 1", "new_column_2": "new column value 2"}],
      )


  if __name__ == "__main__":
      main()
  ```

  **Why:** this is option A. The IDs match part 2, so later parts can join claim → image → policy → telematics. The two small CSVs are the transcript's "uploaded record with a new column".

- [x] **Step 1.3: Run it**

  **Do:** `env -u PYTHONPATH uv run python src/object_storage/prepare_files.py`

  **Check:** three "wrote" lines, with 13000, 1 and 1 rows. Then `head -3 data/object_storage/prepared/claims_metadata/image_metadata.csv` shows `CLM00000001,CHS007920` and so on. Claim 1's policy is `POL0007920`, which matches part 2's `source.claim`.

- [x] **Step 1.4: Check the files** with `git status --short`. You should see `.gitignore` modified and `src/object_storage/` new, but **not** `data/`.

---

### Task 2: Landing volumes and the upload

**Files:** Modify `resources/databricks_end_to_end_project.yml` (the `volumes:` block).

- [x] **Step 2.1: Add the two volumes**

  **Do:** under `volumes:`, after `landing_files`, add:

  ```yaml

      # Part 3 (see docs/transcript_3.txt): files ingested with Auto Loader.
      claims_volume:
        catalog_name: ${var.catalog}
        schema_name: ${resources.schemas.landing.name}
        name: claims
        volume_type: MANAGED
        comment: Claim photos (images/), their metadata CSVs (metadata/), and the cleanSource archive (archive/)
      training_images_volume:
        catalog_name: ${var.catalog}
        schema_name: ${resources.schemas.landing.name}
        name: training_images
        volume_type: MANAGED
        comment: Labelled car-damage images (ok / minor / major) for training the classifier later
  ```

  **Why:** these are the transcript's two landing volumes. The keys end in `_volume`, so they can't be confused with schema or table names.

- [x] **Step 2.2: Validate and deploy**

  **Do:** validate, then deploy.

  **Check:** `Created volumes.claims_volume` and `volumes.training_images_volume`.

- [x] **Step 2.3: Upload the files**

  **Do:**

  ```bash
  V=dbfs:/Volumes/e2e_dev/dev_keqingli1129_landing
  databricks fs cp -r data/object_storage/training_images        $V/training_images                   --profile DEFAULT
  databricks fs cp -r data/object_storage/claims/images          $V/claims/images                     --profile DEFAULT
  databricks fs mkdir $V/claims/metadata --profile DEFAULT   # a single-file cp needs the target folder to exist
  databricks fs cp    data/object_storage/prepared/claims_metadata/image_metadata.csv $V/claims/metadata/image_metadata.csv --profile DEFAULT
  databricks fs mkdir $V/claims/archive --profile DEFAULT
  ```

  **Why:** this plays the role of "someone pushes their files into your bucket". Only the **prepared** CSV is uploaded, never the original. The `archive/` folder is created empty, as in the video.

- [x] **Step 2.4: Check the uploads**

  **Do:** `databricks fs ls $V/training_images --profile DEFAULT | wc -l` (should be 56) and `databricks fs ls $V/claims/images --profile DEFAULT | wc -l` (should be 15). Also look in the Catalog UI: landing → Volumes → `claims` should show `images/`, `metadata/` and `archive/`.

- [x] **Step 2.5: Check the files** with `git status --short`. Only the resources YAML should be modified.

---

### Task 3: Pipeline with the minimal Auto Loader table (`training_images`)

**Files:**
- Create: `resources/object_storage_ingestion.pipeline.yml`
- Create: `src/object_storage_ingestion/transformations/training_images.py`

- [x] **Step 3.1: Create the pipeline resource**

  **Do:** create `resources/object_storage_ingestion.pipeline.yml`:

  ```yaml
  # Object-storage ingestion (see docs/transcript_3.txt): Auto Loader from landing volumes into bronze.

  resources:
    pipelines:
      object_storage_ingestion:
        name: object_storage_ingestion
        catalog: ${var.catalog}
        schema: ${resources.schemas.bronze.name}
        serverless: true
        # root_path is put on sys.path by the pipeline (see part 1, step 9.4).
        root_path: "../src"

        configuration:
          object_storage.training_images_path: /Volumes/${var.catalog}/${resources.schemas.landing.name}/${resources.volumes.training_images_volume.name}
          object_storage.claim_metadata_path: /Volumes/${var.catalog}/${resources.schemas.landing.name}/${resources.volumes.claims_volume.name}/metadata

        libraries:
          - glob:
              include: ../src/object_storage_ingestion/transformations/**
  ```

  **Why:** this is the transcript's new "ingest object storage" ETL pipeline, writing to bronze. The volume paths come from the resources, so nothing is hardcoded.

- [x] **Step 3.2: Create `training_images.py`**

  **Do:** create `src/object_storage_ingestion/transformations/training_images.py`:

  ```python
  from pyspark import pipelines as dp

  SOURCE_PATH = spark.conf.get("object_storage.training_images_path")


  @dp.table(
      comment="Car-damage training images (ok / minor / major in the file name), ingested as binary files.",
      table_properties={"quality": "bronze"},
  )
  def training_images():
      return spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile").load(SOURCE_PATH)
  ```

  **Why:** it's the transcript's "most minimal implementation": `cloudFiles` (Auto Loader) plus `binaryFile`, because these are images. There's no checkpoint or schema location, because the pipeline handles both.

- [x] **Step 3.3: Validate, deploy and run**

  **Do:** validate, deploy, then `env -u PYTHONPATH databricks bundle run object_storage_ingestion --profile DEFAULT`.

  **Check:** the update completes, and `training_images` reports **56** written records.

- [x] **Step 3.4: Look at the table**

  **Do:** `SELECT path, modificationTime, length FROM e2e_dev.dev_keqingli1129_bronze.training_images LIMIT 5;`

  **Check:** the columns are `path`, `modificationTime`, `length` and `content` (the image bytes), as in the transcript. The labels are visible in `path`.

- [x] **Step 3.5: Check the files** with `git status --short`. The pipeline YAML and `src/object_storage_ingestion/` should be new.

---

### Task 4: CSV metadata with schema evolution, part 1 (`addNewColumns`)

**Files:** Create `src/object_storage_ingestion/transformations/claim_images_metadata.py`.

- [ ] **Step 4.1: Create `claim_images_metadata.py`**

  **Do:** create the file:

  ```python
  from pyspark import pipelines as dp

  SOURCE_PATH = spark.conf.get("object_storage.claim_metadata_path")


  @dp.table(
      comment="Metadata of customer-uploaded claim images: which image belongs to which claim and car.",
      table_properties={"quality": "bronze"},
  )
  def claim_images_metadata():
      return (
          spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          # Default mode, written out to make it visible: new columns are added to the table schema.
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
          .load(SOURCE_PATH)
      )
  ```

- [ ] **Step 4.2: Deploy and run**

  **Do:** deploy, then run the pipeline.

  **Check:** `claim_images_metadata` writes **13,000** records, and `training_images` writes **0**, because there are no new files.

- [ ] **Step 4.3: Upload a CSV with a new column (in the UI, like the transcript)**

  **Do:** Catalog → `e2e_dev` → `dev_keqingli1129_landing` → Volumes → `claims` → `metadata` → **Upload to this volume**. Choose `data/object_storage/prepared/schema_evolution/image_metadata_new_column_1.csv`.

  Or use the CLI: `databricks fs cp data/object_storage/prepared/schema_evolution/image_metadata_new_column_1.csv $V/claims/metadata/ --profile DEFAULT`

- [ ] **Step 4.4: Run again and see the new column**

  **Do:** run the pipeline.

  **Check:** 1 record written, because Auto Loader skips the already-read file. The flow may restart once while it adds the column. Then run:

  ```sql
  SELECT claim_no, new_column_1 FROM e2e_dev.dev_keqingli1129_bronze.claim_images_metadata
  WHERE new_column_1 IS NOT NULL;
  ```

  You should see 1 row with `new column value 1`. The other 13,000 rows have `new_column_1 = NULL`.

---

### Task 5: Schema evolution, part 2 (`rescue`)

**Files:** Modify `src/object_storage_ingestion/transformations/claim_images_metadata.py` (Task 4).

- [ ] **Step 5.1: Switch to rescue mode**

  **Do:** in `claim_images_metadata.py`, replace the comment line and the option with:

  ```python
          # Rescue mode: the table schema stays fixed; values of unknown columns go into _rescued_data.
          .option("cloudFiles.schemaEvolutionMode", "rescue")
  ```

- [ ] **Step 5.2: Deploy, upload the second CSV and run**

  **Do:**
  1. Deploy.
  2. Upload `image_metadata_new_column_2.csv` to `claims/metadata/`, in the UI or with `fs cp`.
  3. Run the pipeline.

  **Check:** 1 record written. **No `new_column_2` column** appears in the table.

- [ ] **Step 5.3: See the rescued data**

  **Do:**

  ```sql
  SELECT claim_no, new_column_1, _rescued_data FROM e2e_dev.dev_keqingli1129_bronze.claim_images_metadata
  WHERE _rescued_data IS NOT NULL;
  ```

  **Check:** 1 row. `_rescued_data` holds JSON with `new_column_2`, something like `{"new_column_2":"new column value 2", "_file_path": …}`. `new_column_1` is a normal column, because it already existed.

- [ ] **Step 5.4: Check the files** with `git status --short`. `claim_images_metadata.py` should be new.

---

### Task 6: Claim images with plain PySpark and `cleanSource` archiving

**Files:**
- Create: `src/object_storage/claim_images.py` (a notebook)
- Create: `resources/ingest_claim_images.job.yml`

- [ ] **Step 6.1: Create the notebook**

  **Do:** create `src/object_storage/claim_images.py`:

  ```python
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
  ```

  **Why:** this is the transcript's notebook. The three `cleanSource` settings do the archiving. `availableNow` is the transcript's "batch mode, not a continuously running stream". The last cell shows where the files are.

- [ ] **Step 6.2: Create the job**

  **Do:** create `resources/ingest_claim_images.job.yml`:

  ```yaml
  # Runs the claim-images notebook (Auto Loader + cleanSource archiving). No schedule.
  # Start it with: databricks bundle run ingest_claim_images

  resources:
    jobs:
      ingest_claim_images:
        name: ingest_claim_images

        tasks:
          - task_key: ingest
            notebook_task:
              notebook_path: ../src/object_storage/claim_images.py
              base_parameters:
                claims_volume_path: /Volumes/${var.catalog}/${resources.schemas.landing.name}/${resources.volumes.claims_volume.name}
                target_table: ${var.catalog}.${resources.schemas.bronze.name}.claim_images
  ```

  **Why:** the parameters replace the widget defaults with the target's real names. With no compute settings, the task runs on serverless.

- [ ] **Step 6.3: Validate, deploy and run 1 (ingest)**

  **Do:** validate, deploy, then `env -u PYTHONPATH databricks bundle run ingest_claim_images --profile DEFAULT`.

  **Check:** `TERMINATED SUCCESS`. The last cell prints: rows **15**, images/ **15**, archive/ **0**. Nothing is moved yet, because the docs say the earliest cleanup is two runs later.

- [ ] **Step 6.4: Run 2 (with one new file)**

  **Do:** wait **at least 1 minute** for the retention. Upload one new image, for example a copy: `databricks fs cp data/object_storage/claims/images/1_High.jpg $V/claims/images/6_High.jpg --profile DEFAULT`. Run the job again.

  **Check:** rows **16**. The archive may still be **0**, because this run only "commits" the first 15 files.

- [ ] **Step 6.5: Run 3 (with another new file) and see the archive**

  **Do:** wait at least 1 minute. Upload `…/1_Low.jpg` as `$V/claims/images/6_Low.jpg`. Run the job.

  **Check:** rows **17**, and **archive/ holds the first files** (up to 15, moved there). images/ holds only the newest ones. In the Catalog UI, `claims/archive/` now contains the photos, as in the transcript.

  **If archive/ is still 0:** repeat this step once, with one more new file. Cleanup only runs while new files are being processed.

- [ ] **Step 6.6: Check the files** with `git status --short`. The notebook and job YAML should be new.

---

### Task 7: Update CLAUDE.md

**Files:** Modify `CLAUDE.md`.

- [ ] **Step 7.1: Document part 3**

  **Do:**
  1. Add to the Commands block:

     ```bash
     uv run python src/object_storage/prepare_files.py         # rewrite image metadata IDs to part 2's + make schema-demo CSVs (local)
     databricks bundle run object_storage_ingestion --profile <p>   # Auto Loader: training_images, claim_images_metadata
     databricks bundle run ingest_claim_images --profile <p>        # notebook: claim_images + cleanSource archive
     ```

  2. Add an Architecture bullet:

     ```markdown
     - **Object-storage ingestion** (docs/part3_object_storage_*.md, source material docs/transcript_3.txt): managed volumes `claims` (`images/`, `metadata/`, `archive/`, `_autoloader/`) and `training_images` in landing (resource keys `claims_volume`, `training_images_volume`). Raw files live locally in `data/object_storage/` (gitignored); `src/object_storage/prepare_files.py` rewrites the metadata CSV's `claim_no`/`chassis_no` to part 2's `CLM…`/`CHS…` IDs and writes the schema-evolution demo CSVs into `data/object_storage/prepared/`. Pipeline `object_storage_ingestion` (bronze): `training_images` (cloudFiles binaryFile) and `claim_images_metadata` (cloudFiles csv, `schemaEvolutionMode=rescue` → unknown columns in `_rescued_data`). Job `ingest_claim_images` runs the plain-PySpark notebook `src/object_storage/claim_images.py`: cloudFiles binaryFile → `bronze.claim_images` (a regular Delta table, not pipeline-owned) with `cleanSource=MOVE` to `claims/archive/` (1-minute retention; cleanup only happens while new files are processed, earliest two runs after ingestion).
     ```

  **Check:** `git status --short` shows `CLAUDE.md` modified.
