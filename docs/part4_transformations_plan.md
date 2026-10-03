# Part 4: silver and gold transformations and orchestration (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. Steps use checkboxes (`- [ ]`). **No automated tests.**

**Goal:**

- **Silver:** clean all bronze tables, with data quality checks.
- **Gold:** build aggregated and joined materialized views, with geopy added to the pipeline environment.
- **Lineage:** check it in Unity Catalog.
- **Orchestration:** run everything end to end with a scheduled (paused) Lakeflow Job, as in [transcript_4.txt](transcript_4.txt).

**Design:** [part4_transformations_design.md](part4_transformations_design.md).

**Tech stack:**

- Lakeflow Declarative Pipelines: materialized views, streaming tables and expectations
- the pipeline `environment`, for geopy
- Lakeflow Jobs: `pipeline_task`, `run_job_task`, `depends_on`/`run_if` and a schedule
- Declarative Automation Bundles

## Global constraints

- **Schemas (dev):** bronze is `e2e_dev.dev_keqingli1129_bronze`, silver is `…_silver`, and gold is `…_gold`. No new schemas or catalogs.
- **Profile `DEFAULT`, target `dev`,** and run local commands as `env -u PYTHONPATH …`.
- **Silver reads of the AUTO CDC tables** (claim, policy, customer) must be **batch** reads, as materialized views. A streaming read breaks on their updates and deletes.
- **No tests** and no Claude commits.

---

### Task 1: The `transformations` pipeline with the first silver table (`claim`)

**Files:**
- Create: `resources/transformations.pipeline.yml`
- Create: `src/transformations/transformations/bronze_to_silver.py`

- [x] **Step 1.1: Create the pipeline resource**

  **Do:** create `resources/transformations.pipeline.yml`:

  ```yaml
  # Transformations (see docs/transcript_4.txt): bronze -> silver (cleaning + data quality) -> gold (aggregates, joins).

  resources:
    pipelines:
      transformations:
        # Not just "transformations": the smart_claims_dev bundle already has a pipeline with that name.
        name: e2e_transformations
        catalog: ${var.catalog}
        schema: ${resources.schemas.silver.name}
        serverless: true
        # root_path is put on sys.path by the pipeline (see part 1, step 9.4).
        root_path: "../src"

        configuration:
          transformations.bronze_schema: ${var.catalog}.${resources.schemas.bronze.name}
          transformations.gold_schema: ${var.catalog}.${resources.schemas.gold.name}

        libraries:
          - glob:
              include: ../src/transformations/transformations/**
  ```

  **Why:** this is the transcript's "transformations" pipeline, with **silver** as the default schema. The bronze and gold schema names come in as configuration, so nothing is hardcoded. The transcript notes that its hardcoded names "could be abstracted into pipeline configurations", and this does that.

- [x] **Step 1.2: Create `bronze_to_silver.py` with the claim table**

  **Do:** create `src/transformations/transformations/bronze_to_silver.py`:

  ```python
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
  ```

  **Why:**
  - **`@dp.materialized_view`:** the table is recomputed from a **batch** read of bronze, so updates and deletes from part 2 are reflected. A stream would fail on them.
  - **`@dp.expect_all_or_drop({...})`:** two data quality checks. Rows that fail **any** of them are **dropped** and counted. The alternatives are `expect_all` (warn only) and `expect_all_or_fail` (stop the pipeline).
  - **`severity_level`:** turns the text severity into a number, 1–4, which later analysis and the ML part can compare.

- [x] **Step 1.3: Validate, deploy and run**

  **Do:** validate, deploy, then `env -u PYTHONPATH databricks bundle run transformations --profile DEFAULT`.

  **Check:** `Created pipelines.transformations`, then `Update … COMPLETED`. *(2026-10-02: the first deploy failed with `The pipeline name '[dev keqingli1129] transformations' is already used by another pipeline`, from the `smart_claims_dev` bundle. Fixed by naming it `e2e_transformations`. The resource key stays `transformations`.)* `e2e_dev.dev_keqingli1129_silver.claim` has **13,000** rows, with `severity_level` filled in.

- [x] **Step 1.4: Look at the data quality results**

  **Do:** in the pipeline UI, open the latest update. In the bottom panel's **Table metrics** tab, click the **`2 met`** link in the **Expectations** column of the `claim` row. There's no separate data-quality tab in the pipeline UI. The table's **Quality** tab in the Catalog is a different feature: Data Quality Monitoring.

  **Check:** `valid_claim_no` and `valid_claim_amount` both pass for **100%** of rows, with 0 dropped, as in the transcript.

  **The same numbers in SQL**, from the pipeline's event log. Use the pipeline ID from `databricks bundle summary`:

  ```sql
  SELECT origin.flow_name,
         details:flow_progress:metrics:num_output_rows       AS output_rows,
         details:flow_progress:data_quality:dropped_records  AS dropped,
         details:flow_progress:data_quality:expectations     AS expectations
  FROM event_log('<pipeline id>')
  WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
  ORDER BY timestamp DESC;
  ```

  *(`databricks pipelines list-pipeline-events` doesn't include these details. The `event_log()` SQL function does.)*

- [x] **Step 1.5: Check the files** with `git status --short`. The pipeline YAML and `src/transformations/` should be new.

---

### Task 2: The remaining silver tables

**Files:** Modify `src/transformations/transformations/bronze_to_silver.py` (Task 1).

- [x] **Step 2.1: Add policy and customer (materialized views)**

  **Do:** append to `bronze_to_silver.py`:

  ```python


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
  ```

  **Why:** `abs(premium)` is the transcript's fix for negative premiums. For customers, the transcript **splits** a combined name. Our bronze already has the parts, so we **build** `full_name` instead, and add `age`.

- [x] **Step 2.2: Add the streaming silver tables**

  **Do:** append to `bronze_to_silver.py`:

  ```python


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
  ```

  **Why:**
  - **Telematics finally get **real types**:** part 1 kept them as strings for bronze, and the speed check runs on the **cast** value.
  - **The training image label** moves from the file name into its own column, which the ML part will need.
  - **`_rescued_data` and the demo column** are dropped, as the transcript does.

- [x] **Step 2.3: Deploy and run**

  **Do:** deploy, then run `transformations`.

  **Check:** the graph shows 7 silver tables. Row counts:

  | Table | Rows |
  |---|---|
  | claim | 13,000 |
  | policy | 12,001 |
  | customer | 6,999 |
  | telematics | 60 |
  | training_images | 56 |
  | claim_images | 17 |
  | claim_images_metadata | 13,002 |

  All checks should show 100% passed.

- [ ] **Step 2.4 (optional): See a check drop a bad row** *(skipped on 2026-10-02, can be done any time)*

  **Do:**
  1. In the SQL editor, insert a claim with a negative amount at the **source**:

     ```sql
     INSERT INTO e2e_dev.dev_keqingli1129_source.claim VALUES
       ('CLM99999999', 'POL0000001', DATE'2026-09-30', 'COLLISION', 'Minor Damage', -50.00, current_timestamp());
     ```

  2. Run `cdc_ingestion`, then `transformations`.

  **Check:**
  - **Bronze has it:** `bronze.claim` has `CLM99999999`, because bronze keeps data as it arrived.
  - **Silver doesn't:** `silver.claim` **doesn't** have it.
  - **The UI shows the drop:** the `valid_claim_amount` check in the pipeline UI shows **1 dropped** record.
  - **Clean up afterwards** if you want: `DELETE FROM e2e_dev.dev_keqingli1129_source.claim WHERE claim_no = 'CLM99999999';`, then run both pipelines again.

- [x] **Step 2.5: Check the files** with `git status --short`. `bronze_to_silver.py` should be modified.

---

### Task 3: Gold tables (the first run fails on purpose)

**Files:** Create `src/transformations/transformations/silver_to_gold.py`.

- [x] **Step 3.1: Create `silver_to_gold.py`**

  **Do:** create the file:

  ```python
  """Silver -> gold: aggregates and pre-joined materialized views for BI and apps (see docs/transcript_4.txt)."""

  from geopy.distance import geodesic
  from pyspark import pipelines as dp
  from pyspark.sql import functions as F

  GOLD = spark.conf.get("transformations.gold_schema")  # e.g. e2e_dev.dev_keqingli1129_gold
  GOLD_PROPERTIES = {"quality": "gold"}
  HOUSTON_CENTER = (29.7604, -95.3698)


  @F.udf("double")
  def distance_from_city_center_km(latitude, longitude):
      """Great-circle distance (geopy, offline) from a point to downtown Houston."""
      if latitude is None or longitude is None:
          return None
      return float(geodesic((latitude, longitude), HOUSTON_CENTER).km)


  @dp.materialized_view(
      name=f"{GOLD}.aggregated_telematics",
      comment="Telematics per car: speed statistics, average location and its distance to the city center.",
      table_properties=GOLD_PROPERTIES,
  )
  def aggregated_telematics():
      return (
          spark.read.table("telematics")
          .groupBy("chassis_number")
          .agg(
              F.avg("speed").alias("avg_speed"),
              F.max("speed").alias("max_speed"),
              F.avg("latitude").alias("avg_latitude"),
              F.avg("longitude").alias("avg_longitude"),
              F.count("*").alias("event_count"),
              F.min("event_timestamp").alias("first_event_at"),
              F.max("event_timestamp").alias("last_event_at"),
          )
          .withColumn("distance_from_city_center_km", distance_from_city_center_km("avg_latitude", "avg_longitude"))
      )


  @dp.materialized_view(
      name=f"{GOLD}.customer_claim_policy",
      comment="Each claim with its policy and customer.",
      table_properties=GOLD_PROPERTIES,
  )
  def customer_claim_policy():
      claims = spark.read.table("claim")
      policies = spark.read.table("policy")
      customers = spark.read.table("customer")
      return claims.join(policies, "policy_no").join(customers, "customer_id")


  @dp.materialized_view(
      name=f"{GOLD}.customer_claim_policy_telematics",
      comment="Claims with policy, customer and (where available) the car's aggregated telematics.",
      table_properties=GOLD_PROPERTIES,
  )
  def customer_claim_policy_telematics():
      return spark.read.table(f"{GOLD}.customer_claim_policy").join(
          spark.read.table(f"{GOLD}.aggregated_telematics"), "chassis_number", "left"
      )
  ```

  **Why:**
  - **The aggregate** reduces many small telematics events to one row per car, the transcript's point about streaming data.
  - **The two joins** are the transcript's pre-joined gold tables, kept up to date incrementally as materialized views.
  - **`from geopy.distance import geodesic` at the top** is what will fail next, because the library isn't installed yet.

- [x] **Step 3.2: Deploy and run, and see the failure**

  **Do:** deploy, then run `transformations`.

  **Check:** the update **fails** with `ModuleNotFoundError: No module named 'geopy'`, in `silver_to_gold.py`. This is the transcript's "pipeline failed as I don't have this geopy library installed on the serverless cluster". Nothing in silver or gold changes.

---

### Task 4: Add geopy to the pipeline environment

**Files:** Modify `resources/transformations.pipeline.yml` (Task 1).

- [x] **Step 4.1: Add the environment**

  **Do:** in `resources/transformations.pipeline.yml`, after the `libraries:` block, add:

  ```yaml

        environment:
          dependencies:
            # Pipeline-only library (see CLAUDE.md): used offline for distances in gold.aggregated_telematics.
            - geopy==2.4.1
  ```

  **Why:** this is the transcript's "go into the environment and add this dependency". In a bundle, it lives in the pipeline YAML. `geopy` is used only by this pipeline, so it goes here and not in `pyproject.toml`, as `CLAUDE.md` advises.

- [x] **Step 4.2: Deploy and run**

  **Do:** deploy, then run `transformations`.

  **Check:** `Update … COMPLETED`. The graph shows the 7 silver tables feeding the 3 gold views.

- [x] **Step 4.3: Look at gold**

  **Do:** in the SQL editor:

  ```sql
  SELECT * FROM e2e_dev.dev_keqingli1129_gold.aggregated_telematics ORDER BY chassis_number;
  SELECT count(*) FROM e2e_dev.dev_keqingli1129_gold.customer_claim_policy;
  SELECT claim_no, chassis_number, incident_severity, avg_speed, max_speed, distance_from_city_center_km
  FROM e2e_dev.dev_keqingli1129_gold.customer_claim_policy_telematics
  WHERE avg_speed IS NOT NULL ORDER BY claim_no;
  ```

  **Check:**
  - **`aggregated_telematics`:** **10** rows (`CHS000001`–`10`), each with a distance of 0–40 km.
  - **`customer_claim_policy`:** about **13,000** rows, minus any claims of the deleted customer `C007000`.
  - **The last query:** only the handful of claims (about 11) whose policies cover the part 1 cars.

- [x] **Step 4.4: Check the files** with `git status --short`. `silver_to_gold.py` should be new, and the pipeline YAML modified.

---

### Task 5: Lineage in Unity Catalog

- [x] **Step 5.1: Look at table and column lineage**

  **Do:** Catalog → `e2e_dev` → `dev_keqingli1129_gold` → `customer_claim_policy_telematics` → **Lineage** tab → **See lineage graph**. Expand upstream until you reach bronze, and source for the claims. Then click a column, for example `avg_speed`, to see column lineage.

  **Check:** the graph shows gold ← silver ← bronze, and for claims also ← `source`. `avg_speed` traces back to `silver.telematics.speed` and then to `bronze.telematics.speed`. Unity Catalog built this automatically from the pipeline runs, as in the transcript.

---

### Task 6: Orchestrate end to end with a Lakeflow Job

**Files:** Create `resources/smart_claims_end_to_end.job.yml`.

- [x] **Step 6.1: Create the job**

  **Do:** create `resources/smart_claims_end_to_end.job.yml`:

  ```yaml
  # End-to-end orchestration (see docs/transcript_4.txt): ingest from all three sources, then transform.
  # Hourly schedule, PAUSED: start it by hand with: databricks bundle run smart_claims_end_to_end

  resources:
    jobs:
      smart_claims_end_to_end:
        name: smart_claims_end_to_end

        schedule:
          quartz_cron_expression: "0 0 * * * ?"  # every hour, on the hour
          timezone_id: UTC
          pause_status: PAUSED

        tasks:
          - task_key: ingest_telematics
            pipeline_task:
              pipeline_id: ${resources.pipelines.telematics_ingestion.id}
          - task_key: ingest_cdc
            pipeline_task:
              pipeline_id: ${resources.pipelines.cdc_ingestion.id}
          - task_key: ingest_object_storage
            pipeline_task:
              pipeline_id: ${resources.pipelines.object_storage_ingestion.id}
          - task_key: ingest_claim_images
            run_job_task:
              job_id: ${resources.jobs.ingest_claim_images.id}

          - task_key: transform
            depends_on:
              - task_key: ingest_telematics
              - task_key: ingest_cdc
              - task_key: ingest_object_storage
              - task_key: ingest_claim_images
            run_if: ALL_SUCCESS
            pipeline_task:
              pipeline_id: ${resources.pipelines.transformations.id}
  ```

  **Why:**
  - **The four ingestion tasks** have no `depends_on`, so they run **in parallel**.
  - **`transform` waits for all four** and runs only if **all succeeded** (`run_if: ALL_SUCCESS`), the transcript's dependency setting.
  - **`run_job_task`** reuses the existing claim-images job instead of copying it.
  - **The schedule** is the transcript's "every hour", kept **PAUSED** so it never runs and costs nothing on its own.

- [x] **Step 6.2: Validate and deploy**

  **Do:** validate, then deploy.

  **Check:** `Created jobs.smart_claims_end_to_end`. In the UI, Jobs & Pipelines → the job → **Tasks** shows 4 parallel boxes feeding `transform`, and the schedule shows **Paused**.

- [x] **Step 6.3: Run it end to end once**

  **Do:** `env -u PYTHONPATH databricks bundle run smart_claims_end_to_end --profile DEFAULT`

  **Check:** all 5 tasks end in **Succeeded**. With no new source data, each one finishes quickly, because there's nothing new to process. That's the incremental behaviour the transcript describes. Expect several minutes in total, mostly serverless start-up.

- [x] **Step 6.4: Check the files** with `git status --short`. The job YAML should be new.

---

### Task 7: Update CLAUDE.md

**Files:** Modify `CLAUDE.md`.

- [x] **Step 7.1: Document part 4**

  **Do:**
  1. Add to the Commands block:

     ```bash
     databricks bundle run transformations --profile <p>          # bronze -> silver (quality checks) -> gold
     databricks bundle run smart_claims_end_to_end --profile <p>  # everything: 4 ingestion tasks in parallel, then transformations
     ```

  2. Add an Architecture bullet:

     ```markdown
     - **Transformations** (docs/part4_transformations_*.md, source material docs/transcript_4.txt): pipeline `transformations` (`resources/transformations.pipeline.yml`, default schema silver, config `transformations.bronze_schema` / `transformations.gold_schema`, `environment` has `geopy`) with two files in `src/transformations/transformations/`: `bronze_to_silver.py` (7 silver tables, each with `@dp.expect_all_or_drop` checks; claim/policy/customer are **materialized views** because streaming reads of AUTO CDC targets break on updates/deletes; the rest are streaming tables) and `silver_to_gold.py` (gold MVs published by full name: `aggregated_telematics` with a geopy distance UDF, `customer_claim_policy`, `customer_claim_policy_telematics`). Job `smart_claims_end_to_end` runs the 3 ingestion pipelines + the `ingest_claim_images` job in parallel, then `transformations` (`run_if: ALL_SUCCESS`); its hourly schedule is PAUSED.
     ```

  **Check:** `git status --short` shows `CLAUDE.md` modified.
