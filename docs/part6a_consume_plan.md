# Part 6a: claim checks, dashboard and Genie (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". In the steps marked **(you, UI)**, you click and Claude guides you. Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. **No automated tests.**

**Goal:** a gold table of claim checks, an AI/BI dashboard and a Genie space on top of it, as in the first part of [transcript_6.txt](transcript_6.txt). Each is first built by hand in the UI, then fully defined in the bundle.

**Design:** [part6a_consume_design.md](part6a_consume_design.md).

## Global constraints

- **Names (dev):** gold is `e2e_dev.dev_keqingli1129_gold`, and the pipeline is `e2e_transformations` (resource key `transformations`).
- **Titles start with "E2E",** because `smart_claims_dev` already has a "Claims Investigation" dashboard.
- **SQL warehouse:** `${var.warehouse_id}`, the Serverless Starter Warehouse.
- **Profile `DEFAULT`,** and run local commands as `env -u PYTHONPATH …`. No tests, and no Claude commits.

---

### Task 1: `gold.claim_checks`

**Files:** Modify `src/transformations/transformations/silver_to_gold.py` (part 4).

- [x] **Step 1.1: Add the claim-checks materialized view**

  **Do:** append to `silver_to_gold.py`:

  ```python


  # --- Claim checks (part 6): the business rules used by the dashboard, Genie and the app. ---

  EXPECTED_DAMAGE = {"Trivial Damage": "ok", "Minor Damage": "minor", "Major Damage": "major", "Total Loss": "major"}
  COVERAGE_LIMITS = {"COMPREHENSIVE": 20000, "COLLISION": 15000, "LIABILITY": 10000}
  MAX_SPEED_KMH = 150
  CHECKS = ["severity_match", "amount_within_limit", "policy_valid", "speed_ok"]


  def _as_map(mapping):
      return F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])


  CLAIM_CHECKS_SCHEMA = """
      claim_no STRING COMMENT 'Claim number (key), e.g. CLM00000001',
      policy_no STRING COMMENT 'Policy the claim was made on',
      customer_id STRING COMMENT 'Customer who owns the policy',
      full_name STRING COMMENT 'Customer full name',
      incident_date DATE COMMENT 'Date of the accident',
      incident_type STRING COMMENT 'COLLISION, THEFT, WEATHER, VANDALISM or GLASS',
      incident_severity STRING COMMENT 'Severity claimed by the customer: Trivial Damage < Minor Damage < Major Damage < Total Loss',
      claim_amount DECIMAL(12,2) COMMENT 'Amount claimed, in USD',
      coverage STRING COMMENT 'Policy coverage: COMPREHENSIVE, COLLISION or LIABILITY',
      coverage_limit INT COMMENT 'Maximum claim amount for the coverage, in USD',
      start_date DATE COMMENT 'Policy start date',
      end_date DATE COMMENT 'Policy end date',
      chassis_number STRING COMMENT 'Car chassis number (links to telematics)',
      max_speed DOUBLE COMMENT 'Highest speed recorded by the car telematics, km/h; NULL if the car has no telematics',
      image_name STRING COMMENT 'Photo the customer uploaded for the claim',
      expected_damage STRING COMMENT 'Claimed severity mapped to the model labels ok/minor/major',
      predicted_damage STRING COMMENT 'Damage predicted from the photo by the ML model: ok, minor or major',
      severity_match BOOLEAN COMMENT 'TRUE if the predicted damage equals the expected damage; NULL if no prediction',
      amount_within_limit BOOLEAN COMMENT 'TRUE if claim_amount <= coverage_limit',
      policy_valid BOOLEAN COMMENT 'TRUE if incident_date is within the policy start and end date',
      speed_ok BOOLEAN COMMENT 'TRUE if max_speed <= 150 km/h; NULL if the car has no telematics',
      failed_checks ARRAY<STRING> COMMENT 'Names of the checks that failed',
      claim_status STRING COMMENT 'auto_approved if no check failed, otherwise needs_review'
  """


  @dp.materialized_view(
      name=f"{GOLD}.claim_checks",
      comment="One row per claim with the automatic claim checks and the resulting status (auto_approved / needs_review).",
      table_properties=GOLD_PROPERTIES,
      schema=CLAIM_CHECKS_SCHEMA,
  )
  def claim_checks():
      claims = spark.read.table(f"{GOLD}.customer_claim_policy_telematics")
      photos = spark.read.table("claim_images_metadata").groupBy("claim_no").agg(F.first("image_name").alias("image_name"))
      predictions = spark.read.table(f"{GOLD}.claim_image_predictions").select(
          "image_name", F.col("damage_prediction").alias("predicted_damage")
      )
      checked = (
          claims.join(photos, "claim_no", "left")
          .join(predictions, "image_name", "left")
          .withColumn("expected_damage", _as_map(EXPECTED_DAMAGE)[F.col("incident_severity")])
          .withColumn("coverage_limit", _as_map(COVERAGE_LIMITS)[F.col("coverage")].cast("int"))
          .withColumn("severity_match", F.col("expected_damage") == F.col("predicted_damage"))
          .withColumn("amount_within_limit", F.col("claim_amount") <= F.col("coverage_limit"))
          .withColumn("policy_valid", F.col("incident_date").between(F.col("start_date"), F.col("end_date")))
          .withColumn("speed_ok", F.col("max_speed") <= MAX_SPEED_KMH)
          .withColumn(
              "failed_checks",
              F.filter(F.array(*[F.when(~F.col(c), F.lit(c)) for c in CHECKS]), lambda name: name.isNotNull()),
          )
          .withColumn(
              "claim_status", F.when(F.size("failed_checks") == 0, "auto_approved").otherwise("needs_review")
          )
      )
      return checked.select(
          "claim_no", "policy_no", "customer_id", "full_name", "incident_date", "incident_type",
          "incident_severity", F.col("claim_amount").cast("decimal(12,2)").alias("claim_amount"), "coverage",
          "coverage_limit", "start_date", "end_date", "chassis_number", "max_speed", "image_name",
          "expected_damage", "predicted_damage", *CHECKS, "failed_checks", "claim_status",
      )
  ```

  **Why:**
  - **One place for the rules:** the transcript's app checks the severity against the model, the amount against the policy, the policy dates, and the speed. Putting those rules in **one gold table** means the dashboard, Genie and later the app all agree.
  - **Comments in `schema`:** they explain every column to people and to **Genie**.
  - **Null checks:** a null check (no telematics, no prediction) doesn't count as a failure.

- [x] **Step 1.2: Deploy and run the pipeline**

  **Do:** deploy, then `env -u PYTHONPATH databricks bundle run transformations --profile DEFAULT`.

  **Check:** `gold.claim_checks` is created, with about **12,999** rows.

- [x] **Step 1.3: Look at the results**

  **Do:**

  ```sql
  SELECT claim_status, count(*) FROM e2e_dev.dev_keqingli1129_gold.claim_checks GROUP BY ALL;
  SELECT check, count(*) AS failures
  FROM e2e_dev.dev_keqingli1129_gold.claim_checks LATERAL VIEW explode(failed_checks) AS check
  GROUP BY ALL ORDER BY failures DESC;
  SELECT claim_no, incident_severity, predicted_damage, claim_amount, coverage_limit, max_speed, failed_checks, claim_status
  FROM e2e_dev.dev_keqingli1129_gold.claim_checks WHERE max_speed IS NOT NULL;
  ```

  **Check:** both statuses appear, with mostly `needs_review`. Each check has some failures, and the ~10 claims with telematics show `speed_ok`.

- [x] **Step 1.4: Check the files** with `git status --short`.

---

### Task 2 (you, UI): A practice dashboard

- [x] **Step 2.1: Create the dashboard.** Go to **Dashboards → Create dashboard**, and name it **"Practice – claims summary"**.
- [x] **Step 2.2: Add a dataset with date parameters.** On the **Data** tab, choose **Create from SQL** and paste:

  ```sql
  SELECT incident_type, incident_severity, count(*) AS claims
  FROM e2e_dev.dev_keqingli1129_gold.claim_checks
  WHERE incident_date BETWEEN :start_date AND :end_date
  GROUP BY ALL
  ```

  Set both parameters' type to **Date**, with `start_date` = 2026-01-01 and `end_date` = today. Run it, then rename the dataset to **claims_summary**.
- [x] **Step 2.3: Add a chart.** On the **Canvas**, add a visualization with dataset `claims_summary`, type **Bar**, X = `incident_type`, Y = `SUM(claims)`, Color = `incident_severity`. Add a **filter widget** for the date parameters.
- [x] **Step 2.4: Publish it,** then copy the **dashboard ID** from the URL: `…/dashboardsv3/<ID>/…`.

---

### Task 3: The full dashboard in the bundle

**Files:**
- Create: `resources/claims_consumption.yml`
- Create: `src/consumption/claims_investigation.lvdash.json`

- [x] **Step 3.1: Export the practice dashboard** with `databricks bundle generate dashboard --existing-id <ID> …`, into the scratchpad, as a **format reference**.
- [x] **Step 3.2: Test the full dashboard's queries** with the CLI: status counts, claims by type × severity (with the date parameters), failures per check, and the review list.
- [x] **Step 3.3: Write `claims_investigation.lvdash.json`,** titled **"E2E Claims Investigation"**: the tested datasets; counters (total, auto-approved, needs review); bars (claims by incident type coloured by severity, failures per check); a table (claims needing review); and date filters.
- [x] **Step 3.4: Add the dashboard resource** to `resources/claims_consumption.yml`: key `claims_investigation_dashboard`, `display_name`, `file_path`, and `warehouse_id: ${var.warehouse_id}`.
- [x] **Step 3.5: Validate, deploy and open it.** Check that every widget renders and the date filter changes the numbers.
- [x] **Step 3.6: Check the files** with `git status --short`.

---

### Task 4 (you, UI): A practice Genie space

- [ ] **Step 4.1: Create the space.** Go to **Genie → New**, add the table `e2e_dev.dev_keqingli1129_gold.claim_checks`, pick the **Serverless Starter Warehouse**, and name it **"Practice – claims genie"**.
- [ ] **Step 4.2: Ask** *"How many claims are there per claimed severity?"*, then open **Show code** to see the SQL Genie wrote. Ask a second question of your own.
- [ ] **Step 4.3: Copy the space ID** from the URL: `…/genie/rooms/<ID>`.

---

### Task 5: The full Genie space in the bundle

**Files:**
- Create: `src/consumption/claims_genie.geniespace.json`
- Modify: `resources/claims_consumption.yml`

- [ ] **Step 5.1: Export the practice space** (its serialized definition) as a **format reference**.
- [ ] **Step 5.2: Write `claims_genie.geniespace.json`:** the tables `gold.claim_checks` and `gold.customer_claim_policy_telematics`; instructions (severity order Trivial < Minor < Major < Total Loss; `needs_review` = at least one failed check; amounts in USD; speeds in km/h); and sample questions, for example *How many claims need review, by failed check?*, *What is the average claim amount per coverage?*, *Which claims had a speed over 150 km/h?* and *How often does the model agree with the customer's severity?*
- [ ] **Step 5.3: Add the Genie resource:** key `claims_genie_space`, title **"E2E Claims Genie"**, a description, `warehouse_id: ${var.warehouse_id}` and `file_path`.
- [ ] **Step 5.4: Validate and deploy, then try it yourself** in the UI with the sample questions and your own.
- [ ] **Step 5.5: Check the files** with `git status --short`.

---

### Task 6: Update CLAUDE.md

- [ ] **Step 6.1: Document part 6a:** the `claim_checks` rules and where they live, the dashboard and Genie resources and their JSON files, the "E2E" naming, and that the practice versions exist only in the UI.
