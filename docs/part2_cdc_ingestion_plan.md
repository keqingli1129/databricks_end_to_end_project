# Part 2: CDC into bronze (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. Steps use checkboxes (`- [ ]`). **No automated tests** in part 2, by your choice.

**Goal:** apply every insert, update and delete made in a source database to bronze streaming tables, as in [transcript_2.txt](transcript_2.txt).

**Design:** [part2_cdc_ingestion_design.md](part2_cdc_ingestion_design.md). A `source` schema stands in for the SQL Server, and Delta **Change Data Feed** stands in for SQL Server CDC and the ingestion gateway. The pipeline applies the changes with **AUTO CDC** (`dp.create_auto_cdc_flow`).

**Tech stack:**

- Declarative Automation Bundles
- Lakeflow Declarative Pipelines (`pyspark.pipelines`, AUTO CDC)
- Delta Change Data Feed
- a Jobs SQL task on the Serverless Starter Warehouse

## Global constraints

- **Existing catalogs only.** Add only the schema `source`. dev: `e2e_dev.dev_keqingli1129_source`.
- **Profile `DEFAULT`, target `dev`.** Prod is out of scope.
- **Clear the ROS path for local commands:** use `env -u PYTHONPATH databricks bundle …`.
- **No tests** and no Claude commits.
- **Never recreate the source tables.** No `DROP` and no `CREATE OR REPLACE`, because that breaks the change feed.

---

### Task 1: The `source` schema

**Files:** Modify `resources/databricks_end_to_end_project.yml` (the `schemas:` block).

- [x] **Step 1.1: Add the schema**

  **Do:** in `resources/databricks_end_to_end_project.yml`, add this after the `gold:` schema and before `volumes:`:

  ```yaml

      # Stand-in for the transcript's SQL Server (see docs/transcript_2.txt).
      source:
        catalog_name: ${var.catalog}
        name: source
        comment: Source database stand-in - customer, policy, claim with Change Data Feed on
  ```

  **Why:** the transcript's three tables live in a SQL Server. Ours live in this schema. It's a separate schema, so it's clearly "the source system", not part of the medallion layers.

- [x] **Step 1.2: Validate and deploy**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT`, then `env -u PYTHONPATH databricks bundle deploy --profile DEFAULT`.

  **Check:** `Created schemas.source`. The schema `e2e_dev.dev_keqingli1129_source` exists and is empty.

- [x] **Step 1.3: Check the files** with `git status --short`. Only `resources/databricks_end_to_end_project.yml` should be modified.

---

### Task 2: The warehouse variable

**Files:** Modify `databricks.yml` (the `variables:` block).

- [x] **Step 2.1: Add the lookup variable**

  **Do:** in `databricks.yml`, under `variables:`, after `telematics_source`, add:

  ```yaml
    warehouse_id:
      description: SQL warehouse for SQL tasks (the seed job)
      lookup:
        warehouse: Serverless Starter Warehouse
  ```

  **Why:** a SQL task needs a warehouse **ID**. `lookup` finds it **by name** at deploy time, so no ID is hardcoded, and a different workspace only needs a warehouse with that name.

- [x] **Step 2.2: Validate and see the resolved ID**

  **Do:** `env -u PYTHONPATH databricks bundle validate --strict --profile DEFAULT -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["variables"]["warehouse_id"]["value"])'`

  **Check:** it prints `cdcb7003ae7dd5ab`.

- [x] **Step 2.3: Check the files** with `git status --short`. `databricks.yml` should be modified.

---

### Task 3: The seed script

**Files:** Create `src/source_database/seed.sql`.

- [x] **Step 3.1: Write the table definitions**

  **Do:** create `src/source_database/seed.sql` with:

  ```sql
  -- Stand-in for the transcript's SQL Server: three source tables with Change Data Feed on.
  -- Run by the job seed_source_database, which passes :catalog and :schema.
  -- Safe to re-run: tables are created only if missing and filled only while empty.
  -- Never DROP or CREATE OR REPLACE these tables: that breaks the change feed read by cdc_ingestion.

  USE CATALOG IDENTIFIER(:catalog);
  USE SCHEMA IDENTIFIER(:schema);

  CREATE TABLE IF NOT EXISTS customer (
    customer_id   STRING NOT NULL,
    first_name    STRING,
    last_name     STRING,
    date_of_birth DATE,
    email         STRING,
    zip_code      STRING
  )
  COMMENT 'Source customers (stand-in for SQL Server)'
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

  CREATE TABLE IF NOT EXISTS policy (
    policy_no      STRING NOT NULL,
    customer_id    STRING,
    chassis_number STRING,
    make           STRING,
    model          STRING,
    model_year     INT,
    coverage       STRING,
    premium        DECIMAL(10, 2),
    deductible     INT,
    start_date     DATE,
    end_date       DATE
  )
  COMMENT 'Source policies (stand-in for SQL Server)'
  TBLPROPERTIES (delta.enableChangeDataFeed = true);

  CREATE TABLE IF NOT EXISTS claim (
    claim_no          STRING NOT NULL,
    policy_no         STRING,
    incident_date     DATE,
    incident_type     STRING,
    incident_severity STRING,
    claim_amount      DECIMAL(12, 2),
    updated_at        TIMESTAMP
  )
  COMMENT 'Source claims (stand-in for SQL Server)'
  TBLPROPERTIES (delta.enableChangeDataFeed = true);
  ```

  **Why:**
  - **`USE CATALOG/SCHEMA IDENTIFIER(:catalog)`:** reads the job's parameters. `:catalog` is a named parameter marker, and `IDENTIFIER()` lets it be used as a name.
  - **`delta.enableChangeDataFeed = true`:** this is the transcript's "enable change tracking and CDC". From now on, Delta records every insert, update and delete on these tables.

- [x] **Step 3.2: Add the seed data**

  **Do:** append to `src/source_database/seed.sql`:

  ```sql

  -- 7,000 customers.
  INSERT INTO customer
  SELECT
    format_string('C%06d', id) AS customer_id,
    first_name,
    last_name,
    date_add(DATE'1950-01-01', CAST(rand(1) * 18250 AS INT)) AS date_of_birth,
    lower(concat(first_name, '.', last_name, id, '@example.com')) AS email,
    element_at(array('77002', '77003', '77004', '77006', '77007', '77008', '77019', '77024', '77056', '77098'),
               CAST((id * 7) % 10 AS INT) + 1) AS zip_code
  FROM (
    SELECT
      id,
      element_at(array('James', 'Maria', 'Robert', 'Linda', 'Michael', 'Aisha', 'Wei', 'Carlos', 'Priya', 'David'),
                 CAST(id % 10 AS INT) + 1) AS first_name,
      element_at(array('Smith', 'Lopez', 'Nguyen', 'Johnson', 'Garcia', 'Patel', 'Kim', 'Brown', 'Davis', 'Martinez'),
                 CAST((id DIV 10) % 10 AS INT) + 1) AS last_name
    FROM range(1, 7001)
  )
  WHERE NOT EXISTS (SELECT 1 FROM customer);

  -- 12,000 policies; every customer gets at least one; POL0000001-10 cover the part 1 telematics cars CHS000001-10.
  INSERT INTO policy
  SELECT
    format_string('POL%07d', id) AS policy_no,
    format_string('C%06d', (id - 1) % 7000 + 1) AS customer_id,
    format_string('CHS%06d', id) AS chassis_number,
    element_at(array('Toyota', 'Honda', 'Ford', 'Tesla', 'BMW', 'Hyundai'), CAST(id % 6 AS INT) + 1) AS make,
    element_at(array('Camry', 'Civic', 'F-150', 'Model 3', 'X5', 'Elantra'), CAST(id % 6 AS INT) + 1) AS model,
    CAST(2015 + id % 11 AS INT) AS model_year,
    element_at(array('COMPREHENSIVE', 'COLLISION', 'LIABILITY'), CAST(id % 3 AS INT) + 1) AS coverage,
    CAST(round(600 + rand(2) * 1400, 2) AS DECIMAL(10, 2)) AS premium,
    element_at(array(250, 500, 1000), CAST((id DIV 3) % 3 AS INT) + 1) AS deductible,
    start_date,
    add_months(start_date, 12) AS end_date
  FROM (SELECT id, date_add(DATE'2026-01-01', -CAST(id % 365 AS INT)) AS start_date FROM range(1, 12001))
  WHERE NOT EXISTS (SELECT 1 FROM policy);

  -- 13,000 claims spread over the policies.
  INSERT INTO claim
  SELECT
    format_string('CLM%08d', id) AS claim_no,
    format_string('POL%07d', (id * 7919) % 12000 + 1) AS policy_no,
    date_add(DATE'2026-01-01', CAST(rand(3) * 270 AS INT)) AS incident_date,
    element_at(array('COLLISION', 'THEFT', 'WEATHER', 'VANDALISM', 'GLASS'), CAST(id % 5 AS INT) + 1) AS incident_type,
    element_at(array('Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss'), CAST(id % 4 AS INT) + 1) AS incident_severity,
    CAST(round(100 + rand(4) * 24900, 2) AS DECIMAL(12, 2)) AS claim_amount,
    current_timestamp() AS updated_at
  FROM range(1, 13001)
  WHERE NOT EXISTS (SELECT 1 FROM claim);
  ```

  **Why:**
  - **`range(1, N+1)`** produces the numbers `id = 1…N`. Each row's values are derived from `id`, or from `rand(seed)` for random-looking amounts and dates.
  - **The links:** the `id % …` patterns decide how rows connect. `customer_id` cycles through all 7,000 customers, and `claim.policy_no` is spread across the policies.
  - **`WHERE NOT EXISTS (SELECT 1 FROM <table>)`** makes each insert run **only while the table is empty**, so re-running the job adds nothing.
  - **Known test rows:** `CLM00000003` gets severity index 3 % 4 + 1 = 4, which is **Total Loss**. Task 7 changes it, just like the transcript.

- [x] **Step 3.3: Check the files** with `git status --short`. `src/source_database/` should be new.

---

### Task 4: The seed job

**Files:** Create `resources/seed_source_database.job.yml`.

- [ ] **Step 4.1: Create the job**

  **Do:** create `resources/seed_source_database.job.yml`:

  ```yaml
  # Creates and fills the source tables (stand-in for the transcript's SQL Server).
  # Safe to re-run. No schedule; start it with: databricks bundle run seed_source_database

  resources:
    jobs:
      seed_source_database:
        name: seed_source_database

        tasks:
          - task_key: seed
            sql_task:
              file:
                path: ../src/source_database/seed.sql
                source: WORKSPACE
              warehouse_id: ${var.warehouse_id}
              parameters:
                catalog: ${var.catalog}
                schema: ${resources.schemas.source.name}
  ```

  **Why:** this runs the whole `.sql` file on the SQL warehouse. `parameters` become `:catalog` and `:schema` inside the script. `source: WORKSPACE` means the file comes from the bundle's uploaded files.

- [ ] **Step 4.2: Validate and deploy**

  **Do:** validate, then deploy.

  **Check:** `Created jobs.seed_source_database`.

- [ ] **Step 4.3: Run it**

  **Do:** `env -u PYTHONPATH databricks bundle run seed_source_database --profile DEFAULT`

  **Check:** `TERMINATED SUCCESS`. The warehouse starts automatically if it's stopped, which takes about a minute. **If it fails on `:catalog`**, meaning the parameter syntax isn't accepted, stop and look at the error before changing anything.

- [ ] **Step 4.4: Look at the source data**

  **Do:** in the SQL editor:

  ```sql
  SELECT 'customer' AS t, count(*) FROM e2e_dev.dev_keqingli1129_source.customer
  UNION ALL SELECT 'policy', count(*) FROM e2e_dev.dev_keqingli1129_source.policy
  UNION ALL SELECT 'claim',  count(*) FROM e2e_dev.dev_keqingli1129_source.claim;
  SELECT * FROM e2e_dev.dev_keqingli1129_source.claim LIMIT 5;
  ```

  **Check:** 7000, 12000 and 13000 rows, and readable sample claims.

- [ ] **Step 4.5: Re-run the job to show it's safe to repeat**

  **Do:** run the job again, then run the count query again.

  **Check:** the counts are **still** 7000 / 12000 / 13000.

- [ ] **Step 4.6: Check the files** with `git status --short`. The job YAML should be new.

---

### Task 5: The CDC pipeline with the first table (customer)

**Files:**
- Create: `resources/cdc_ingestion.pipeline.yml`
- Create: `src/cdc_ingestion/transformations/customer.py`

- [ ] **Step 5.1: Create the pipeline resource**

  **Do:** create `resources/cdc_ingestion.pipeline.yml`:

  ```yaml
  # CDC ingestion (see docs/transcript_2.txt): applies inserts/updates/deletes from the source schema
  # into bronze streaming tables with AUTO CDC. Stands in for the Lakeflow Connect ingestion pipeline.

  resources:
    pipelines:
      cdc_ingestion:
        name: cdc_ingestion
        catalog: ${var.catalog}
        schema: ${resources.schemas.bronze.name}
        serverless: true
        # root_path is put on sys.path by the pipeline (see part 1, step 9.4).
        root_path: "../src"

        configuration:
          cdc.source_schema: ${var.catalog}.${resources.schemas.source.name}

        libraries:
          - glob:
              include: ../src/cdc_ingestion/transformations/**
  ```

  **Why:** this is the transcript's "ingestion pipeline", writing into bronze. The configuration tells the code where the source tables are. There's no `environment` block, because this code needs no extra packages.

- [ ] **Step 5.2: Create the customer table**

  **Do:** create `src/cdc_ingestion/transformations/customer.py`:

  ```python
  from pyspark import pipelines as dp
  from pyspark.sql import functions as F

  SOURCE = f"{spark.conf.get('cdc.source_schema')}.customer"
  CHANGE_FEED_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


  @dp.temporary_view()
  def customer_changes():
      """Every insert/update/delete on the source table; updates keep only the new values."""
      return (
          spark.readStream.option("readChangeFeed", "true")
          .table(SOURCE)
          .filter("_change_type != 'update_preimage'")
      )


  dp.create_streaming_table(name="customer", comment="Customers from the source database, kept in sync by AUTO CDC.")

  dp.create_auto_cdc_flow(
      target="customer",
      source="customer_changes",
      keys=["customer_id"],
      sequence_by="_commit_version",
      apply_as_deletes=F.expr("_change_type = 'delete'"),
      except_column_list=CHANGE_FEED_COLUMNS,
      stored_as_scd_type=1,
  )
  ```

  **Why:**
  - **The temporary view** reads the source's **change feed** as a stream. Each row carries `_change_type` (`insert`, `update_preimage`, `update_postimage` or `delete`) and `_commit_version`, the order of the change. The first run returns the current rows as inserts, which is the transcript's snapshot.
  - **`create_streaming_table`** declares the bronze target. **`create_auto_cdc_flow`** applies changes by key:
    - an unknown key becomes an insert, and a known key becomes an update. Together that's an **upsert**.
    - `_change_type = 'delete'` removes the row.
    - `sequence_by` makes sure an older change can never overwrite a newer one.
  - **SCD Type 1** keeps only the latest values, so bronze mirrors the source, like the transcript's tables.

- [ ] **Step 5.3: Validate, deploy and run**

  **Do:** validate, deploy, then `env -u PYTHONPATH databricks bundle run cdc_ingestion --profile DEFAULT`.

  **Check:** `Created pipelines.cdc_ingestion`, then `Update … is COMPLETED`, and `e2e_dev.dev_keqingli1129_bronze.customer` has **7000** rows.

- [ ] **Step 5.4: Check the files** with `git status --short`. The pipeline YAML and `src/cdc_ingestion/` should be new.

---

### Task 6: policy and claim

**Files:**
- Create: `src/cdc_ingestion/transformations/policy.py`
- Create: `src/cdc_ingestion/transformations/claim.py`

- [ ] **Step 6.1: Create `policy.py`**

  **Do:** create `src/cdc_ingestion/transformations/policy.py`:

  ```python
  from pyspark import pipelines as dp
  from pyspark.sql import functions as F

  SOURCE = f"{spark.conf.get('cdc.source_schema')}.policy"
  CHANGE_FEED_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


  @dp.temporary_view()
  def policy_changes():
      """Every insert/update/delete on the source table; updates keep only the new values."""
      return (
          spark.readStream.option("readChangeFeed", "true")
          .table(SOURCE)
          .filter("_change_type != 'update_preimage'")
      )


  dp.create_streaming_table(name="policy", comment="Policies from the source database, kept in sync by AUTO CDC.")

  dp.create_auto_cdc_flow(
      target="policy",
      source="policy_changes",
      keys=["policy_no"],
      sequence_by="_commit_version",
      apply_as_deletes=F.expr("_change_type = 'delete'"),
      except_column_list=CHANGE_FEED_COLUMNS,
      stored_as_scd_type=1,
  )
  ```

- [ ] **Step 6.2: Create `claim.py`**

  **Do:** create `src/cdc_ingestion/transformations/claim.py`:

  ```python
  from pyspark import pipelines as dp
  from pyspark.sql import functions as F

  SOURCE = f"{spark.conf.get('cdc.source_schema')}.claim"
  CHANGE_FEED_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


  @dp.temporary_view()
  def claim_changes():
      """Every insert/update/delete on the source table; updates keep only the new values."""
      return (
          spark.readStream.option("readChangeFeed", "true")
          .table(SOURCE)
          .filter("_change_type != 'update_preimage'")
      )


  dp.create_streaming_table(name="claim", comment="Claims from the source database, kept in sync by AUTO CDC.")

  dp.create_auto_cdc_flow(
      target="claim",
      source="claim_changes",
      keys=["claim_no"],
      sequence_by="_commit_version",
      apply_as_deletes=F.expr("_change_type = 'delete'"),
      except_column_list=CHANGE_FEED_COLUMNS,
      stored_as_scd_type=1,
  )
  ```

  **Why:** these are the same pattern as `customer.py`, one file per dataset. Only the table and key change.

- [ ] **Step 6.3: Deploy and run**

  **Do:** deploy, then run `cdc_ingestion`.

  **Check:** the pipeline graph shows the three flows side by side, just as the transcript ingests the three tables in parallel. Bronze has **customer 7000** (unchanged, since there are no new changes), **policy 12000** and **claim 13000**. In the Catalog, all three are **streaming tables**, next to `telematics`.

- [ ] **Step 6.4: Check the files** with `git status --short`. `policy.py` and `claim.py` should be new.

---

### Task 7: Test incremental loading (insert, update, delete)

**Files:** Create `src/source_database/changes.sql`.

- [ ] **Step 7.1: Write the change script**

  **Do:** create `src/source_database/changes.sql`:

  ```sql
  -- The transcript's incremental test, for the dev source schema. Run it in the SQL editor, section by section.

  -- 1. Checks BEFORE (run on bronze): customer exists, claim is 'Total Loss', policy does not exist yet.
  SELECT * FROM e2e_dev.dev_keqingli1129_bronze.customer WHERE customer_id = 'C007000';
  SELECT claim_no, incident_severity FROM e2e_dev.dev_keqingli1129_bronze.claim WHERE claim_no = 'CLM00000003';
  SELECT * FROM e2e_dev.dev_keqingli1129_bronze.policy WHERE policy_no = 'POL9999001';

  -- 2. Changes in the SOURCE (plays the DataGrip part).
  INSERT INTO e2e_dev.dev_keqingli1129_source.policy VALUES
    ('POL9999001', 'C000001', 'CHS999001', 'Tesla', 'Model Y', 2026, 'COMPREHENSIVE', 1899.00, 500,
     DATE'2026-09-01', DATE'2027-09-01');

  UPDATE e2e_dev.dev_keqingli1129_source.claim
  SET incident_severity = 'Minor Damage', updated_at = current_timestamp()
  WHERE claim_no = 'CLM00000003';

  DELETE FROM e2e_dev.dev_keqingli1129_source.customer WHERE customer_id = 'C007000';

  -- 3. Now run the pipeline cdc_ingestion, then re-run the checks in section 1:
  --    policy row appears, claim severity is 'Minor Damage', customer returns no rows.
  ```

  **Why:** this mirrors the transcript exactly: the prepared check queries, one insert (policy), one update (a claim's severity from Total Loss to Minor Damage) and one delete (customer).

- [ ] **Step 7.2: Run the checks before the changes**

  **Do:** run section 1 in the SQL editor.

  **Check:** the customer is found, the claim is `Total Loss`, and there's no policy row.

- [ ] **Step 7.3: Make the three changes in the source**

  **Do:** run section 2.

  **Check:** each statement reports 1 row affected.

- [ ] **Step 7.4: Run the pipeline**

  **Do:** `env -u PYTHONPATH databricks bundle run cdc_ingestion --profile DEFAULT`

  **Check:** it completes. In the pipeline UI, each table shows its change: policy **upserted 1**, claim **upserted 1**, customer **deleted 1**. That's the transcript's "three changes were recognized".

- [ ] **Step 7.5: Run the checks after the changes**

  **Do:** run section 1 again.

  **Check:** the policy `POL9999001` exists, `CLM00000003` is `Minor Damage`, and the customer `C007000` returns **no rows**. Counts: customer 6999, policy 12001, claim 13000.

- [ ] **Step 7.6: Check the files** with `git status --short`. `changes.sql` should be new.

---

### Task 8: Update CLAUDE.md

**Files:** Modify `CLAUDE.md`.

- [ ] **Step 8.1: Document part 2**

  **Do:**
  1. Add to the Commands block:

     ```bash
     databricks bundle run seed_source_database --profile <p>  # create/fill the source tables (idempotent)
     databricks bundle run cdc_ingestion --profile <p>         # apply source inserts/updates/deletes to bronze
     ```

  2. Add an Architecture bullet:

     ```markdown
     - **CDC ingestion** (docs/part2_cdc_ingestion_*.md, source material docs/transcript_2.txt): schema `source` (tables `customer`, `policy`, `claim`, Change Data Feed on) stands in for the transcript's SQL Server — Lakeflow Connect's gateway needs classic compute, which Free Edition lacks. The job `seed_source_database` (SQL task on `var.warehouse_id`, looked up by name "Serverless Starter Warehouse") runs `src/source_database/seed.sql` with `:catalog`/`:schema` parameters; it creates tables only if missing and fills them only while empty. Never DROP or CREATE OR REPLACE the source tables: that breaks the change feed. The pipeline `cdc_ingestion` (bronze) reads each table's change feed (minus `update_preimage`) and applies it with `dp.create_auto_cdc_flow` (SCD Type 1, `sequence_by=_commit_version`, deletes applied) into `bronze.customer/policy/claim`. `src/source_database/changes.sql` holds the insert/update/delete test.
     ```

  **Check:** `git status --short` shows `CLAUDE.md` modified.
