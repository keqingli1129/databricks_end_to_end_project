# Part 6b: the claims app (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". In the steps marked **(you, UI)**, you click and Claude guides you. Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. **No automated tests.**

**Goal:** a Streamlit Databricks App with a customer mode (submit a claim with a photo; the model and the rules decide at once) and an admin mode (an overview and a per-claim analysis), reading from Lakebase synced tables, as in the last part of [transcript_6.txt](transcript_6.txt).

**Design:** [part6b_app_design.md](part6b_app_design.md).

## Global constraints

- **Lakebase:** the project `smart-claims-dev` (branch `production`) belongs to the `smart_claims_dev` bundle. **Only reference it: never create, change or destroy the project or its `databricks_postgres` database.** Ours is the database `e2e_claims`.
- **Names (dev):** gold is `e2e_dev.dev_keqingli1129_gold`, landing is `e2e_dev.dev_keqingli1129_landing`, the endpoint is `dev_keqingli1129_e2e-claims-damage-level`, and the pipeline is `e2e_transformations` (key `transformations`).
- **One copy of the rules:** `src/claims_app/claim_rules.py`. The pipeline and the app import it; nothing else defines the rules.
- **Skills:** load `databricks-lakebase` (Tasks 1–3) and `databricks-apps-python` (Tasks 4–6) before those tasks.
- **Profile `DEFAULT`,** and run local commands as `env -u PYTHONPATH …`. No tests, and no Claude commits.

---

### Task 1: Our database, and the continuous-sync check

**Files:** Create `resources/claims_app.yml`.

- [x] **Step 1.1: Read up.** Load the `databricks-lakebase` skill and the synced-table docs: which sources a `CONTINUOUS` synced table accepts (tables with Change Data Feed? materialized views? "read-time CDF" with `pipeline_channel: PREVIEW`?), and what `postgres_synced_tables` needs (`synced_table_id` format, `branch`, `postgres_database`, `new_pipeline_spec.storage_catalog`/`storage_schema`).

  **Check:** a short answer to "can `gold.claim_checks` (an MV) be a continuous source, and how?", shown to you before anything is created.

  **Result (docs, 2026-10-03):** yes, in principle. Synced tables accept materialized views, and Continuous needs a change data feed. **Materialized views use automatic (read-time) change data feed**, which needs:
  - row tracking (on by default for serverless MVs)
  - `pipelines.externalMetadata.enabled = true` on the MV (table property) or on its pipeline
  - the **PREVIEW channel** on the MV's pipeline **or** on the pipeline that reads it, which here is the synced table's `new_pipeline_spec.pipeline_channel: PREVIEW`

  The skill's note that bundles can't do `postgres_synced_tables` yet is out of date: the Lakebase bundle docs and this CLI's schema both have it. Untested on Free Edition, so step 1.3 is the real test.

- [x] **Step 1.2: Add the database and the first synced table.** First, in `silver_to_gold.py`, add `"pipelines.externalMetadata.enabled": "true"` to `claim_checks`'s `table_properties`, then deploy and run `transformations` so the MV has it. Then, in `resources/claims_app.yml`:

  ```yaml
  resources:
    postgres_databases:
      e2e_claims_database:
        parent: projects/smart-claims-dev/branches/production
        database_id: e2e-claims          # resource id (hyphens); exact rules checked in 1.1
        postgres_database: e2e_claims
    postgres_synced_tables:
      claim_checks_synced:
        synced_table_id: ${var.catalog}.${resources.schemas.gold.name}.claim_checks_pg
        source_table_full_name: ${var.catalog}.${resources.schemas.gold.name}.claim_checks
        primary_key_columns: [claim_no]
        scheduling_policy: CONTINUOUS
        branch: projects/smart-claims-dev/branches/production
        postgres_database: e2e_claims
        new_pipeline_spec:
          storage_catalog: ${var.catalog}
          storage_schema: ${resources.schemas.gold.name}
          pipeline_channel: PREVIEW        # automatic CDF from a materialized view
  ```

  The field details are adjusted to what step 1.1 found. **Check:** `bundle validate` passes, and it shows the dev names of the database and the synced table.

- [x] **Step 1.3: Deploy and watch the sync.** Deploy, then follow the synced table's state (`databricks postgres get-synced-table …`, and its pipeline) until it's online or fails.

  **Check:**
  - In the Lakebase SQL editor (database `e2e_claims`), `SELECT count(*) FROM <gold schema>.claim_checks_pg` → 12,999.
  - `databricks_postgres` (smart_claims_dev's database) is unchanged.

- [x] **Step 1.4: Decide.**
  - **It works:** skip Task 3.
  - **It fails because the source is an MV:** do Task 3 (the fallback you chose) before Task 2's synced table.

  Record the result in the design's "The plan's first step".

  **Result (2026-10-03): it works, so Task 3 is skipped.**
  - The first deploy failed because the shared compute `projects/smart-claims-dev/branches/production/endpoints/primary` was **disabled** (`spec.disabled: true`, last active 2026-09-26). You chose to enable it with `databricks postgres update-endpoint … spec.disabled`.
  - After that, `claim_checks_pg` went `ONLINE_CONTINUOUS_UPDATE`. In `e2e_claims_dev`, `dev_keqingli1129_gold.claim_checks_pg` has 12,999 rows (1,687 / 11,312), and `databricks_postgres` is untouched.
  - Postgres types: `failed_checks` is **`jsonb`**, `claim_amount` is `numeric`. So `app.app_claims.failed_checks` must be `jsonb` too, for the `UNION ALL`.
- [x] **Step 1.5: Check the files** with `git status --short`.

---

### Task 2: The shared rules and `gold.policy_lookup`

**Files:**
- Create: `src/claims_app/claim_rules.py`
- Modify: `src/transformations/transformations/silver_to_gold.py` (parts 4 and 6a)
- Modify: `resources/claims_app.yml`

- [x] **Step 2.1: Create `claim_rules.py`.** Move the constants out of `silver_to_gold.py` and add `check_claim`:

  ```python
  """The claim rules: the single copy, used by the pipeline (gold.claim_checks, gold.policy_lookup) and the app."""

  from datetime import date

  EXPECTED_DAMAGE = {"Trivial Damage": "ok", "Minor Damage": "minor", "Major Damage": "major", "Total Loss": "major"}
  COVERAGE_LIMITS = {"COMPREHENSIVE": 20000, "COLLISION": 15000, "LIABILITY": 10000}
  MAX_SPEED_KMH = 150
  CHECKS = ["severity_match", "amount_within_limit", "policy_valid", "speed_ok"]


  def check_claim(policy: dict, incident_date: date, incident_severity: str, claim_amount: float,
                  predicted_damage: str | None) -> dict:
      """Same rules as gold.claim_checks; a check is None when it can't run (no prediction, no telematics)."""
      expected = EXPECTED_DAMAGE.get(incident_severity)
      checks = {
          "severity_match": None if predicted_damage is None else expected == predicted_damage,
          "amount_within_limit": claim_amount <= policy["coverage_limit"],
          "policy_valid": policy["start_date"] <= incident_date <= policy["end_date"],
          "speed_ok": None if policy["max_speed"] is None else policy["max_speed"] <= MAX_SPEED_KMH,
      }
      failed = [name for name in CHECKS if checks[name] is False]
      return {**checks, "expected_damage": expected, "failed_checks": failed,
              "claim_status": "needs_review" if failed else "auto_approved"}
  ```

- [x] **Step 2.2: Use it in the pipeline.**
  - In `silver_to_gold.py`, replace the four constants with `from claims_app.claim_rules import CHECKS, COVERAGE_LIMITS, EXPECTED_DAMAGE, MAX_SPEED_KMH`. The pipeline's `root_path` is `../src`, so the import resolves.
  - Append the MV `policy_lookup`: one row per policy from silver `policy` + `customer` + gold `aggregated_telematics` (left join on the chassis number). Columns: `policy_no`, `customer_id`, `full_name`, `coverage`, `coverage_limit` (from `COVERAGE_LIMITS`), `start_date`, `end_date`, `chassis_number`, `max_speed`, each with a `COMMENT` in a `schema` DDL, as for `claim_checks`.
- [x] **Step 2.3: Deploy and run** `transformations`.

  **Check:**
  - `claim_checks` still has 1,687 / 11,312 auto-approved / needs review. This shows the rules didn't change.
  - `policy_lookup` has one row per policy (about 12,000), and 10 of them have a `max_speed`.
- [x] **Step 2.4: Add `policy_lookup_synced`** (key `policy_no`) to `resources/claims_app.yml`, built the same way as `claim_checks_synced`, or from `gold.policy_lookup_sync` if Task 3 was needed. Deploy.

  **Check:** the row count in Postgres matches.
- [x] **Step 2.5: Check the files** with `git status --short`.

---

### Task 3 (only if step 1.4 says so): The MERGE fallback

**Files:**
- Create: `src/consumption/sync_tables.sql`
- Modify: `resources/smart_claims_end_to_end.job.yml`
- Modify: `resources/claims_app.yml`

- [ ] **Step 3.1: Write `sync_tables.sql`:** `CREATE TABLE IF NOT EXISTS gold.claim_checks_sync … TBLPROPERTIES (delta.enableChangeDataFeed = true)` (and the same for `policy_lookup_sync`), then a `MERGE` from each MV, using `WHEN NOT MATCHED BY SOURCE THEN DELETE` so deletes reach Postgres too. It uses the `:catalog`/`:schema` named parameters, like `seed.sql`.
- [ ] **Step 3.2: Add a task `sync_for_app`** (a SQL file task on `${var.warehouse_id}`, `depends_on: transform`) to `smart_claims_end_to_end`. Run it once by hand.
- [ ] **Step 3.3: Point the synced tables at the `_sync` tables,** then deploy.

  **Check:** both sync and counts match.
- [ ] **Step 3.4: Check the files** with `git status --short`.

---

### Task 4: The app skeleton, connected to Postgres

**Files:**
- Create: `src/claims_app/app.yaml`, `requirements.txt`, `app.py`, `db.py`
- Modify: `resources/claims_app.yml`

- [x] **Step 4.1: Load `databricks-apps-python`**, and write:
  - **`app.yaml`:** `command: ["streamlit", "run", "app.py"]`, plus `env` entries from the app resources: the Postgres host and database, the endpoint name, the volume path.
  - **`requirements.txt`:** `streamlit`, `psycopg[binary]`, `databricks-sdk`, `pandas`, with versions pinned.
  - **`db.py`:** connections with an OAuth token from `WorkspaceClient()` for every new connection. Startup DDL: schema `app`, sequence `app.claim_no_seq`, and table `app.app_claims` (the columns in the design). Query helpers.
  - **`app.py`:** the sidebar mode switch, and for now a page that shows the number of rows in both synced tables.

  **Done differently (2026-10-03): no `app.yaml`.** The bundle's app resource has a `config` block (`command`, and `env` with `value` or `value_from`). Unlike `app.yaml`, it can use `${…}`, which the per-target Postgres schema name (`SYNCED_SCHEMA`) needs. So the command and env go in `claims_app.yml` (step 4.2). `requirements.txt` uses version ranges: `psycopg[binary]`, `psycopg-pool`, and `databricks-sdk>=0.81` for `w.postgres`; Streamlit comes with the runtime.
- [x] **Step 4.2: Add the app resource:** key `claims_app`, a name valid for apps, `source_code_path: ../src/claims_app`, and these resources:
  - `postgres` (`branch`, `database`, `CAN_CONNECT_AND_CREATE`)
  - `serving_endpoint` (`${resources.model_serving_endpoints.claims_damage_level_endpoint.name}`, `CAN_QUERY`)
  - `uc_securable` for the `claims` volume (`READ_VOLUME` and `WRITE_VOLUME`)

  Validate.
- [x] **Step 4.3: Deploy and start it:** `bundle run claims_app`.
- [x] **Step 4.4: Grant read access:** let the app's service principal read the synced tables (`GRANT USAGE ON SCHEMA …` / `GRANT SELECT ON ALL TABLES IN SCHEMA …`, run as you in the Lakebase SQL editor).

  **Check:** the page shows 12,999 and the policy count.

  **Result (2026-10-03): the page shows 12,999 / 12,000 / 0.** Two problems on the way:
  - **`LAKEBASE_ENDPOINT` isn't injected** by the `postgres` app resource, despite the skill's reference. The pool's log showed `error connecting in 'pool-23': 'LAKEBASE_ENDPOINT'` (a `KeyError`). It's now passed in from `var.lakebase_endpoint` in `config.env`.
  - **Failed `@st.cache_resource` calls aren't cached,** so every page load started a new pool that kept retrying. `get_pool()` now closes a pool whose setup fails.

  `db.diagnose()` shows which variables are set, plus one direct connection attempt, whenever reading fails. The GRANT is manual: redo it if a synced table is recreated or the app gets a new service principal. App logs are only in the UI (Compute → Apps → Logs), because the CLI profile is a personal access token.
- [x] **Step 4.5: Check the files** with `git status --short`.

---

### Task 5: Customer mode

**Files:**
- Modify: `src/claims_app/app.py`
- Create: `src/claims_app/services.py` (endpoint call, volume read/write)

- [x] **Step 5.1: The photo and the prediction:**
  - an uploader, and the photo shown next to the model's label
  - the endpoint called with `{"dataframe_records": [{"content": "<base64>"}]}` through the SDK
  - one retry with a 120 s timeout, then NULL

  **Result (2026-10-03):** `47-major (3).png` → **MAJOR**. The page first kept looping between "Connecting" and "Running". smart_claims_dev's `src/app/app.yaml` had the fix:
  - Streamlit flags in the app's `config.command`: `--server.enableWebsocketCompression=false` (the Apps proxy mishandles compressed websockets), and `--server.enableXsrfProtection=false --server.enableCORS=false` (otherwise uploads fail with a 400 behind the proxy)
  - `streamlit==1.65.0`, pinned in `requirements.txt` (overriding the runtime's 1.38) and in pyproject's dev group
- [ ] **Step 5.2: The form and the submit:**
  - the fields from the design
  - the policy lookup in the synced `policy_lookup`
  - `check_claim`
  - the photo saved to `claims/app_uploads/<claim_no>.<ext>` first, then the row inserted with `APP%08d` from the sequence
  - the result box (approved / needs review, each check ✓ / ✗ / –)
- [ ] **Step 5.3: Deploy, then try it (you, UI).**
  - **Claim 1:** submit a matching claim on `POL0000003` (telematics, max speed 148.2, so `speed_ok` passes; `POL0000001` is at 159.1 and would fail it), for example an image from `training_images` whose label matches your severity, with an amount under the limit.
  - **Claim 2:** a claim over the limit.
  - **Claim 3:** an unknown policy.

  **Check:**
  - Claim 1 is approved, or explains why not.
  - Claim 2 is `needs_review` with `amount_within_limit` ✗.
  - Claim 3 shows the message and saves nothing.
  - The rows and photos exist.
- [ ] **Step 5.4: Check the files** with `git status --short`.

---

### Task 6: Admin mode

**Files:** Modify `src/claims_app/app.py`, `src/claims_app/db.py`

- [ ] **Step 6.1: Overview:**
  - counters (total, auto-approved, needs review)
  - counts per claimed severity (in the order Trivial → Total Loss)
  - filters for severity, status and source
  - the list from `claim_checks UNION ALL app_claims`, app claims first, `LIMIT 500`, with filters in SQL
- [ ] **Step 6.2: Analysis:**
  - a claim search (prefilled with a pipeline claim)
  - the photo, from `images/`, `archive/` or `app_uploads/`
  - the details and each check with its numbers
  - the status
- [ ] **Step 6.3: Deploy, then try it (you, UI).**

  **Check:**
  - The counters equal 12,999 plus your app claims.
  - The severity filter changes the list.
  - Your `APP…` claims from Task 5 appear first, and open in Analysis with their photo.
  - A pipeline claim's photo shows.
- [ ] **Step 6.4: Check the files** with `git status --short`.

---

### Task 7: Wrap-up

- [ ] **Step 7.1: Update CLAUDE.md** (part 6b):
  - the shared Lakebase project and **the warning never to destroy `smart_claims_dev`**
  - `claim_rules.py` as the single copy of the rules
  - the resources and the sync mode (and the fallback, if it was used)
  - `app_claims` and the `APP` claim numbers
  - how to start the app, and the GRANT step
- [ ] **Step 7.2: Pause the cost (you decide):** keep the app and the continuous sync running, or switch both synced tables to `SNAPSHOT` (or stop the app) until you need them.
- [ ] **Step 7.3: Check the files** with `git status --short`.
