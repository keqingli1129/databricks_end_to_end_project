# Part 6b: the claims app (design)

This design follows the last third of [transcript_6.txt](transcript_6.txt): a **Databricks App** with a **customer mode** (submit a claim with a photo; the model and the business rules decide at once) and an **admin mode** (an overview of all claims, and an analysis of one claim). Following the transcript, the app reads from **Lakebase** (Postgres) synced tables instead of the SQL warehouse, and calls the **serving endpoint** from part 5. Part 6a's `gold.claim_checks` and its rules are the starting point: see [part6a_consume_design.md](part6a_consume_design.md).

## Free Edition and workspace facts

| Topic | Fact | Consequence |
|---|---|---|
| Apps | up to 3 per account, stop after 24 h; 1 in use (`smart-claims-dev`) | this app uses a 2nd slot; restart it with `bundle run` |
| Lakebase | **one project per account**: `smart-claims-dev` (owned by the `smart_claims_dev` bundle), one branch `production`, its data in database `databricks_postgres` | we **share** the project: our own database `e2e_claims` in that branch |
| Bundle support (CLI 1.15) | `postgres_databases` (with `parent` = another project's branch), `postgres_synced_tables`, `apps` | everything of ours is a bundle resource; the project is only referenced |
| Serving endpoint | `dev_keqingli1129_e2e-claims-damage-level`, scale-to-zero, ~1 min cold start | the app retries once, and treats a failed prediction as "not run" |
| Serverless quota | continuous sync = pipelines that never stop | a "pause when done" step |

## Your choices

- **Lakebase:** share the existing project by adding our own database. Risk accepted: a `bundle destroy` of `smart_claims_dev` would delete the project, and our database with it.
- **Submissions:** an app-owned Postgres table. They aren't fed back into the lakehouse, so they aren't in the dashboard or Genie.
- **Framework:** Streamlit (Python).
- **Synced data:** two tables, `gold.claim_checks` (admin) and a new `gold.policy_lookup` (one row per policy, so every policy can be found).
- **Sync mode:** **continuous**. Fallback, if a materialized view can't be a continuous source: a job task MERGEs both MVs into plain Delta tables with Change Data Feed (`gold.claim_checks_sync`, `gold.policy_lookup_sync`), and those are synced instead.
- **Rules (A):** one pure-Python module, used by both the pipeline and the app.
- **No automated tests,** and no Claude commits. Profile `DEFAULT`, target `dev`.

## Architecture

```
                 ┌──────────── Lakebase project smart-claims-dev (NOT ours) ────────────┐
                 │  branch production                                                   │
 gold.claim_checks ──continuous sync──►  database e2e_claims (OURS)                     │
 gold.policy_lookup ─continuous sync──►    <gold schema>.claim_checks   (synced, read-only)│
                 │                          <gold schema>.policy_lookup  (synced, read-only)│
                 │                          app.app_claims               (app writes)      │
                 └──────────────────────────────────▲────────────────────────────────────┘
                                                    │ psycopg, OAuth token from the SDK
  claims volume ◄── photo upload / display ──  Streamlit app  ──► serving endpoint
  (claims/app_uploads/, images/, archive/)     src/claims_app/     (damage classifier)
```

### Bundle resources (new file `resources/claims_app.yml`)

| Key | Type | What |
|---|---|---|
| `e2e_claims_database` | `postgres_databases` | database `e2e_claims`, `parent: projects/smart-claims-dev/branches/production` |
| `claim_checks_synced` | `postgres_synced_tables` | source `gold.claim_checks` (or `_sync`), key `claim_no`, continuous, into `e2e_claims` |
| `policy_lookup_synced` | `postgres_synced_tables` | source `gold.policy_lookup` (or `_sync`), key `policy_no`, continuous, into `e2e_claims` |
| `claims_app` | `apps` | Streamlit, `source_code_path: ../src/claims_app`. App resources: the endpoint (`CAN_QUERY`), the Postgres database, the `claims` volume (read/write). |

The exact dev names (app names allow only lowercase letters, digits and hyphens; development mode may prefix database and synced-table IDs) are checked with `bundle validate` in the plan.

### Code

| Path | Contents |
|---|---|
| `src/claims_app/claim_rules.py` | **The single copy of the rules:** `EXPECTED_DAMAGE`, `COVERAGE_LIMITS`, `MAX_SPEED_KMH`, `CHECKS`, and `check_claim(policy, claim, predicted_damage)`. Pure Python, no Spark. |
| `src/claims_app/app.py` | Streamlit: the mode switch, customer form, admin tabs |
| `src/claims_app/` helpers | Postgres access (connections with SDK OAuth tokens, queries, `app_claims` DDL), endpoint call, volume read/write |
| `src/claims_app/app.yaml`, `requirements.txt` | how the app starts, and its libraries |
| `src/transformations/transformations/silver_to_gold.py` | `claim_checks` imports the constants from `claims_app.claim_rules` (the pipeline's `root_path` is `../src`); new MV `policy_lookup` |

The module lives in the app's folder because an app deploys only its own source folder; the pipeline can import from anywhere under `src/`.

### `gold.policy_lookup`

One row per policy, from silver `policy` and `customer` and gold `aggregated_telematics` (left join on the chassis number). Columns: `policy_no` (key), `customer_id`, `full_name`, `coverage`, `coverage_limit` (from `COVERAGE_LIMITS`), `start_date`, `end_date`, `chassis_number`, `max_speed` (NULL without telematics), with column comments.

## Data flow

### Customer mode: submitting a claim

1. **Photo:** upload a JPG or PNG. The app sends it base64-encoded to the endpoint (`{"dataframe_records": [{"content": "<b64>"}]}`) and shows the prediction (ok / minor / major) at once. If the call fails, it retries once with a 120 s timeout; if it still fails, the prediction is NULL.
2. **Form:** policy number, incident date, incident type (COLLISION, THEFT, WEATHER, VANDALISM, GLASS), self-assessed severity (Trivial / Minor / Major Damage / Total Loss), claim amount, location, collision type, vehicles involved, notes.
3. **Policy lookup** in the synced `policy_lookup` by `policy_no`. If it's not found, show "Unknown policy number" and save nothing.
4. **Checks:** `check_claim` returns `severity_match`, `amount_within_limit`, `policy_valid`, `speed_ok` (each TRUE / FALSE / NULL; NULL = not run), `failed_checks`, and `claim_status` (`auto_approved` / `needs_review`). These are the same rules and constants as `gold.claim_checks`.
5. **Save:** claim number `APP%08d` from a Postgres sequence (so it can't clash with `CLM…`). The photo is written first, to `/Volumes/<catalog>/<landing schema>/claims/app_uploads/<claim_no>.<ext>`; then one row goes into `app.app_claims`. If the photo upload fails, no row is inserted.
6. **Result:** approved: "Approved. Your refund arrives within 3–5 business days", with the claim number. Needs review: the claim number and the failed checks in plain words.

### `app.app_claims`

It has the `claim_checks` columns that matter to the app (claim, policy, customer, incident, amount, coverage and limit, policy dates, chassis, `max_speed`, `image_name`, expected and predicted damage, the four checks, `failed_checks` as `jsonb`, matching how the sync stores the array, `claim_status`), plus `location`, `collision_type`, `vehicles_involved`, `notes` and `submitted_at`. The app creates the schema `app`, the sequence and the table on startup (`IF NOT EXISTS`); its service principal owns them.

### Admin mode: reading

- **All claims** = the synced `claim_checks` rows `UNION ALL` the `app_claims` rows, over their shared columns, with a `source` column (`pipeline` / `app`). A submitted claim shows up at once.
- **Photos:** pipeline claims are found by `image_name` in `claims/images/`, or in `claims/archive/` when Auto Loader moved them; app claims are in `claims/app_uploads/`.
- The app's service principal needs `SELECT` on the synced tables' schema: a `GRANT` step in the plan.

## Screens

There's one page, with the mode switch in the sidebar.

- **Customer:** the photo uploader with the prediction next to the photo; the form; **Submit** (disabled until there's a photo and a policy number); the result box with each check marked ✓ / ✗ / – (not run).
- **Admin → Overview:** counters (total, auto-approved, needs review); counts per claimed severity (in the order Trivial → Total Loss); filters for severity, status and source; the claims list, newest app claims first, **at most 500 rows** (filtered in SQL).
- **Admin → Analysis:** a claim search (prefilled with a pipeline claim); the photo; the claim, customer and policy details; each check with its numbers (e.g. "12,000 ≤ 15,000"); the status.
- Plain Streamlit widgets with a light theme and no custom CSS. There's no login for admin mode, as in the transcript; access to the app is limited by workspace permissions.

## Error handling

| Situation | Behavior |
|---|---|
| Endpoint cold or failing | spinner, one retry (120 s), then prediction NULL → `severity_match` NULL; the claim can still be submitted |
| Unknown policy | a message; nothing saved |
| Postgres token expiry (~1 h) | a fresh OAuth token from the SDK for each new connection |
| Photo upload fails | no row inserted (photo first, row second) |
| Missing grants on synced tables | a clear error; the plan's GRANT step prevents it |

## Operations

- **Start the app:** `databricks bundle run claims_app`. It stops itself after 24 h on Free Edition.
- **Pause the sync when done:** the continuous sync pipelines run all the time. Switch them to snapshot, or destroy the synced tables, when you don't need the app.
- **Shared project:** never `bundle destroy` the `smart_claims_dev` bundle while this app is wanted. CLAUDE.md will say so.

## The plan's first step

Check whether a **materialized view** can be the source of a **continuous** synced table. If it can't, use the fallback above (the MERGE task into `gold.*_sync` Delta tables with Change Data Feed, added to `smart_claims_end_to_end` after `transform`).

**Result (2026-10-03): it can.** The MV has Change Data Feed and row tracking on. With the table property `pipelines.externalMetadata.enabled` and `pipeline_channel: PREVIEW` on the synced table's pipeline, `claim_checks_pg` runs online in continuous mode, so no fallback is needed. The shared compute had to be **enabled** first: it was disabled, not just scaled to zero.
