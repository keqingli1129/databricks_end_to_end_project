# Part 6a: claim checks, AI/BI dashboard and Genie space (design)

This design follows the first two thirds of [transcript_6.txt](transcript_6.txt): consuming the gold data through an **AI/BI dashboard** and a **Genie space**. A gold table of **claim checks**, the transcript app's business rules, is built first so that every consumer uses the same rules. The **Databricks App** (customer and admin modes, Lakebase, the serving endpoint) is **part 6b**, planned separately afterwards.

## Free Edition and workspace facts

| Topic | Fact | Consequence |
|---|---|---|
| Dashboards and Genie | not limited | build both |
| Apps | up to 3 per account, auto-stop after 24 h; 1 in use (`smart-claims-dev`) | part 6b |
| Lakebase | **one project per account**, already used by the `smart_claims_dev` project | decided in part 6b |
| SQL warehouse | one, 2X-Small (Serverless Starter Warehouse) | used by the dashboard and Genie (`var.warehouse_id`) |
| Name clashes | `[dev keqingli1129] Claims Investigation` exists, from `smart_claims_dev` | our titles start with **"E2E"** |

## Your choices

- **Scope (A):** checks, dashboard and Genie now; the app as its own plan.
- **Rules:** as proposed (below).
- **Dashboard (C):** you build a small one in the UI, guided step by step, then it's exported and I build the full one in the bundle.
- **Genie (C):** the same approach.
- **No automated tests,** and no Claude commits. Profile `DEFAULT`, target `dev`.

## `gold.claim_checks`

This is a materialized view in `silver_to_gold.py` (pipeline `e2e_transformations`), with one row per claim in `gold.customer_claim_policy_telematics` (about 12,999).

| Input | Join | Gives |
|---|---|---|
| `gold.customer_claim_policy_telematics` | base | the claim, policy, customer and telematics columns (`max_speed` is null without telematics) |
| `silver.claim_images_metadata` | left join on `claim_no`, one row per claim (`first(image_name)`, which removes part 3's two demo duplicates) | `image_name` |
| `gold.claim_image_predictions` (written by part 5's job) | left join on `image_name` | `predicted_damage` |

**Rules** (named constants at the top of the code):

| Column | Rule |
|---|---|
| `expected_damage` | Trivial Damage → `ok`, Minor Damage → `minor`, Major Damage → `major`, Total Loss → `major` |
| `coverage_limit` | COMPREHENSIVE 20,000 · COLLISION 15,000 · LIABILITY 10,000 |
| `severity_match` | `expected_damage = predicted_damage` (null if there's no prediction) |
| `amount_within_limit` | `claim_amount <= coverage_limit` |
| `policy_valid` | `incident_date BETWEEN start_date AND end_date` |
| `speed_ok` | `max_speed <= 150` (null without telematics, so the check isn't run) |
| `failed_checks` | an array of the names of the checks that are **false** (nulls ignored) |
| `claim_status` | `auto_approved` if `failed_checks` is empty, else `needs_review` |

Every output column has a **comment**, given through the MV's `schema` DDL, so Genie understands the table. Because the source data is random, most claims will be `needs_review`. That's expected.

## Dashboard and Genie

| Item | Practice version (you, in the UI) | Full version (bundle) |
|---|---|---|
| Dashboard | "Practice – claims summary": one dataset with `:start_date`/`:end_date` parameters, and one bar chart | **"E2E Claims Investigation"**: datasets with date parameters; counters (total, auto-approved, needs review); claims by incident type × severity; failed checks; review list. File `src/consumption/claims_investigation.lvdash.json`. |
| Genie | "Practice – claims genie": `gold.claim_checks`, plus one question | **"E2E Claims Genie"**: `gold.claim_checks` and `gold.customer_claim_policy_telematics`, instructions (severity order, status meaning, currency) and 4–5 sample questions. File `src/consumption/claims_genie.geniespace.json`. |

- **Export, then write:** the practice versions are **exported** (`databricks bundle generate dashboard` / the Genie API) to get the exact JSON format your workspace uses. The full JSON files are written from that.
- **One resource file:** both full versions are in `resources/claims_consumption.yml`, with keys `claims_investigation_dashboard` and `claims_genie_space`, and warehouse `${var.warehouse_id}`.
- **Tested first:** every dashboard SQL query is run with the CLI before deploying, following the AI/BI dashboard skill's rule.
- **The practice versions stay in the workspace,** so you can delete them in the UI whenever you like. They aren't in the bundle.
