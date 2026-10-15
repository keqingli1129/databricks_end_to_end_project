# Part 7: moving the project from AWS (Free Edition) to Azure Databricks: what changed, step by step

> **Status: done 2026-10-15.** Target `azure_dev` runs parts 1–5 and 6a on an Azure Databricks **trial** workspace. The serving endpoint and the claims app (part 6b) are not on Azure: the trial has no Model Serving. Plan with ticks: [part7_azure_plan.md](part7_azure_plan.md). Design: [part7_azure_design.md](part7_azure_design.md).

## The result in one table

| | AWS: targets `dev` / `prod` | Azure: target `azure_dev` (`azure_prod` = CI, [part 9](part9_cicd_plan.md)) |
|---|---|---|
| Workspace | Free Edition, `dbc-b5c9918e-c2d2.cloud.databricks.com` | Premium **trial** `e2e-learning`, `adb-7405618954475016.16.azuredatabricks.net`, East US 2, Hybrid |
| CLI profile | `DEFAULT` | `azure` (you, workspace admin) · `azure-account` (`dbadmin`, account admin) |
| Account console | none | yes, as `dbadmin@keqingli1129gmail.onmicrosoft.com` |
| Catalog storage | Databricks Default Storage | **your** ADLS Gen2 `ste2elearningkl`, container `dabuc` |
| Catalogs | `e2e_dev`, `e2e_prod` (by hand) | `e2e_dev`, `e2e_prod` (by hand, with a managed location) |
| Part 1 telematics | Confluent topic `telematics`, scope `kafka_dev` | **same topic**, scope `kafka_azure_dev` |
| Part 2 CDC | simulated: seed job + Change Data Feed + AUTO CDC | **the same simulation** (real Lakeflow Connect: optional [runbook](part7b_lakeflow_connect_switch.md)) |
| Part 3 files | volumes in `landing` | same volumes, files uploaded again |
| Part 4 silver/gold | ✅ | ✅ (needed a fix, step 6) |
| Part 5 model | version 2 + serving endpoint | **version 1**, batch predictions; **no endpoint** (trial) |
| Part 6a dashboard, Genie | ✅ | ✅ |
| Part 6b app + Lakebase | ✅ (shares `smart-claims-dev`'s Lakebase project) | ❌ needs the endpoint; waits for a Premium upgrade |
| Cost | free | DBUs free until about **2026-10-28**, then Premium rates; storage costs cents a day |

Same in both: all code in `src/`, the job and pipeline definitions, the schema and table names inside the catalog (`dev_keqingli1129_bronze` …), the warehouse name ("Serverless Starter Warehouse"), and every command apart from `-t` / `--profile`.

---

## Step 1. Azure: subscription, budget, resource group

| What | Value |
|---|---|
| Subscription | pay-as-you-go on the personal Gmail Microsoft account ("Azure subscription 1"); a Free Trial subscription can't host Azure Databricks |
| Budget | `e2e-learning`, $50/month, alerts at 50% and 90% |
| Resource group | `rg-e2e-learning`, East US 2: everything we create lives here, so cleanup is one delete |

New on Azure: you pay for the cloud resources yourself (VMs, storage). Free Edition hid all of that.

## Step 2. The workspace

Portal → Azure Databricks → Create: `e2e-learning`, East US 2, **Trial (Premium - 14-Days Free DBUs)**, **Hybrid**, Secure Cluster Connectivity (no public IP), no own VNet.

- **Check the Review page says `trial`, not `premium`.** Ours first said `premium`: the trial option hadn't stuck.
- Azure creates a **managed resource group** `databricks-rg-e2e-learning-…` (VNet, the workspace storage account `dbstorage…`, an access connector, later the VMs). Databricks owns it; don't edit it.
- Don't click **Upgrade to Premium** unless you mean it: it ends the free DBUs.

## Step 3. Who's admin

| Role | Who | How it happened |
|---|---|---|
| Azure **Owner** | your Gmail account | you created the subscription |
| **Workspace admin** | your Gmail account | first launch of the workspace with the Owner role |
| **Account admin** | `dbadmin@keqingli1129gmail.onmicrosoft.com` | first Global Administrator to sign in to the account console |

**Gotcha:** a personal (Gmail) Microsoft account **can't sign in to accounts.azuredatabricks.net** ("Selected user account does not exist in tenant 'Microsoft Services'"). Fix: create a member user in your own tenant (Entra ID → Users → New user `dbadmin`, role **Global Administrator**), sign in as that user in a separate browser profile, register MFA (Microsoft Authenticator).

AWS Free Edition has no account console, so none of this existed there.

## Step 4. CLI profiles

```bash
databricks auth login --host https://adb-7405618954475016.16.azuredatabricks.net --profile azure
databricks auth login --host https://accounts.azuredatabricks.net --account-id 1d1ca9dc-3448-4226-8f99-d518d2ef65db --profile azure-account
```

**Gotcha:** the first login, run from the VS Code terminal, hung after the browser's "success". Running it again worked; if it hangs, open the printed URL in a browser **on this machine** (for `azure-account`: in `dbadmin`'s browser window).

## Step 5. Storage for the catalogs

`databricks metastores summary` showed **`storage_root = None`**: the automatically created metastore has no storage, so `CREATE CATALOG` without a location fails. On Free Edition, Default Storage handled this. On Azure we built the chain ourselves:

```text
storage account ste2elearningkl (ADLS Gen2: hierarchical namespace ON), container dabuc
   ▲ role "Storage Blob Data Contributor"
access connector ac-e2e-learning (managed identity, no secret)       ← Azure portal
   ▲
storage credential e2e_learning_cred                                 ← Databricks CLI
   ▲
external location e2e_learning_uc = abfss://dabuc@ste2elearningkl.dfs.core.windows.net/
   ▲
CREATE CATALOG e2e_prod / e2e_dev  MANAGED LOCATION '…/e2e_prod' / '…/e2e_dev'
```

- Don't use the `dbstorage…` account or the `unity-catalog-access-connector` in the managed resource group: they belong to the workspace, and the catalogs should outlive it.
- `databricks storage-credentials validate` (READ/LIST/WRITE/DELETE/HIERARCHICAL_NAMESPACE_ENABLED all PASS) proves the chain before anything uses it.

## Step 6. Bundle changes (YAML and code)

| File | Change | Why | AWS effect |
|---|---|---|---|
| `databricks.yml` | variables `serving_model_version` (default "2"), `name_suffix` (default `dev`), `gateway_driver_node_type`, `gateway_node_type`; targets **`azure_dev`** and **`azure_prod`** | Azure targets with their own host, catalogs, names | none (defaults = AWS values; `prod` got `name_suffix: prod`) |
| `resources/damage_classifier.yml` | serving endpoint moved under `targets: dev / prod` (YAML anchor + alias), version from `var.serving_model_version`; `azure_dev` **commented out** | the endpoint needs an existing model version, and the trial has no Model Serving | none |
| `resources/claims_app.yml` | app, Lakebase database, synced tables under `targets: dev / prod`; names use `var.name_suffix` | they point at AWS's Lakebase project and need the endpoint | none (same names) |
| `resources/ingest_db_cdc_insurance.pipeline.yml`, `seed_source_database.job.yml` | **unchanged**: every target shares the simulation | simplest; real Lakeflow Connect is the optional runbook | none |
| `src/transformations/transformations/silver_to_gold.py` | `_claim_image_predictions()`: empty predictions while `gold.claim_image_predictions` doesn't exist, checked with `WorkspaceClient().tables.exists` | **deadlock on a fresh workspace** (see step 8) | same results (the table exists there) |

**Proof that AWS didn't change:** `databricks bundle plan -t dev --profile DEFAULT` → `0 to add, 0 to change, 0 to delete, 32 unchanged` (the AWS Lakebase compute has to be switched on for the plan, then off again).

## Step 7. First deploy

```bash
databricks bundle plan   -t azure_dev --profile azure    # 27 to add
databricks bundle deploy -t azure_dev --profile azure
```

7 schemas, 3 volumes, secret scope `kafka_azure_dev`, 5 pipelines, 6 jobs, experiment, registered model, dashboard, Genie space. On AWS the same deploy has 32 resources; the 5 missing on Azure are the endpoint, the app, the Lakebase database and the two synced tables.

**Gotcha:** the **Genie space fails** on a fresh workspace ("Table … claim_checks does not exist"): Genie checks that its tables exist when it's created. Everything else is created; a redeploy after gold exists creates it.

## Step 8. Running the parts, in order

| Step | Command (all with `-t azure_dev --profile azure`) | Result on Azure |
|---|---|---|
| Part 2 source | `bundle run seed_source_database` | 7000 / 12000 / 13000, Change Data Feed on |
| Part 2 bronze | `bundle run cdc_ingestion` | 7000 / 12000 / 13000 |
| Part 2 change test | `src/source_database/changes.sql` in the SQL editor (works unchanged: same names), then `cdc_ingestion` | 6999 / 12001 / 13000, `CLM00000003` = Minor Damage |
| Part 1 secrets | `databricks secrets put-secret kafka_azure_dev bootstrap_servers / api_key / api_secret` (typed at the prompt) | 3 keys |
| Part 1 | `bundle run telematics_simulator`, then `bundle run telematics_ingestion` | 50 events, 10 cars |
| Part 3 files | `databricks fs cp -r data/object_storage/… dbfs:/Volumes/e2e_dev/dev_keqingli1129_landing/…` (training images, claim images, the **prepared** metadata CSV, empty `archive/`) | 56 + 15 + 1 files |
| Part 4 | `bundle run smart_claims_end_to_end` | **failed in `transform`** (deadlock, below) |
| Part 4 (after the fix) | `bundle deploy`, `bundle run transformations` | silver 7 tables, gold 5 MVs; `claim_checks` 12,999 |
| Genie | `bundle deploy` | created: the first fully green deploy |
| Part 5 | `bundle run damage_classifier` | version 1, alias `prod`, test accuracy 0.73; 15 claim photos scored |
| Part 4 again | `bundle run transformations` | `severity_match` filled: 2,375 auto_approved, 10,624 needs_review |
| Part 5 endpoint | add `azure_dev` to the endpoint wrapper, deploy | **`FEATURE_DISABLED`: no Model Serving on trial workspaces** → commented out |

**The deadlock.** `gold.claim_checks` reads `gold.claim_image_predictions`, which the **ML job** writes; the ML job needs **silver** from the same pipeline. A pipeline analyzes every dataset before it runs, so on a fresh workspace the missing table failed the whole update, and silver never got built. AWS never saw it because part 5 ran before part 6a added `claim_checks`. The fix returns empty predictions until the table exists (`severity_match` NULL = "not run"), so the order on a new workspace is: pipeline → ML job → pipeline.

- First attempt used `spark.catalog.tableExists`: **blocked inside pipelines** (`PY4J_BLOCKED_API`). The Databricks SDK's `tables.exists` (Unity Catalog REST API) works.
- `databricks bundle run transformations --validate-only` tests the analysis step in about 2 minutes without writing anything.

## Step 9. Other differences we noticed

- **`samples`** is a **Delta Sharing** catalog on Azure ("Shares received"), a regular catalog on Free Edition. Reading it works the same.
- **Kafka is shared.** Both clouds read topic `telematics` with their own checkpoints. Azure's first run started from the earliest kept event; older AWS events had already expired (Confluent retention).
- **Model versions are per workspace.** Azure starts at 1, hence `var.serving_model_version`.
- **The metastore owner** is a system id on Azure (it was created automatically); you're workspace admin, not metastore admin, and that's enough to create catalogs.

## Day-to-day commands

```bash
# AWS                                                   # Azure
databricks bundle deploy -t dev --profile DEFAULT       databricks bundle deploy -t azure_dev --profile azure
databricks bundle run <key> -t dev --profile DEFAULT    databricks bundle run <key> -t azure_dev --profile azure
```

A code change in `src/` is shared: each cloud gets it at its next deploy.

## Not done (later, optional)

| What | Needs | Where |
|---|---|---|
| Serving endpoint + claims app (part 6b) on Azure | **Upgrade the workspace to Premium** (ends the free DBUs); own Lakebase project | uncomment the block in `damage_classifier.yml`; [part7_azure_plan.md](part7_azure_plan.md) Tasks 12.3–13 |
| Real Lakeflow Connect from Azure SQL | about $10–20 (gateway VMs) | [part7b_lakeflow_connect_switch.md](part7b_lakeflow_connect_switch.md) |
| CI/CD to `azure_prod` | push, then the first workflow run | [part9_cicd_plan.md](part9_cicd_plan.md) Task 10 (on hold) |

## Stopping and cleaning up

- **Between sessions:** nothing runs on its own (dev mode pauses schedules; pipelines are triggered; the warehouse auto-stops). Costs while idle: storage, cents a day.
- **When the trial ends (about 2026-10-28):** either upgrade (pay per use) or delete everything: Azure portal → **Resource groups → `rg-e2e-learning` → Delete resource group**. That removes the workspace, its managed resource group, the storage account and the access connector. The AWS project is untouched. Optionally remove the `azure_*` targets from `databricks.yml` afterwards.
