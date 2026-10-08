# Part 7: the whole project on Azure Databricks, with real Lakeflow Connect (design)

Parts 1–6 run in the Free Edition workspace (AWS). This part deploys the **same bundle** to an **Azure Databricks trial workspace** as a new target, `azure_dev`. There, part 2 finally runs the way [transcript_2.txt](transcript_2.txt) does it: **Lakeflow Connect for SQL Server**, with an **ingestion gateway** on classic compute, reading a real **Azure SQL Database**.

This is a learning exercise, not a production system. Keep it simple, and stop anything that costs money between sessions.

## Goals

- Learn how one bundle deploys to **two clouds** through targets and per-target settings.
- Learn the real Lakeflow Connect database pattern: change tracking + CDC on the source, UC connection, gateway, staging volume, ingestion pipeline.
- Get parts 1, 3–6 working on Azure without changing their code.

## Out of scope

- **`azure_prod`:** defined as a placeholder target and **not deployed**, maybe never.
- **GCP targets** (`gcp_dev`/`gcp_prod`): a later round, built the same way.
- **The query-based SQL Server connector** (Public Preview, no gateway): noted, not used.
- **Automated tests:** you chose none. The checks are row counts and the insert/update/delete test.

## Constraints

- **Budget: a pay-as-you-go subscription on your personal (Gmail) Microsoft account, with no free credit.**
  - Your $200 free credit was already used on that account.
  - Azure free-trial subscriptions can't host Azure Databricks anyway.
  - The district account is out: it belongs to the district's tenant.
  - The Azure Databricks **Premium trial waives DBUs for 14 days**, but Azure VM, disk and storage costs bill to your card.
  - **Target: about $30–50 in total.**
  - A budget alert at $50 (alerts at 50% and 90%). Everything goes in one resource group.
- **Region: East US 2.** It has serverless compute, Databricks Apps, Lakebase, Lakeflow Connect and CPU model serving.
- **Workspace type: Hybrid (classic + serverless),** not "Serverless": the gateway must run on classic compute.
- **vCPU quota:** the gateway driver needs **at least 8 cores**, plus one worker (about 12 vCPUs, about $0.80/hour while running). Request a quota increase before the first gateway run.
- **Azure SQL free offer:** 100,000 vCore-seconds per month per database, up to 10 free databases per subscription, on any subscription type.
  - Set "behavior when free limit reached" to **"Auto-pause the database until next month"**, so it never bills. That choice can't be changed later.
  - A connected gateway keeps the database awake, which uses up the free amount in about 1–3 days.
  - **Stop the gateway after every session.** That also stops the VM cost.
- **The bundle doesn't own catalogs:** create `e2e_dev` by hand in the Azure workspace, as on AWS.
- **No git commits** by Claude. You commit on `main`.
- **Secrets never go in git:** the Azure SQL password goes into the UC connection by prompt; Kafka values go into the secret scope by prompt.

## Architecture

```text
Azure resource group rg-e2e-learning (one region)
├─ Azure SQL logical server  (SQL auth, public endpoint, "Allow Azure services" = Yes)
│   └─ database claims (free offer)
│       dbo.customer · dbo.policy · dbo.claim   ← change tracking + CDC on, DDL capture set up
└─ Azure Databricks workspace (Trial Premium, Unity Catalog)
    catalog e2e_dev (made by hand)
      UC connection azure_sql_claims  (host, port 1433, user databricks_ingest)
            │
            ▼
      pipeline "gateway"  (classic VM, runs while started)
        reads CT/CDC from Azure SQL → staging volume in landing
            │
            ▼
      pipeline cdc_ingestion  (serverless, ingestion_definition, triggered)
        applies upserts and deletes, SCD Type 1
            ▼
      bronze.customer · policy · claim  ──►  silver ──►  gold ──►  dashboard · Genie · app
      (parts 1, 3, 4, 5, 6: same code as AWS)
```

| Transcript | Here |
|---|---|
| SQL Server on RDS | Azure SQL Database (free offer) |
| enable change tracking and CDC, DDL-capture script | the same, in T-SQL (`src/source_database/azure/setup_cdc.sql`), following the current Databricks docs |
| UC connection | `azure_sql_claims`, created once by CLI |
| ingestion gateway (classic VM) → staging volume | gateway pipeline, a bundle resource in `azure_dev` |
| ingestion pipeline → bronze | `cdc_ingestion` with `ingestion_definition`, a bundle resource in `azure_dev` |
| test: INSERT policy, UPDATE claim severity, DELETE customer | `src/source_database/azure/changes.sql` |

## Targets

```yaml
targets:
  # --- AWS (Free Edition) ---
  dev:          # unchanged (comment added)
  prod:         # unchanged (comment added)
  # --- Azure (Azure Databricks trial) ---
  azure_dev:    # mode development, host adb-….azuredatabricks.net, catalog e2e_dev
  azure_prod:   # placeholder: mode production, never deployed for now
```

- The Azure workspace has its **own metastore**, so the catalog name `e2e_dev` doesn't clash with AWS.
- **CLI profile `azure`:** `databricks bundle deploy -t azure_dev --profile azure`.
- The existing targets keep their names. Renaming them would make bundle state treat them as new targets and deploy a second copy.

### What differs per target

| | `dev` / `prod` (AWS) | `azure_dev` |
|---|---|---|
| Part 2 source | Delta `source` schema plus job `seed_source_database` | Azure SQL plus gateway |
| Part 2 pipeline `cdc_ingestion` | Python files, `readChangeFeed` plus AUTO CDC | `ingestion_definition` (Lakeflow Connect) |
| Lakebase | references `smart-claims-dev` (Free Edition allows one project) | owns its own project (target-level `postgres_projects` resource); `var.lakebase_branch` / `var.lakebase_endpoint` point at it |
| Serving endpoint version | `"2"` | the version trained on Azure (starts at `"1"`) |
| `var.gateway_driver_node_type` / `var.gateway_node_type` | not used | `Standard_D8ds_v5` / `Standard_D4ds_v5` (12 vCPU) |

**How part 2 differs per target:** a bundle can't remove a shared resource for one target, and target overrides *merge* into shared definitions. A Python-file pipeline and an `ingestion_definition` pipeline can't be merged. So:
- An included resource file may have its own `targets:` section. So `ingest_cdc_insurance.pipeline.yml` and `seed_source_database.job.yml` wrap their content in `targets: dev / prod`, using a YAML anchor on `dev` and an alias in `prod`. Their paths and keys stay the same, so the AWS deployment state still matches.
- A new file, `resources/azure_cdc_ingestion.yml`, defines `targets: azure_dev` with the gateway plus its own `cdc_ingestion`.
- **Keeping the key `cdc_ingestion` on both clouds** means `smart_claims_end_to_end` (`${resources.pipelines.cdc_ingestion.id}`) works unchanged.
- The `source` schema stays shared; on Azure it's simply empty.

**The serving endpoint** pins a version that has to exist when it's deployed. On Azure the model is trained after the first deploy, so:
- `entity_version` becomes a per-target variable.
- The endpoint comes up in a later deploy, after `train_and_register`.
- The endpoint and everything that needs it or the gold tables (the claims app, its Lakebase database and the synced tables) use the same per-target wrapper. `azure_dev` is added to those wrappers only after training (endpoint) and after gold exists (app).
- App names and Lakebase database ids don't allow underscores, so a new variable `name_suffix` (`dev`, `prod`, `azure-dev`) replaces `${bundle.target}` in those two names. The AWS names stay the same.

## Part 2 on Azure

### Source tables (`src/source_database/azure/seed.sql`, T-SQL)

- **The same tables, columns, row counts and number rules** as [seed.sql](../src/source_database/seed.sql): customer 7,000, policy 12,000, claim 13,000.
- **The same key formats:** `C000001`, `POL0000001`, `CLM00000001`.
- **The same chassis rule:** `CHS%06d` from the policy number, so `POL0000001`–`10` match the part 1 telematics cars.
- **Primary keys** on `customer_id`, `policy_no` and `claim_no`; change tracking needs them.
- **Safe to re-run:** create only if missing, fill only while empty.
- **You run it** in VS Code with the `mssql` extension. A bundle SQL task runs on a Databricks warehouse, so it can't run T-SQL.

**Type mapping:** `STRING` becomes `NVARCHAR(n)`, `DATE` stays `DATE`, `DECIMAL` stays `DECIMAL`, `INT` stays `INT`, and `TIMESTAMP` becomes `DATETIME2`. Bronze has to come out with the types silver expects. If Lakeflow Connect lands different types or extra columns, fix it with a small change in silver. Check this during the plan.

### Change tracking, CDC and the ingest user (`src/source_database/azure/setup_cdc.sql`)

- First run Databricks' **`utility_script.sql`** (from the Lakeflow Connect docs) as the server admin. It creates the procedures `lakeflowSetupChangeTracking`, `lakeflowSetupChangeDataCapture` and `lakeflowFixPermissions`, plus the DDL-capture objects.
- **Change tracking** on the 3 tables via `lakeflowSetupChangeTracking`. Databricks recommends it for tables with primary keys.
- The transcript also enables **CDC**, so we enable it too via `lakeflowSetupChangeDataCapture`, to see it. When both are on, the connector uses change tracking.
- **Ingest user `databricks_ingest`:** a **login in `master` plus a user in `claims`**. A contained user won't work, because the required server role `##MS_DatabaseConnector##` only accepts logins. Then `lakeflowFixPermissions` grants what the docs list.

### Pipelines (bundle resources, `azure_dev` only)

- **Gateway** `sql_server_gateway`:
  - `gateway_definition` with `connection_name: azure_sql_claims`, plus `gateway_storage_catalog` / `gateway_storage_schema` (the landing schema) and `gateway_storage_name`.
  - A `clusters:` block with `driver_node_type_id: ${var.gateway_driver_node_type}` (8 cores) and `node_type_id: ${var.gateway_node_type}`.
  - The docs run the gateway with `continuous: true`, but **development mode drops `continuous`** (see part 1). The plan checks whether a started gateway update keeps running anyway. If not, we start it by hand each session.
- **`cdc_ingestion`:**
  - `serverless: true`, plus `ingestion_definition` with `ingestion_gateway_id: ${resources.pipelines.sql_server_gateway.id}` and `table_configuration: {scd_type: SCD_TYPE_1}`.
  - Objects: `claims.dbo.customer/policy/claim` → `${var.catalog}.${resources.schemas.bronze.name}.<table>`.
  - It lands **streaming tables**, and deleting the pipeline drops them.
  - Triggered only, because Lakeflow Connect has no continuous mode.

## Parts 1, 3–6 on Azure (one-off setup only)

| Part | Setup |
|---|---|
| Base | Create catalog `e2e_dev` by hand. Check what the auto-created warehouse is called: Azure docs say "Starter Warehouse", and `var.warehouse_id` looks up "Serverless Starter Warehouse". If they differ, override the lookup in `azure_dev`. |
| 1 | Secret scope `kafka_azure_dev` comes from the bundle; put in the **same Confluent values**. Both clouds read the same topic, each with its own checkpoint. |
| 3 | Upload `data/object_storage/prepared/` and the images to the Azure volumes with `databricks fs cp --profile azure`. |
| 4 | Nothing extra. |
| 5 | Run `damage_classifier` to train and register version 1, then deploy the endpoint with that version. |
| 6a | Deploys as it is. |
| 6b | Own Lakebase project, `var.lakebase_*`, the manual GRANT to the app's service principal, then `bundle run claims_app`. |

**Order:** Azure setup → base deploy → part 2 → parts 1 and 3 → 4 → 5 → 6a → 6b. Each part is checked before the next, so finished parts keep working if time or credit runs out.

## Cost control

- **Things that cost money while running:** the gateway (classic VM, and it keeps Azure SQL awake), Lakebase compute, the continuous synced-table pipeline, and the app.
- **After every session, run a pause checklist** in the plan: stop the gateway, the app and the sync pipeline, and disable Lakebase compute.
- The serving endpoint scales to zero by itself.
- **When the trial ends:** delete the resource group. The `azure_*` targets either stay in the repo as a record or get removed; that's your call then.

## Checks

- **Part 2:**
  - Azure SQL counts 7000 / 12000 / 13000 match bronze.
  - After `azure/changes.sql` and a pipeline run: `C007000` is gone, `CLM00000003` is "Minor Damage", and `POL9999001` exists (6999 / 12001 / 13000).
- **Parts 1, 3–6:** the same checks as on AWS: row counts, the data-quality event log, gold tables, dashboard, Genie answers, and an app claim submission.

## Units

| File | Responsibility |
|---|---|
| `databricks.yml` | AWS comments, targets `azure_dev` and `azure_prod`, variables `serving_model_version`, `name_suffix`, `gateway_driver_node_type`, `gateway_node_type` |
| `resources/ingest_cdc_insurance.pipeline.yml`, `resources/seed_source_database.job.yml` | content wrapped in `targets: dev / prod` (AWS only) |
| `resources/damage_classifier.yml` | endpoint wrapped per target; `entity_version: ${var.serving_model_version}` |
| `resources/claims_app.yml` | Lakebase database, synced tables and app wrapped per target; `name_suffix` in the app name and database id |
| `resources/azure_cdc_ingestion.yml` | gateway and Lakeflow Connect `cdc_ingestion` (`azure_dev`) |
| `resources/azure_lakebase.yml` | `azure_dev`'s own Lakebase project |
| `src/source_database/azure/seed.sql` | T-SQL tables and data |
| `src/source_database/azure/setup_cdc.sql` | change tracking, CDC, DDL capture, ingest user |
| `src/source_database/azure/changes.sql` | insert/update/delete test |
| `docs/part7_azure_plan.md` | step-by-step plan |
| `CLAUDE.md` | Azure target, commands and gotchas (last step) |
