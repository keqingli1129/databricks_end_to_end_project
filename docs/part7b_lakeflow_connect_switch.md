# Part 7b: switch `azure_dev` from the simulated source to real Lakeflow Connect (runbook, for later)

> **Status:** not started. Written 2026-10-15 so we can switch later. Today `azure_dev` runs part 2 **like Free Edition**: the job `seed_source_database` fills a Delta `source` schema, and `cdc_ingestion` reads its change feed (see the route note at the top of [part7_azure_plan.md](part7_azure_plan.md)).
>
> **How we run it:** one step at a time. Claude explains a step before running it, ticks its box, and waits for "go". Claude never commits.

**Goal:** part 2 on `azure_dev` as in [transcript_2.txt](transcript_2.txt): an operational **Azure SQL** database `claims`, with **change tracking** on, read by a **Lakeflow Connect ingestion gateway**, landing in bronze through a managed **ingestion pipeline**. Everything downstream (silver, gold, dashboard, app) stays unchanged.

## Before and after

```text
NOW (simulation, every target)                      AFTER (azure_dev only)
job seed_source_database (SQL warehouse)            Azure SQL Database `claims` (free offer)
  └─▶ e2e_dev.dev_<user>_source.customer/…            dbo.customer / dbo.policy / dbo.claim, change tracking + CDC
      (Delta, Change Data Feed on)                         │  login databricks_ingest, UC connection azure_sql_claims
        │                                                  ▼
pipeline cdc_ingestion (Python, serverless)         pipeline sql_server_gateway (classic VMs, 8+4 cores)
  readChangeFeed + AUTO CDC (SCD 1)                   reads changes → staging volume in landing
        │                                                  ▼
        ▼                                           pipeline cdc_ingestion (ingestion_definition, serverless)
bronze.customer / policy / claim  ◀──── same key, same tables ────  applies upserts/deletes (SCD 1)
```

| | Simulation (now) | Lakeflow Connect (after) |
|---|---|---|
| Source | Delta tables in schema `source`, filled by `seed.sql` | Azure SQL `claims`, filled by `src/source_database/azure/seed.sql` (T-SQL) |
| Change capture | Delta Change Data Feed | SQL Server change tracking (CDC as backup) |
| Ingestion | our Python pipeline (`src/cdc_ingestion/`) | managed: gateway + `ingestion_definition` pipeline, no code |
| Pipeline key | `cdc_ingestion` | `cdc_ingestion` (kept, so `smart_claims_end_to_end` is unchanged) |
| Targets | `dev`, `prod`, `azure_dev`, `azure_prod` | `azure_dev` only. **`azure_prod` keeps the simulation** (CI stays cheap, see [part9_cicd_plan.md](part9_cicd_plan.md)); AWS can't run a gateway at all. |
| Change test | `src/source_database/changes.sql` (Databricks SQL) | `src/source_database/azure/changes.sql` (T-SQL in VS Code) |

## Cost and the trial

- **The gateway is the only real cost:** classic VMs `Standard_D8ds_v5` + `Standard_D4ds_v5` (12 vCPU), about **$0.80/hour while running**, billed by Azure **even during the Databricks trial** (only DBUs are free). Start it for a session; **stop it after every session** (step S12).
- **Azure SQL free offer:** 100,000 vCore-seconds a month. With "auto-pause until next month" it never bills. A running gateway keeps the database awake, which uses up the free amount in about 1–3 days of gateway time; after that the database pauses until next month.
- Expected total: **about $10–20** if the gateway only runs while we work.
- The DBU trial ends about **2026-10-28**. After that, the serverless parts bill at Premium rates.

## Decisions already made

- Names (from part 7): resource group `rg-e2e-learning`, East US 2, server `e2e-claims-<unique>`, database **`claims`**, admin `sqladmin`, ingest login **`databricks_ingest`**, UC connection **`azure_sql_claims`**, pipelines **`sql_server_gateway`** and **`cdc_ingestion`**.
- Change tracking retention **14 days**: a gateway stopped for longer forces a full re-snapshot (fine for learning).
- **Passwords never go in files or git:** typed into the portal, VS Code, or a silent shell prompt.
- **AWS must not notice:** `bundle plan -t dev` shows **0 changes** after the YAML steps.

---

## Steps

### S1. vCPU quota (portal) — do first, approval can take a while

**Why:** the gateway's driver needs **at least 8 cores**, plus a 4-core worker. Pay-as-you-go subscriptions start with small per-family quotas, and a cluster that can't get VMs fails to start.

- [ ] **S1.1** **Subscriptions → Azure subscription 1 → Usage + quotas.** Filter Provider **Compute**, Region **East US 2**. Read **Total Regional vCPUs** and **Standard DDSv5 Family vCPUs**.
- [ ] **S1.2** If either limit is below **16**: pencil icon / **New quota request** → **16**. Small increases are usually approved automatically within minutes.

**Check:** both limits ≥ 16.

### S2. Azure SQL server and the free `claims` database (portal)

**Why:** the transcript's "SQL Server on RDS".

- [ ] **S2.1** **SQL databases → + Create.** At the top, **Apply offer** (free offer). Resource group `rg-e2e-learning`, database **`claims`**. Server: **Create new**, name `e2e-claims-<something unique>` (becomes `<name>.database.windows.net`), **East US 2**, authentication **SQL authentication**, admin `sqladmin` + a strong password (password manager).
- [ ] **S2.2** Same page: **"Behavior when free limit reached" = "Auto-pause the database until next month"**. It can't be changed later.
- [ ] **S2.3** Tab **Networking:** **Public endpoint**; **"Allow Azure services and resources to access this server" = Yes** (the gateway VMs come in this way); **"Add current client IP address" = Yes** (VS Code). **Review + create**.
- [ ] **S2.4** VS Code: install **SQL Server (mssql)**, add a connection: server `<name>.database.windows.net`, database `claims`, SQL login `sqladmin`, encrypt **Mandatory**. Run `SELECT @@VERSION;`.

**Check:** "Microsoft SQL Azure …". The first query after a pause can take about a minute (the serverless database wakes up).

### S3. Source tables and data (T-SQL)

**Why:** the same three tables, row counts and key rules as the simulation, now with **primary keys** (change tracking needs them). `CLM00000003` is "Total Loss", `POL0000001`–`10` own the telematics cars `CHS000001`–`10`.

- [ ] **S3.1** Create `src/source_database/azure/seed.sql` with the T-SQL in [part7_azure_plan.md, Task 3.1](part7_azure_plan.md) (`IF OBJECT_ID … CREATE TABLE`, then `GENERATE_SERIES` inserts guarded by `IF NOT EXISTS`, so it's safe to re-run).
- [ ] **S3.2** Run the whole file in VS Code on `claims`.

**Check:** 7000 / 12000 / 13000 rows; `CLM00000003` = Total Loss; `POL0000001` → `CHS000001`. A second run leaves the counts unchanged.

### S4. Change tracking, CDC and the ingest login (T-SQL)

**Why:** the transcript enables change tracking + CDC and runs Databricks' DDL-capture script. Databricks now ships one **utility script** for all of it; with both on, the connector uses change tracking.

- [ ] **S4.1** Download https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql to `data/azure/utility_script.sql` (`data/` is gitignored). Read its header; run it on **`claims`** as `sqladmin` (set `@CreateDdlSupportingObjects = 1` if the header documents it).
- [ ] **S4.2** VS Code on database **`master`**, typed directly, **not saved to a file**:

```sql
CREATE LOGIN databricks_ingest WITH PASSWORD = '<a strong password>';
ALTER SERVER ROLE ##MS_DatabaseConnector## ADD MEMBER databricks_ingest;   -- if it fails: ALTER ROLE … ADD MEMBER …
```

- [ ] **S4.3** Create `src/source_database/azure/setup_cdc.sql` with the content of [part7_azure_plan.md, Task 4.3](part7_azure_plan.md) (`CREATE USER databricks_ingest`, `lakeflowSetupChangeTracking` with 14 days, `lakeflowSetupChangeDataCapture`, `lakeflowFixPermissions`, plus checks) and run it on `claims`.

**Check:** `sys.change_tracking_tables` lists the 3 tables; `is_tracked_by_cdc = 1` for all 3; connected as `databricks_ingest`, `SELECT COUNT(*) FROM dbo.claim` returns 13000.

### S5. Unity Catalog connection `azure_sql_claims` (CLI)

**Why:** Lakeflow Connect reads through a UC **connection** (host, port, user, password). Connections can't be bundle resources, so it's made once by hand. The password is read silently and never saved:

- [ ] **S5.1**

```bash
read -s -p "databricks_ingest password: " PW; echo
databricks connections create --profile azure --json "{
  \"name\": \"azure_sql_claims\", \"connection_type\": \"SQLSERVER\",
  \"comment\": \"Part 7b: Azure SQL claims database (Lakeflow Connect)\",
  \"options\": {\"host\": \"<server>.database.windows.net\", \"port\": \"1433\",
                \"user\": \"databricks_ingest\", \"password\": \"$PW\"}}"
unset PW
databricks connections get azure_sql_claims --profile azure
```

**Check:** `connection_type: SQLSERVER`, owner you.

### S6. YAML: the simulation stays on `dev`, `prod`, `azure_prod`

**Why:** today the stand-in files have a plain `resources:` block, so **every** target gets them. `azure_dev` must stop getting them, and an included file can scope resources to targets with its own `targets:` section. The anchor (`&…`) on `dev` and aliases (`*…`) on the others avoid copies. Keys and content stay identical, so AWS's deployment state still matches. (This is part 7's deferred steps 7.2/7.3, now with `azure_prod` listed too.)

- [ ] **S6.1** `resources/ingest_db_cdc_insurance.pipeline.yml`: replace `resources:` with

```yaml
# CDC ingestion (see docs/transcript_2.txt): applies inserts/updates/deletes from the source schema
# into bronze streaming tables with AUTO CDC. Stands in for the Lakeflow Connect ingestion pipeline.
# Simulation targets only; azure_dev runs real Lakeflow Connect under the same key (resources/azure_cdc_ingestion.yml).

targets:
  dev:
    resources:
      pipelines:
        cdc_ingestion: &sim_cdc_ingestion
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
  prod:
    resources:
      pipelines:
        cdc_ingestion: *sim_cdc_ingestion
  azure_prod:
    resources:
      pipelines:
        cdc_ingestion: *sim_cdc_ingestion
```

- [ ] **S6.2** `resources/seed_source_database.job.yml`: the same wrapper: anchor `&sim_seed_source_database` on `seed_source_database` under `dev`, aliases under `prod` and `azure_prod`. Comment: `# Simulation targets only; on azure_dev the source is Azure SQL (src/source_database/azure/).`
- [ ] **S6.3** Prove AWS is unchanged (Lakebase compute on for the plan, then off again):

```bash
EP=projects/smart-claims-dev/branches/production/endpoints/primary
databricks postgres update-endpoint $EP spec.disabled --json '{"spec": {"disabled": false}}' --profile DEFAULT
databricks bundle plan -t dev --profile DEFAULT          # expected: 0 to add, 0 to change, 0 to delete
databricks postgres update-endpoint $EP spec.disabled --json '{"spec": {"disabled": true}}' --profile DEFAULT
databricks bundle validate -t azure_prod --profile azure  # still has cdc_ingestion + seed job
```

**Check:** `dev` plan 0 changes; `azure_prod` validates with the stand-in.

### S7. YAML: Lakeflow Connect for `azure_dev`

- [ ] **S7.1** Create `resources/azure_cdc_ingestion.yml` with the content of [part7_azure_plan.md, Task 8.2](part7_azure_plan.md): `targets: azure_dev:` with pipelines **`sql_server_gateway`** (`gateway_definition` on connection `azure_sql_claims`, staging in the landing schema, clusters from `var.gateway_driver_node_type` / `var.gateway_node_type`, 1 worker) and **`cdc_ingestion`** (`ingestion_definition` with `ingestion_gateway_id: ${resources.pipelines.sql_server_gateway.id}`, `SCD_TYPE_1`, objects `claims.dbo.customer/policy/claim` → `${var.catalog}.${resources.schemas.bronze.name}`).
- [ ] **S7.2** `databricks bundle validate -t azure_dev --profile azure`.

**Check:** validate passes; the summary for `azure_dev` shows `sql_server_gateway` and `cdc_ingestion`, and **no** `seed_source_database`.

### S8. Switch `azure_dev`: read the plan first

**Why:** the existing `cdc_ingestion` on `azure_dev` is the Python stand-in. A pipeline can't turn from code into a managed ingestion pipeline in place, so the bundle **deletes and recreates** it. Deleting a pipeline **drops the tables it owns**: `bronze.customer/policy/claim` (if the stand-in ever ran). They're rebuilt from Azure SQL in S10. The seed job is deleted too; the Delta `source` tables stay (they're plain tables, not owned by a bundle resource) and can be dropped by hand.

- [ ] **S8.1** `databricks bundle plan -t azure_dev --profile azure`. Expected: **create** `pipelines.sql_server_gateway`, **delete + create** (or recreate) `pipelines.cdc_ingestion`, **delete** `jobs.seed_source_database`, everything else unchanged (plus the Genie space if gold doesn't exist yet). Anything else: stop and look together.
- [ ] **S8.2** `databricks bundle deploy -t azure_dev --profile azure`.

**Check:** **Jobs & Pipelines** shows `[dev …] sql_server_gateway` and `[dev …] cdc_ingestion` (type "Ingestion"); the seed job is gone.

### S9. Start the gateway

- [ ] **S9.1** `databricks bundle run sql_server_gateway -t azure_dev --profile azure --no-wait`. Watch it in the UI; the first classic VM start takes about 5–10 minutes. Note whether the update **keeps running** (normal for a gateway) or finishes (development mode drops `continuous`; then each session starts with this step).
- [ ] **S9.2** If it fails with a quota error: back to S1. If it can't reach `<server>.database.windows.net:1433`: check S2.3's "Allow Azure services". Errors:

```sql
SELECT timestamp, level, message FROM event_log('<gateway pipeline id>')
WHERE level IN ('ERROR', 'WARN') ORDER BY timestamp DESC LIMIT 20;
```

**Check:** the gateway update is RUNNING without errors.

### S10. First load into bronze

- [ ] **S10.1** `databricks bundle run cdc_ingestion -t azure_dev --profile azure` (the first run snapshots the 3 tables).
- [ ] **S10.2** Check counts and types:

```sql
SELECT (SELECT COUNT(*) FROM e2e_dev.dev_keqingli1129_bronze.customer),   -- 7000
       (SELECT COUNT(*) FROM e2e_dev.dev_keqingli1129_bronze.policy),     -- 12000
       (SELECT COUNT(*) FROM e2e_dev.dev_keqingli1129_bronze.claim);      -- 13000
DESCRIBE TABLE e2e_dev.dev_keqingli1129_bronze.claim;
```

  Compare with the simulation: `claim_amount` DECIMAL(12,2), `incident_date` DATE, `updated_at` TIMESTAMP. If Lakeflow Connect lands `TIMESTAMP_NTZ` or extra columns (e.g. its own metadata), decide together whether `bronze_to_silver.py` needs a cast or a column drop.
- [ ] **S10.3** `databricks bundle run transformations -t azure_dev --profile azure`: silver `claim`/`policy`/`customer` are materialized views (batch reads), so they simply re-read the new bronze.

**Check:** 7000 / 12000 / 13000 in bronze; silver and gold rebuild without errors.

### S11. The transcript's change test, in Azure SQL

- [ ] **S11.1** Create `src/source_database/azure/changes.sql` with the content of [part7_azure_plan.md, Task 10.1](part7_azure_plan.md) (INSERT `POL9999001`, UPDATE `CLM00000003` to "Minor Damage", DELETE `C007000`).
- [ ] **S11.2** "Before" checks in Databricks; run the file in VS Code; with the gateway running, `databricks bundle run cdc_ingestion -t azure_dev --profile azure`; re-check.

**Check:** 6999 / 12001 / 13000, `CLM00000003` = "Minor Damage". **Part 2 runs on real Lakeflow Connect.**

### S12. Pause after every session

- [ ] **S12.1** Stop the gateway (this stops the VM cost and lets Azure SQL pause): `databricks pipelines stop <gateway pipeline id> --profile azure`. Check in the portal (**managed resource group `databricks-rg-e2e-learning-…` → Virtual machines**) that no VMs remain after a few minutes.
- [ ] **S12.2** Nothing else: `cdc_ingestion` is triggered (it stops after each run); Azure SQL auto-pauses after idle.

---

## Switching back to the simulation

If the gateway costs too much or the trial ends:

1. In `resources/azure_cdc_ingestion.yml`, remove the `azure_dev` block (or delete the file); in the two stand-in files, add `azure_dev` aliases next to `azure_prod`.
2. `bundle plan -t azure_dev`: expect delete of the gateway, recreate of `cdc_ingestion` as the Python pipeline, create of the seed job. Then deploy, run `seed_source_database`, run `cdc_ingestion`.
3. Delete the Azure SQL server in the portal (or keep it: paused, it costs nothing) and `databricks connections delete azure_sql_claims --profile azure`.

## Where this came from

- Design: [part7_azure_design.md](part7_azure_design.md) ("Part 2 on Azure").
- Original steps: [part7_azure_plan.md](part7_azure_plan.md) Tasks 2, 3, 4, 6, 8, 9.3–9.6, 10 (skipped on 2026-10-14 for the "like Free Edition" route). This runbook replaces them and adds S6 (wrappers incl. `azure_prod`) and S8 (switching an already-deployed `azure_dev`).
