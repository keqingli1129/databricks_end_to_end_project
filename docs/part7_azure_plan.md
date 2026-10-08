# Part 7: the whole project on Azure Databricks (step-by-step plan)

> **How we run this plan:** one step at a time. Claude does a step, explains it, ticks its box, and waits for "go". Claude never runs `git add` or `git commit`; you commit when you want.

**Goal:** deploy this bundle to an Azure Databricks trial workspace as target `azure_dev`, with part 2 running on real Lakeflow Connect (an ingestion gateway reading change tracking from an Azure SQL Database), and parts 1, 3–6 running unchanged.

**Architecture:** see [part7_azure_design.md](part7_azure_design.md). Part 2 and the resources that need data to exist before they can deploy (the serving endpoint, the synced tables and the app) move into **per-target sections** inside their own resource files. Then each target gets only what it can deploy. `azure_dev` adds a gateway and an `ingestion_definition` pipeline under the **same key, `cdc_ingestion`**, so the end-to-end job is unchanged.

**Tech stack:** Declarative Automation Bundles (CLI v1.15), Lakeflow Connect for SQL Server, Azure SQL Database (free offer, T-SQL), Unity Catalog connections, Lakebase, VS Code `mssql` extension.

## Global constraints

- **Azure:** pay-as-you-go subscription on your **personal Gmail** Microsoft account. Budget alert at **$50**. Everything goes in resource group **`rg-e2e-learning`** in **East US 2**.
- **Databricks workspace:** pricing tier **Trial (Premium – 14 days free DBUs)**, workspace type **Hybrid**. CLI profile **`azure`**.
- **Azure SQL:** server admin with SQL authentication. Database **`claims`** with the **free offer**, set to "**Auto-pause the database until next month**".
- **Ingest user:** login and user **`databricks_ingest`**. **UC connection: `azure_sql_claims`.**
- **Never put passwords in files or git.** Type them at prompts, or into the portal or VS Code.
- **Leave the AWS targets `dev`/`prod` working.** After the restructure, `bundle plan -t dev` must show **no changes**.
- **No automated tests.** The checks are the row counts and queries in each task.
- **Cost:** after every session, run the **pause checklist** (Task 14).

---

### Task 1: Azure subscription, budget and resource group (portal)

**Why:** everything Azure bills goes on one subscription, and everything we create goes in one resource group, so cleanup is one delete.

- [ ] **1.1** Sign in at https://portal.azure.com with your Gmail Microsoft account. Under **Subscriptions**, check for a subscription. If there isn't one, create **Pay-As-You-Go**.
- [ ] **1.2** **Cost Management → Budgets → Add:** scope = the subscription, name `e2e-learning`, amount **50**, monthly. Email alerts at **50%** and **90%** (actual cost), sent to your Gmail.
- [ ] **1.3** **Resource groups → Create:** name `rg-e2e-learning`, region **East US 2**.

**Check:** the resource group is listed, and the budget shows $0 used.

---

### Task 2: Azure SQL server and the free `claims` database (portal)

**Why:** this is the transcript's "SQL Server on RDS".

- [ ] **2.1** **SQL databases → Create.** At the top, click **"Apply offer"** for the free offer.
  - Resource group `rg-e2e-learning`.
  - Database name **`claims`**.
  - Server: **Create new**, name `e2e-claims-<something unique>` (it becomes `<name>.database.windows.net`), location **East US 2**, authentication **SQL authentication**, admin login `sqladmin`, plus a strong password. Keep that password in your password manager.
- [ ] **2.2** On the same page, **"Behavior when free limit reached" = "Auto-pause the database until next month"**. This choice can't be changed later.
- [ ] **2.3** **Networking** tab: connectivity **Public endpoint**. **"Allow Azure services and resources to access this server" = Yes**; this is the path for the gateway VM. **"Add current client IP address" = Yes**, so VS Code can connect. Then **Review + create**.
- [ ] **2.4** In VS Code, install the extension **SQL Server (mssql)**. Add a connection: server `<name>.database.windows.net`, database `claims`, SQL login `sqladmin`, encrypt **Mandatory**. Run `SELECT @@VERSION;`.

**Check:** the query returns "Microsoft SQL Azure …". The first query after a pause can take about a minute while the serverless database wakes up.

---

### Task 3: Source tables and data (T-SQL)

**Files:**
- Create: `src/source_database/azure/seed.sql`

**Why:** the same three tables as the AWS stand-in ([seed.sql](../src/source_database/seed.sql)), with **primary keys**, which change tracking needs. Keys and severities follow the same rules, so `CLM00000003` is still "Total Loss", and `POL0000001`–`10` still own the telematics cars `CHS000001`–`10`.

- [ ] **3.1** Create `src/source_database/azure/seed.sql`:

```sql
-- Part 7 (Azure): the transcript's SQL Server tables, in Azure SQL Database `claims`.
-- Same columns, row counts and key rules as ../seed.sql (the AWS stand-in). Values that use rand() there use
-- modular arithmetic here, so they differ from AWS but are the same on every run.
-- Run in VS Code (mssql extension), connected to database `claims` as the server admin.
-- Safe to re-run: tables are created only if missing and filled only while empty.

IF OBJECT_ID(N'dbo.customer', N'U') IS NULL
CREATE TABLE dbo.customer (
  customer_id   NVARCHAR(10)  NOT NULL CONSTRAINT pk_customer PRIMARY KEY,
  first_name    NVARCHAR(50)  NULL,
  last_name     NVARCHAR(50)  NULL,
  date_of_birth DATE          NULL,
  email         NVARCHAR(100) NULL,
  zip_code      NVARCHAR(10)  NULL
);

IF OBJECT_ID(N'dbo.policy', N'U') IS NULL
CREATE TABLE dbo.policy (
  policy_no      NVARCHAR(12)  NOT NULL CONSTRAINT pk_policy PRIMARY KEY,
  customer_id    NVARCHAR(10)  NULL,
  chassis_number NVARCHAR(12)  NULL,
  make           NVARCHAR(30)  NULL,
  model          NVARCHAR(30)  NULL,
  model_year     INT           NULL,
  coverage       NVARCHAR(20)  NULL,
  premium        DECIMAL(10,2) NULL,
  deductible     INT           NULL,
  start_date     DATE          NULL,
  end_date       DATE          NULL
);

IF OBJECT_ID(N'dbo.claim', N'U') IS NULL
CREATE TABLE dbo.claim (
  claim_no          NVARCHAR(12)  NOT NULL CONSTRAINT pk_claim PRIMARY KEY,
  policy_no         NVARCHAR(12)  NULL,
  incident_date     DATE          NULL,
  incident_type     NVARCHAR(20)  NULL,
  incident_severity NVARCHAR(20)  NULL,
  claim_amount      DECIMAL(12,2) NULL,
  updated_at        DATETIME2     NULL
);
GO

-- 7,000 customers.
IF NOT EXISTS (SELECT 1 FROM dbo.customer)
INSERT INTO dbo.customer (customer_id, first_name, last_name, date_of_birth, email, zip_code)
SELECT
  CONCAT('C', FORMAT(s.id, '000000')),
  n.first_name,
  n.last_name,
  DATEADD(DAY, (s.id * 7919) % 18250, CAST('1950-01-01' AS DATE)),
  LOWER(CONCAT(n.first_name, '.', n.last_name, s.id, '@example.com')),
  CHOOSE((s.id * 7) % 10 + 1, '77002', '77003', '77004', '77006', '77007', '77008', '77019', '77024', '77056', '77098')
FROM (SELECT value AS id FROM GENERATE_SERIES(1, 7000)) AS s
CROSS APPLY (
  SELECT
    CHOOSE(s.id % 10 + 1, 'James', 'Maria', 'Robert', 'Linda', 'Michael', 'Aisha', 'Wei', 'Carlos', 'Priya', 'David') AS first_name,
    CHOOSE((s.id / 10) % 10 + 1, 'Smith', 'Lopez', 'Nguyen', 'Johnson', 'Garcia', 'Patel', 'Kim', 'Brown', 'Davis', 'Martinez') AS last_name
) AS n;

-- 12,000 policies; every customer gets at least one; POL0000001-10 cover the part 1 telematics cars CHS000001-10.
IF NOT EXISTS (SELECT 1 FROM dbo.policy)
INSERT INTO dbo.policy (policy_no, customer_id, chassis_number, make, model, model_year, coverage, premium, deductible,
                        start_date, end_date)
SELECT
  CONCAT('POL', FORMAT(s.id, '0000000')),
  CONCAT('C', FORMAT((s.id - 1) % 7000 + 1, '000000')),
  CONCAT('CHS', FORMAT(s.id, '000000')),
  CHOOSE(s.id % 6 + 1, 'Toyota', 'Honda', 'Ford', 'Tesla', 'BMW', 'Hyundai'),
  CHOOSE(s.id % 6 + 1, 'Camry', 'Civic', 'F-150', 'Model 3', 'X5', 'Elantra'),
  2015 + s.id % 11,
  CHOOSE(s.id % 3 + 1, 'COMPREHENSIVE', 'COLLISION', 'LIABILITY'),
  CAST(600 + (s.id * 7919) % 140000 / 100.0 AS DECIMAL(10,2)),
  CHOOSE((s.id / 3) % 3 + 1, 250, 500, 1000),
  d.start_date,
  DATEADD(MONTH, 12, d.start_date)
FROM (SELECT value AS id FROM GENERATE_SERIES(1, 12000)) AS s
CROSS APPLY (SELECT DATEADD(DAY, -(s.id % 365), CAST('2026-01-01' AS DATE)) AS start_date) AS d;

-- 13,000 claims spread over the policies.
IF NOT EXISTS (SELECT 1 FROM dbo.claim)
INSERT INTO dbo.claim (claim_no, policy_no, incident_date, incident_type, incident_severity, claim_amount, updated_at)
SELECT
  CONCAT('CLM', FORMAT(s.id, '00000000')),
  CONCAT('POL', FORMAT((s.id * 7919) % 12000 + 1, '0000000')),
  DATEADD(DAY, (s.id * 104729) % 270, CAST('2026-01-01' AS DATE)),
  CHOOSE(s.id % 5 + 1, 'COLLISION', 'THEFT', 'WEATHER', 'VANDALISM', 'GLASS'),
  CHOOSE(s.id % 4 + 1, 'Trivial Damage', 'Minor Damage', 'Major Damage', 'Total Loss'),
  CAST(100 + (s.id * 7907) % 2490000 / 100.0 AS DECIMAL(12,2)),
  SYSUTCDATETIME()
FROM (SELECT value AS id FROM GENERATE_SERIES(1, 13000)) AS s;
GO
```

- [ ] **3.2** Run the whole file in VS Code, connected to `claims`.
- [ ] **3.3** Check:

```sql
SELECT (SELECT COUNT(*) FROM dbo.customer) AS customers,   -- 7000
       (SELECT COUNT(*) FROM dbo.policy)   AS policies,    -- 12000
       (SELECT COUNT(*) FROM dbo.claim)    AS claims;      -- 13000
SELECT claim_no, incident_severity FROM dbo.claim WHERE claim_no = 'CLM00000003';   -- Total Loss
SELECT policy_no, chassis_number FROM dbo.policy WHERE policy_no = 'POL0000001';    -- CHS000001
```

Run the file a second time. The counts must stay the same.

---

### Task 4: Change tracking, CDC and the ingest user (T-SQL)

**Files:**
- Create: `src/source_database/azure/setup_cdc.sql`
- Download (not committed): `data/azure/utility_script.sql`. `data/` is gitignored.

**Why:** the transcript turns on change tracking and CDC and runs Databricks' DDL-capture script. Databricks now ships one **utility script** that does all of this. When both are on, the connector uses change tracking, which Databricks recommends for tables with primary keys.

- [ ] **4.1** Download https://learn.microsoft.com/en-us/azure/databricks/_extras/documents/utility_script.sql to `data/azure/utility_script.sql`, read its header, and run it in VS Code on database **`claims`** as `sqladmin`. If the header documents `@CreateDdlSupportingObjects`, set it to `1`.
- [ ] **4.2** Connect VS Code to database **`master`** and run the following, typed directly in the editor, **not saved to a file**:

```sql
CREATE LOGIN databricks_ingest WITH PASSWORD = '<type a strong password here>';
ALTER SERVER ROLE ##MS_DatabaseConnector## ADD MEMBER databricks_ingest;
```

  The Databricks docs write `ALTER ROLE ##MS_DatabaseConnector## …`. In Azure SQL Database, server roles use `ALTER SERVER ROLE` in `master`. If one form fails, try the other. Keep this password for Task 8.
- [ ] **4.3** Create `src/source_database/azure/setup_cdc.sql`:

```sql
-- Part 7 (Azure): change tracking + CDC and the ingest user for Lakeflow Connect (see docs/transcript_2.txt).
-- Run in database `claims` as the server admin, AFTER utility_script.sql and after the login exists in master.
-- Change tracking retention is 14 days: the gateway is stopped between sessions, and a gap longer than the
-- retention forces a full re-snapshot.

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'databricks_ingest')
  CREATE USER databricks_ingest FOR LOGIN databricks_ingest;
GO

EXEC dbo.lakeflowSetupChangeTracking
  @Tables = 'dbo.customer,dbo.policy,dbo.claim', @User = 'databricks_ingest', @Retention = '14 DAYS';
EXEC dbo.lakeflowSetupChangeDataCapture
  @Tables = 'dbo.customer,dbo.policy,dbo.claim', @User = 'databricks_ingest';
EXEC dbo.lakeflowFixPermissions
  @User = 'databricks_ingest', @Tables = 'dbo.customer,dbo.policy,dbo.claim';
GO

-- Checks.
SELECT dbo.lakeflowUtilityVersion() AS utility_version, dbo.lakeflowDetectPlatform() AS platform;
SELECT OBJECT_SCHEMA_NAME(object_id) AS table_schema, OBJECT_NAME(object_id) AS table_name
FROM sys.change_tracking_tables;                                   -- customer, policy, claim
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name IN ('customer', 'policy', 'claim');   -- all 1
```

- [ ] **4.4** Run it on `claims`.

**Check:** the last three queries return what the comments say. Then connect VS Code as `databricks_ingest` and run `SELECT COUNT(*) FROM dbo.claim;`, which should return 13000.

---

### Task 5: Azure Databricks workspace, CLI profile and catalog

**Why:** the workspace is the Azure twin of Free Edition. It must be **Hybrid**, because the gateway runs on classic compute.

- [ ] **5.1** **Azure Databricks → Create:** resource group `rg-e2e-learning`, workspace name `e2e-learning`, region **East US 2**, pricing tier **Trial (Premium – 14 Days Free DBUs)**, workspace type **Hybrid**. Keep the default (Databricks-managed) networking. Then **Review + create**. Azure also creates a managed resource group (`databricks-rg-…`); don't touch it.
- [ ] **5.2** **Launch Workspace** and sign in with the same Gmail Microsoft account. Copy the URL (`https://adb-<id>.<n>.azuredatabricks.net`).
- [ ] **5.3** Create the CLI profile:

```bash
databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net --profile azure
databricks current-user me --profile azure          # your user; note userName (decides dev_<short_name>_ prefixes)
databricks metastores current --profile azure       # Unity Catalog is attached
databricks warehouses list --profile azure          # note the starter warehouse's exact name
```

- [ ] **5.4** Create the catalog **`e2e_dev`** by hand: Catalog Explorer → **+ → Create catalog** → name `e2e_dev`, type Standard. If it **requires a storage location** (the metastore has no root storage), cancel. Use the **workspace catalog** that Azure created automatically instead (named after the workspace, for example `e2e_learning`). Task 7 then sets `catalog` for `azure_dev` to that name.

**Check:** `databricks catalogs get <catalog> --profile azure` returns it. Write down the catalog name and the warehouse name.

---

### Task 6: vCPU quota for the gateway (portal)

**Why:** the gateway's driver needs **at least 8 cores**, plus one worker. Pay-as-you-go subscriptions start with small per-family quotas.

- [ ] **6.1** **Subscriptions → your subscription → Usage + quotas.** Filter Provider **Compute** and Region **East US 2**. Check **Total Regional vCPUs** and **Standard DDSv5 Family vCPUs**.
- [ ] **6.2** If either is below **16**, click the pencil icon or **New quota request** and set it to **16**. Small increases are usually approved automatically within minutes.

**Check:** both show a limit of 16 or more. The gateway uses `Standard_D8ds_v5` (8 vCPU driver) and `Standard_D4ds_v5` (4 vCPU worker): 12 vCPU, about $0.68/hour while running.

---

### Task 7: Restructure the bundle for several clouds (no change on AWS)

**Files:**
- Modify: `databricks.yml`: comments on the AWS targets, new variables, targets `azure_dev` and `azure_prod`
- Modify: `resources/ingest_cdc_insurance.pipeline.yml`, `resources/seed_source_database.job.yml`: wrap in `targets: dev / prod`
- Modify: `resources/damage_classifier.yml`: endpoint moves into `targets: dev / prod`; `entity_version` from a variable
- Modify: `resources/claims_app.yml`: database, synced tables and app move into `targets: dev / prod`; names use `var.name_suffix`

**Why:** a bundle can't leave a shared resource out for one target. But **included files may have their own `targets:` section**, and a resource defined there exists only in that target. A YAML **anchor** (`&name`) on the `dev` block and an **alias** (`*name`) in `prod` avoid copying it. The keys don't change, so the AWS deployment state still matches.

App names and Lakebase database IDs **don't allow underscores**, so `e2e-claims-${bundle.target}` would break for `azure_dev`. A new variable, `name_suffix`, keeps the AWS names exactly as they are.

- [ ] **7.1** In `databricks.yml`:
  - Add variables:

```yaml
  serving_model_version:
    description: Version of the damage classifier the serving endpoint serves (versions are per workspace)
    default: "2"
  name_suffix:
    description: Target name without underscores, for app names and Lakebase database ids
    default: dev
  gateway_driver_node_type:
    description: Lakeflow Connect gateway driver VM (needs at least 8 cores). Azure targets only.
    default: Standard_D8ds_v5
  gateway_node_type:
    description: Lakeflow Connect gateway worker VM. Azure targets only.
    default: Standard_D4ds_v5
```

  - Above `dev:`, add the comment `# --- AWS (Free Edition, host dbc-b5c9918e-c2d2) ---`. Under `prod`'s `variables`, add `name_suffix: prod`.
  - After `prod`, add:

```yaml
  # --- Azure (Azure Databricks trial workspace e2e-learning, East US 2; see docs/part7_azure_design.md) ---
  azure_dev:
    mode: development
    workspace:
      host: https://adb-<id>.<n>.azuredatabricks.net
    variables:
      catalog: e2e_dev                 # or the workspace catalog name from Task 5.4
      schema: ${workspace.current_user.short_name}
      name_suffix: azure-dev
      serving_model_version: "1"
  # Placeholder only: not deployed (and maybe never). Fill in before a first deploy.
  azure_prod:
    mode: production
    workspace:
      host: https://adb-<id>.<n>.azuredatabricks.net
      root_path: /Workspace/Users/<azure user name>/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: e2e_prod
      schema: prod
      name_suffix: azure-prod
```

  If Task 5.3 showed a starter warehouse that **isn't** named "Serverless Starter Warehouse", also add under `azure_dev.variables`:

```yaml
      warehouse_id:
        lookup:
          warehouse: <exact name from Task 5.3>
```

- [ ] **7.2** `resources/ingest_cdc_insurance.pipeline.yml`: replace the top-level `resources:` with a targets wrapper. The body is unchanged, only indented:

```yaml
# CDC ingestion (see docs/transcript_2.txt): applies inserts/updates/deletes from the source schema
# into bronze streaming tables with AUTO CDC. Stands in for the Lakeflow Connect ingestion pipeline.
# AWS targets only: Azure runs real Lakeflow Connect under the same key (resources/azure_cdc_ingestion.yml).

targets:
  dev:
    resources:
      pipelines:
        cdc_ingestion: &aws_cdc_ingestion
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
        cdc_ingestion: *aws_cdc_ingestion
```

- [ ] **7.3** `resources/seed_source_database.job.yml`: the same wrapper, with anchor `&aws_seed_source_database` on `seed_source_database` under `dev`, and `seed_source_database: *aws_seed_source_database` under `prod`. Add the comment `# AWS targets only: on Azure the source is Azure SQL (src/source_database/azure/).`
- [ ] **7.4** `resources/damage_classifier.yml`: cut the `model_serving_endpoints:` block out of `resources:`. Append it under a wrapper, with `entity_version: ${var.serving_model_version}`:

```yaml
# The endpoint needs a model version that exists, so it is per target: a new workspace deploys it only after
# train_and_register has created a version there (Azure: Task 12).
targets:
  dev:
    resources:
      model_serving_endpoints: &serving_endpoints
        # Key differs from the registered model's key: resource keys must be unique across all types.
        claims_damage_level_endpoint:
          name: e2e-claims-damage-level
          config:
            served_entities:
              - entity_name: ${var.catalog}.${resources.schemas.gold.name}.${resources.registered_models.claims_damage_level.name}
                entity_version: ${var.serving_model_version}
                workload_size: Small
                scale_to_zero_enabled: true
  prod:
    resources:
      model_serving_endpoints: *serving_endpoints
```

- [ ] **7.5** `resources/claims_app.yml`:
  - Keep the `variables:` block at the top.
  - Wrap the whole `resources:` block as `targets: dev: resources: &claims_app_resources …` and `prod: resources: *claims_app_resources`.
  - Change `database_id: e2e-claims-${bundle.target}` to `database_id: e2e-claims-${var.name_suffix}`, and the app's `name: e2e-claims-${bundle.target}` to `name: e2e-claims-${var.name_suffix}`. For `dev`/`prod` these resolve to the same names as today.
  - Add a header comment: "Needs gold tables and the endpoint to exist, so it is per target (Azure: Task 13)."
- [ ] **7.6** Validate both clouds' configs (no deploy):

```bash
databricks bundle validate -t dev --profile DEFAULT
databricks bundle validate -t azure_dev --profile azure
databricks bundle summary -t dev --profile DEFAULT | grep -E "cdc_ingestion|seed_source|claims_damage_level_endpoint|claims_app"
```

  If validate rejects anchors or `targets:` in an included file, stop and choose a fallback together (for example, writing the `prod` copy out in full).
- [ ] **7.7** Prove AWS is unchanged. Bundle planning reads the synced tables, so enable the Lakebase compute first, then disable it again:

```bash
databricks postgres update-endpoint projects/smart-claims-dev/branches/production/endpoints/primary spec.disabled --json '{"spec": {"disabled": false}}' --profile DEFAULT
databricks bundle plan -t dev --profile DEFAULT          # expected: no create / delete / recreate
databricks postgres update-endpoint projects/smart-claims-dev/branches/production/endpoints/primary spec.disabled --json '{"spec": {"disabled": true}}' --profile DEFAULT
```

**Check:** both validations pass, and the plan for `dev` lists no changes.

---

### Task 8: UC connection and the Lakeflow Connect pipelines (`azure_dev` only)

**Files:**
- Create: `resources/azure_cdc_ingestion.yml`

- [ ] **8.1** Create the UC connection. The password is read silently and never saved:

```bash
read -s -p "databricks_ingest password: " PW; echo
databricks connections create --profile azure --json "{
  \"name\": \"azure_sql_claims\",
  \"connection_type\": \"SQLSERVER\",
  \"comment\": \"Part 7: Azure SQL claims database (Lakeflow Connect)\",
  \"options\": {\"host\": \"<server>.database.windows.net\", \"port\": \"1433\",
                \"user\": \"databricks_ingest\", \"password\": \"$PW\"}}"
unset PW
databricks connections get azure_sql_claims --profile azure
```

  (The other way is Catalog Explorer → External data → Connections → Create connection → SQL Server, as in the transcript.)
- [ ] **8.2** Create `resources/azure_cdc_ingestion.yml`:

```yaml
# Part 7 (see docs/transcript_2.txt): real Lakeflow Connect for SQL Server, reading Azure SQL database `claims`.
# Gateway (classic VM, reads change tracking) -> staging volume in landing -> cdc_ingestion (serverless,
# applies upserts/deletes into bronze). Same key cdc_ingestion as the AWS stand-in, so the end-to-end job works.
# azure_dev only. The gateway costs money while it runs: stop it after every session (plan Task 14).

targets:
  azure_dev:
    resources:
      pipelines:
        sql_server_gateway:
          name: sql_server_gateway
          catalog: ${var.catalog}
          schema: ${resources.schemas.landing.name}
          # The docs run gateways continuously; development mode drops this (plan Task 9 checks what remains).
          continuous: true
          gateway_definition:
            connection_name: azure_sql_claims
            gateway_storage_catalog: ${var.catalog}
            gateway_storage_schema: ${resources.schemas.landing.name}
            gateway_storage_name: sql_server_gateway_storage
          clusters:
            - label: default
              driver_node_type_id: ${var.gateway_driver_node_type}
              node_type_id: ${var.gateway_node_type}
              num_workers: 1

        cdc_ingestion:
          name: cdc_ingestion
          catalog: ${var.catalog}
          schema: ${resources.schemas.bronze.name}
          serverless: true
          continuous: false  # Lakeflow Connect is triggered only
          ingestion_definition:
            ingestion_gateway_id: ${resources.pipelines.sql_server_gateway.id}
            table_configuration:
              scd_type: SCD_TYPE_1
            objects:
              - table:
                  source_catalog: claims
                  source_schema: dbo
                  source_table: customer
                  destination_catalog: ${var.catalog}
                  destination_schema: ${resources.schemas.bronze.name}
              - table:
                  source_catalog: claims
                  source_schema: dbo
                  source_table: policy
                  destination_catalog: ${var.catalog}
                  destination_schema: ${resources.schemas.bronze.name}
              - table:
                  source_catalog: claims
                  source_schema: dbo
                  source_table: claim
                  destination_catalog: ${var.catalog}
                  destination_schema: ${resources.schemas.bronze.name}
```

- [ ] **8.3** `databricks bundle validate -t azure_dev --profile azure`

**Check:** the connection shows `SQLSERVER`, and validate passes.

---

### Task 9: First Azure deploy and part 2 end to end

- [ ] **9.1** `databricks bundle plan -t azure_dev --profile azure`. Read it: schemas, volumes, pipelines and jobs get created; there's no endpoint, synced tables or app yet.
- [ ] **9.2** `databricks bundle deploy -t azure_dev --profile azure`
- [ ] **9.3** Start the gateway: `databricks bundle run sql_server_gateway --profile azure --no-wait`. In the UI (Jobs & Pipelines → `[dev …] sql_server_gateway`), watch it start; the first classic VM start takes about 5–10 minutes. Note whether the update **keeps running** (gateway behaviour) or finishes, because development mode dropped `continuous`. If it finishes, write that down: each session then starts with this step.
- [ ] **9.4** Check the gateway event log for connection errors. If it can't reach `<server>.database.windows.net:1433`, re-check Task 2.3's "Allow Azure services".

```sql
SELECT timestamp, level, message FROM event_log('<gateway pipeline id>')
WHERE level IN ('ERROR', 'WARN') ORDER BY timestamp DESC LIMIT 20;
```

- [ ] **9.5** `databricks bundle run cdc_ingestion --profile azure`. The first run loads a snapshot of the 3 tables.
- [ ] **9.6** Check bronze: counts, plus the columns and types silver expects:

```sql
SELECT (SELECT COUNT(*) FROM <catalog>.dev_<user>_bronze.customer),   -- 7000
       (SELECT COUNT(*) FROM <catalog>.dev_<user>_bronze.policy),     -- 12000
       (SELECT COUNT(*) FROM <catalog>.dev_<user>_bronze.claim);      -- 13000
DESCRIBE TABLE <catalog>.dev_<user>_bronze.claim;
```

  Compare with AWS: `claim_amount` DECIMAL(12,2), `incident_date` DATE, and `updated_at` TIMESTAMP. If Lakeflow Connect lands `TIMESTAMP_NTZ` or extra columns, decide together whether silver needs a cast.

**Check:** 7000 / 12000 / 13000 in bronze.

---

### Task 10: The transcript's change test on Azure SQL

**Files:**
- Create: `src/source_database/azure/changes.sql`

- [ ] **10.1** Create `src/source_database/azure/changes.sql`:

```sql
-- Part 7 (Azure): the transcript's incremental test. Run in VS Code on database `claims`.
-- Before: in the Azure workspace SQL editor, C007000 exists, CLM00000003 is 'Total Loss', POL9999001 is missing
-- (tables <catalog>.dev_<user>_bronze.customer / claim / policy).

INSERT INTO dbo.policy (policy_no, customer_id, chassis_number, make, model, model_year, coverage, premium, deductible,
                        start_date, end_date)
VALUES ('POL9999001', 'C000001', 'CHS999001', 'Tesla', 'Model Y', 2026, 'COMPREHENSIVE', 1899.00, 500,
        '2026-09-01', '2027-09-01');

UPDATE dbo.claim
SET incident_severity = 'Minor Damage', updated_at = SYSUTCDATETIME()
WHERE claim_no = 'CLM00000003';

DELETE FROM dbo.customer WHERE customer_id = 'C007000';

-- After: with the gateway running, run the pipeline cdc_ingestion, then re-check bronze:
-- the policy row appears, the claim is 'Minor Damage', the customer returns no rows (6999 / 12001 / 13000).
```

- [ ] **10.2** Run the "before" checks in Databricks, then the file in VS Code.
- [ ] **10.3** With the gateway running, `databricks bundle run cdc_ingestion --profile azure`, then re-run the checks.

**Check:** 6999 / 12001 / 13000, with `CLM00000003` = "Minor Damage". **Part 2 on Azure is done.** Run the pause checklist (Task 14) if you're stopping here.

---

### Task 11: Parts 1, 3 and 4 on Azure

- [ ] **11.1** Kafka secrets (the bundle created scope `kafka_azure_dev`). Paste the same Confluent values as in `kafka_dev`:

```bash
databricks secrets put-secret kafka_azure_dev bootstrap_servers --profile azure
databricks secrets put-secret kafka_azure_dev api_key --profile azure
databricks secrets put-secret kafka_azure_dev api_secret --profile azure
```

- [ ] **11.2** `databricks bundle run telematics_simulator --profile azure`, then `databricks bundle run telematics_ingestion --profile azure`. Check that `bronze.telematics` has rows. Both clouds read the same topic, so AWS sees these events on its next run too.
- [ ] **11.3** Upload the part 3 files (prepared locally in part 3) to the Azure volumes. Run `ls data/object_storage/` first to confirm the folder names:

```bash
databricks fs cp -r data/object_storage/prepared/ dbfs:/Volumes/<catalog>/dev_<user>_landing/claims/metadata/ --profile azure
databricks fs cp -r data/object_storage/<claim images folder>/ dbfs:/Volumes/<catalog>/dev_<user>_landing/claims/images/ --profile azure
databricks fs cp -r data/object_storage/<training images folder>/ dbfs:/Volumes/<catalog>/dev_<user>_landing/training_images/ --profile azure
```

  (Use the same source folders as part 3's plan, `docs/part3_object_storage_plan.md`.)
- [ ] **11.4** With the gateway running: `databricks bundle run smart_claims_end_to_end --profile azure`. It runs all four ingestion tasks, then transformations.

**Check:** silver and gold have rows, and the data-quality numbers show up in `event_log('<transformations pipeline id>')`, as on AWS. `gold.claim_checks` has 13,000 rows (minus claims whose customer was deleted).

---

### Task 12: Part 5 (model) and the serving endpoint on Azure

**Files:**
- Modify: `resources/damage_classifier.yml` (add `azure_dev` to the endpoint wrapper)

- [ ] **12.1** `databricks bundle run damage_classifier --profile azure` (all three tasks). It registers **version 1** in this workspace and moves the `prod` alias.
- [ ] **12.2** Check: `databricks registered-models get <catalog>.dev_<user>_gold.dev_<user>_claims_damage_level --profile azure` shows version 1. Also check that `gold.damage_predictions` has rows.
- [ ] **12.3** Add to the endpoint wrapper in `resources/damage_classifier.yml`:

```yaml
  azure_dev:
    resources:
      model_serving_endpoints: *serving_endpoints
```

- [ ] **12.4** `databricks bundle deploy -t azure_dev --profile azure`, and wait for the endpoint to become READY (about 10–20 minutes). `azure_dev` has `serving_model_version: "1"`, so it serves the version just trained.

**Check:** the endpoint is READY in Serving. Because it scales to zero, the first query times out; retry.

---

### Task 13: Parts 6a and 6b on Azure

**Files:**
- Create: `resources/azure_lakebase.yml`
- Modify: `databricks.yml` (`azure_dev` Lakebase variables)
- Modify: `resources/claims_app.yml` (add `azure_dev` to the wrapper)

- [ ] **13.1** Part 6a was deployed in Task 9. Open the dashboard "E2E Claims Investigation" and the Genie space in the Azure workspace, and check that they show data.
- [ ] **13.2** Create `resources/azure_lakebase.yml`. On Azure there's no one-project limit, so `azure_dev` owns its own project:

```yaml
# Part 7: azure_dev's own Lakebase project (Free Edition shares smart_claims_dev's; see resources/claims_app.yml).
targets:
  azure_dev:
    resources:
      postgres_projects:
        e2e_claims_project:
          project_id: e2e-claims-azure-dev
          display_name: e2e-claims-azure-dev
          pg_version: 17
```

- [ ] **13.3** `databricks bundle deploy -t azure_dev --profile azure`. Then list the default branch, endpoint and roles. Development mode may prefix the project id, so take the id from the first command:

```bash
databricks postgres list-projects --profile azure
databricks postgres list-branches projects/<project id> --profile azure
databricks postgres list-endpoints projects/<project id>/branches/<branch> --profile azure
databricks postgres list-roles projects/<project id>/branches/<branch> --profile azure
```

- [ ] **13.4** In `databricks.yml`, under `azure_dev.variables`, set the actual paths from 13.3:

```yaml
      lakebase_branch: projects/<project id>/branches/<branch>
      lakebase_endpoint: projects/<project id>/branches/<branch>/endpoints/<endpoint>
```

  `claims_app.yml` uses `${var.lakebase_branch}/roles/${workspace.current_user.short_name}` as the database owner. If 13.3 lists no role with that id, stop and decide together (for example, add a `postgres_roles` resource).
- [ ] **13.5** Add to the wrapper in `resources/claims_app.yml`:

```yaml
  azure_dev:
    resources: *claims_app_resources
```

- [ ] **13.6** `databricks bundle deploy -t azure_dev --profile azure`. It creates database `e2e_claims_azure_dev`, the two synced tables (continuous) and the app `e2e-claims-azure-dev`.
- [ ] **13.7** The manual GRANT, as on AWS. Get the app's service principal client id from `databricks apps get e2e-claims-azure-dev --profile azure`. Then, as yourself in database `e2e_claims_azure_dev` (Lakebase SQL editor):

```sql
GRANT USAGE ON SCHEMA "dev_<user>_gold" TO "<app service principal client id>";
GRANT SELECT ON ALL TABLES IN SCHEMA "dev_<user>_gold" TO "<app service principal client id>";
```

- [ ] **13.8** `databricks bundle run claims_app --profile azure`. Open the app URL and submit one claim with a photo in customer mode. Check that it appears in admin mode.

**Check:** the app works end to end on Azure. **The whole project now runs on both clouds.**

---

### Task 14: Pause checklist (after every session) and wrap-up

**Files:**
- Modify: `CLAUDE.md` (Azure target, commands, gotchas)

- [ ] **14.1** **Pause checklist.** Run it after every session, and save it for later:

```bash
databricks pipelines stop <sql_server_gateway pipeline id> --profile azure         # stops the VM; lets Azure SQL pause
databricks apps stop e2e-claims-azure-dev --profile azure                           # once the app exists
databricks pipelines stop <synced tables pipeline id> --profile azure               # once synced tables exist
databricks postgres update-endpoint <lakebase_endpoint> spec.disabled --json '{"spec": {"disabled": true}}' --profile azure
```

  In the portal, check the next day that **Cost Management → Cost analysis** for `rg-e2e-learning` (and the `databricks-rg-…` group) looks as expected.
  **Resume** in reverse: enable Lakebase compute, `databricks pipelines start-update <synced pipeline id>`, `bundle run claims_app`, then `bundle run sql_server_gateway --no-wait`.
- [ ] **14.2** Update `CLAUDE.md`:
  - a "Multi-cloud targets" bullet: AWS `dev`/`prod`, `azure_dev`, the `azure_prod` placeholder, and the per-target wrappers with anchors;
  - the Azure part 2 commands, the Azure gotchas (Hybrid workspace, quota, free-offer drain, change tracking retention, underscores in names), and the pause checklist.
- [ ] **14.3** **Teardown,** when you're done with Azure:
  - Run `databricks bundle destroy -t azure_dev --profile azure`.
  - In the portal, delete the resource group `rg-e2e-learning`. That removes the workspace, its managed resource group and the SQL server.
  - Then decide whether the `azure_*` targets stay in the repo as a record.
