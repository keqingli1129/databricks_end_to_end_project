# Part 9: CI/CD with GitHub Actions to Azure Databricks (step-by-step plan)

> **How we run this plan:** one step at a time. Claude does a step, explains it, ticks its box, and waits for "go". Claude never runs `git add` or `git commit`; you commit when you want.

**Goal:** every push to `main` on GitHub runs lint and tests, then deploys the bundle to the Azure Databricks target **`azure_prod`** as a service principal, as in [DAB_cicd.txt](DAB_cicd.txt). No person deploys prod by hand any more.

**Setup (changed 2026-10-14: real, not pretend):** we build it for real on an **Azure Databricks Premium trial** workspace, the same one part 7 uses. **Phase A** walks through everything from the Azure subscription to the catalogs; on the way you become **workspace admin** and **account admin**. Tasks 1–5 then create `e2e-deployer`.

**The trial clock:** DBUs are free for **14 days** from the moment the workspace is created (A2). Azure VMs, storage and network bill to your card from day one. Write the creation date down, and finish (or delete the resource group) before day 14.

## Who does what (in a real company)

A workspace has to exist before CI/CD can deploy to it, and usually the data team doesn't create it. Here you play every role. In part 7, Tasks 1 and 5 are the "platform team" work.

| What | Usually done by | How |
|---|---|---|
| Azure subscription, resource group, network | cloud / platform team | Azure portal, usually Terraform or Bicep |
| Azure Databricks workspaces (dev, prod; often separate subscriptions) | platform team, with Owner or Contributor on the resource group | the same Terraform (`azurerm_databricks_workspace`). Whoever first launches the workspace with that Azure role becomes a workspace admin automatically. |
| First Databricks **account admin** | a **Microsoft Entra Global Administrator**, at their first sign-in to accounts.azuredatabricks.net | they then make a small admin group the account admins |
| Unity Catalog metastore | automatic for new workspaces (one per region), or the platform team | attached to the workspace |
| Catalogs `e2e_dev` / `e2e_prod`, groups, workspace assignment | platform team / data platform admin | Terraform (`databricks` provider) or by hand |
| Service principal `e2e-deployer`, its grants, the federation policy (Tasks 1–5) | platform team, often **on request** from the data team | a ticket: "SP for repo X, environment `azure-prod`, User on the prod workspace, `USE CATALOG` + `CREATE SCHEMA` on `e2e_prod`" |
| `databricks.yml`, the workflow, the code (Tasks 6–11) | **the data team (you)** | git |

## Identities in this plan

| Identity | Kind | Used by | For |
|---|---|---|---|
| You (Azure user, e.g. your Gmail Microsoft account) | person; account admin + workspace admin here | you, in the browser and with CLI profiles `azure` / `azure-account` | the one-off setup (Tasks 1–5), reading prod, local `bundle validate` |
| **`e2e-deployer`** | service principal (a machine identity) | **GitHub Actions only** | deploying `azure_prod`, and the identity prod jobs and pipelines **run as** |
| GitHub's OIDC token | not an account: a signed, minutes-long proof "this is repo X, environment `azure-prod`" | each workflow run | exchanged for an `e2e-deployer` token (Task 5) |
| The claims app's service principal | service principal, created automatically with the app | the running app | not part of this plan |

## Architecture

```text
git push main ──▶ GitHub Actions: workflow "Deploy azure_prod"  (environment azure-prod)
                   │ 1. GitHub issues a short-lived OIDC token
                   │    sub = repo:keqingli1129/databricks_end_to_end_project:environment:azure-prod
                   ▼
                 Databricks account: service principal e2e-deployer
                   federation policy: "trust GitHub tokens with exactly this sub"
                   │ 2. token exchanged for a Databricks OAuth token (no stored secret)
                   ▼
   job "test":    ruff check · ruff format --check · pytest (Databricks Connect, serverless)
   job "deploy":  needs test ─▶ databricks bundle deploy -t azure_prod
                   ▼
                 Azure workspace: /Workspace/Users/<sp app id>/.bundle/databricks_end_to_end_project/azure_prod
                 jobs and pipelines owned by e2e-deployer, run_as e2e-deployer, catalog e2e_prod
```

## Decisions (how this differs from the transcript)

| Transcript | Here | Why |
|---|---|---|
| Entra ID app (`az ad sp create-for-rbac`) with **Contributor** on the resource group | **Databricks-managed** service principal `e2e-deployer`, **no Azure role** | Bundle deploys only call the Databricks API. Contributor would let CI change or delete Azure resources (the workspace itself), which it never needs. |
| SP is **workspace admin** | ordinary workspace user + exactly the grants it needs (Task 3) | least privilege, see [access_control_guide.md](access_control_guide.md) section 4 |
| OAuth secret → PAT with no expiry → GitHub secret `SP_TOKEN` | **workload identity federation** (GitHub OIDC). Nothing secret is stored anywhere. | Databricks' current recommendation. A never-expiring token in GitHub is the weakest link; here, deleting the federation policy cuts CI off instantly. The transcript's way is kept as a fallback in Task 5. |
| trigger: push to `master` | push to `main`, docs-only changes skipped, plus a manual "Run workflow" button | your default branch is `main`; docs commits shouldn't redeploy |
| `uv sync`/test, then deploy | the same two jobs, but CI installs a slim **`test`** dependency group | the `dev` group pulls the CUDA build of `torch` (several GB) |
| target `prod`, `run_as` the SP | target **`azure_prod`** (part 7 placeholder) gets `run_as`, `root_path` and `permissions` | the AWS `dev`/`prod` targets stay as they are |

## Global constraints

- **Names:** service principal **`e2e-deployer`**, GitHub environment **`azure-prod`**, workflow file **`.github/workflows/deploy-azure-prod.yml`**, CLI profiles **`azure`** (workspace, yours) and **`azure-account`** (account console).
- **Repo:** `keqingli1129/databricks_end_to_end_project` (public or private, either works).
- **No secrets in git or in GitHub secrets.** The service principal's application ID and the host are not secret; they go in GitHub environment **variables**.
- **Leave AWS `dev`/`prod` unchanged.** `databricks bundle plan -t dev --profile DEFAULT` must still show no changes at the end.
- **No new automated tests.** CI runs the tests that already exist.
- **CI deploys; it doesn't run.** Running the data jobs in prod stays a manual decision (optional Task 12).
- **Azure:** pay-as-you-go subscription on your personal Gmail Microsoft account (not the district account), resource group **`rg-e2e-learning`**, region **East US 2**, workspace **`e2e-learning`**: the same names as part 7, so Phase A **is** part 7 Tasks 1 and 5. Do it once; tick it in both plans.

---

## Phase A: from Azure subscription to catalogs

Who you are in each step: **A1–A2** as an Azure **Owner** (the Azure role), **A3** you become **workspace admin**, **A4** you become **account admin**, **A5** as workspace admin you create the catalogs and become their **owner**.

### Task A1: Subscription, budget, resource group (Azure portal)

**Why:** everything Azure bills goes on one subscription; everything we create goes in one resource group, so cleanup is a single delete. Creating the subscription with your Gmail Microsoft account also creates an Entra ID **tenant** ("Default Directory") in which you are **Global Administrator**. That matters in A4.

- [x] **A1.1** Sign in at https://portal.azure.com with your Gmail Microsoft account. **Subscriptions:** if there's none, **+ Add → Pay-As-You-Go** (needs a card). A "Free Trial" subscription won't work: Azure Databricks can't be created on it.
- [x] **A1.2** **Cost Management → Budgets → + Add:** scope the subscription, name `e2e-learning`, **Monthly**, amount **50**. Alerts: **Actual** at **50 %** and **90 %**, sent to your Gmail.
- [x] **A1.3** **Resource groups → + Create:** subscription as above, name **`rg-e2e-learning`**, region **East US 2**, **Review + create**.
- [x] **A1.4** Check your role: **rg-e2e-learning → Access control (IAM) → View my access** shows **Owner** (inherited from the subscription). Owner or Contributor is what lets you create the workspace.

**Check:** the resource group is listed; the budget shows $0 used.

### Task A2: Create the workspace `e2e-learning` (Azure portal)

**Why:** in Azure, a Databricks workspace is an Azure **resource**, so it's created with an Azure role, not a Databricks one. **Hybrid** because part 7's gateway needs classic compute; serverless works in a Hybrid workspace too.

- [x] **A2.1** Portal search **Azure Databricks → + Create**. Tab **Basics**:

| Field | Value | Notes |
|---|---|---|
| Subscription | your pay-as-you-go | |
| Resource group | `rg-e2e-learning` | |
| Workspace name | `e2e-learning` | becomes part of the managed resource group name |
| Region | **East US 2** | the portal defaults to Central US; change it |
| Pricing tier | **Trial (Premium - 14-Days Free DBUs)** | Premium is needed for Unity Catalog permissions, serverless, apps |
| Workspace type | **Hybrid** | Serverless is greyed out with the trial tier anyway |
| Managed Resource Group name | leave empty | Azure names it `databricks-rg-e2e-learning-…` |

- [x] **A2.2** Tab **Networking:** "Secure Cluster Connectivity (No Public IP)" = **Yes** (default); "Deploy in your own Virtual Network" = **No**. (Your own VNet, private link etc. are what a platform team would add in a company.)
- [x] **A2.3** Tabs **Encryption**, **Security & compliance:** leave the defaults. **Tags** (optional): `project = e2e`. Then **Review + create → Create**. It takes about 3–5 minutes.
- [x] **A2.4** **Write down today's date:** the 14-day DBU trial starts now.

**Check:** the workspace resource shows **Status: Succeeded** and a **Workspace URL** `https://adb-<id>.<n>.azuredatabricks.net`. A second resource group `databricks-rg-e2e-learning-…` exists; never edit it (it holds the workspace's VNet, storage account and, later, cluster VMs).

### Task A3: First launch: you become workspace admin (browser + CLI)

**Why:** the first time someone with Owner/Contributor on the workspace resource opens it, Azure Databricks adds them as a **workspace admin**. New workspaces get **Unity Catalog automatically**: a metastore for the region is attached, and a **workspace catalog** (named after the workspace) is created.

- [x] **A3.1** Workspace resource → **Launch Workspace**, sign in with the same Microsoft account.
- [x] **A3.2** Top right, your name → **Settings**: an **Advanced / Identity and access** area is visible, which only admins see. **Identity and access → Users**: you're listed in the `admins` group.
- [x] **A3.3** **Catalog** (left bar): you see `samples`, `system`, and the workspace catalog (e.g. `e2e_learning`).
- [x] **A3.4** CLI profile for the workspace (OAuth sign-in as you, in the browser):

```bash
databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net --profile azure
databricks current-user me --profile azure        # userName: what dev_<short_name>_ prefixes and your user folder use
databricks metastores current --profile azure     # Unity Catalog is attached: note metastore_id
databricks metastores summary --profile azure     # storage_root empty? => A5 needs the storage steps
databricks warehouses list --profile azure        # note the starter warehouse's exact name
```

**Check:** `current-user me` shows `"groups": [... "admins" ...]`; `metastores current` returns a metastore. Write down the warehouse name and whether `storage_root` is empty.

### Task A4: Account console: you become account admin (browser + CLI)

**Why:** service principals and federation policies (Tasks 1, 5) are **account** objects. On Azure, the first person to sign in to the account console must be an Entra **Global Administrator**; they become the first **account admin**. A1 made you Global Administrator of your own tenant.

- [x] **A4.1** Open https://accounts.azuredatabricks.net and sign in. You land on the account console: **Workspaces** lists `e2e-learning`.
  - **Personal (Gmail) Microsoft accounts can't sign in here** ("Selected user account does not exist in tenant 'Microsoft Services'…"): without a tenant hint, a personal account is sent to Microsoft's own tenant. The fix (done 2026-10-14) is a **member user in your own tenant**:
    1. Portal → **Microsoft Entra ID → Overview:** note the **Primary domain** (`keqingli1129gmail.onmicrosoft.com`).
    2. **Users → + New user → Create new user:** `dbadmin@<primary domain>`, display name `Databricks Admin`, auto-generated password (copy it), **Assignments → Global Administrator**.
    3. In a separate browser profile (or incognito with third-party cookies allowed), sign in to the account console as `dbadmin`: set a new password and register **Microsoft Authenticator** (security defaults require MFA for admins; if `mysignins.microsoft.com/register` hangs blank, allow third-party cookies or open https://aka.ms/mfasetup).
    4. As the first Global Administrator to sign in, `dbadmin` becomes the first **account admin**.
  - Result: **your Gmail user = workspace admin** (profile `azure`), **`dbadmin` = account admin** (profile `azure-account`). The same split a company makes: a dedicated admin identity for the account.
- [x] **A4.2** Top right, your user menu: copy the **Account ID** (a UUID). It's the `--account-id` below and the federation policy's audience (Task 5).
- [x] **A4.3** **User management → Users → you → Roles:** **Account admin** is on.
- [x] **A4.4** Account-level CLI profile:

```bash
databricks auth login --host https://accounts.azuredatabricks.net --account-id <account id> --profile azure-account
databricks account workspaces list --profile azure-account     # e2e-learning, with its workspace id
databricks account metastores list --profile azure-account     # the East US 2 metastore from A3
```

**Check:** both commands list the workspace and the metastore.

### Task A5: Catalogs `e2e_dev` and `e2e_prod` (Catalog Explorer / SQL)

**Why:** each target has its own catalog, made by hand, never owned by the bundle (see CLAUDE.md). With automatic Unity Catalog, **workspace admins have `CREATE CATALOG`** on the metastore. Whoever creates a catalog becomes its **owner**. `e2e_dev` is for part 7, `e2e_prod` for CI.

- [x] **A5.1** (n/a 2026-10-14: no storage root, so A5.2) If A3.4 showed a **storage_root** (or Catalog Explorer's **Create catalog** dialog says the storage location is optional): **Catalog → + → Create catalog**, name `e2e_prod`, type **Standard**, no storage location → **Create**. Repeat for `e2e_dev`. Skip A5.2.
- [x] **A5.2** If there's **no storage root** (`CREATE CATALOG` fails with "Metastore storage root URL does not exist"), give the catalogs their own storage first. This is the admin storage setup from section 8 of [access_control_guide.md](access_control_guide.md):
  1. **Storage account** (portal → Storage accounts → + Create): resource group `rg-e2e-learning`, name `ste2elearningkl` (lowercase, no dashes), East US 2, **Standard**, **LRS**. Tab **Advanced: Enable hierarchical namespace = on** (that makes it ADLS Gen2). Create, then **Containers → + Container** `dabuc`.
  2. **Access connector** (portal → **Access Connector for Azure Databricks** → + Create): resource group `rg-e2e-learning`, name `ac-e2e-learning`, East US 2. It gets a system-assigned **managed identity**: the identity Databricks uses to reach the storage. Copy its **Resource ID** (`/subscriptions/…/accessConnectors/ac-e2e-learning`).
  3. **Storage account → Access control (IAM) → + Add role assignment:** role **Storage Blob Data Contributor**, assign to **Managed identity → Access Connector for Azure Databricks → ac-e2e-learning**.
  4. Workspace → **Catalog → External data → Credentials → Create credential:** type **Azure Managed Identity**, name `e2e_learning_cred`, access connector ID = the Resource ID.
  5. **External data → External locations → Create external location:** name `e2e_learning_uc`, URL `abfss://dabuc@ste2elearningkl.dfs.core.windows.net/`, credential `e2e_learning_cred` → **Test connection** (all green).
  6. In a SQL editor:

```sql
CREATE CATALOG e2e_prod MANAGED LOCATION 'abfss://dabuc@ste2elearningkl.dfs.core.windows.net/e2e_prod';
CREATE CATALOG e2e_dev  MANAGED LOCATION 'abfss://dabuc@ste2elearningkl.dfs.core.windows.net/e2e_dev';
```

**Check:** `databricks catalogs get e2e_prod --profile azure` returns it with `"owner"` = your user name; the same for `e2e_dev`. (This also settles part 7 Task 5.4: `azure_dev` keeps catalog `e2e_dev`.)

---

### Task 1: Create the service principal `e2e-deployer` (account console)

**Why:** CI needs its own identity (transcript: "a service principal that's going to control my workspace"). A machine identity doesn't leave the company, has no password, and its rights are exactly what we grant.

- [x] **1.1** Account console (https://accounts.azuredatabricks.net) → **User management → Service principals → Add service principal** → **Databricks managed**, name `e2e-deployer`.
- [x] **1.2** Open it and write down two IDs:
  - **Application ID** (a UUID): used in grants, `run_as`, and the workflow.
  - **Numeric ID** (the number in the URL, e.g. `3659993829438643`): used for the federation policy.
- [x] **1.3** Find it with the account profile from A4.4:

```bash
databricks account service-principals list --profile azure-account --filter 'displayName eq "e2e-deployer"'
```

**Check:** the list shows `e2e-deployer` with its `applicationId` and `id`.

---

### Task 2: Give it the workspace (account console)

**Why:** an account identity can't do anything in a workspace until it's assigned (access guide, step 1). The transcript makes it **admin**; we make it a plain **User**.

- [x] **2.1** Account console → **Workspaces → e2e-learning → Permissions → Add permissions** → `e2e-deployer`, permission **User**.
- [x] **2.2** In the workspace: **Settings → Identity and access → Service principals → e2e-deployer → Configurations**, entitlements: **Workspace access** and **Databricks SQL access** on. Leave **Allow unrestricted cluster creation** off. (Turn it on only if `azure_prod` ever gets the Lakeflow Connect gateway, which runs on classic compute.)

**Check:** `databricks service-principals list --profile azure` (workspace level) shows `e2e-deployer` with both entitlements.

---

### Task 3: Grants it needs to deploy and run (SQL + UI, once)

**Why:** a deploy creates schemas, volumes and a registered model inside `e2e_prod`, and resolves the warehouse lookup. The catalog is made by hand (the bundle deliberately doesn't own catalogs, see CLAUDE.md), so its grants are made by hand too. Everything the SP **creates** it then owns, so nothing more is needed below the catalog.

- [x] **3.1** `e2e_prod` exists and you own it (A5).
- [x] **3.2** In a SQL editor (as you):

```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG e2e_prod TO `<application id>`;
-- samples is readable by everyone, so the existing tests need nothing extra.
```

- [x] **3.3** **SQL Warehouses → Serverless Starter Warehouse → Permissions:** add `e2e-deployer` with **Can use**. (The `warehouse_id` lookup in `databricks.yml` fails without it, because the SP can't see the warehouse.)
- [x] **3.4** Serverless: in the account console, **Settings → Feature enablement**, check that serverless compute is on for the workspace. If the workspace uses **serverless budget policies**, give `e2e-deployer` **User** on one.

**Check:** `SHOW GRANTS ON CATALOG e2e_prod;` lists the application id with `USE CATALOG`, `CREATE SCHEMA`.

---

### Task 4: GitHub environment and variables (github.com)

**Why:** the **environment** name becomes part of the OIDC token's subject, so only jobs that declare `environment: azure-prod` can sign in as `e2e-deployer`. It's also where an approval gate lives.

- [x] **4.1** Repo → **Settings → Environments → New environment** → `azure-prod`.
- [x] **4.2** **Deployment branches and tags:** "Selected branches" → `main`. A job on any other branch then can't use this environment, and so can't get the token.
- [ ] **4.3** Optional: **Required reviewers** → yourself. Each run then waits for your click before the jobs start. Good for learning; you can remove it later.
- [x] **4.4** **Environment variables** (not secrets):
  - `DATABRICKS_HOST` = `https://adb-<id>.<n>.azuredatabricks.net`
  - `DATABRICKS_CLIENT_ID` = the application ID from 1.2

**Check:** the environment shows both variables and the branch rule.

---

### Task 5: Federation policy: trust GitHub's token (account CLI)

**Why:** this replaces the transcript's "generate secret → create PAT → paste in GitHub". The policy says: a token issued by GitHub, for **this repo**, in **this environment**, may act as `e2e-deployer`. Databricks compares the subject **exactly**.

- [x] **5.1** Find the subject format. Repos created (or renamed or transferred) **on or after 2026-07-15** use the immutable form with numeric IDs; older ones use names:
  - older: `repo:keqingli1129/databricks_end_to_end_project:environment:azure-prod`
  - newer: `repo:keqingli1129@<owner id>/databricks_end_to_end_project@<repo id>:environment:azure-prod`

  `curl -s https://api.github.com/repos/keqingli1129/databricks_end_to_end_project` (fields `created_at`, `owner.id`, `id`) tells you which applies. **Ours: created 2026-09-27, owner id 3473089, repo id 1389960991 → the newer form.**
- [x] **5.2** Create the policy (done 2026-10-14 as `dbadmin`; `e2e-deployer`'s numeric id is 146817550015497). Two audiences: the **account ID** (what Databricks recommends) and the **workspace token endpoint**, because CLI 1.15 takes its default audience from the workspace's metadata (seen in the A3 debug log). The subject alone still pins it to this repo and environment.

```bash
databricks account service-principal-federation-policy create 146817550015497 --policy-id github-azure-prod --profile azure-account --json '{
  "description": "GitHub Actions: repo keqingli1129/databricks_end_to_end_project, environment azure-prod (part 9)",
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["1d1ca9dc-3448-4226-8f99-d518d2ef65db", "https://adb-7405618954475016.16.azuredatabricks.net/oidc/v1/token"],
    "subject": "repo:keqingli1129@3473089/databricks_end_to_end_project@1389960991:environment:azure-prod"
  }
}'
```

**Check:** `databricks account service-principal-federation-policy list 146817550015497 --profile azure-account` shows `github-azure-prod`.

> **Fallback, the transcript's way** (only if federation isn't possible): service principal → **Secrets → Generate secret**, store `DATABRICKS_CLIENT_SECRET` as an **environment secret** in `azure-prod`, and in the workflow drop `DATABRICKS_AUTH_TYPE` and `id-token: write`. Prefer this OAuth secret over a PAT: it expires (max 2 years) and the CLI refreshes its tokens itself.

---

### Task 6: Make lint pass (one-off cleanup)

**Files:** whatever ruff touches (about 15 files under `src/` and `tests/`).

**Why:** CI fails on the first run otherwise. Today `ruff check` reports **16 errors** (all auto-fixable) and `ruff format --check` would reformat **15 files**, 9 of them Markdown files in `docs/` (ruff 0.16 formats code blocks in Markdown too).

- [x] **6.0** Added `extend-exclude = ["docs"]` under `[tool.ruff]` in `pyproject.toml`: the plans' snippets record what was run, so ruff leaves them alone (decided 2026-10-14).
- [x] **6.1** `env -u PYTHONPATH uv run ruff check --fix . && env -u PYTHONPATH uv run ruff format .` (21 fixes, 5 files reformatted)
- [x] **6.2** Read the diff (`git diff --stat`, then any file with logic changes, not just whitespace). Notebook sources (`src/**/*.py` with `# COMMAND ----------`) must keep their cell markers.
- [x] **6.3** `env -u PYTHONPATH uv run pytest` still passes (7 passed) against your AWS profile.

**Check:** `ruff check .` and `ruff format --check .` both print no problems.

---

### Task 7: A slim `test` dependency group (`pyproject.toml`)

**Files:**
- Modify: `pyproject.toml`

**Why:** `uv sync --dev` installs the CUDA build of `torch`, `mlflow` and `streamlit`. The tests need only pytest, ruff and Databricks Connect. A separate group keeps CI to about a minute. `dev` includes it, so nothing changes locally.

- [x] **7.1** In `[dependency-groups]`, move the three test tools into a new group and include it from `dev`:

```toml
[dependency-groups]
# What CI installs: lint and the tests (they run on Databricks Connect, so no local Spark).
test = [
    "pytest",
    "ruff",
    "databricks-connect~=18.0.0",
]
dev = [
    { include-group = "test" },
    "databricks-dlt",
    "ipykernel",
    # ... the rest unchanged
]
```

- [x] **7.2** Try CI's install locally in a throw-away environment:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/ci-venv env -u PYTHONPATH uv sync --no-dev --group test --frozen
UV_PROJECT_ENVIRONMENT=/tmp/ci-venv env -u PYTHONPATH uv run --no-dev --group test pytest
```

- [x] **7.3** `env -u PYTHONPATH uv sync --dev` still works, and `uv.lock` is updated (commit it).

**Check (2026-10-14):** in the throw-away env: 33 packages, **354 MB** (local `.venv`: 6.1 GB), no `torch`/`mlflow`; `ruff check`, `ruff format --check` and **7 tests** pass.

---

### Task 8: Fill in the `azure_prod` target (`databricks.yml`)

**Files:**
- Modify: `databricks.yml` (the `azure_prod` placeholder from part 7)

**Prerequisite: part 7 Task 7** (the per-target restructure). Without it, `azure_prod` would get every resource, including the serving endpoint (pinned to model version "2", which doesn't exist on Azure) and the claims app (pointing at Free Edition's Lakebase project), and the first deploy would fail.

**Why:** the transcript: "you have to have a `run_as`, because we're going to deploy jobs and on prod it's a requirement". Three things change:
- **`root_path`** under the service principal's folder, so there's a single prod copy whoever deploys.
- **`run_as`**: jobs and pipelines run as `e2e-deployer`, never as a person who might leave.
- **`permissions`**: the SP manages everything; you can view and run, but changes go through git.

- [x] **8.1** Set the target (done 2026-10-14 with application id `4553f370-2290-4d8d-ba5d-c022b6f80672` and `keqingli1129@gmail.com`):

```yaml
  # Deployed only by GitHub Actions (.github/workflows/deploy-azure-prod.yml) as service principal e2e-deployer.
  azure_prod:
    mode: production
    workspace:
      host: https://adb-<id>.<n>.azuredatabricks.net
      root_path: /Workspace/Users/<application id>/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: <application id>
    variables:
      catalog: e2e_prod
      schema: prod
      name_suffix: azure-prod
    permissions:
      - service_principal_name: <application id>
        level: CAN_MANAGE
      - user_name: <your Azure user name>
        level: CAN_VIEW
```

- [x] **8.2** *(decided 2026-10-14: option A, all targets share the CDC stand-in; endpoint and app stay `dev`/`prod` only)* Resources that are **per target** (part 7's wrappers) only exist in `azure_prod` if it's listed in their file:
  - `cdc_ingestion`: `smart_claims_end_to_end` refers to it, so `azure_prod` **must** get it. Either add `azure_prod` to `resources/azure_cdc_ingestion.yml` (real gateway: then turn on cluster creation in 2.2 and `GRANT USE CONNECTION ON CONNECTION azure_sql_claims TO \`<application id>\``), or to the AWS-style stand-in wrapper (Python pipeline + seed job). For a first CI run, the stand-in is simpler.
  - Serving endpoint, claims app, Lakebase, synced tables: **leave `azure_prod` out** for now. The endpoint needs a model version that exists in prod (after `train_and_register` has run there), and the app needs gold tables. Adding them later is just another commit, deployed by CI.
- [x] **8.3** Validate as yourself (validate doesn't need the SP's rights):

```bash
databricks bundle validate -t azure_prod --profile azure
databricks bundle validate -t dev --profile DEFAULT     # AWS still fine
```

**Check:** both validate with no errors; the `azure_prod` output shows the root path under the application id. Validating as yourself prints one **recommendation** ("permissions should include the current deployment identity"): expected, because in CI the deployer is `e2e-deployer`, which is listed. Don't add yourself with `CAN_MANAGE`.

---

### Task 9: The workflow (`.github/workflows/deploy-azure-prod.yml`)

**Files:**
- Create: `.github/workflows/deploy-azure-prod.yml`

**Why:** the transcript's file: a test job, then a deploy job, one run at a time. Start with **only the manual trigger** (Task 11 adds `push`), so nothing deploys until we've watched it work once.

- [x] **9.1** Create the file (done 2026-10-14; the real file adds `DATABRICKS_SERVERLESS_COMPUTE_ID: auto` to the test job and `--group test` to each `uv run`, so it is the source of truth):

```yaml
# Lint, test, then deploy the bundle to azure_prod as service principal e2e-deployer (docs/part9_cicd_plan.md).
# Auth: GitHub OIDC token -> Databricks workload identity federation. No stored secrets.
name: Deploy azure_prod

on:
  workflow_dispatch:

# One deploy at a time; a newer push waits instead of racing the older one.
concurrency:
  group: azure-prod
  cancel-in-progress: false

permissions:
  id-token: write   # lets the job request the OIDC token
  contents: read

env:
  DATABRICKS_AUTH_TYPE: github-oidc
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}

jobs:
  test:
    runs-on: ubuntu-latest
    environment: azure-prod   # part of the token's subject; must match the federation policy
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
        with:
          python-version: "3.12"
      - run: uv sync --no-dev --group test --frozen
      - run: uv run --no-dev ruff check .
      - run: uv run --no-dev ruff format --check .
      - run: uv run --no-dev pytest   # Databricks Connect, serverless, as e2e-deployer

  deploy:
    needs: test
    runs-on: ubuntu-latest
    environment: azure-prod
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6   # the bundle builds the wheel with `uv build --wheel`
        with:
          python-version: "3.12"
      - uses: databricks/setup-cli@main
        with:
          version: 1.15.0             # same CLI as local (`databricks --version`)
      - run: databricks current-user me   # shows who we are: the application id
      - run: databricks bundle validate -t azure_prod
      - run: databricks bundle deploy -t azure_prod
```

- [x] **9.2** Notes to read before running:
  - `vars.*` are the environment variables from 4.4; they're available because the jobs declare `environment: azure-prod`.
  - `DATABRICKS_HOST` must equal the target's `workspace.host`, or the CLI refuses.
  - `uv run --no-dev` stops uv from installing the `dev` group again on `run`.

**Check:** the file is in the default branch (the manual button only appears for workflows on the default branch). Commit and push.

---

### Task 10: First run, by hand

**Why:** watch each stage once, the way the transcript does ("it's authenticated, now it's deploying the bundle").

- [ ] **10.1** GitHub → **Actions → Deploy azure_prod → Run workflow** (branch `main`), or `gh workflow run deploy-azure-prod.yml --ref main`. Approve it if you set a reviewer in 4.3.
- [ ] **10.2** Follow the log. Typical first-run failures:

| Error | Cause | Fix |
|---|---|---|
| `invalid_grant` / "no matching federation policy" | subject mismatch | compare the token's `sub` (format from 5.1) with the policy, character for character |
| `PERMISSION_DENIED ... CREATE SCHEMA` | grants | Task 3.2 |
| `warehouse "Serverless Starter Warehouse" not found` | SP can't see it | Task 3.3 |
| `run_as` / "service principal user" error | someone other than the SP deployed | only CI deploys prod; a person needs the **Service principal: User** role on `e2e-deployer` to do it |
| pytest can't start a session | serverless not allowed for the SP | Task 3.4 |

- [ ] **10.3** Check the workspace:

```bash
databricks bundle summary -t azure_prod --profile azure
databricks jobs list --profile azure | grep -i smart_claims
```

  In the UI: the jobs show **Run as: e2e-deployer**; Workspace → Users → `<application id>` → `.bundle/databricks_end_to_end_project/azure_prod/artifacts` holds the wheel (as in the transcript).

**Check:** both jobs green; `bundle summary` lists the resources; schemas `bronze`/`silver`/`gold` etc. exist in `e2e_prod`, owned by the application id.

---

### Task 11: Deploy on every push to `main`

**Files:**
- Modify: `.github/workflows/deploy-azure-prod.yml`

- [ ] **11.1** Extend `on:`:

```yaml
on:
  workflow_dispatch:
  push:
    branches: [main]
    paths-ignore:          # docs-only commits don't redeploy
      - "docs/**"
      - "**/*.md"
```

- [ ] **11.2** Prove it end to end, as the transcript suggests ("change your bundle, then see if it triggers"): change something harmless and visible, e.g. a job `description` in `resources/sample_job.job.yml`. Commit and push.
- [ ] **11.3** Push a docs-only change and see that **no** run starts.

**Check:** the code push deploys and the new description shows in the prod job; the docs push doesn't trigger.

---

### Task 12 (optional): Run the pipeline after deploying

**Why:** deploying only updates definitions. The transcript skips running the job ("it's going to take a while"); here it's an opt-in.

- [ ] **12.1** Add a `run` job after `deploy`, only for manual runs with a checkbox:

```yaml
on:
  workflow_dispatch:
    inputs:
      run_end_to_end:
        description: Run smart_claims_end_to_end after deploying
        type: boolean
        default: false
# ...
  run:
    needs: deploy
    if: github.event_name == 'workflow_dispatch' && inputs.run_end_to_end
    runs-on: ubuntu-latest
    environment: azure-prod
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
        with:
          version: 1.15.0
      - run: databricks bundle run smart_claims_end_to_end -t azure_prod
```

  Before the first run in prod: the source data must exist (`seed_source_database`, Kafka secrets in `kafka_azure_prod` or `telematics_source: files`, files in the prod `landing` volume). Those are one-off manual steps, as on AWS.

**Check:** a manual run with the box ticked runs the job; the run in Databricks is by `e2e-deployer`.

---

### Task 13: Wrap-up

- [ ] **13.1** `databricks bundle plan -t dev --profile DEFAULT` shows no changes (AWS untouched).
- [ ] **13.2** Add a short **CI/CD** paragraph to `CLAUDE.md` (workflow file, SP, environment, "only CI deploys azure_prod").
- [ ] **13.3** **To switch CI off** (cost or safety): delete the federation policy (`databricks account service-principal-federation-policy delete <numeric id> github-azure-prod --profile azure-account`) or disable the workflow (Actions → … → Disable workflow). Deployed resources stay; prod schedules are already paused.

## Later (not in this plan)

- A **pull-request workflow** (`ruff` + `bundle validate`) for when you work on branches. It needs its own subject (`…:pull_request`), so best a second, read-only service principal.
- `azure_dev` deployed per PR, then promotion to `azure_prod` with an approval: the full dev → prod flow.
- Adding the endpoint, app and Lakebase to `azure_prod` once a model version and gold data exist there.
