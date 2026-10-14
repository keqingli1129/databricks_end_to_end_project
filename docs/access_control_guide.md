# Access control in Azure Databricks (and the extra setup on AWS)

How Azure Databricks decides who can do what, what each level controls, what we recommend, and how it applies to this project. The AWS section at the end covers only what's different there.

> **Status:** reference only. Nothing in this project sets these controls yet. You own every object, which is why everything works for you. The bundle only gives you `CAN_MANAGE` on the prod target (`databricks.yml`).

## The big picture

Access is checked in layers. A request passes **all** of them, and each layer has its own switch:

```
Entra ID user / group / service principal
   │  0. Identity: used directly (automatic identity management), or synced by SCIM
   ▼
Databricks account ──▶ 1. Workspace access   (assigned to the workspace? entitlements?)
   │
   ├─▶ 2. Unity Catalog data        (GRANT on catalog / schema / table / volume / model)
   │      └─▶ 3. Fine-grained       (row filters, column masks, dynamic views)
   ├─▶ 4. Object permissions        (jobs, pipelines, dashboards, Genie, apps, endpoints, notebooks)
   ├─▶ 5. Run-as identity           (whose rights a job / dashboard / app uses)
   ├─▶ 6. Secrets                   (secret scope permissions)
   └─▶ 7. Lakebase                  (Postgres roles and GRANT, a separate system)

   8. Storage: Databricks itself reaches ADLS through an access connector (managed identity). Users never do.
```

Two rules hold everywhere:

- **No grant means no access.** Unity Catalog has no explicit DENY. To take access away, remove the grant, or narrow the broader grant it inherits from.
- **Owners can do everything** on what they own, including granting to others. Ownership is a hidden permission, so choose owners deliberately.

## Where things live: Unity Catalog vs workspace

Unity Catalog isn't inside a workspace. It sits at account level and is attached to workspaces:

```
Databricks account   (identities; on Azure, read from Entra ID)
├── Unity Catalog metastore (one per region)   → DATA: catalogs, schemas, tables, views, volumes,
│        attached to ▼                            functions, registered models     — governed by GRANT
├── Workspace (dev)     ┐ OBJECTS: notebooks, jobs, pipelines, dashboards, Genie spaces, apps,
└── Workspace (prod)    ┘ serving endpoints, ML experiments, SQL warehouses, secret scopes
                          — governed by object permissions
```

A registered model (and its versions) is Unity Catalog data; its MLflow experiment and serving endpoint are workspace objects. One metastore can serve many workspaces, so a grant on a schema applies in each of them, unless the catalog is **bound** to certain workspaces only. Deleting a workspace doesn't delete the data.

## Giving a group access: the three steps

Create the group once, in Entra ID. Then, for each group:

| Step | Where | Answers | Without it |
|---|---|---|---|
| **1. Assign the group to the workspace,** with entitlements | account console → Workspaces → *workspace* → Permissions, or workspace Settings → Identity and access | can they sign in to this workspace, and as what kind of user? | they can't sign in, so no grant helps |
| **2. Grant Unity Catalog privileges** | SQL `GRANT`, Catalog Explorer → Permissions, or bundle `grants:` | which data can they read or write? | they sign in but see no tables |
| **3. Set object permissions** (when needed) | the object's Share/Permissions, or bundle `permissions:` | which jobs, dashboards, Genie spaces, apps can they open or run? | they can't open the dashboard or app |

- Step 1 is **per workspace**: a group that uses dev and prod is assigned to both.
- Step 2 is **per catalog or schema, once**: it applies in every workspace attached to the metastore.
- Not every group needs step 2. App-only users need steps 1 and 3 (`CAN_USE` on the app) and **no** data grants, because the app's service principal reads the data.

Example, `claims_analysts` using the dashboard and Genie in prod:

1. Assign `claims_analysts` to the prod workspace with **Consumer access** (or Databricks SQL access, if they write their own queries).
2. Grant data:
   ```sql
   GRANT USE CATALOG ON CATALOG e2e_prod TO `claims_analysts`;
   GRANT USE SCHEMA, SELECT ON SCHEMA e2e_prod.gold TO `claims_analysts`;
   ```
3. Give `CAN_RUN` on the dashboard and the Genie space.

Sections 1, 2 and 4 below explain each step in detail.

---

## 0. Identities: who can be granted anything

A **principal** is anything that can receive a permission:

| Principal | Is | Named in grants as |
|---|---|---|
| User | a person | email, e.g. `ana@company.com` |
| Group | a set of users and service principals | its name, e.g. `` `claims_analysts` `` |
| Service principal | a non-human identity for jobs, apps and CI | its application (client) id, a UUID |

**On Azure, identities come from Entra ID. You don't keep a second set.**

- **Automatic identity management** (the default for newer accounts): Databricks reads users, groups and service principals **directly from Entra ID**. Type an Entra group name in a grant and it's found. Membership changes in Entra apply right away, and nested groups work. Databricks still keeps a small record of each identity in the account (its id, so grants can point at it), created the first time the identity is used. That record follows Entra; you never edit it.
- **SCIM provisioning** (older): an Entra enterprise application pushes the assigned users and groups into the Databricks account on a schedule. Nested group membership isn't carried over.

Groups are managed **in Entra**, and permissions are managed **in Databricks**.

**Account-level vs workspace-local groups:** Unity Catalog grants accept only **account-level** groups (the ones from Entra, or created in the account console). Old workspace-local groups can't receive data grants. Don't create new ones.

**Admin roles** sit above all of this, so keep them small:

| Role | Can |
|---|---|
| Account admin | manage the account: identities, workspaces, metastores |
| Workspace admin | manage one workspace: users, settings, all objects in it |
| Metastore admin | manage Unity Catalog objects and grants across the metastore |

**Recommend:** grant to **groups only**, never to individual people. Use service principals for anything automated. Keep admin roles in a small group, not with individuals.

## 1. Workspace access

A principal must be **assigned to the workspace** before anything else works. This is done in the account console, or in the workspace's Settings → Identity and access. Then **entitlements** decide what kind of user it is:

| Entitlement | Allows |
|---|---|
| Workspace access | use the Data Science & Engineering workspace (notebooks, jobs) |
| Databricks SQL access | use SQL warehouses, the SQL editor, dashboards |
| Allow unrestricted cluster creation | create classic clusters (cost!) |
| Consumer access | a view-only consumer experience (dashboards, Genie, apps) without full workspace access |

**Recommend:** assign groups to the workspace, not users. Business users get consumer access only. Don't give cluster creation to everyone: prefer serverless, or cluster policies.

## 2. Unity Catalog data

This governs catalogs, schemas, tables, views, volumes, functions and models. Objects form a tree, `catalog → schema → table/view/volume/function/model`, and **a grant on a parent covers all current and future children**.

To read a table you need all three of:

1. `USE CATALOG` on the catalog
2. `USE SCHEMA` on the schema
3. `SELECT` on the table, or on its schema or catalog through inheritance

| Privilege | On | Allows |
|---|---|---|
| `USE CATALOG` / `USE SCHEMA` | catalog / schema | reach the objects inside (no data access by itself) |
| `BROWSE` | catalog, schema | see names and metadata only |
| `SELECT` | table, view (or a parent) | read |
| `MODIFY` | table | insert, update, delete, merge |
| `CREATE TABLE`, `CREATE VOLUME`, `CREATE FUNCTION`, `CREATE MODEL`… | schema | create objects |
| `READ VOLUME` / `WRITE VOLUME` | volume | read / write files |
| `EXECUTE` | function, model | call a UDF, load a registered model |
| `MANAGE` | any | manage grants on it without being the owner |
| `ALL PRIVILEGES` | any | every applicable privilege |

```sql
GRANT USE CATALOG ON CATALOG e2e_prod TO `claims_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA e2e_prod.gold TO `claims_analysts`;

SHOW GRANTS ON SCHEMA e2e_prod.gold;           -- who has what
REVOKE SELECT ON SCHEMA e2e_prod.gold FROM `claims_analysts`;
```

**Recommend:**
- **Grant at the schema level**, which matches the medallion layers: readers on gold, engineers on bronze and silver. Avoid per-table grants unless a table is special. Avoid catalog-wide `SELECT`.
- **Separate catalogs per environment** (`e2e_dev`, `e2e_prod`, as here), with different groups on each. Developers get `ALL PRIVILEGES` in dev, and only `SELECT` (or nothing) in prod.
- **Make a group or a service principal the owner** of prod catalogs and schemas, not a person (`ALTER SCHEMA … OWNER TO `data_platform``). Otherwise access breaks when the person leaves.
- Use `BROWSE` for discoverability. People can find data and then request access.

## 3. Fine-grained: rows and columns

These sit **on top of** `SELECT`. The user still needs `SELECT`; these limit *which rows* and *which values* they see.

| Mechanism | Controls | Attach with |
|---|---|---|
| **Column mask** | a column's value, per user | `ALTER TABLE … ALTER COLUMN … SET MASK <function>` |
| **Row filter** | which rows are visible | `ALTER TABLE … SET ROW FILTER <function> ON (<column>)` |
| **Dynamic view** | both, but only for readers of the view | a view using `is_account_group_member()` / `current_user()` |

Example for this project: hide customer emails from everyone except `pii_readers`.

```sql
CREATE FUNCTION e2e_prod.silver.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '****') END;

ALTER TABLE e2e_prod.silver.customer ALTER COLUMN email SET MASK e2e_prod.silver.mask_email;
```

**Recommend:** use masks and filters on the **base tables** that hold personal data. Here that's `customer` (`first_name`, `last_name`, `date_of_birth`, `email`) in bronze and silver, and every gold table that carries them. Masks follow the table everywhere: SQL, notebooks, dashboards, Genie. Use dynamic views only when you can't change the table. Newer accounts also offer **ABAC** (policies driven by governed tags, e.g. "mask every column tagged `pii`"), which scales better than one mask per column.

## 4. Object permissions (workspace objects)

These decide who can see, run or edit each object. They are separate from data grants: running a job doesn't give you its tables, and reading the tables doesn't give you the job.

| Object | Levels (lowest → highest) |
|---|---|
| Jobs | `CAN_VIEW` → `CAN_MANAGE_RUN` → `IS_OWNER` / `CAN_MANAGE` |
| Pipelines | `CAN_VIEW` → `CAN_RUN` → `CAN_MANAGE` / `IS_OWNER` |
| Dashboards | `CAN_READ` → `CAN_RUN` → `CAN_EDIT` → `CAN_MANAGE` |
| Genie spaces | `CAN_READ` → `CAN_RUN` → `CAN_EDIT` → `CAN_MANAGE` |
| Apps | `CAN_USE` → `CAN_MANAGE` |
| Serving endpoints | `CAN_VIEW` → `CAN_QUERY` → `CAN_MANAGE` |
| Experiments, notebooks, folders | `CAN_READ` → `CAN_RUN` / `CAN_EDIT` → `CAN_MANAGE` |
| SQL warehouses | `CAN_USE` → `CAN_MONITOR` → `CAN_MANAGE` |

**Set them in the bundle, not in the UI.** A UI change is lost or overwritten on the next deploy. A `permissions` block in a target applies to every resource the bundle deploys there. It uses the general levels `CAN_VIEW` / `CAN_RUN` / `CAN_MANAGE`, which the bundle maps to each object type's own levels:

```yaml
targets:
  prod:
    permissions:
      - group_name: data_engineers
        level: CAN_MANAGE
      - group_name: claims_analysts
        level: CAN_VIEW
```

A single resource can add its own entries, e.g. on `claims_investigation_dashboard`:

```yaml
      permissions:
        - group_name: claims_analysts
          level: CAN_RUN
```

Unity Catalog grants can also live in the bundle, on the schema, volume and registered-model resources (`grants:` with `principal` and `privileges`). The catalogs aren't bundle-managed here, so their `USE CATALOG` grants are made by hand.

**Recommend:** in prod, people get at most `CAN_MANAGE_RUN` / `CAN_RUN`. Only the deploying identity, ideally a service principal, gets `CAN_MANAGE`. In dev, development mode already keeps each developer's copies separate.

## 5. Run-as identity: whose rights are used

This is the layer that surprises people most. It decides **whose** data access is used when something runs:

| Thing | Runs with the rights of | So |
|---|---|---|
| Job, pipeline | its `run_as` (default: whoever deployed it) | in prod, set `run_as` to a service principal; never a person |
| Dashboard, **published with embedded credentials** | the publisher | viewers see the data **without** their own `SELECT`; masks apply as for the publisher |
| Dashboard, published without embedded credentials | each viewer | viewers need `SELECT`; masks apply per viewer |
| Genie space | **each person asking** | users need their own `SELECT`; masks apply per user |
| App (default) | the **app's service principal** | users need only `CAN_USE`; everyone sees what the app's service principal sees |
| App with **on-behalf-of-user** auth | each user | per-user data access, at the cost of extra setup |
| Serving endpoint | callers need `CAN_QUERY`; the model's own data access uses the endpoint's identity | |

**Recommend:** decide this deliberately for each consumer. For **personal data**, prefer *per-viewer* access (Genie; dashboards without embedded credentials), so masks and filters apply per person. Embed credentials only for dashboards whose data everyone in the audience may see. For apps, give the app's service principal the **minimum** grants it needs.

## 6. Secrets

A secret scope (e.g. `kafka_dev` here) has `READ` / `WRITE` / `MANAGE` permissions. Anyone who can **run code** with `READ` on a scope can read its values: Databricks hides them in output, but code can still use or leak them.

**Recommend:** one scope per environment and system. Give `READ` only to the service principal that runs the job or pipeline, and to the engineers who maintain it. On Azure, scopes can be **backed by Azure Key Vault**, so secrets are rotated and audited in Key Vault.

## 7. Lakebase (Postgres)

Lakebase databases use **Postgres roles and Postgres `GRANT`**, a separate system that Unity Catalog doesn't control. Databricks identities sign in as Postgres roles. Grants on schemas and tables inside the database are made with SQL in the database. Bundles can't manage them; this project's manual `GRANT` to the app's service principal on `claim_checks_pg` is an example.

**Recommend:** keep Postgres grants in a small, versioned SQL script, and re-run it whenever a synced table or the app's service principal is recreated.

## 8. Storage (admins only)

Databricks, not users, reaches ADLS: through an **access connector** (an Azure managed identity), wrapped in a Unity Catalog **storage credential** and **external location**. Users are granted `READ FILES` / `WRITE FILES` / `CREATE EXTERNAL TABLE` on the external location, never Azure RBAC on the storage account.

**Recommend:** use managed tables and volumes (as this project does) wherever possible. Then there's nothing to grant at the storage level. Don't give people direct Azure RBAC on the storage account behind Unity Catalog: that bypasses Unity Catalog.

---

## Recommendations in one list

1. Manage **people in Entra groups**, and **permissions in Databricks**, granted to groups only.
2. **Least privilege, at schema level,** following bronze → silver → gold.
3. **Separate dev and prod catalogs** with different groups; developers read-only (or nothing) in prod.
4. **Groups or service principals own prod objects,** and **prod jobs and pipelines `run_as` a service principal**.
5. **Mask or filter personal data on the base tables;** prefer per-viewer access where it's shown.
6. **Object permissions and schema grants in the bundle YAML,** so they're reviewed, versioned and repeatable.
7. **Secrets** in Key Vault-backed scopes, with `READ` for the runners only.
8. Small **admin groups**; review regularly with the audit log (`system.access.audit`) and `SHOW GRANTS`.

## Applied to this project

| Group (Entra) | Gets |
|---|---|
| `data_engineers` | dev: `ALL PRIVILEGES` on `e2e_dev`; prod: `CAN_MANAGE_RUN` on jobs, `CAN_RUN` on pipelines, `SELECT` on all prod schemas |
| `claims_analysts` | prod: `USE CATALOG` + `USE SCHEMA`/`SELECT` on gold; `CAN_RUN` on the dashboard and Genie space; personal data masked |
| `pii_readers` | sees unmasked customer columns (used inside the mask functions) |
| `claims_staff` | `CAN_USE` on the claims app only; no data grants (the app's service principal reads the data) |
| service principal `e2e-deployer` | owner of prod schemas; `run_as` for prod jobs and pipelines; `CAN_MANAGE` on prod objects; `READ` on `kafka_prod` |
| the app's service principal | `SELECT` on what the app shows; `CAN_QUERY` on the serving endpoint; Postgres grants in its database |

## Checking access

```sql
SHOW GRANTS ON SCHEMA e2e_prod.gold;                       -- direct grants
SHOW GRANTS `claims_analysts` ON SCHEMA e2e_prod.gold;     -- one group's
SELECT grantee, privilege_type, inherited_from             -- including inherited
FROM e2e_prod.information_schema.table_privileges
WHERE table_schema = 'gold';
SELECT event_time, user_identity.email, action_name, request_params   -- who changed grants
FROM system.access.audit
WHERE action_name ILIKE '%permission%' OR action_name ILIKE '%grant%'
ORDER BY event_time DESC LIMIT 100;
```

To test a role, sign in as a member of the group, or use a service principal in that group. Owners and admins always see everything, so testing as yourself proves nothing.

---

## AWS: the extra setup

Everything above, layers 1–7, works **the same on Databricks on AWS**: the same grants, permissions, masks, run-as rules, secret scopes and Lakebase roles. What's different is how identities arrive and how storage is reached.

### Identities: connect your identity provider yourself

AWS has no directory that Databricks reads automatically, and **automatic identity management is Azure-only**. Groups still live in your identity provider (Entra ID works on AWS too, as do Okta, AWS IAM Identity Center, Google Workspace and others). You connect it once, in the **account console**:

| Step | Where | Does |
|---|---|---|
| 1. **Single sign-on** (SAML 2.0 or OIDC) | account console → Settings → Security → Single sign-on, plus the identity provider's Databricks app | people sign in with their company account |
| 2. **SCIM provisioning** | account console → Settings → User provisioning (gives a SCIM URL and token) → the identity provider's provisioning settings | copies the assigned users, groups and memberships into the Databricks account, and keeps them in sync |
| 3. **Identity federation** | account console → Workspaces → *workspace* → Permissions | assigns the synced groups to the workspace |

With Entra ID as the identity provider: add the Databricks enterprise application from the Entra gallery, set up SAML SSO, then on its Provisioning tab paste the SCIM URL and token and assign the groups.

Differences from Azure:
- Users and groups are **copied on a schedule** (Entra's provisioning cycle, often about 40 minutes), not read live. A removed user keeps access until the next sync, so remove urgent cases by hand as well.
- **Nested groups aren't carried over** by Entra's SCIM provisioning. Flatten them, or assign the inner groups too.
- **Service principals are Databricks-managed only** (no Entra app identity). Create them in the account console or workspace settings, and give them an OAuth secret for machine-to-machine sign-in.

### Storage: IAM roles instead of managed identities

Unity Catalog reaches S3 through an **AWS IAM role** (a storage credential with a trust policy for Databricks), wrapped in external locations. It's the same model as the Azure access connector, and users still get `READ FILES` etc. on the external location, never IAM permissions on the bucket. Workspaces on **Default Storage** (like this project's Free Edition workspace) skip this entirely.

### Free Edition

Free Edition has **no account console**, so no SSO, SCIM or identity federation: it can't use your Entra groups. Groups and service principals created by hand in workspace settings are fine for **practising** grants and masks. For real users and groups, use the Azure workspace (part 7).
