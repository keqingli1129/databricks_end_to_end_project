# Part 2: change data capture (CDC) from a source database into bronze (design)

This design follows [transcript_2.txt](transcript_2.txt): ingest every **insert, update and delete** from a relational database into bronze streaming tables with Lakeflow Connect for SQL Server.

## Why not Lakeflow Connect itself

- **No database the cloud can reach:** there's no SQL Server or other database reachable from Databricks' cloud. This is the same reason Kafka isn't used in part 1.
- **No classic compute:** Lakeflow Connect's **ingestion gateway always runs on classic compute** ("currently not available in serverless"). This Free Edition workspace only has serverless.

So the source database and its change log are replaced with what Databricks has built in. The **ingestion side is the real thing**: AUTO CDC, the same mechanism the Lakeflow Connect ingestion pipeline uses to apply upserts and deletes.

## Constraints

- **Existing catalogs only** (`e2e_dev` / `e2e_prod`). Add only a schema.
- **No automated tests.** You chose this for part 2. The checks are validate, deploy, row counts, and the insert/update/delete test.
- **No git commits** by Claude. You commit yourself.
- **Profile `DEFAULT`, target `dev`.**

## Architecture

```text
e2e_dev.dev_keqingli1129_source          (stands in for the SQL Server)
  customer · policy · claim   ← Delta tables, Change Data Feed ON
        ▲ seeded once by job `seed_source_database` (SQL task on "Serverless Starter Warehouse")
        ▲ INSERT / UPDATE / DELETE by hand in the SQL editor (plays DataGrip's role)
        │  change feed: each change + _change_type + _commit_version
        ▼
pipeline `cdc_ingestion`  (serverless, triggered, default schema bronze)
  per table: temporary view (change feed minus update_preimage) → AUTO CDC, SCD Type 1, deletes applied
        ▼
e2e_dev.dev_keqingli1129_bronze.customer · policy · claim   (streaming tables next to telematics)
```

| Transcript | Here |
|---|---|
| SQL Server (RDS) with 3 tables | schema `source` with 3 Delta tables |
| enable change tracking + CDC on the database and tables, DDL-capture script | `TBLPROPERTIES (delta.enableChangeDataFeed = true)` |
| Unity Catalog connection (host, port, user, password) | not needed, because the source is in the same workspace |
| ingestion gateway (classic VM) → staging volume in landing | the Delta change feed, built in, with no VM |
| ingestion pipeline: upserts and deletes into bronze | `cdc_ingestion` with `dp.create_auto_cdc_flow` |
| first run = snapshot | a streaming change-feed read with no start version returns the current snapshot as inserts, then later changes |
| test: INSERT policy, UPDATE claim severity, DELETE customer | the same, in `src/source_database/changes.sql` |

## Source tables

| Table (key) | Rows | Columns |
|---|---|---|
| `customer` (`customer_id`) | 7,000 | `customer_id`, `first_name`, `last_name`, `date_of_birth`, `email`, `zip_code` |
| `policy` (`policy_no`) | 12,000 | `policy_no`, `customer_id`, `chassis_number`, `make`, `model`, `model_year`, `coverage`, `premium`, `deductible`, `start_date`, `end_date` |
| `claim` (`claim_no`) | 13,000 | `claim_no`, `policy_no`, `incident_date`, `incident_type`, `incident_severity`, `claim_amount`, `updated_at` |

- **Key formats:** `customer_id` is `C000001` and so on, `policy_no` is `POL0000001`, and `claim_no` is `CLM00000001`.
- **Chassis numbers** are `CHS%06d` from the policy number, so `POL0000001`–`POL0000010` belong to the part 1 telematics cars `CHS000001`–`CHS000010`.
- **Every customer has at least one policy** (`customer_id` = `(n − 1) % 7000 + 1`).
- **`incident_severity`** uses the transcript's values: Trivial Damage, Minor Damage, Major Damage and Total Loss.

## Units

| File | Responsibility |
|---|---|
| `resources/databricks_end_to_end_project.yml` | adds the schema resource `source` |
| `databricks.yml` | adds the variable `warehouse_id`, with `lookup: warehouse: Serverless Starter Warehouse` |
| `src/source_database/seed.sql` | `USE CATALOG/SCHEMA IDENTIFIER(:catalog/:schema)`. Creates the 3 tables **if missing** with Change Data Feed on, and fills each **only while it's empty**. So it's safe to re-run. It never drops or replaces a table, because that would break the change feed. |
| `resources/seed_source_database.job.yml` | one `sql_task` running `seed.sql` on `${var.warehouse_id}`, with the parameters `catalog` and `schema` |
| `resources/cdc_ingestion.pipeline.yml` | serverless and triggered, with default schema bronze, `root_path: ../src`, and configuration `cdc.source_schema` |
| `src/cdc_ingestion/transformations/{customer,policy,claim}.py` | one dataset per file: a temporary view reads the change feed and filters out `update_preimage`. Then `dp.create_streaming_table` and `dp.create_auto_cdc_flow` with keys = the table's key, `sequence_by="_commit_version"`, `apply_as_deletes = _change_type = 'delete'`, `except_column_list` = the 3 change-feed columns, and SCD Type 1. |
| `src/source_database/changes.sql` | the transcript's check queries, the 3 changes, then the same checks again |

## Error handling

- **Re-running the seed** is harmless: the tables aren't recreated and the data isn't doubled.
- **Recreating a source table** (dropping it, or `CREATE OR REPLACE`) breaks the streaming change-feed read. The fix is a full refresh of `cdc_ingestion`. The seed script avoids this by design.
- **Deletes** arrive as `_change_type = 'delete'` and remove the key's row in bronze (SCD Type 1).
- **Updates** appear in the feed twice, as `update_preimage` and `update_postimage`. Only the postimage is applied.
