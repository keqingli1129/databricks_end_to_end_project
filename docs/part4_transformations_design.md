# Part 4: silver and gold transformations, data quality, and orchestration (design)

This design follows [transcript_4.txt](transcript_4.txt):

- **Silver:** clean every bronze table, with built-in **data quality checks** (expectations).
- **Gold:** aggregate and pre-join into **materialized views**.
- **Libraries:** add a Python library to a pipeline's environment.
- **Lineage:** check it in Unity Catalog.
- **Orchestration:** run ingestion and transformations end to end with a **Lakeflow Job** on a schedule.

All of it can be built in Free Edition: it's serverless only, and it adds tables only to the existing `silver` and `gold` schemas.

## Constraints and decisions

- **Existing catalogs and schemas:** dev silver is `e2e_dev.dev_keqingli1129_silver`, and gold is `…_gold`.
- **No automated tests** and no Claude commits. Profile `DEFAULT`, target `dev`.
- **Code layout, as in the transcript (option A):** one pipeline `transformations` with **two files**, `bronze_to_silver.py` and `silver_to_gold.py`.
- **Extra library (option A):** `geopy`, used **offline** (`geodesic` distance), so no internet is needed. The lesson is a first failing run, then adding it to the pipeline's `environment`.
- **Cleaning adapted to our data:** our bronze differs from the video's. Claims already have dates, and customers already have first and last names. But the telematics are all strings, and the image labels sit in file paths. So each table's cleaning is chosen to fit **our** data.
- **Silver reads of the CDC tables are batch reads (materialized views).** `bronze.customer`, `policy` and `claim` are AUTO CDC targets with **updates and deletes** (part 2's test). A streaming read of such a table fails at the first update or delete. A materialized view applies them correctly and, on serverless, refreshes incrementally. Append-only bronze tables stay streaming.

## Architecture

```text
bronze (parts 1–3)                        pipeline transformations (serverless, default schema silver, root_path ../src)
  claim, policy, customer  (AUTO CDC) ──► bronze_to_silver.py: silver.claim / policy / customer          (materialized views)
  telematics, training_images,        ──►                      silver.telematics / training_images /     (streaming tables)
  claim_images, claim_images_metadata                          claim_images / claim_images_metadata
                                           silver_to_gold.py:  gold.aggregated_telematics  (+ geopy)       (materialized views)
                                                               gold.customer_claim_policy
                                                               gold.customer_claim_policy_telematics
job smart_claims_end_to_end (hourly schedule, PAUSED):
  ingest_telematics ┐
  ingest_cdc        ├──(all succeeded)──► transform (pipeline transformations)
  ingest_object_storage │
  ingest_claim_images ┘   (run_job_task → the existing ingest_claim_images job)
```

## Silver (`src/transformations/transformations/bronze_to_silver.py`)

Each table has two checks, using `@dp.expect_all_or_drop`, as in the transcript.

| Silver table | Kind | Checks (bad rows dropped) | Cleaning |
|---|---|---|---|
| `claim` | MV | `claim_no IS NOT NULL`; `claim_amount >= 0` | adds `severity_level`: Trivial Damage 1, Minor Damage 2, Major Damage 3, Total Loss 4 |
| `policy` | MV | `policy_no IS NOT NULL`; `end_date > start_date` | `premium = abs(premium)` |
| `customer` | MV | `customer_id IS NOT NULL`; `email LIKE '%@%'` | adds `full_name = first + ' ' + last`, and `age` in whole years |
| `telematics` | ST | `chassis_number IS NOT NULL`; `speed BETWEEN 0 AND 250` | casts `speed`, `latitude` and `longitude` to double and `event_timestamp` to timestamp; `ingested_at = stream_metadata.timestamp`; drops `raw_json` and `stream_metadata` |
| `training_images` | ST | `label IN ('ok','minor','major')`; `length > 0` | `file_name`, plus `label` extracted from the file name |
| `claim_images` | ST | `image_name IS NOT NULL`; `length > 0` | `image_name` (the file name from `path`) |
| `claim_images_metadata` | ST | `claim_no IS NOT NULL`; `image_id IS NOT NULL` | `image_id` cast to int; drops `_rescued_data` and `new_column_1` |

- **Where bronze is read:** by its full name, from configuration `transformations.bronze_schema` = `e2e_dev.dev_keqingli1129_bronze`. Bronze belongs to other pipelines.
- **Where silver goes:** silver tables use bare names, so they land in the default schema.
- **Duplicate demo rows:** the two schema-demo rows from part 3 repeat claims 1 and 2 in `claim_images_metadata`. Silver keeps them, because removing duplicates from a stream needs a watermark, and gold doesn't use this table.

## Gold (`src/transformations/transformations/silver_to_gold.py`)

All three are materialized views, published with full names `${gold}.<table>`, from configuration `transformations.gold_schema`, and with table property `quality=gold`.

| Gold table | Logic |
|---|---|
| `aggregated_telematics` | `silver.telematics` grouped by `chassis_number`: `avg_speed`, `max_speed`, `avg_latitude`, `avg_longitude`, `event_count`, `first_event_at`, `last_event_at`, plus `distance_from_city_center_km`, a Python UDF using `geopy.distance.geodesic` to downtown Houston (29.7604, −95.3698) |
| `customer_claim_policy` | `silver.claim ⋈ silver.policy` on `policy_no`, then `⋈ silver.customer` on `customer_id` (inner joins: the deleted customer `C007000`'s claims drop out) |
| `customer_claim_policy_telematics` | `customer_claim_policy` **left join** `aggregated_telematics` on `chassis_number`. About 11 claims, on policies `POL0000001`–`10`, get telematics, and the rest get NULLs. |

## Units

| File | Responsibility |
|---|---|
| `resources/transformations.pipeline.yml` | pipeline `transformations`: catalog, schema `${resources.schemas.silver.name}`, serverless, `root_path: ../src`, configuration with the bronze and gold full schema names, and a glob over `src/transformations/transformations/**`. `environment.dependencies: [geopy==2.4.1]` is added in the plan **after** the deliberate failure. |
| `src/transformations/transformations/bronze_to_silver.py` | the 7 silver datasets |
| `src/transformations/transformations/silver_to_gold.py` | the 3 gold MVs, and a module-level `from geopy.distance import geodesic` (that import causes the deliberate failure) |
| `resources/smart_claims_end_to_end.job.yml` | 4 parallel ingestion tasks (3 `pipeline_task`, 1 `run_job_task`), then `transform` (`pipeline_task`, `depends_on` all 4, `run_if: ALL_SUCCESS`); `schedule` hourly with Quartz `0 0 * * * ?`, UTC, `pause_status: PAUSED` |

## Error handling and expected behaviour

- **The geopy failure:** the first gold run fails with `ModuleNotFoundError: No module named 'geopy'`. This is planned. After `geopy` is added to `environment`, the next run succeeds.
- **All checks pass on the current data** (100%, as in the video). An optional plan step shows a check **dropping** a row: a claim with a negative amount is inserted at the source and flows through `cdc_ingestion`.
- **The end-to-end job** uses serverless compute for every task. The schedule stays **PAUSED**, and runs are started by hand.
