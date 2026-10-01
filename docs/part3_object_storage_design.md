# Part 3: incremental file ingestion from object storage with Auto Loader (design)

This design follows [transcript_3.txt](transcript_3.txt): ingest files from object storage into bronze with **Auto Loader**, show **schema evolution** on CSV, and **archive** processed files with `cloudFiles.cleanSource`.

Unlike parts 1 and 2, this can all be built in Free Edition. **Managed volumes** replace the transcript's S3-backed volumes, and nothing outside Databricks is needed.

## Constraints

- **Existing catalogs only:** add only volumes, in the existing landing schema.
- **No automated tests**, as in part 2. The checks are validate, deploy, row counts and looking at the files.
- **No git commits by Claude**, and the images stay out of git (`data/` is gitignored).
- **Profile `DEFAULT`, target `dev`.**

## Your files (staged in `data/object_storage/`)

| Folder | Content |
|---|---|
| `training_images/` | 56 PNGs, 1024×1024, with the label in the file name: 17 `ok`, 21 `minor`, 18 `major` |
| `claims/images/` | 15 photos: 5 cars × `High` / `Low` / `Medium`. Named `.jpg`, but PNG inside. |
| `claims/metadata/image_metadata.csv` | 13,001 rows: `image_name, image_id, claim_no, chassis_no`. The original IDs are UUIDs and VINs. |

**IDs are rewritten to match part 2** (your option A), so that later parts can join claims, images, policies and telematics. Row *n* (1…13,000) gets:

- **`claim_no`:** `CLM%08d(n)`, part 2's claim *n*.
- **`chassis_no`:** that claim's policy's chassis, using part 2's formula: `POL = (n × 7919) % 12000 + 1`, then `CHS%06d` with the same number.
- **The rest:** `image_name` and `image_id` stay as they are, and row 13,001 is dropped.

The original CSV is left untouched. The rewritten copy goes to `data/object_storage/prepared/`.

## Architecture

```text
data/object_storage/ (laptop, gitignored) ── prepare_files.py ── databricks fs cp ──┐
                                                                                  ▼
e2e_dev.dev_keqingli1129_landing
  training_images/                 (volume) 56 PNGs
  claims/                          (volume)
    images/                        15 photos (+2 extra uploaded during the archive demo)
    metadata/                      image_metadata.csv (+ the 2 schema-demo CSVs, uploaded in the UI)
    archive/                       ← cleanSource MOVE destination
    _autoloader/claim_images/      ← checkpoint + schema location (set by hand in the notebook)
        │
        ├─ pipeline object_storage_ingestion (serverless, default schema bronze)
        │    training_images         ← cloudFiles binaryFile                       (transcript table 1)
        │    claim_images_metadata   ← cloudFiles csv; addNewColumns, then rescue  (transcript table 2)
        └─ job ingest_claim_images → notebook src/object_storage/claim_images.py
             claim_images            ← cloudFiles binaryFile + cleanSource MOVE, availableNow  (transcript table 3)
```

| Transcript | Here |
|---|---|
| S3-backed volumes `claims` and `training_images` in landing | managed volumes with the same names in landing |
| minimal Auto Loader on images, 56 records | `training_images.py`, 56 PNGs |
| CSV metadata; `addNewColumns` (default) adds a new column, null for old rows | `claim_images_metadata.py`, then upload `…_new_column_1.csv` |
| switch to `rescue`; an unknown column goes into `_rescued_data` | edit one option, then upload `…_new_column_2.csv` |
| plain PySpark notebook: `cleanSource` MOVE, 1-minute retention, archive folder, `availableNow` | `claim_images.py`, run by the job `ingest_claim_images` |
| "sometimes you need to re-execute" | three runs: the docs say the earliest cleanup is run N+2, and cleanup only happens while there are new files |

## Units

| File | Responsibility |
|---|---|
| `.gitignore` | adds `data/` |
| `src/object_storage/prepare_files.py` | runs locally with plain Python (stdlib only). It writes `data/object_storage/prepared/claims_metadata/image_metadata.csv` (with the IDs rewritten) and `prepared/schema_evolution/image_metadata_new_column_1.csv` and `…_new_column_2.csv`. The demo files have one row each, copied from rows 1 and 2, plus the extra columns. |
| `resources/databricks_end_to_end_project.yml` | adds the volume resources `claims_volume` (name `claims`) and `training_images_volume` (name `training_images`) |
| `resources/object_storage_ingestion.pipeline.yml` | serverless, bronze, `root_path: ../src`, with configuration `object_storage.training_images_path` and `object_storage.claim_metadata_path` |
| `src/object_storage_ingestion/transformations/training_images.py` | `@dp.table`: `readStream.format("cloudFiles")` with `cloudFiles.format=binaryFile`, and table property `quality=bronze` |
| `src/object_storage_ingestion/transformations/claim_images_metadata.py` | `@dp.table`: `cloudFiles` CSV with `header=true` and an explicit `cloudFiles.schemaEvolutionMode` (`addNewColumns`, later `rescue`) |
| `src/object_storage/claim_images.py` | a notebook (Databricks source format) with widgets `claims_volume_path` and `target_table`. It sets `cleanSource` = MOVE, `retentionDuration` = `1 minute` and `moveDestination` = `…/claims/archive`, plus a checkpoint and schema location under `…/claims/_autoloader/claim_images`. It runs with `trigger(availableNow=True)` and `.toTable(...)`. |
| `resources/ingest_claim_images.job.yml` | one notebook task on serverless, with `base_parameters` for the volume path and the target table |

## Rules from the Auto Loader docs

- **`cleanSource`** needs DBR 16.4+, which serverless meets. Its values are `OFF` / `DELETE` / `MOVE`. MOVE has **no minimum retention** (DELETE requires more than 7 days).
- **The move destination** must be in the **same volume** as the source, must **not be inside the source directory** (otherwise archived files are ingested again), and needs write permission. `claims/archive` next to `claims/images` meets all three.
- **Cleanup timing:** cleanup happens **only while a batch of files is being processed**, and the earliest a file can be cleaned is **run N+2** after it was ingested.
- **In a pipeline,** checkpoint and schema locations are managed automatically. In the notebook they're set explicitly, which is the transcript's point about plain PySpark.
- **`schemaEvolutionMode`:** the default is `addNewColumns`, and `rescue` puts unknown columns into `_rescued_data`. With `addNewColumns`, a new column can make the flow restart once. The pipeline retries, so the update still completes.

## Error handling

- **Re-running the pipeline or job** only processes new files (Auto Loader's checkpoint).
- **The demo CSVs duplicate a claim on purpose**, rows 1 and 2 with extra columns. Bronze keeps data as it arrived. Duplicates are a silver-layer concern.
- **If files don't move to `archive/` after run 3,** check that the retention (1 minute) has passed and that the run had at least one new file. If both are true, run once more with another new file.
