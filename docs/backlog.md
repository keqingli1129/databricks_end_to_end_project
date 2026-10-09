# Backlog: optional improvements (not applied)

Ideas that came up during the project walkthrough (2026-10-06 to 2026-10-08). None of them is needed for the current dev workspace. Each one has the reason, the code, and when it's worth doing.

---

## 1. Create the volume folders in code (`dbutils.fs.mkdirs`)

**Problem:** some folders in the `claims` volume were created by hand in part 3 (`databricks fs mkdir`, see [part3_object_storage_plan.md](part3_object_storage_plan.md) lines 175 and 177). Auto Loader *reads* from `images/` and `metadata/` and fails with "path does not exist" if a folder is missing. So a fresh workspace needs manual steps before the first run.

The bundle can't create folders inside a volume. It only creates the volume. Writers (the simulator, the app, Spark checkpoints) create their own folders automatically. Only **readers** need a folder to exist in advance.

**Fix:** create the folders in the reading code. `mkdirs` is idempotent: it does nothing if the folder exists, and it never deletes anything.

**a) Notebook [src/object_storage/claim_images.py](../src/object_storage/claim_images.py)**: add this after the paths are defined (after `METADATA_PATH = ...`):

```python
# Create the folders this notebook reads from or moves into. mkdirs is a no-op if they already exist,
# so a fresh workspace needs no manual `databricks fs mkdir`.
for path in (SOURCE_PATH, ARCHIVE_PATH):
    dbutils.fs.mkdirs(path)
```

`METADATA_PATH` is left out because Spark creates the checkpoint folder itself.

**b) Pipeline file [src/object_storage_ingestion/transformations/claim_images_metadata.py](../src/object_storage_ingestion/transformations/claim_images_metadata.py)**: `dbutils` isn't injected into pipeline files, so import it (as `telematics_raw.py` does). Call `mkdirs` at **module level**, never inside the `@dp.table` function, because the engine decides when and how often it calls that function:

```python
from databricks.sdk.runtime import dbutils
from pyspark import pipelines as dp

SOURCE_PATH = spark.conf.get("object_storage.claim_metadata_path")

# Auto Loader needs the folder to exist; create it (no-op if present) so a fresh workspace works without setup.
dbutils.fs.mkdirs(SOURCE_PATH)
```

After applying it, mark the `fs mkdir` lines in the part 3 plan as optional.

**When:** before deploying to a new workspace, i.e. **part 7 (Azure)**.

---

## 2. Job with a file-arrival trigger

**Problem:** nothing watches the volume folders. Auto Loader only looks for new files while its stream runs, so new files wait until someone runs `bundle run` (or the paused hourly schedule of `smart_claims_end_to_end` is started).

**Fix:** a job whose `trigger.file_arrival` watches a folder. Databricks checks the path about once a minute and starts the job when new files land. It uses no compute while idle. A pipeline can't have a trigger of its own, so the job wraps it in a `pipeline_task`.

New file `resources/ingest_files_on_arrival.job.yml`:

```yaml
resources:
  jobs:
    ingest_files_on_arrival:
      name: ingest_files_on_arrival

      trigger:
        file_arrival:
          # Watch ONE folder. The trailing / means "anything inside this folder".
          url: /Volumes/${var.catalog}/${resources.schemas.landing.name}/${resources.volumes.claims_volume.name}/metadata/
          min_time_between_triggers_seconds: 300   # at most one run every 5 min
          wait_after_last_change_seconds: 60       # wait until uploads go quiet for 1 min

      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.object_storage_ingestion.id}
```

**Watch out:**

- **Watch a narrow folder, never the whole `claims/` volume.** The ingestion writes into `claims/` itself: the checkpoint in `_autoloader/` and the `cleanSource` moves into `archive/`. Those writes would look like new files and re-trigger the job endlessly.
- One trigger watches one path. Watching both `images/` and `metadata/` needs two jobs, or a parent folder that holds only incoming files.
- **Development mode deploys triggers paused**, as it does schedules. It's mainly useful in prod, or in a test where you unpause it on purpose.

**Compared with the alternatives:**

| | Reacts to new files | Compute when idle | Delay |
|---|---|---|---|
| Manual run / paused schedule (today) | no | none | until you run it |
| File-arrival trigger | yes | none | about 1–2 min + start-up |
| Continuous pipeline (`continuous: true`, dropped in dev) | yes | 24/7 | seconds |

**When:** to see automatic triggering in action, or for a prod deployment.
