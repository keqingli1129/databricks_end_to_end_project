# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Shared agent guidance lives in AGENTS.md (read the `databricks-core` skill before anything else):

@AGENTS.md

## Commands

Everything uses `uv` (Python 3.12 only, per `pyproject.toml`) and the Databricks CLI. The `DEFAULT` profile points at the bundle's workspace host. Pass `--profile <name>` explicitly, and don't pick a profile for the user.

```bash
uv sync --dev                                   # install deps (pytest, ruff, databricks-connect 18.0, databricks-dlt); versions pinned by `databricks environments setup-local`, don't hand-edit that block
uv run pytest                                   # run all tests (needs workspace auth, see below)
uv run pytest tests/sample_taxis_test.py::test_find_all_taxis   # single test
uv run ruff check . && uv run ruff format .     # lint / format (line-length 120)

databricks bundle validate --profile <p>        # check bundle config
databricks bundle deploy --profile <p>          # deploy to dev (the default target)
databricks bundle deploy -t prod --profile <p>  # deploy to prod
databricks bundle run sample_job --profile <p>  # run the job
databricks bundle run databricks_end_to_end_project_etl --profile <p>   # run the pipeline
databricks bundle run databricks_end_to_end_project_etl --refresh <table_name> --profile <p>  # refresh one dataset
databricks bundle run telematics_ingestion --profile <p>   # ingest new telematics into bronze
databricks bundle run telematics_simulator --profile <p>   # send 50 fake events (sink per var.telematics_source)
uv run simulate --dry-run --count 5                        # print fake events locally, no Databricks/Kafka
databricks bundle run seed_source_database --profile <p>   # create/fill the CDC source tables (safe to re-run)
databricks bundle run cdc_ingestion --profile <p>          # apply source inserts/updates/deletes to bronze
uv run python src/object_storage/prepare_files.py          # local: rewrite image-metadata IDs to part 2's, make schema-demo CSVs
databricks bundle run object_storage_ingestion --profile <p>   # Auto Loader: training_images, claim_images_metadata
databricks bundle run ingest_claim_images --profile <p>        # notebook: claim_images + cleanSource archive
```

## Tests run against a remote workspace

There is no local Spark. `tests/conftest.py` starts a **Databricks Connect** session in `pytest_configure`, before any test is collected. If no cluster or serverless ID is configured, it falls back to serverless (`DATABRICKS_SERVERLESS_COMPUTE_ID=auto`). So every test run needs valid CLI auth and network access. Tests read real Unity Catalog tables such as `samples.nyctaxi.trips`. Fixtures:

- `spark`: the Databricks Connect session.
- `load_fixture("name.json|csv")`: loads a file from `fixtures/` as a DataFrame.

The shell sources ROS 2 Jazzy, which sets `PYTHONPATH=/opt/ros/jazzy/...`. pytest then autoloads ROS's `launch_testing` plugin, which fails with `No module named 'yaml'`. Run tests with that variable cleared: `env -u PYTHONPATH uv run pytest`.

Library code imports `spark` from `databricks.sdk.runtime`, so the same module works in local tests, in the wheel job task and in notebooks.

## Architecture

This is a Databricks Declarative Automation Bundle (formerly Asset Bundle) built from the `default-python` template. `databricks.yml` defines the bundle, pulls in `resources/*.yml`, and builds the package as a wheel with `uv build --wheel`.

- **Targets**: `dev` is the default and uses `mode: development`. Resources get a `[dev <user>]` prefix, schedules are paused, and the schema is `${workspace.current_user.short_name}`. `prod` deploys to a fixed user root path with schema `prod`. Its daily schedule is paused (`pause_status: PAUSED` in the job YAML). The UI can't pause bundle-managed jobs, so change it there and redeploy.
- **Catalogs**: each target sets its own catalog: `dev` uses `e2e_dev` and `prod` uses `e2e_prod`, with no default. **Bundles can manage catalogs, but this project deliberately doesn't.** Create a target's catalog by hand before its first deploy, with the UI or `CREATE CATALOG`. Don't add a `catalogs` resource. There are two reasons:
  1. **The bundle can't create them here.** This workspace uses Default Storage, so creating a catalog through the REST API, which is what bundles and `databricks catalogs create` use, fails with `Metastore storage root URL does not exist`.
  2. **Keeping them unowned protects the data.** A catalog created by hand could be adopted into the bundle with `bundle deployment bind`, but then `bundle destroy` would delete it.
- **Schema**: the bundle owns it (`resources/databricks_end_to_end_project.yml`, which holds all project schemas and volumes), so `bundle destroy` deletes the schema and its tables. In `dev`, development mode renames it to `dev_<user>_<schema>`, so the job and pipeline must refer to `${resources.schemas.databricks_end_to_end_project_schema.name}`, not `${var.schema}`.
- **Shared package** (`src/databricks_end_to_end_project/`): plain Python that is built into the wheel. `main.py` is the `main` console entry point. It takes `--catalog`/`--schema` and runs `USE CATALOG/SCHEMA`.
- **Lakeflow Declarative Pipeline** (`src/databricks_end_to_end_project_etl/`): serverless, defined in `resources/databricks_end_to_end_project_etl.pipeline.yml`. Every file under `transformations/**` is loaded by a glob, so a new dataset file is picked up without changing any config. Convention: one dataset per file, using `from pyspark import pipelines as dp` with `@dp.table`. Datasets refer to each other by bare table name, resolved in the pipeline's catalog and schema. The pipeline YAML installs the project with `--editable ${workspace.file_path}`, **but that alone does not make `import databricks_end_to_end_project` work inside a pipeline**: the editable install's `.pth` file is never processed there, so `src/` is not on `sys.path`. The pipeline does put its `root_path` on `sys.path`, so any pipeline whose code imports the shared package needs `root_path: "../src"` (see `telematics_ingestion`). This template pipeline still has the narrower root path and works only because its transformations don't import the package. Because pipeline dependencies are cached during development, add pipeline-only libraries to the pipeline YAML's `environment`, not to `pyproject.toml`.
- **Job** (`resources/sample_job.job.yml`): runs once a day. `notebook_task` (`src/sample_notebook.ipynb`) runs first. Then two tasks run in parallel: `python_wheel_task`, which calls `main` from `../dist/*.whl`, and `refresh_pipeline`, which runs the pipeline.
- **Medallion schemas and landing volume** (`resources/databricks_end_to_end_project.yml`): schemas `landing`, `bronze`, `silver`, `gold` (dev names them `dev_<user>_<layer>`; reference as `${resources.schemas.<layer>.name}`) and a managed volume `files` in landing. The bundle can create schemas and volumes inside the hand-made catalogs.
- **Telematics ingestion** (design and step-by-step plan in `docs/part1_kafka_ingestion_*.md`, source material `docs/transcript_1.txt`): pipeline `telematics_ingestion` (`resources/telematics_ingestion.pipeline.yml`, serverless, default schema bronze, `root_path: ../src`) with streaming tables `telematics_raw` → `telematics`. `var.telematics_source` picks the source for both the pipeline (`telematics.source` config) and the simulator job (`--sink`):
  - `files` (default): simulator JSON files in `/Volumes/<catalog>/<landing schema>/files/telematics/`, read with Auto Loader and reshaped into Kafka's columns.
  - `kafka`: Confluent Cloud via SASL_SSL/PLAIN. Credentials live in the bundle-defined secret scope `kafka_<target>` (keys `bootstrap_servers`, `api_key`, `api_secret`; set with `databricks secrets put-secret`; currently placeholders). Switching source requires a full refresh of `telematics_raw`.
  - Both sources produce the same columns, so `parse_telematics` (bronze keeps every field as a string; typing belongs in silver) works unchanged.
- **Telematics package** (`src/databricks_end_to_end_project/telematics/`): `events.py` (fake events), `kafka_config.py` (Spark and producer settings, secret reading), `parsing.py`, `simulator.py` (the `simulate` console script: `--dry-run`, `--sink files --landing-path …`, or `--sink kafka`). Tests exist only for `events` and `kafka_config`; the user chose to skip tests for the rest.
- **CDC ingestion** (design and step-by-step plan in `docs/part2_cdc_ingestion_*.md`, source material `docs/transcript_2.txt`). The transcript uses Lakeflow Connect from SQL Server, which can't run here: there's no reachable database, and its ingestion gateway needs classic compute, which Free Edition lacks. The stand-in:
  - **Source:** schema `source` (dev: `dev_<user>_source`) holds `customer` (key `customer_id`), `policy` (`policy_no`) and `claim` (`claim_no`), all with Change Data Feed on. Policies `POL0000001`–`10` use the part 1 telematics chassis numbers `CHS000001`–`10`.
  - **Seeding:** the job `seed_source_database` is a SQL task on `var.warehouse_id` (a `lookup` of "Serverless Starter Warehouse"). It runs `src/source_database/seed.sql`, which reads the `:catalog`/`:schema` named parameters, creates tables only if missing, and fills them only while empty.
  - **Never `DROP` or `CREATE OR REPLACE` the source tables.** That breaks the change feed; the fix would be a full refresh of `cdc_ingestion`.
  - **Pipeline:** `cdc_ingestion` (`resources/cdc_ingestion.pipeline.yml`, bronze, config `cdc.source_schema`) has one file per table in `src/cdc_ingestion/transformations/`. Each is a `@dp.temporary_view` over `readChangeFeed` (minus `update_preimage`), then `dp.create_streaming_table` and `dp.create_auto_cdc_flow` (SCD Type 1, `sequence_by="_commit_version"`, deletes applied, change-feed columns excluded).
  - **Test:** `src/source_database/changes.sql` holds the insert/update/delete test for dev.
- **Object-storage ingestion** (design and step-by-step plan in `docs/part3_object_storage_*.md`, source material `docs/transcript_3.txt`):
  - **Volumes:** managed volumes in landing, `claims` (`images/`, `metadata/`, `archive/`, `_autoloader/`) and `training_images`. Their resource keys are `claims_volume` and `training_images_volume`.
  - **Raw files:** these live locally in `data/object_storage/`, which is gitignored and must never be committed (about 112 MB of images).
  - **The prepare script:** `src/object_storage/prepare_files.py` rewrites the metadata CSV's UUID `claim_no` and VIN `chassis_no` to part 2's `CLM%08d(n)` and `CHS%06d((n*7919)%12000+1)`. It writes the result, plus the schema-evolution demo CSVs, into `data/object_storage/prepared/`. Upload with `databricks fs cp` (paths need the `dbfs:/Volumes/...` prefix; a single-file `cp` needs the target folder to exist).
  - **Pipeline `object_storage_ingestion` (bronze):** `training_images` (cloudFiles `binaryFile`, 56 rows) and `claim_images_metadata` (cloudFiles csv, now `schemaEvolutionMode=rescue`: unknown columns go into `_rescued_data`; `new_column_1` exists from the earlier `addNewColumns` demo). Under `addNewColumns`, a new column makes `bundle run` print `Error: update cancelled`; the pipeline then starts a fresh update itself, with cause `SCHEMA_CHANGE`.
  - **Job `ingest_claim_images`:** runs the plain-PySpark notebook `src/object_storage/claim_images.py` (Databricks `.py` notebook source; widgets overridden by `base_parameters`). It reads cloudFiles `binaryFile` with a hand-set checkpoint and schema location under `claims/_autoloader/claim_images`, uses `trigger(availableNow=True)`, and writes to `bronze.claim_images`, a regular Delta table that no pipeline owns.
  - **Archiving:** `cleanSource=MOVE` to `claims/archive/`, with a 1-minute retention. Cleanup only happens during runs that have new files. A file moves once it was committed in an earlier run and is older than the retention, and only a limited batch moves per run. So expect files to move over the next one or two runs, not immediately.
- `explorations/` notebooks are gitignored, so treat them as scratch work.
