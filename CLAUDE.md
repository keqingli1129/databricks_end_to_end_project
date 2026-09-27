# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Shared agent guidance lives in AGENTS.md (read the `databricks-core` skill before anything else):

@AGENTS.md

## Commands

Everything uses `uv` (Python 3.12 only, per `pyproject.toml`) and the Databricks CLI. The `DEFAULT` and `FreeEdition` profiles both point at the bundle's workspace host. Don't pick one for the user; pass `--profile <name>` explicitly.

```bash
uv sync --dev                                   # install deps (pytest, ruff, databricks-connect 16.4, databricks-dlt)
uv run pytest                                   # run all tests (needs workspace auth, see below)
uv run pytest tests/sample_taxis_test.py::test_find_all_taxis   # single test
uv run ruff check . && uv run ruff format .     # lint / format (line-length 120)

databricks bundle validate --profile <p>        # check bundle config
databricks bundle deploy --profile <p>          # deploy to dev (the default target)
databricks bundle deploy -t prod --profile <p>  # deploy to prod
databricks bundle run sample_job --profile <p>  # run the job
databricks bundle run databricks_end_to_end_project_etl --profile <p>   # run the pipeline
databricks bundle run databricks_end_to_end_project_etl --refresh <table_name> --profile <p>  # refresh one dataset
```

## Tests run against a remote workspace

There is no local Spark. `tests/conftest.py` starts a **Databricks Connect** session in `pytest_configure`, before any test is collected. If no cluster or serverless ID is configured, it falls back to serverless (`DATABRICKS_SERVERLESS_COMPUTE_ID=auto`). So every test run needs valid CLI auth and network access. Tests read real Unity Catalog tables such as `samples.nyctaxi.trips`. Fixtures:

- `spark`: the Databricks Connect session.
- `load_fixture("name.json|csv")`: loads a file from `fixtures/` as a DataFrame.

The shell sources ROS 2 Jazzy, which sets `PYTHONPATH=/opt/ros/jazzy/...`. pytest then autoloads ROS's `launch_testing` plugin, which fails with `No module named 'yaml'`. Run tests with that variable cleared: `env -u PYTHONPATH uv run pytest`.

Library code imports `spark` from `databricks.sdk.runtime`, so the same module works in local tests, in the wheel job task and in notebooks.

## Architecture

This is a Databricks Declarative Automation Bundle (formerly Asset Bundle) built from the `default-python` template. `databricks.yml` defines the bundle, pulls in `resources/*.yml`, and builds the package as a wheel with `uv build --wheel`.

- **Targets**: `dev` is the default and uses `mode: development`. Resources get a `[dev <user>]` prefix, schedules are paused, and the schema is `${workspace.current_user.short_name}`. `prod` deploys to a fixed user root path with schema `prod`. Both targets use catalog `workspace`. Resources read the catalog and schema through `${var.catalog}` / `${var.schema}`, never hardcoded names.
- **Shared package** (`src/databricks_end_to_end_project/`): plain Python that is built into the wheel. `main.py` is the `main` console entry point. It takes `--catalog`/`--schema` and runs `USE CATALOG/SCHEMA`.
- **Lakeflow Declarative Pipeline** (`src/databricks_end_to_end_project_etl/`): serverless, defined in `resources/databricks_end_to_end_project_etl.pipeline.yml`. Every file under `transformations/**` is loaded by a glob, so a new dataset file is picked up without changing any config. Convention: one dataset per file, using `from pyspark import pipelines as dp` with `@dp.table`. Datasets refer to each other by bare table name, resolved in the pipeline's catalog and schema. The pipeline gets project dependencies through `--editable ${workspace.file_path}`. Because pipeline dependencies are cached during development, add pipeline-only libraries to the pipeline YAML's `environment`, not to `pyproject.toml`.
- **Job** (`resources/sample_job.job.yml`): runs once a day. `notebook_task` (`src/sample_notebook.ipynb`) runs first. Then two tasks run in parallel: `python_wheel_task`, which calls `main` from `../dist/*.whl`, and `refresh_pipeline`, which runs the pipeline.
- `explorations/` notebooks are gitignored, so treat them as scratch work.
