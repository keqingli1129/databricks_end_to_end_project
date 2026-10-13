# Part 8: shared custom logger (design)

This design follows [databricks_logging.txt](databricks_logging.txt): extend Python's `logging` module with a custom `logging.Handler` that collects each record's fields plus notebook and job context, and stores them in a Delta table to query later. It keeps the video's idea and changes three things that don't fit this project: the write timing, the context source and the compute.

## What changes from the video

| Video | This project | Why |
|---|---|---|
| One `spark.createDataFrame(...).write` per log call | records are **buffered** and written in one batch | each write is a Spark job; serverless is slow to start and has a quota (Free Edition) |
| Logger lives in the notebook | one module in the shared package | every part of the project can import it |
| Context from `dbutils.notebook.entry_point...getContext().toJson()` | explicit values first, then `dbruntime.databricks_repl_context.get_context()`, then `NULL` | `entry_point` is often blocked (`Py4JSecurityException`) on serverless/shared compute |
| Table handler always on | table logging is **opt-in** (`enable_table_logging`) | pipelines can't write tables from a handler; local scripts have no Spark |

## Your choices

- **Who uses it (C):** notebooks/jobs (console + table), pipelines (console only), local scripts (console only), all through one `get_logger(__name__)`.
- **Table location (A):** new bundle-owned schema `ops` (dev: `dev_<user>_ops`), table `app_logs`. Callers pass the full name.
- **Flush timing (B):** an explicit `flush_logs()` at the end, plus automatic writes at 500 buffered records, immediately on ERROR/CRITICAL, and at process exit.
- **No automated tests,** checked by hand. No Claude commits. Profile `DEFAULT`, target `dev`.
- A line-by-line explanation of the logger during implementation.

## Module: `src/databricks_end_to_end_project/logging_utils.py`

Not `logging.py`: that name would shadow the standard library module.

```python
from databricks_end_to_end_project.logging_utils import get_logger, enable_table_logging, flush_logs

log = get_logger(__name__)
enable_table_logging(log_table, job_id=..., job_run_id=..., task_key=...)   # notebooks/jobs only
log.info("resized %d images", n)
flush_logs()                                                               # last cell / end of main()
```

| Function / class | Does |
|---|---|
| `get_logger(name)` | returns `logging.getLogger(name)`; on first use, gives the project logger `databricks_end_to_end_project` one console handler (`%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s`, level INFO). Repeated calls add nothing, so lines never print twice. Names outside the package are put under the project logger (`databricks_end_to_end_project.<name>`), so notebooks get the same handlers. |
| `enable_table_logging(table, **context)` | outside Databricks (no `DATABRICKS_RUNTIME_VERSION`): one warning, nothing attached. Otherwise attaches one `DeltaTableHandler` for `table` (a second call replaces it). |
| `flush_logs()` | writes the handler's buffer, if a handler is attached. |
| `DeltaTableHandler(logging.Handler)` | `emit` turns the record into a row and appends it to a buffer; flushes at 500 rows or on level ≥ ERROR. `flush` builds one DataFrame with a fixed schema and runs `saveAsTable(table, mode="append")`. No own `atexit`: `logging.shutdown()`, which Python runs at exit, already calls `flush()` on every handler, so the wheel task and scripts flush on exit. |

Spark comes from `databricks.sdk.runtime`, like the rest of the package, and is imported inside `flush`, so importing the module never starts a Spark session.

## Table `<catalog>.<ops schema>.app_logs`

Created on the first write (append mode). Columns from the video are marked ★.

| Column | Type | Source |
|---|---|---|
| `created_at` ★ | TIMESTAMP | `record.created` (when the log call happened) |
| `level` ★ | STRING | `record.levelname` |
| `logger_name` | STRING | `record.name` |
| `func_name` ★ | STRING | `record.funcName` |
| `line_no` ★ | INT | `record.lineno` |
| `message` ★ | STRING | `record.getMessage()` |
| `exception` | STRING | traceback text when `exc_info` is set (`log.exception(...)`), else `NULL` |
| `notebook_path` ★ | STRING | context |
| `job_id`, `job_run_id`, `task_key` | STRING | context |
| `session_id` | STRING | random UUID per `enable_table_logging` call, to group one run's lines |

`notebook_id` from the video is left out; the path is what identifies the code.

**Context order**, per field: the value passed to `enable_table_logging`, else `get_context()` from `dbruntime.databricks_repl_context` (inside `try`/`except`), else `NULL`. Jobs pass `{{job.id}}`, `{{job.run_id}}` and `{{task.name}}` (dynamic value references) as notebook `base_parameters`.

**Checked 2026-10-12:** `get_context()` gives `notebook_path` on serverless, so notebooks don't pass it.

## Error handling

The logger must never break the job it logs. If the table write fails (permissions, quota block, missing schema), `flush` prints one warning to stderr and **keeps the rows in the buffer**. Automatic writes (500 rows, ERROR) then stop, so a broken table doesn't cost a Spark write per log line; the next `flush_logs()` (or exit) retries them. Console logging is unaffected.

## Importing from notebooks

Notebook tasks have neither the wheel nor an editable install. A notebook under `src/<area>/` adds the parent folder to `sys.path` in its first cell:

```python
import os, sys; sys.path.insert(0, os.path.abspath(".."))
```

That is the notebook version of the pipeline fix `root_path: ../src`. Wheel tasks (`telematics_simulator`, `sample_job`) and `telematics_ingestion` already import the package.

## Files

| File | Change |
|---|---|
| `src/databricks_end_to_end_project/logging_utils.py` | new |
| `resources/unity_catalog.yml` | add schema `ops` ("Operational data - application logs") |
| `src/object_storage/claim_images.py` | `sys.path` line; logger instead of its 3 `print`s; `enable_table_logging`; `flush_logs()` at the end |
| `resources/ingest_claim_images.job.yml` | `base_parameters` `log_table`, `job_id`, `job_run_id`, `task_key` |
| `src/databricks_end_to_end_project/telematics/simulator.py` | console logger instead of its 2 summary `print`s; `--dry-run` keeps `print` (that output is the result) |
| `CLAUDE.md` | a "Logging" bullet |

Out of scope for now: `src/ml/` notebooks, pipeline transformation files, the claims app (not in the package). Each can adopt the logger later with the same few lines.

## Manual check

1. **Local, no serverless needed:** `uv run simulate --dry-run --count 2` prints as before; a one-liner calling `enable_table_logging('x.y.z')` then `get_logger('t').info('hi')` prints one console line and the "not on Databricks" warning.
2. **After the quota resets:** `databricks bundle deploy --profile DEFAULT`, `databricks bundle run ingest_claim_images --profile DEFAULT`, then `SELECT * FROM e2e_dev.dev_keqingli1129_ops.app_logs ORDER BY created_at`. Expect the notebook's lines with `job_id`/`job_run_id`/`task_key` filled, and answer the `notebook_path` question.
