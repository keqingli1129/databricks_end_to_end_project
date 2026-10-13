# How to use the shared logger

The module is `src/databricks_end_to_end_project/logging_utils.py`. Every line goes to the **console** (stderr). In notebooks and jobs, lines can **also** go to a Delta table, `<catalog>.<ops schema>.app_logs`. Background: [part8_logging_design.md](part8_logging_design.md), source material [databricks_logging.txt](databricks_logging.txt).

## The three functions

| Function | Use it | Does |
|---|---|---|
| `get_logger(name)` | everywhere | returns a logger under `databricks_end_to_end_project`, with console output `time LEVEL name:line message`, level INFO |
| `enable_table_logging(table, **context)` | once, near the top of a notebook or job | also buffers every project log line for `table`. Off by itself (one warning) when not on Databricks |
| `flush_logs()` | at the end of a notebook or job | writes the buffered lines to the table |

Lines are written to the table automatically when **500** are buffered, **at once on ERROR/CRITICAL**, and when the Python process exits. A notebook's process doesn't exit after its last cell, so **notebooks must call `flush_logs()`**.

## Write log calls like this

```python
log.info("rows in target: %d", n)        # yes: %-style arguments
log.info(f"rows in target: {n}")         # works, but the text is built even when nobody uses it
log.warning("no files in %s", path)
try:
    ...
except Exception:
    log.exception("ingestion failed")    # ERROR + traceback in the `exception` column, written at once
    raise                                # keep failing the job; logging doesn't swallow errors
```

`log.debug(...)` lines are dropped (the level is INFO).

## In package code (`src/databricks_end_to_end_project/...`)

```python
from databricks_end_to_end_project.logging_utils import get_logger

log = get_logger(__name__)   # e.g. databricks_end_to_end_project.telematics.simulator
```

Console output only. Don't call `enable_table_logging` in library code; the notebook or job that uses it decides.

## In a pipeline (`dp` transformation files)

Same as package code: `get_logger(__name__)`, console only, visible in the pipeline's driver logs. **Never** call `enable_table_logging` in a pipeline: a pipeline can't write tables from a log handler. The pipeline must have `root_path: ../src` to import the package (see `telematics_ingestion`).

## In a notebook job task (console + table)

Notebook tasks don't have the package installed. Copy the pattern from `src/object_storage/claim_images.py`:

**1. First code cell: make the package importable.**

```python
import os
import sys

sys.path.insert(0, os.path.abspath(".."))  # notebook in src/<area>/, so ".." is src/

from databricks_end_to_end_project.logging_utils import enable_table_logging, flush_logs, get_logger
```

For a notebook deeper than `src/<area>/`, use as many `..` as it takes to reach `src/`.

**2. Widgets cell: log table and job context.**

```python
dbutils.widgets.text("log_table", "e2e_dev.dev_keqingli1129_ops.app_logs")
dbutils.widgets.text("job_id", "")  # filled by the job; empty when run by hand
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("task_key", "")

log = get_logger("<area>.<notebook name>")   # not __name__: in a notebook that is just "__main__"
enable_table_logging(
    dbutils.widgets.get("log_table"),
    job_id=dbutils.widgets.get("job_id"),
    job_run_id=dbutils.widgets.get("job_run_id"),
    task_key=dbutils.widgets.get("task_key"),
)
```

**3. Last cell:**

```python
flush_logs()
```

**4. In the job YAML,** add to the task's `base_parameters`:

```yaml
              log_table: ${var.catalog}.${resources.schemas.ops.name}.app_logs
              job_id: "{{job.id}}"
              job_run_id: "{{job.run_id}}"
              task_key: "{{task.name}}"
```

`${...}` is filled by the bundle at deploy; `{{...}}` is filled by the Jobs service at each run (keep the quotes). Context keys other than `notebook_path`, `job_id`, `job_run_id`, `task_key` raise `TypeError`.

## In a wheel task or local script

`get_logger(__name__)` only. A wheel task can also call `enable_table_logging(...)` (pass the table and context as task parameters); its buffered lines are written at process exit. Locally, `enable_table_logging` prints `Not on Databricks: table logging to … is off` and does nothing else.

## Reading the logs

```sql
SELECT created_at, level, logger_name, func_name, line_no, message, exception,
       notebook_path, job_id, job_run_id, task_key, session_id
FROM e2e_dev.dev_keqingli1129_ops.app_logs
WHERE level IN ('ERROR', 'CRITICAL')          -- or: job_run_id = '<run id>', session_id = '<uuid>'
ORDER BY created_at DESC
```

`created_at` is when the log call happened (UTC). `session_id` is one random id per `enable_table_logging` call, so it groups one run's lines. The table is created by the first write (append mode); the bundle owns the `ops` schema, so `bundle destroy` deletes the logs.

## If something goes wrong

- **A table write fails** (permissions, quota block, missing schema): one `logging_utils: could not write logs to …` line on stderr. The job keeps running, the lines stay buffered, automatic writes stop, and the next `flush_logs()` (or exit) retries.
- **Re-running cells** is safe: handlers are found by name, so lines never print or save twice.
- **`ModuleNotFoundError: databricks_end_to_end_project`** in a notebook: the `sys.path` line is missing, or the `..` count doesn't reach `src/`.

## Not verified yet

Written 2026-10-12 while serverless was blocked by the Free Edition quota, so nothing has run on Databricks yet. On the first real run, check:

1. That the `sys.path` line works in a notebook job (it assumes the working directory is the notebook's folder).
2. Whether `notebook_path` is filled from `dbruntime.databricks_repl_context.get_context()`. If it stays NULL, pass `notebook_path=` explicitly like the job ids.

The first run needs `databricks bundle deploy --profile DEFAULT`, which creates the `ops` schema. While the Lakebase compute is disabled (part 6b pause), deploys fail: enable it for the deploy, then disable it again (see CLAUDE.md).
