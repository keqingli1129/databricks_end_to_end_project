# Part 8: shared custom logger (step-by-step plan)

> **How this plan is run:** Claude carries out one step at a time, explains what it did and why, then **pauses** for your "go". Nothing is committed by Claude. Each task ends with a `git status` check so you can commit it yourself. **No automated tests.**

**Goal:** one logger for the whole project, as in [databricks_logging.txt](databricks_logging.txt): console logging everywhere, plus an opt-in Delta table (`ops.app_logs`) with each record's fields and the notebook/job context.

**Design:** [part8_logging_design.md](part8_logging_design.md). **How to use:** [part8_logging_usage.md](part8_logging_usage.md).

> **Status 2026-10-12:** Tasks 1-2 done. **Tasks 3-5 dropped at your request**: the simulator keeps its `print`s, and nothing is deployed or run yet. The usage guide replaces Task 5, and its "Not verified yet" section holds Task 4's open checks.

## Global constraints

- **Module:** `src/databricks_end_to_end_project/logging_utils.py` (never `logging.py`). Project logger name `databricks_end_to_end_project`.
- **Table (dev):** `e2e_dev.dev_keqingli1129_ops.app_logs`; in bundle config always `${var.catalog}.${resources.schemas.ops.name}.app_logs`.
- **Writes:** buffered; flush at 500 rows, on ERROR/CRITICAL, on `flush_logs()`, and at exit (`logging.shutdown`). A failed write never raises.
- **Spark** is imported from `databricks.sdk.runtime`, inside `flush` only.
- **Profile `DEFAULT`,** target `dev`, local commands as `env -u PYTHONPATH …`. No tests, no Claude commits.
- **Serverless is blocked by the Free Edition quota** (2026-10-12). Tasks 1-3 need no compute (except `bundle validate`, which doesn't either). Task 4 waits for the reset.

---

### Task 1: the logger module

**Files:** Create `src/databricks_end_to_end_project/logging_utils.py`.

- [x] **Step 1.1: Write the module**

  **Do:** create the file:

  ```python
  """Shared logging: console output everywhere, plus an opt-in Delta table on Databricks.

  Based on docs/databricks_logging.txt; see docs/part8_logging_design.md.

      log = get_logger(__name__)
      enable_table_logging("<catalog>.<ops schema>.app_logs", job_id=..., job_run_id=..., task_key=...)
      log.info("resized %d images", n)
      flush_logs()
  """

  import logging
  import os
  import sys
  import uuid
  from datetime import UTC, datetime

  PROJECT_LOGGER = "databricks_end_to_end_project"
  CONSOLE_HANDLER = "e2e_console"
  TABLE_HANDLER = "e2e_table"
  CONSOLE_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s"
  FLUSH_AT = 500  # buffered rows that trigger a write
  CONTEXT_FIELDS = ("notebook_path", "job_id", "job_run_id", "task_key")
  LOG_SCHEMA = (
      "created_at TIMESTAMP, level STRING, logger_name STRING, func_name STRING, line_no INT, message STRING, "
      "exception STRING, notebook_path STRING, job_id STRING, job_run_id STRING, task_key STRING, session_id STRING"
  )

  _FORMATTER = logging.Formatter()  # only used for formatException


  def _project_logger() -> logging.Logger:
      """The parent of every project logger; gets its console handler on first use."""
      project = logging.getLogger(PROJECT_LOGGER)
      if _find_handler(project, CONSOLE_HANDLER) is None:
          console = logging.StreamHandler()  # stderr
          console.set_name(CONSOLE_HANDLER)
          console.setFormatter(logging.Formatter(CONSOLE_FORMAT))
          project.addHandler(console)
          project.setLevel(logging.INFO)
          project.propagate = False  # don't print a second time through the root logger
      return project


  def _find_handler(logger: logging.Logger, name: str) -> logging.Handler | None:
      return next((h for h in logger.handlers if h.get_name() == name), None)


  def get_logger(name: str) -> logging.Logger:
      """Return a logger under the project logger, e.g. get_logger(__name__)."""
      _project_logger()
      if name != PROJECT_LOGGER and not name.startswith(PROJECT_LOGGER + "."):
          name = f"{PROJECT_LOGGER}.{name}"  # e.g. "__main__" in a notebook
      return logging.getLogger(name)


  def _runtime_context() -> dict:
      """Notebook path and job ids from the Databricks REPL context; {} where it isn't available."""
      try:
          from dbruntime.databricks_repl_context import get_context

          ctx = get_context()
      except Exception:  # noqa: BLE001 - not on Databricks, or the context API changed
          return {}
      found = {
          "notebook_path": getattr(ctx, "notebookPath", None),
          "job_id": getattr(ctx, "jobId", None),
          "job_run_id": getattr(ctx, "currentRunId", None),
      }
      return {key: str(value) for key, value in found.items() if value}


  class DeltaTableHandler(logging.Handler):
      """Buffers log records and appends them to a Delta table in batches."""

      def __init__(self, table: str, context: dict):
          super().__init__()
          self.set_name(TABLE_HANDLER)
          self.table = table
          self.context = context
          self.session_id = str(uuid.uuid4())
          self.buffer: list[tuple] = []
          self.write_failed = False

      def emit(self, record: logging.LogRecord) -> None:
          self.buffer.append(
              (
                  datetime.fromtimestamp(record.created, tz=UTC),
                  record.levelname,
                  record.name,
                  record.funcName,
                  record.lineno,
                  record.getMessage(),
                  _FORMATTER.formatException(record.exc_info) if record.exc_info else None,
                  *(self.context.get(field) for field in CONTEXT_FIELDS),
                  self.session_id,
              )
          )
          if not self.write_failed and (len(self.buffer) >= FLUSH_AT or record.levelno >= logging.ERROR):
              self.flush()

      def flush(self) -> None:
          with self.lock:
              if not self.buffer:
                  return
              rows = list(self.buffer)
              try:
                  from databricks.sdk.runtime import spark

                  spark.createDataFrame(rows, LOG_SCHEMA).write.mode("append").saveAsTable(self.table)
              except Exception as e:  # noqa: BLE001 - logging must never break the job
                  if not self.write_failed:
                      print(f"logging_utils: could not write logs to {self.table}: {e!r}", file=sys.stderr)
                  self.write_failed = True  # keep the rows; stop automatic writes, flush_logs() retries
                  return
              del self.buffer[: len(rows)]
              self.write_failed = False


  def enable_table_logging(table: str, **context: str) -> None:
      """Also write project log records to `table` (full name). Context keys: notebook_path, job_id, job_run_id, task_key."""
      project = _project_logger()
      unknown = set(context) - set(CONTEXT_FIELDS)
      if unknown:
          raise TypeError(f"unknown context fields: {sorted(unknown)}")
      if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
          project.warning("Not on Databricks: table logging to %s is off", table)
          return
      found = _runtime_context()
      merged = {field: context.get(field) or found.get(field) for field in CONTEXT_FIELDS}
      old = _find_handler(project, TABLE_HANDLER)
      if old is not None:
          old.flush()
          project.removeHandler(old)
      project.addHandler(DeltaTableHandler(table, merged))


  def flush_logs() -> None:
      """Write buffered records now. Call it at the end of a notebook or job."""
      handler = _find_handler(logging.getLogger(PROJECT_LOGGER), TABLE_HANDLER)
      if handler is not None:
          handler.flush()
  ```

- [x] **Step 1.2: Line-by-line explanation**

  **Do:** walk through the module section by section (constants → `_project_logger`/`get_logger` → `_runtime_context` → `DeltaTableHandler.emit`/`flush` → `enable_table_logging`/`flush_logs`), mapping each part to the video (`LogRecord` attributes, extending `logging.Handler.emit`, the schema, append mode) and to the design's changes (buffering, handler names instead of globals so a re-run notebook cell doesn't add handlers twice, `logging.shutdown` flushing at exit). Pause for questions.

- [x] **Step 1.3: Local check (no compute)**

  ```bash
  env -u PYTHONPATH uv run python -c "
  from databricks_end_to_end_project.logging_utils import get_logger, enable_table_logging, flush_logs
  enable_table_logging('x.y.z')
  log = get_logger('check'); get_logger('check')
  log.info('hello %s', 'world'); flush_logs()"
  ```

  **Expect:** exactly two lines on stderr: the `WARNING databricks_end_to_end_project:… Not on Databricks: table logging to x.y.z is off` line, then `INFO databricks_end_to_end_project.check:5 hello world` (line 5 of the `-c` string). Two `get_logger('check')` calls still print the line only once.. Then `env -u PYTHONPATH uv run ruff check src/databricks_end_to_end_project/logging_utils.py && env -u PYTHONPATH uv run ruff format src/databricks_end_to_end_project/logging_utils.py`.

- [x] **Step 1.4: `git status`**: one new file. You commit.

---

### Task 2: `ops` schema and the claim-images notebook

**Files:** Modify `resources/unity_catalog.yml`, `src/object_storage/claim_images.py` (part 3), `resources/ingest_claim_images.job.yml` (part 3).

- [x] **Step 2.1: Add the `ops` schema**

  **Do:** in `resources/unity_catalog.yml`, after the `source` schema:

  ```yaml

      # Part 8 (see docs/databricks_logging.txt): operational data, kept out of the medallion layers.
      ops:
        catalog_name: ${var.catalog}
        name: ops
        comment: Operational data - application logs (app_logs)
  ```

- [x] **Step 2.2: Use the logger in the notebook**

  **Do:** in `src/object_storage/claim_images.py`:

  a. Insert a new cell after the markdown cell (before the widgets cell):

  ```python
  # COMMAND ----------

  import os
  import sys

  sys.path.insert(0, os.path.abspath(".."))  # this notebook is in src/object_storage/, so ".." is src/ (shared package)

  from databricks_end_to_end_project.logging_utils import enable_table_logging, flush_logs, get_logger
  ```

  b. Extend the widgets cell:

  ```python
  dbutils.widgets.text("claims_volume_path", "/Volumes/e2e_dev/dev_keqingli1129_landing/claims")
  dbutils.widgets.text("target_table", "e2e_dev.dev_keqingli1129_bronze.claim_images")
  dbutils.widgets.text("log_table", "e2e_dev.dev_keqingli1129_ops.app_logs")
  dbutils.widgets.text("job_id", "")  # filled by the job ({{job.id}}); empty when run by hand
  dbutils.widgets.text("job_run_id", "")
  dbutils.widgets.text("task_key", "")

  CLAIMS_VOLUME = dbutils.widgets.get("claims_volume_path")
  TARGET_TABLE = dbutils.widgets.get("target_table")

  log = get_logger("object_storage.claim_images")
  enable_table_logging(
      dbutils.widgets.get("log_table"),
      job_id=dbutils.widgets.get("job_id"),
      job_run_id=dbutils.widgets.get("job_run_id"),
      task_key=dbutils.widgets.get("task_key"),
  )
  ```

  c. Wrap the stream so a failure is logged (and, being ERROR, written at once):

  ```python
  log.info("ingesting %s into %s", SOURCE_PATH, TARGET_TABLE)
  try:
      query = (
          ...unchanged, indented...
      )
      query.awaitTermination()
  except Exception:
      log.exception("claim image ingestion failed")  # ERROR, so it is written to the log table at once
      raise
  ```

  The whole stream is inside `try`, because a bad path or option fails at `.toTable(...)`, before `awaitTermination()`.

  d. Replace the three `print`s in the last cell:

  ```python
  log.info("rows in target: %d", spark.table(TARGET_TABLE).count())
  log.info("files still in images/: %d", len(dbutils.fs.ls(SOURCE_PATH)))
  log.info("files in archive/: %d", len(dbutils.fs.ls(ARCHIVE_PATH)))
  flush_logs()
  ```

- [x] **Step 2.3: Pass the log table and job context from the job**

  **Do:** in `resources/ingest_claim_images.job.yml`, add to `base_parameters`:

  ```yaml
                log_table: ${var.catalog}.${resources.schemas.ops.name}.app_logs
                job_id: "{{job.id}}"
                job_run_id: "{{job.run_id}}"
                task_key: "{{task.name}}"
  ```

- [x] **Step 2.4: Validate**

  `databricks bundle validate --profile DEFAULT`. **Expect:** `Validation OK!`. Then `git status` (3 modified files). You commit.

---

### Task 3: the simulator's console logging

**Files:** Modify `src/databricks_end_to_end_project/telematics/simulator.py` (part 1; its tests cover only `events`/`kafka_config`).

- [ ] **Step 3.1: Replace the two summary prints**

  **Do:** add the import and a module logger:

  ```python
  from databricks_end_to_end_project.logging_utils import get_logger
  from databricks_end_to_end_project.telematics import events, kafka_config

  log = get_logger(__name__)
  ```

  Then `print(f"Sent {args.count} events to topic {args.topic}")` → `log.info("Sent %d events to topic %s", args.count, args.topic)` and `print(f"Wrote {args.count} event files to {args.landing_path}")` → `log.info("Wrote %d event files to %s", args.count, args.landing_path)`. `_print_send` keeps `print` (the dry-run output is the result).

- [ ] **Step 3.2: Check**

  `env -u PYTHONPATH uv run simulate --dry-run --count 2 --interval 0` prints two `[dry-run]` lines as before. `env -u PYTHONPATH uv run ruff check . && env -u PYTHONPATH uv run ruff format .`. `git status`. You commit.

---

### Task 4: deploy and check on Databricks (after the quota resets)

- [ ] **Step 4.1: Deploy**

  The Lakebase compute is disabled (part 6b pause), and every deploy fails while it is. Enable it, deploy, disable it:

  ```bash
  databricks postgres update-endpoint projects/smart-claims-dev/branches/production/endpoints/primary spec.disabled --json '{"spec": {"disabled": false}}' --profile DEFAULT
  databricks bundle deploy --profile DEFAULT
  databricks postgres update-endpoint projects/smart-claims-dev/branches/production/endpoints/primary spec.disabled --json '{"spec": {"disabled": true}}' --profile DEFAULT
  ```

  **Expect:** the deploy creates schema `dev_keqingli1129_ops`. It also re-creates the missing serving endpoint `dev_keqingli1129_e2e-claims-damage-level` (gone since the quota block); that's wanted.

- [ ] **Step 4.2: Run the job**

  `databricks bundle run ingest_claim_images --profile DEFAULT`. **Expect:** success; the run output shows the `INFO …claim_images` lines. If it fails with `ModuleNotFoundError: databricks_end_to_end_project`, the notebook's working directory isn't its folder: stop and decide together (fallback: a `%pip install` of the wheel).

- [ ] **Step 4.3: Query the table**

  ```sql
  SELECT created_at, level, func_name, line_no, message, notebook_path, job_id, job_run_id, task_key, session_id
  FROM e2e_dev.dev_keqingli1129_ops.app_logs ORDER BY created_at
  ```

  **Expect:** 4 rows (`ingesting…`, 3 counts), one `session_id`, `job_id`/`job_run_id`/`task_key` filled. **Record whether `notebook_path` is filled** (the design's open question). If it's NULL, pass it explicitly (a widget defaulting to the notebook path) and note it in the design.

---

### Task 5: docs

- [ ] **Step 5.1: CLAUDE.md**

  **Do:** add after the "Telematics package" bullet:

  ```markdown
  - **Logging** (design and plan in `docs/part8_logging_*.md`, source material `docs/databricks_logging.txt`): `src/databricks_end_to_end_project/logging_utils.py`. `get_logger(__name__)` logs to the console everywhere (pipelines, scripts, jobs). In notebooks and jobs, `enable_table_logging(<full table name>, job_id=…, job_run_id=…, task_key=…)` also buffers records for `ops.app_logs` (schema key `ops`; jobs pass `{{job.id}}`, `{{job.run_id}}`, `{{task.name}}`), written at 500 rows, on ERROR, at exit, and on `flush_logs()`: call it at the end of a notebook, because a notebook's process doesn't exit. Notebook tasks don't have the package installed; they import it after `sys.path.insert(0, os.path.abspath(".."))` (see `src/object_storage/claim_images.py`). Never name a module `logging.py`.
  ```

  Add the Task 4 `notebook_path` finding to that bullet. `git status`. You commit.
