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
