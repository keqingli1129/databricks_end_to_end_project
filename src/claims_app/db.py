"""Postgres (Lakebase) access for the claims app: connections, the app's own tables, and queries.

The app's Postgres resource injects PGHOST, PGPORT, PGDATABASE, PGUSER (the app's service principal) and
LAKEBASE_ENDPOINT. The password is an OAuth token that lasts one hour, so every new connection gets a token
that is at most TOKEN_TTL_S old, and pooled connections are recycled before that.
"""

import os
import time

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

SYNCED_SCHEMA = os.environ["SYNCED_SCHEMA"]  # e.g. dev_keqingli1129_gold: where claim_checks_pg/policy_lookup_pg live
TOKEN_TTL_S = 45 * 60

_workspace = WorkspaceClient()
_token = {"value": None, "created": 0.0}


def _password() -> str:
    if _token["value"] is None or time.time() - _token["created"] > TOKEN_TTL_S:
        credential = _workspace.postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"])
        _token["value"], _token["created"] = credential.token, time.time()
    return _token["value"]


class _TokenConnection(psycopg.Connection):
    """A connection that always logs in with a fresh enough OAuth token."""

    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs["password"] = _password()
        return super().connect(conninfo, **kwargs)


def make_pool() -> ConnectionPool:
    return ConnectionPool(
        kwargs={
            "host": os.environ["PGHOST"],
            "port": int(os.environ.get("PGPORT", "5432")),
            "dbname": os.environ["PGDATABASE"],
            "user": os.environ["PGUSER"],
            "sslmode": "require",
            "row_factory": dict_row,
        },
        connection_class=_TokenConnection,
        min_size=1,
        max_size=4,
        max_lifetime=TOKEN_TTL_S,
        check=ConnectionPool.check_connection,  # drops connections that died while the compute was suspended
        open=True,
    )


# The app's own tables. The app's service principal creates them, so it owns them. failed_checks is jsonb because
# that is how the sync stores claim_checks.failed_checks, and admin mode combines both with UNION ALL.
APP_DDL = """
CREATE SCHEMA IF NOT EXISTS app;
CREATE SEQUENCE IF NOT EXISTS app.claim_no_seq;
CREATE TABLE IF NOT EXISTS app.app_claims (
    claim_no            text PRIMARY KEY,           -- APP00000001, APP00000002, ...
    policy_no           text NOT NULL,
    customer_id         text,
    full_name           text,
    incident_date       date NOT NULL,
    incident_type       text NOT NULL,
    incident_severity   text NOT NULL,
    claim_amount        numeric(12, 2) NOT NULL,
    coverage            text,
    coverage_limit      integer,
    start_date          date,
    end_date            date,
    chassis_number      text,
    max_speed           double precision,
    image_name          text,
    expected_damage     text,
    predicted_damage    text,
    severity_match      boolean,
    amount_within_limit boolean,
    policy_valid        boolean,
    speed_ok            boolean,
    failed_checks       jsonb NOT NULL,
    claim_status        text NOT NULL,
    location            text,
    collision_type      text,
    vehicles_involved   integer,
    notes               text,
    submitted_at        timestamptz NOT NULL DEFAULT now()
);
"""


def diagnose() -> list[str]:
    """Why can't the pool connect? The pool only reports a timeout, so try one direct connection and report it."""
    names = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGSSLMODE", "LAKEBASE_ENDPOINT", "SYNCED_SCHEMA"]
    lines = [f"{name}: {'set' if os.environ.get(name) else 'MISSING'}" for name in names]
    try:
        _password()
        lines.append("OAuth token for Postgres: OK")
        with _TokenConnection.connect(
            host=os.environ["PGHOST"], dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"], sslmode="require"
        ) as conn:
            lines.append(f"Direct connection: OK as {conn.execute('SELECT current_user').fetchone()[0]}")
    except Exception as error:  # noqa: BLE001 - the point is to show any error
        lines.append(f"Direct connection failed: {type(error).__name__}: {error}")
    return lines


def init_app_tables(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute(APP_DDL)


def table_counts(pool: ConnectionPool) -> dict:
    """Row counts of the two synced tables and the app's claims."""
    query = sql.SQL(
        "SELECT (SELECT count(*) FROM {claims}) AS claim_checks, "
        "(SELECT count(*) FROM {policies}) AS policy_lookup, "
        "(SELECT count(*) FROM app.app_claims) AS app_claims"
    ).format(
        claims=sql.Identifier(SYNCED_SCHEMA, "claim_checks_pg"),
        policies=sql.Identifier(SYNCED_SCHEMA, "policy_lookup_pg"),
    )
    with pool.connection() as conn:
        return conn.execute(query).fetchone()


def get_policy(pool: ConnectionPool, policy_no: str) -> dict | None:
    """A policy from the synced gold.policy_lookup, or None if there's no such policy."""
    query = sql.SQL("SELECT * FROM {policies} WHERE policy_no = %s").format(
        policies=sql.Identifier(SYNCED_SCHEMA, "policy_lookup_pg")
    )
    with pool.connection() as conn:
        return conn.execute(query, [policy_no]).fetchone()


def next_claim_no(pool: ConnectionPool) -> str:
    """APP00000001, APP00000002, ...: can't clash with the pipeline's CLM numbers."""
    with pool.connection() as conn:
        number = conn.execute("SELECT nextval('app.claim_no_seq')").fetchone()["nextval"]
    return f"APP{number:08d}"


APP_CLAIM_COLUMNS = [
    "claim_no", "policy_no", "customer_id", "full_name", "incident_date", "incident_type", "incident_severity",
    "claim_amount", "coverage", "coverage_limit", "start_date", "end_date", "chassis_number", "max_speed",
    "image_name", "expected_damage", "predicted_damage", "severity_match", "amount_within_limit", "policy_valid",
    "speed_ok", "failed_checks", "claim_status", "location", "collision_type", "vehicles_involved", "notes",
]  # fmt: skip


def insert_app_claim(pool: ConnectionPool, claim: dict) -> None:
    """One row into app.app_claims; claim has a value for every APP_CLAIM_COLUMNS name."""
    values = {**claim, "failed_checks": Jsonb(claim["failed_checks"])}
    query = sql.SQL("INSERT INTO app.app_claims ({columns}) VALUES ({values})").format(
        columns=sql.SQL(", ").join(map(sql.Identifier, APP_CLAIM_COLUMNS)),
        values=sql.SQL(", ").join(map(sql.Placeholder, APP_CLAIM_COLUMNS)),
    )
    with pool.connection() as conn:
        conn.execute(query, values)


# --- Admin mode: pipeline claims (synced claim_checks) and app claims together. ---

# The columns both tables share, with a source column; pipeline claims have no submitted_at.
_ALL_CLAIMS = """
    SELECT claim_no, policy_no, full_name, incident_date, incident_type, incident_severity, claim_amount, coverage,
           coverage_limit, predicted_damage, failed_checks, claim_status, 'pipeline' AS source,
           NULL::timestamptz AS submitted_at
    FROM {synced}
    UNION ALL
    SELECT claim_no, policy_no, full_name, incident_date, incident_type, incident_severity, claim_amount, coverage,
           coverage_limit, predicted_damage, failed_checks, claim_status, 'app' AS source, submitted_at
    FROM app.app_claims
"""


def _all_claims() -> sql.Composed:
    return sql.SQL(_ALL_CLAIMS).format(synced=sql.Identifier(SYNCED_SCHEMA, "claim_checks_pg"))


def claim_summary(pool: ConnectionPool) -> dict:
    """Counts over all claims: total, per status, from the app, and per claimed severity."""
    totals = sql.SQL(
        "SELECT count(*) AS total, "
        "count(*) FILTER (WHERE claim_status = 'auto_approved') AS auto_approved, "
        "count(*) FILTER (WHERE claim_status = 'needs_review') AS needs_review, "
        "count(*) FILTER (WHERE source = 'app') AS from_app "
        "FROM ({all_claims}) a"
    ).format(all_claims=_all_claims())
    per_severity = sql.SQL("SELECT incident_severity, count(*) AS n FROM ({all_claims}) a GROUP BY 1").format(
        all_claims=_all_claims()
    )
    with pool.connection() as conn:
        summary = conn.execute(totals).fetchone()
        summary["per_severity"] = {row["incident_severity"]: row["n"] for row in conn.execute(per_severity)}
    return summary


def list_claims(pool: ConnectionPool, severity: str | None, status: str | None, source: str | None, limit: int = 500):
    """Filtered claims, app claims first (newest first), then pipeline claims by incident date."""
    conditions = [
        sql.SQL("{column} = {value}").format(column=sql.Identifier(column), value=sql.Literal(value))
        for column, value in [("incident_severity", severity), ("claim_status", status), ("source", source)]
        if value is not None
    ]
    query = sql.SQL(
        "SELECT * FROM ({all_claims}) a {where} "
        "ORDER BY source = 'app' DESC, submitted_at DESC NULLS LAST, incident_date DESC, claim_no LIMIT {limit}"
    ).format(
        all_claims=_all_claims(),
        where=sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions) if conditions else sql.SQL(""),
        limit=sql.Literal(limit),
    )
    with pool.connection() as conn:
        return conn.execute(query).fetchall()


def get_claim(pool: ConnectionPool, claim_no: str) -> dict | None:
    """One claim with every column, from the app's claims or the synced pipeline claims; None if it isn't found."""
    synced = sql.Identifier(SYNCED_SCHEMA, "claim_checks_pg")
    with pool.connection() as conn:
        for table, source in [(sql.SQL("app.app_claims"), "app"), (synced, "pipeline")]:
            claim = conn.execute(sql.SQL("SELECT * FROM {} WHERE claim_no = %s").format(table), [claim_no]).fetchone()
            if claim is not None:
                return {**claim, "source": source}
    return None


def latest_app_claim_no(pool: ConnectionPool) -> str | None:
    with pool.connection() as conn:
        row = conn.execute("SELECT max(claim_no) AS claim_no FROM app.app_claims").fetchone()
    return row["claim_no"]
