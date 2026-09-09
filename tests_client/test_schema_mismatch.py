"""Schema drift produces an actionable error, not a psycopg traceback.

This is the seam that keeps the queue's internals free to move, so it is tested
against real divergent schemas rather than by mocking psycopg exceptions. Each
case builds a Postgres schema that is wrong in a specific way and points a
Client at it through ``search_path``.
"""

from __future__ import annotations

from collections.abc import Iterator
from urllib.parse import quote

import psycopg
import pytest

from conveyor_client import Client, SchemaMismatch

# The jobs table as it stood before 'cancelled' was added to the enum: exactly
# what a database initialised before db/migrations/001 still looks like.
BEFORE_CANCELLED = """
CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'dead');
CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT        NOT NULL DEFAULT gen_random_uuid()::text,
    payload         JSONB       NOT NULL,
    status          job_status  NOT NULL DEFAULT 'queued',
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 5,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    UNIQUE (idempotency_key)
);
"""

# A table that has lost a column the SDK selects.
MISSING_COLUMN = BEFORE_CANCELLED.replace("    last_error      TEXT,\n", "")

EMPTY = ""


@pytest.fixture
def divergent(dsn: str, schema: None) -> Iterator[object]:
    """Build a throwaway Postgres schema and hand back a Client pointed at it."""
    created: list[str] = []

    def build(name: str, ddl: str) -> Client:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
            conn.execute(f"CREATE SCHEMA {name}")
            if ddl:
                conn.execute(f"SET search_path = {name}; {ddl}")
        created.append(name)
        # search_path deliberately excludes public: with it, an empty schema
        # would just fall through to the real jobs table and prove nothing.
        sep = "&" if "?" in dsn else "?"
        return Client(f"{dsn}{sep}options={quote(f'-csearch_path={name}')}")

    yield build

    with psycopg.connect(dsn, autocommit=True) as conn:
        for name in created:
            conn.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def test_cancel_on_a_database_without_the_cancelled_value(divergent) -> None:
    """The exact failure a production database gets if it skips the migration."""
    with divergent("drift_no_cancel", BEFORE_CANCELLED) as client:
        job = client.enqueue("h", {})  # enqueue still works; only cancel needs the value

        with pytest.raises(SchemaMismatch) as excinfo:
            client.cancel(job.id)

    message = str(excinfo.value)
    assert "job_status" in message
    assert "db/migrations/001_add_cancelled_status.sql" in message


def test_a_missing_column(divergent) -> None:
    with divergent("drift_no_column", MISSING_COLUMN) as client:
        with pytest.raises(SchemaMismatch, match="missing a column"):
            client.enqueue("h", {})


def test_no_jobs_table_at_all(divergent) -> None:
    with divergent("drift_empty", EMPTY) as client:
        with pytest.raises(SchemaMismatch, match="does not exist"):
            client.get(1)


def test_schema_mismatch_is_a_conveyor_error() -> None:
    """So a caller can catch one thing for "the queue said no"."""
    from conveyor_client import ConveyorError

    assert issubclass(SchemaMismatch, ConveyorError)
