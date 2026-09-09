"""Fixtures for tests that run against a real Postgres.

Set DATABASE_URL, or start the Compose service (``docker compose up -d db``)
which listens on localhost:5433. Tests are skipped, not failed, if the
database is unreachable, so the reason is loud in the pytest summary.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import DictRow

from conveyor.db import connect, dsn_from_env

SCHEMA = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


@pytest.fixture(scope="session")
def dsn() -> str:
    dsn = dsn_from_env()
    try:
        with connect(dsn) as conn:
            conn.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable at {dsn} ({exc}); run: docker compose up -d db")
    return dsn


@pytest.fixture(scope="session")
def schema(dsn: str) -> None:
    """Apply db/schema.sql from scratch so tests never depend on stale state."""
    with connect(dsn) as conn:
        conn.execute("DROP TABLE IF EXISTS jobs CASCADE")
        conn.execute("DROP TYPE IF EXISTS job_status CASCADE")
        conn.execute(SCHEMA.read_text())
        conn.commit()


@pytest.fixture
def connect_fn(dsn: str, schema: None) -> Iterator[Callable[[], psycopg.Connection[DictRow]]]:
    """Factory returning a fresh connection; threads must each call it themselves."""
    opened: list[psycopg.Connection[DictRow]] = []

    def factory() -> psycopg.Connection[DictRow]:
        conn = connect(dsn)
        opened.append(conn)
        return conn

    yield factory
    for conn in opened:
        conn.close()


@pytest.fixture
def conn(connect_fn: Callable[[], psycopg.Connection[DictRow]]) -> psycopg.Connection[DictRow]:
    conn = connect_fn()
    conn.execute("TRUNCATE jobs RESTART IDENTITY")
    conn.commit()
    return conn
