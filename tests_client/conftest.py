"""Fixtures for the SDK's tests. Real Postgres, no ``conveyor`` import.

These tests are deliberately in their own directory rather than under ``tests/``.
That package's ``conftest.py`` imports ``conveyor.db``, and inheriting it would
make the SDK's own test suite depend on the internals the SDK exists to hide.
Everything here is built from ``psycopg``, ``conveyor_client`` and the schema
file — nothing else. ``tests_client/test_independence.py`` enforces that for the
package itself.

The schema is read from ``db/schema.sql`` on purpose. It is the one thing the SDK
does depend on, so if it moves, these tests should be the loud failure.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from conveyor_client import Client

SCHEMA = Path(__file__).resolve().parent.parent / "db" / "schema.sql"
DEFAULT_DSN = "postgresql://conveyor:conveyor@localhost:5433/conveyor"


@pytest.fixture(scope="session")
def dsn() -> str:
    """Skip loudly rather than fail when there is no database to talk to."""
    dsn = os.environ.get("DATABASE_URL", DEFAULT_DSN)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable at {dsn} ({exc}); run: docker compose up -d db")
    return dsn


@pytest.fixture(scope="session")
def schema(dsn: str) -> None:
    """Apply db/schema.sql from scratch, so no test depends on stale state."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP VIEW IF EXISTS dead_letter")
        conn.execute("DROP TABLE IF EXISTS jobs CASCADE")
        conn.execute("DROP TYPE IF EXISTS job_status CASCADE")
        conn.execute(SCHEMA.read_text())


@pytest.fixture
def raw(dsn: str, schema: None) -> Iterator[psycopg.Connection]:
    """A plain connection, for setting up and asserting on rows behind the SDK's back.

    Tests use this to check what actually landed in the ``payload`` column and to
    force jobs into states the SDK cannot produce, such as ``running``.
    """
    with psycopg.connect(dsn, autocommit=True, row_factory=psycopg.rows.dict_row) as conn:
        conn.execute("TRUNCATE jobs RESTART IDENTITY")
        yield conn


@pytest.fixture
def client(dsn: str, raw: psycopg.Connection) -> Iterator[Client]:
    """A Client against a truncated table. Depends on ``raw`` so ordering is fixed."""
    with Client(dsn, max_size=4) as client:
        yield client
