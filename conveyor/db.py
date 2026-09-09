"""Connection helper. One connection per worker or thread; no pool."""

from __future__ import annotations

import os

import psycopg
from psycopg.rows import DictRow, dict_row

DEFAULT_DSN = "postgresql://conveyor:conveyor@localhost:5433/conveyor"


def dsn_from_env() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_DSN)


def connect(dsn: str | None = None) -> psycopg.Connection[DictRow]:
    """Open a connection with autocommit *off*.

    Every queue operation runs in an explicit transaction and commits itself,
    so callers never share a connection between threads.
    """
    return psycopg.connect(dsn or dsn_from_env(), row_factory=dict_row, autocommit=False)
