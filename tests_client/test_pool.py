"""Pooling: bounded connections, reuse, and lifecycle."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from psycopg_pool import PoolClosed, PoolTimeout

from conveyor_client import Client


def _backends(raw: psycopg.Connection, name: str) -> int:
    return raw.execute(
        "SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = %s", (name,)
    ).fetchone()["n"]


def _assert_backends_drain(raw: psycopg.Connection, name: str, timeout: float = 5.0) -> None:
    """Wait for Postgres to tear down the pool's backends.

    ``close()`` returning means the client let go, not that the server has
    finished reaping the sockets; pg_stat_activity can still list them for a
    moment afterwards. The guarantee worth testing is that they *do* go away, so
    poll for it rather than racing it.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _backends(raw, name) == 0:
            return
        time.sleep(0.05)
    pytest.fail(f"{_backends(raw, name)} backend(s) named {name!r} still open after close()")


def test_concurrent_work_never_exceeds_max_size(dsn: str, raw: psycopg.Connection) -> None:
    """Sixteen threads share four connections; they queue rather than open more."""
    name = "conveyor-client-pooltest"
    peak = 0
    lock = threading.Lock()

    with Client(dsn, min_size=1, max_size=4, application_name=name) as client:

        def work(n: int) -> int:
            nonlocal peak
            job = client.enqueue("h", {"n": n})
            with lock:
                peak = max(peak, _backends(raw, name))
            return job.id

        with ThreadPoolExecutor(max_workers=16) as pool:
            ids = [f.result() for f in [pool.submit(work, n) for n in range(16)]]

    assert len(set(ids)) == 16, "every thread enqueued its own job"
    assert 0 < peak <= 4, f"pool opened {peak} backends with max_size=4"

    # And the pool actually closed them on exit.
    _assert_backends_drain(raw, name)


def test_connections_are_reused_not_reopened(dsn: str, raw: psycopg.Connection) -> None:
    """Thirty sequential calls on a one-connection pool use one backend throughout."""
    name = "conveyor-client-reuse"
    with Client(dsn, min_size=1, max_size=1, application_name=name) as client:
        pids = set()
        for n in range(30):
            client.enqueue("h", {"n": n})
            pids.add(
                raw.execute(
                    "SELECT pid FROM pg_stat_activity WHERE application_name = %s", (name,)
                ).fetchone()["pid"]
            )
    assert len(pids) == 1, f"expected one reused backend, saw {len(pids)}"


def test_close_is_idempotent_and_use_after_close_raises(dsn: str) -> None:
    client = Client(dsn)
    client.close()
    client.close()

    with pytest.raises(PoolClosed):
        client.enqueue("h", {})


def test_context_manager_closes_on_exception(dsn: str, raw: psycopg.Connection) -> None:
    name = "conveyor-client-ctx"
    with pytest.raises(RuntimeError):
        with Client(dsn, application_name=name):
            raise RuntimeError("boom")
    _assert_backends_drain(raw, name)


def test_unreachable_database_fails_at_construction(schema: None) -> None:
    """A bad DSN raises where the DSN is written, not at the first enqueue."""
    with pytest.raises((PoolTimeout, psycopg.OperationalError)):
        Client("postgresql://nobody@localhost:1/nothing", connect_timeout=2, min_size=1)
