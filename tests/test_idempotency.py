"""Enqueueing twice with the same key never creates two jobs."""

from __future__ import annotations

import threading
from datetime import timedelta

from conveyor.queue import ack, claim, enqueue, get_job


def count(conn) -> int:
    n = conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"]
    conn.commit()
    return n


def test_same_key_returns_existing_job(conn):
    a, created_a = enqueue(conn, {"v": 1}, idempotency_key="order-42")
    b, created_b = enqueue(conn, {"v": 2}, idempotency_key="order-42")
    assert created_a is True and created_b is False
    assert a.id == b.id
    assert b.payload == {"v": 1}, "second payload is ignored, first one wins"
    assert count(conn) == 1


def test_key_survives_completion(conn):
    a, _ = enqueue(conn, {"v": 1}, idempotency_key="k")
    held = claim(conn, worker_id="w", visibility_timeout=timedelta(seconds=5))
    ack(conn, held.id, held.attempts)
    b, created = enqueue(conn, {"v": 1}, idempotency_key="k")
    assert created is False and b.id == a.id
    assert get_job(conn, a.id).status == "succeeded"
    assert count(conn) == 1


def test_concurrent_same_key_creates_one_row(conn, connect_fn):
    n = 16
    ids: list[int] = []
    created_flags: list[bool] = []
    lock = threading.Lock()
    start = threading.Barrier(n)

    def go() -> None:
        c = connect_fn()
        start.wait()
        job, created = enqueue(c, {"same": True}, idempotency_key="race")
        with lock:
            ids.append(job.id)
            created_flags.append(created)

    threads = [threading.Thread(target=go) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(set(ids)) == 1
    assert created_flags.count(True) == 1
    assert count(conn) == 1


def test_distinct_and_missing_keys_create_distinct_jobs(conn):
    a, _ = enqueue(conn, {}, idempotency_key="a")
    b, _ = enqueue(conn, {}, idempotency_key="b")
    c, _ = enqueue(conn, {})
    d, _ = enqueue(conn, {})
    assert len({a.id, b.id, c.id, d.id}) == 4
    assert c.idempotency_key != d.idempotency_key
