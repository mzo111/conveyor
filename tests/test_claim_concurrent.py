"""Two workers must never claim the same job. Real threads, real connections."""

from __future__ import annotations

import threading
from datetime import timedelta

from conveyor.queue import claim, enqueue

TIMEOUT = timedelta(seconds=30)


def test_many_threads_drain_queue_without_duplicates(conn, connect_fn):
    n_jobs, n_threads = 200, 16
    expected = {enqueue(conn, {"i": i})[0].id for i in range(n_jobs)}

    claimed: list[int] = []
    lock = threading.Lock()
    start = threading.Barrier(n_threads)

    def drain(idx: int) -> None:
        c = connect_fn()
        start.wait()  # all threads hit the queue at the same instant
        while (job := claim(c, worker_id=f"w{idx}", visibility_timeout=TIMEOUT)) is not None:
            with lock:
                claimed.append(job.id)

    threads = [threading.Thread(target=drain, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()

    assert len(claimed) == n_jobs, "some job claimed twice or missed"
    assert set(claimed) == expected

    rows = conn.execute("SELECT status, attempts FROM jobs").fetchall()
    conn.commit()
    assert all(r["status"] == "running" and r["attempts"] == 1 for r in rows)


def test_single_job_single_winner(conn, connect_fn):
    n_threads = 16
    job, _ = enqueue(conn, {"only": True})

    winners: list[str] = []
    lock = threading.Lock()
    start = threading.Barrier(n_threads)

    def race(idx: int) -> None:
        c = connect_fn()
        start.wait()
        got = claim(c, worker_id=f"w{idx}", visibility_timeout=TIMEOUT)
        if got is not None:
            with lock:
                winners.append(f"w{idx}")

    threads = [threading.Thread(target=race, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(winners) == 1
    row = conn.execute("SELECT claimed_by, attempts FROM jobs WHERE id = %s", (job.id,)).fetchone()
    conn.commit()
    assert row["claimed_by"] == winners[0]
    assert row["attempts"] == 1


def test_claim_respects_run_at_and_order(conn):
    later = conn.execute("SELECT now() + interval '1 hour' AS t").fetchone()["t"]
    conn.commit()
    future, _ = enqueue(conn, {"when": "later"}, run_at=later)
    first, _ = enqueue(conn, {"n": 1})
    second, _ = enqueue(conn, {"n": 2})

    assert claim(conn, worker_id="w", visibility_timeout=TIMEOUT).id == first.id
    assert claim(conn, worker_id="w", visibility_timeout=TIMEOUT).id == second.id
    assert claim(conn, worker_id="w", visibility_timeout=TIMEOUT) is None
    assert future.status == "queued"
