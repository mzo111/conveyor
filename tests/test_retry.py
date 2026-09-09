"""Backoff schedule, attempt counting, and the dead terminal state."""

from __future__ import annotations

from datetime import timedelta

import pytest

from conveyor.queue import backoff_delay, claim, enqueue, get_job
from conveyor.worker import Worker


def test_backoff_schedule_is_exponential_and_capped():
    assert [backoff_delay(a).total_seconds() for a in range(1, 7)] == [1, 2, 4, 8, 16, 32]
    assert backoff_delay(20).total_seconds() == 300
    assert backoff_delay(3, base=0.5, factor=3.0, cap=10).total_seconds() == 4.5
    assert backoff_delay(3, base=0.5, factor=3.0, cap=4).total_seconds() == 4
    with pytest.raises(ValueError):
        backoff_delay(0)


def test_backoff_jitter_only_adds_within_fraction():
    for _ in range(200):
        d = backoff_delay(3, jitter=0.5).total_seconds()
        assert 4.0 <= d <= 6.0


def test_failed_job_is_rescheduled_per_backoff_then_dies(conn):
    base = 0.05
    job, _ = enqueue(conn, {"x": 1}, max_attempts=3)
    calls: list[int] = []

    def always_fail(j):
        calls.append(j.attempts)
        raise RuntimeError(f"nope {j.attempts}")

    worker = Worker(
        conn,
        always_fail,
        worker_id="w",
        poll_interval=0.01,
        backoff=lambda attempt: backoff_delay(attempt, base=base),
    )

    # Attempt 1 fails -> requeued with delay base * 2**0.
    assert worker.run_once() is True
    j = get_job(conn, job.id)
    assert (j.status, j.attempts, j.last_error) == ("queued", 1, "RuntimeError: nope 1")
    assert (j.run_at - j.updated_at).total_seconds() == pytest.approx(base, abs=0.01)
    assert j.visibility_deadline is None

    # Not claimable until run_at passes.
    assert claim(conn, worker_id="x", visibility_timeout=timedelta(seconds=1)) is None
    wait_until_runnable(conn, job.id)

    # Attempt 2 fails -> delay base * 2**1.
    assert worker.run_once() is True
    j = get_job(conn, job.id)
    assert (j.status, j.attempts) == ("queued", 2)
    assert (j.run_at - j.updated_at).total_seconds() == pytest.approx(base * 2, abs=0.01)
    wait_until_runnable(conn, job.id)

    # Attempt 3 is the last permitted one -> dead, terminal, never claimable.
    assert worker.run_once() is True
    j = get_job(conn, job.id)
    assert (j.status, j.attempts, j.last_error) == ("dead", 3, "RuntimeError: nope 3")
    assert j.finished_at is not None
    assert worker.run_once() is False
    assert calls == [1, 2, 3]


def test_success_after_retry_ends_succeeded(conn):
    job, _ = enqueue(conn, {"x": 1}, max_attempts=3)
    seen: list[int] = []

    def flaky(j):
        seen.append(j.attempts)
        if j.attempts == 1:
            raise ValueError("first time only")

    worker = Worker(conn, flaky, backoff=lambda a: timedelta(0))
    assert worker.run_once() and worker.run_once()
    j = get_job(conn, job.id)
    assert j.status == "succeeded" and j.attempts == 2 and seen == [1, 2]
    # last_error from attempt 1 is kept for diagnosis; that is intentional.
    assert j.last_error == "ValueError: first time only"


def wait_until_runnable(conn, job_id: int) -> None:
    """Sleep on the DB clock so the test does not depend on host/DB clock skew."""
    conn.execute(
        "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM run_at - now()))) FROM jobs WHERE id = %s",
        (job_id,),
    )
    conn.commit()
