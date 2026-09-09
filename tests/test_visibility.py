"""Lease expiry, reclaim, and the fencing token on ack/nack."""

from __future__ import annotations

import time
from datetime import timedelta

from conveyor.queue import ack, claim, enqueue, get_job, nack
from conveyor.reaper import reap, reap_expired

SHORT = timedelta(milliseconds=200)


def test_expired_lease_is_returned_to_queue_and_reclaimable(conn, connect_fn):
    job, _ = enqueue(conn, {"x": 1})
    first = claim(conn, worker_id="w1", visibility_timeout=SHORT)
    assert first is not None and first.attempts == 1

    assert reap(conn) == 0, "lease still valid; reaper must not touch it"
    time.sleep(0.3)
    assert reap(conn) == 1

    after = get_job(conn, job.id)
    assert after.status == "queued"
    assert after.attempts == 1, "expired attempt still counts"
    assert after.visibility_deadline is None
    assert after.last_error == "visibility deadline expired"

    other = connect_fn()
    second = claim(other, worker_id="w2", visibility_timeout=timedelta(seconds=30))
    assert second is not None and second.id == job.id and second.attempts == 2

    # The original holder comes back late. Its ack/nack must be rejected so it
    # cannot overwrite w2's in-flight attempt. (It still ran the job: that is
    # the at-least-once window, and nothing here can undo it.)
    assert ack(conn, job.id, attempt=1) is False
    assert nack(conn, job.id, attempt=1, error="late", retry_in=SHORT) is False
    still = get_job(conn, job.id)
    assert still.status == "running" and still.attempts == 2 and still.claimed_by == "w2"

    assert ack(other, job.id, attempt=2) is True
    assert get_job(conn, job.id).status == "succeeded"


def test_reaper_races_ack_exactly_one_wins(conn):
    job, _ = enqueue(conn, {"x": 1})
    held = claim(conn, worker_id="w1", visibility_timeout=SHORT)
    time.sleep(0.3)
    # Lease is expired but the reaper has not run yet: the worker's ack lands
    # first and wins; the reaper then finds nothing running.
    assert ack(conn, job.id, held.attempts) is True
    assert reap(conn) == 0
    assert get_job(conn, job.id).status == "succeeded"


def test_expired_final_attempt_goes_dead_not_queued(conn):
    job, _ = enqueue(conn, {"x": 1}, max_attempts=1)
    claim(conn, worker_id="w1", visibility_timeout=SHORT)
    time.sleep(0.3)
    assert reap(conn) == 1
    after = get_job(conn, job.id)
    assert after.status == "dead"
    assert after.finished_at is not None
    assert claim(conn, worker_id="w2", visibility_timeout=SHORT) is None


def test_reaper_ignores_unexpired_and_finished_jobs(conn):
    a, _ = enqueue(conn, {"a": 1})
    b, _ = enqueue(conn, {"b": 1})
    live = claim(conn, worker_id="w", visibility_timeout=timedelta(seconds=30))
    done = claim(conn, worker_id="w", visibility_timeout=SHORT)
    ack(conn, done.id, done.attempts)
    time.sleep(0.3)
    assert reap(conn) == 0
    assert get_job(conn, live.id).status == "running"
    assert get_job(conn, done.id).status == "succeeded"


def test_reap_expired_returns_affected_rows(conn):
    a, _ = enqueue(conn, {"a": 1})
    b, _ = enqueue(conn, {"b": 1}, max_attempts=1)
    claim(conn, worker_id="w", visibility_timeout=SHORT)
    claim(conn, worker_id="w", visibility_timeout=SHORT)
    time.sleep(0.3)
    rows = sorted(reap_expired(conn), key=lambda r: r["id"])
    assert [(r["id"], r["attempts"], r["status"]) for r in rows] == [
        (a.id, 1, "queued"),
        (b.id, 1, "dead"),
    ]
    assert all(r["reaped_at"] is not None for r in rows)
