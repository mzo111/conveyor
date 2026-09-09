"""Lease extension: a slow handler survives with heartbeating, is reaped without it."""

from __future__ import annotations

import threading
import time
from datetime import timedelta

from conveyor.queue import ack, claim, enqueue, extend_lease, get_job
from conveyor.reaper import reap
from conveyor.worker import Worker

SHORT = timedelta(milliseconds=300)


def test_extend_lease_is_fenced_by_attempt(conn, connect_fn):
    job, _ = enqueue(conn, {"x": 1})
    held = claim(conn, worker_id="w1", visibility_timeout=SHORT)
    before = get_job(conn, job.id).visibility_deadline

    assert extend_lease(conn, job.id, held.attempts, visibility_timeout=timedelta(seconds=30))
    after = get_job(conn, job.id).visibility_deadline
    assert after > before + timedelta(seconds=20)

    assert not extend_lease(conn, job.id, held.attempts + 1, visibility_timeout=SHORT)
    assert not extend_lease(conn, job.id, held.attempts - 1, visibility_timeout=SHORT)
    assert get_job(conn, job.id).visibility_deadline == after, "wrong attempt must not touch it"


def test_stale_worker_cannot_revive_expired_lease(conn, connect_fn):
    job, _ = enqueue(conn, {"x": 1})
    held = claim(conn, worker_id="w1", visibility_timeout=SHORT)
    time.sleep(0.4)
    assert reap(conn) == 1
    assert not extend_lease(conn, job.id, held.attempts, visibility_timeout=SHORT)
    assert get_job(conn, job.id).status == "queued"

    other = connect_fn()
    second = claim(other, worker_id="w2", visibility_timeout=SHORT)
    assert second.attempts == 2
    assert not extend_lease(conn, job.id, held.attempts, visibility_timeout=timedelta(seconds=30))
    assert extend_lease(other, job.id, second.attempts, visibility_timeout=timedelta(seconds=30))


class _ReaperThread:
    """Runs reap() continuously on its own connection; counts what it reaped."""

    def __init__(self, connect_fn, interval: float = 0.05) -> None:
        self.conn = connect_fn()
        self.interval = interval
        self.reaped = 0
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.reaped += reap(self.conn)
            self._stop.wait(self.interval)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join(timeout=5)


def test_slow_handler_succeeds_with_heartbeat(conn, connect_fn):
    job, _ = enqueue(conn, {"slow": True}, max_attempts=1)
    ran = []

    def slow(j):
        ran.append(j.attempts)
        time.sleep(1.2)  # four times the lease

    worker = Worker(conn, slow, visibility_timeout=SHORT, heartbeat_interval=0.1)
    with _ReaperThread(connect_fn) as reaper:
        assert worker.run_once()
    j = get_job(conn, job.id)
    assert j.status == "succeeded" and j.attempts == 1
    assert ran == [1]
    assert reaper.reaped == 0, "lease was extended, so nothing expired"
    assert worker.lease_lost is False


def test_slow_handler_never_acks_without_heartbeat(conn, connect_fn):
    """The failure the chaos harness surfaced: every attempt outlives the lease."""
    job, _ = enqueue(conn, {"slow": True}, max_attempts=1)
    ran = []

    def slow(j):
        ran.append(j.attempts)
        time.sleep(1.2)

    worker = Worker(conn, slow, visibility_timeout=SHORT, heartbeat_interval=0)
    with _ReaperThread(connect_fn) as reaper:
        assert worker.run_once()
    j = get_job(conn, job.id)
    assert ran == [1], "the handler did run to completion"
    assert reaper.reaped == 1, "but the reaper took the lease away mid-run"
    assert j.status == "dead" and j.last_error == "visibility deadline expired"
    assert ack(conn, job.id, 1) is False, "and the late ack is fenced out"
