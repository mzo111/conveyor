"""Dead-letter view: list, show, replay; attempts stay monotonic across replay."""

from __future__ import annotations

from datetime import timedelta

from conveyor.deadletter import get_dead, list_dead, replay
from conveyor.queue import ack, claim, enqueue, get_job
from conveyor.worker import Worker


def kill_job(conn, max_attempts: int = 2):
    job, _ = enqueue(conn, {"doomed": True}, max_attempts=max_attempts)

    def boom(j):
        raise RuntimeError(f"attempt {j.attempts} failed")

    worker = Worker(conn, boom, backoff=lambda a: timedelta(0))
    for _ in range(max_attempts):
        assert worker.run_once()
    return job


def test_dead_jobs_are_listed_with_last_error(conn):
    dead = kill_job(conn)
    alive, _ = enqueue(conn, {"fine": True})

    listed = list_dead(conn)
    assert [j.id for j in listed] == [dead.id]
    assert listed[0].last_error == "RuntimeError: attempt 2 failed"
    assert listed[0].attempts == 2

    assert get_dead(conn, dead.id).id == dead.id
    assert get_dead(conn, alive.id) is None

    view = conn.execute("SELECT id, last_error FROM dead_letter").fetchall()
    conn.commit()
    assert [(r["id"], r["last_error"]) for r in view] == [
        (dead.id, "RuntimeError: attempt 2 failed")
    ]


def test_replay_requeues_with_fresh_budget_and_monotonic_attempts(conn):
    dead = kill_job(conn, max_attempts=2)
    assert replay(conn, dead.id) is True

    j = get_job(conn, dead.id)
    assert j.status == "queued"
    assert j.attempts == 2, "attempts is the fencing token; it must not reset"
    assert j.max_attempts == 4, "fresh budget of the original size on top of what was used"
    assert j.finished_at is None
    assert j.last_error == "RuntimeError: attempt 2 failed", "kept until overwritten"

    done = []
    worker = Worker(conn, lambda job: done.append(job.attempts))
    assert worker.run_once()
    j = get_job(conn, dead.id)
    assert j.status == "succeeded" and j.attempts == 3 and done == [3]


def test_replay_extra_attempts_and_non_dead(conn):
    dead = kill_job(conn, max_attempts=1)
    assert replay(conn, dead.id, extra_attempts=5) is True
    assert get_job(conn, dead.id).max_attempts == 6

    assert replay(conn, dead.id) is False, "already queued"
    held = claim(conn, worker_id="w", visibility_timeout=timedelta(seconds=5))
    assert replay(conn, dead.id) is False, "running"
    ack(conn, held.id, held.attempts)
    assert replay(conn, dead.id) is False, "succeeded"
    assert replay(conn, 999_999) is False, "missing"


def test_zombie_ack_from_before_replay_is_rejected(conn):
    dead = kill_job(conn, max_attempts=1)
    replay(conn, dead.id)
    held = claim(conn, worker_id="w2", visibility_timeout=timedelta(seconds=5))
    assert held.attempts == 2
    # A worker that held attempt 1 before the job died comes back now.
    assert ack(conn, dead.id, attempt=1) is False
    assert get_job(conn, dead.id).status == "running"
    assert ack(conn, dead.id, attempt=2) is True
