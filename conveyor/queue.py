"""Queue operations: enqueue, claim, ack, nack, and the retry backoff schedule.

Delivery guarantee
==================
This queue is **at-least-once**. A job's handler may run more than once; it
will never be silently dropped.

Why not effectively-once: the claim transaction commits *before* the handler
runs (the handler runs outside any database transaction, which is what makes
a visibility lease necessary in the first place). Between the handler
finishing its side effects and the ack committing there is a window in which
either of these can happen:

  1. the worker process dies, so the ack never happens; the lease expires, the
     reaper requeues the job, and another worker runs it again; or
  2. the lease expired *while* the handler was still running (job slower than
     the visibility timeout); the reaper requeued it, a second worker claimed
     it, and now two handlers are executing the same job concurrently.

`ack` and `nack` carry the attempt number as a fencing token, so the stale
worker in case 2 cannot mark the job succeeded or failed on top of the newer
attempt; its result is discarded. That protects the queue's bookkeeping, not
the outside world: the duplicate side effects already happened. Effectively-once
would need the handler's side effects to commit atomically with the ack (same
transaction, same database) or a handler that is idempotent on job id. Neither
is provided here.

A consequence of case 2 worth stating on its own: a handler that *always*
outlives its lease is never acked. Every attempt is reaped before it can
finish, every ack is rejected by the fence, and after ``max_attempts`` the
job is ``dead`` with ``last_error='visibility deadline expired'`` even though
its side effects ran every time. The chaos harness surfaced exactly this. The
remedy is lease extension: ``extend_lease`` pushes the deadline forward and the
worker calls it on a heartbeat while the handler runs, fenced by the attempt
number so a worker that already lost its lease cannot revive it. Heartbeating
narrows case 2 to "the worker process stalled for longer than the lease"; it
does not close case 1, so delivery stays at-least-once.

Idempotent enqueue
==================
`enqueue` with the same `idempotency_key` yields the same job row, never two.
That is a guarantee about *enqueueing*, not about *execution*: the one job can
still run twice for the reasons above.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import DictRow

_JOB_COLUMNS = (
    "id, idempotency_key, payload, status, attempts, max_attempts, run_at, "
    "visibility_deadline, claimed_by, last_error, created_at, updated_at, claimed_at, finished_at"
)


@dataclass(frozen=True, slots=True)
class Job:
    id: int
    idempotency_key: str
    payload: Any
    status: str
    attempts: int
    max_attempts: int
    run_at: datetime
    visibility_deadline: datetime | None
    claimed_by: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    claimed_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_row(cls, row: DictRow) -> Job:
        return cls(**row)


def enqueue(
    conn: psycopg.Connection[DictRow],
    payload: Any,
    *,
    idempotency_key: str | None = None,
    max_attempts: int = 5,
    run_at: datetime | None = None,
) -> tuple[Job, bool]:
    """Insert a job. Returns ``(job, created)``.

    With an ``idempotency_key`` the insert is idempotent: a second call with the
    same key returns the existing row (whatever its status, even ``succeeded``
    or ``dead``) and ``created=False``. Dedup scope is the lifetime of the row.

    Concurrency: two transactions inserting the same key at once are serialized
    by the unique index. The loser blocks until the winner commits, then takes
    the ``DO NOTHING`` branch, then its follow-up SELECT (a fresh READ COMMITTED
    snapshot) sees the winner's committed row.
    """
    with conn.transaction():
        cur = conn.execute(
            f"""
            INSERT INTO jobs (idempotency_key, payload, max_attempts, run_at)
            VALUES (COALESCE(%s, gen_random_uuid()::text), %s, %s, COALESCE(%s, now()))
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING {_JOB_COLUMNS}
            """,
            (idempotency_key, json.dumps(payload), max_attempts, run_at),
        )
        row = cur.fetchone()
        if row is not None:
            return Job.from_row(row), True
        cur = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE idempotency_key = %s", (idempotency_key,)
        )
        row = cur.fetchone()
        assert row is not None, "unique conflict but no row: key was deleted concurrently"
        return Job.from_row(row), False


def claim(
    conn: psycopg.Connection[DictRow],
    *,
    worker_id: str,
    visibility_timeout: timedelta,
) -> Job | None:
    """Atomically take the oldest claimable job and lease it until now()+timeout.

    Why two concurrent workers can never claim the same job
    ---------------------------------------------------------
    Each call is one transaction containing one statement. The inner SELECT
    takes a row-level lock (``FOR UPDATE``) on the tuple it returns, and that
    lock is held until this transaction commits. Snapshot isolation on its own
    would *not* be enough: under READ COMMITTED two transactions can both have
    snapshots in which the same row is ``queued``. The row lock is what turns
    "both saw it" into "only one gets it".

    Without ``SKIP LOCKED`` the second worker would block on that lock, then
    (after the first committed) re-evaluate the row, find ``status='running'``,
    and move on. That is still correct, just serialized. ``SKIP LOCKED`` makes
    the second worker treat the locked row as if it did not exist and continue
    down the index to the next candidate, so claims are non-blocking.
    ``LIMIT 1`` is applied as rows are locked, so exactly one row is locked.

    The UPDATE in the same transaction sets ``status='running'``; once we
    commit, the row no longer matches ``status='queued'`` for anyone, and the
    lock is released. The handler runs *after* this commit, outside any
    transaction, protected only by the lease. That is the at-least-once window
    described in the module docstring.
    """
    with conn.transaction():
        cur = conn.execute(
            f"""
            WITH candidate AS (
                SELECT id
                FROM jobs
                WHERE status = 'queued' AND run_at <= now()
                ORDER BY run_at, id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE jobs AS j
            SET status = 'running',
                attempts = j.attempts + 1,
                visibility_deadline = now() + %(timeout)s,
                claimed_by = %(worker)s,
                claimed_at = now(),
                updated_at = now()
            FROM candidate
            WHERE j.id = candidate.id
            RETURNING {", ".join("j." + c.strip() for c in _JOB_COLUMNS.split(","))}
            """,
            {"timeout": visibility_timeout, "worker": worker_id},
        )
        row = cur.fetchone()
    return Job.from_row(row) if row is not None else None


def ack(conn: psycopg.Connection[DictRow], job_id: int, attempt: int) -> bool:
    """Mark attempt ``attempt`` of ``job_id`` succeeded.

    Returns False if the lease was lost: the row is no longer ``running`` at
    that attempt number (the reaper requeued it, and possibly someone else has
    already claimed attempt+1). The caller's work is *not* undone; see the
    module docstring.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = 'succeeded',
                visibility_deadline = NULL,
                finished_at = now(),
                updated_at = now()
            WHERE id = %s AND status = 'running' AND attempts = %s
            """,
            (job_id, attempt),
        )
        return cur.rowcount == 1


def nack(
    conn: psycopg.Connection[DictRow],
    job_id: int,
    attempt: int,
    *,
    error: str,
    retry_in: timedelta,
) -> bool:
    """Record a failed attempt.

    Requeues the job with ``run_at = now() + retry_in`` unless this was the
    last permitted attempt, in which case the job becomes ``dead`` (terminal).
    Same fencing as ``ack``; returns False if the lease was lost.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 'dead'::job_status
                              ELSE 'queued'::job_status END,
                run_at = CASE WHEN attempts >= max_attempts THEN run_at
                              ELSE now() + %s END,
                finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                visibility_deadline = NULL,
                last_error = %s,
                updated_at = now()
            WHERE id = %s AND status = 'running' AND attempts = %s
            """,
            (retry_in, error, job_id, attempt),
        )
        return cur.rowcount == 1


def extend_lease(
    conn: psycopg.Connection[DictRow],
    job_id: int,
    attempt: int,
    *,
    visibility_timeout: timedelta,
) -> bool:
    """Push the lease of attempt ``attempt`` forward to now()+timeout (heartbeat).

    Fenced exactly like ``ack``: the row must still be ``running`` at this
    attempt number. A worker whose lease already expired and was reaped (or
    reclaimed by someone else at attempt+1) gets False and cannot extend a
    lease it no longer holds. Returns True if the deadline moved.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            UPDATE jobs
            SET visibility_deadline = now() + %s,
                updated_at = now()
            WHERE id = %s AND status = 'running' AND attempts = %s
            """,
            (visibility_timeout, job_id, attempt),
        )
        return cur.rowcount == 1


def get_job(conn: psycopg.Connection[DictRow], job_id: int) -> Job | None:
    with conn.transaction():
        row = conn.execute(f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,)).fetchone()
    return Job.from_row(row) if row is not None else None


def backoff_delay(
    attempt: int,
    *,
    base: float = 1.0,
    factor: float = 2.0,
    cap: float = 300.0,
    jitter: float = 0.0,
) -> timedelta:
    """Delay before retrying after failed attempt number ``attempt`` (1-based).

    Schedule: ``min(base * factor**(attempt-1), cap)`` seconds, so with the
    defaults 1s, 2s, 4s, 8s, ... capped at 5 minutes. ``jitter`` is a fraction
    of the delay added uniformly at random (0.0 = deterministic; tests rely on
    that). Jitter is what stops a burst of failures retrying in lockstep.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    seconds = min(base * factor ** (attempt - 1), cap)
    if jitter:
        seconds += random.uniform(0, seconds * jitter)
    return timedelta(seconds=seconds)
