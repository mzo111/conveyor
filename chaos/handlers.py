"""Handlers run inside the chaos workers. Loaded via ``--handler chaos.handlers:<name>``.

Both handlers do the same work in three separate commits on their own
connection, so a SIGKILL can land between any two of them:

1. record ``started`` in ``chaos_executions``
2. sleep ``pre``; raise if the job is configured to fail; otherwise apply the
   side effect (one row in ``chaos_effects``) and record ``effect_at``
3. sleep ``post``; record ``finished_at``; return so the worker acks

The only difference: ``idempotent`` guards the effect with a unique key row
inserted in the same transaction (``chaos_effect_keys``), so a retry after a
kill finds the key and inserts nothing. ``naive`` inserts unconditionally.

Phase transitions are printed to stdout as ``CHAOS job=<id> attempt=<n>
phase=<started|effect_done|finished>`` so the harness knows where a worker is
when it decides to kill it.
"""

from __future__ import annotations

import os
import time

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect
from conveyor.queue import Job

_conn: psycopg.Connection[DictRow] | None = None


class ChaosFailure(Exception):
    """Raised by jobs configured to fail; ends in the dead-letter view."""


def _connection() -> psycopg.Connection[DictRow]:
    global _conn
    if _conn is None or _conn.closed:
        _conn = connect()
    return _conn


def _say(job: Job, phase: str) -> None:
    print(f"CHAOS job={job.id} attempt={job.attempts} phase={phase}", flush=True)


def _execute(job: Job, *, idempotent: bool) -> None:
    p = job.payload
    run_id, worker, pid = p["run_id"], job.claimed_by, os.getpid()
    conn = _connection()

    with conn.transaction():
        conn.execute(
            "INSERT INTO chaos_executions (run_id, job_id, attempt, worker, pid) "
            "VALUES (%s, %s, %s, %s, %s)",
            (run_id, job.id, job.attempts, worker, pid),
        )
    _say(job, "started")

    time.sleep(p["pre"])
    if p["fail"]:
        raise ChaosFailure(f"job {p['i']} is configured to fail (attempt {job.attempts})")

    with conn.transaction():
        if idempotent:
            # The idempotency mechanism: claim the key and write the effect in
            # one transaction. Either both commit or neither does, and a second
            # execution of the same job finds the key and writes nothing.
            inserted = (
                conn.execute(
                    "INSERT INTO chaos_effect_keys (run_id, job_id) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING RETURNING job_id",
                    (run_id, job.id),
                ).fetchone()
                is not None
            )
        else:
            inserted = True
        if inserted:
            conn.execute(
                "INSERT INTO chaos_effects (run_id, job_id, attempt, worker) "
                "VALUES (%s, %s, %s, %s)",
                (run_id, job.id, job.attempts, worker),
            )
        conn.execute(
            "UPDATE chaos_executions SET effect_at = clock_timestamp(), effect_inserted = %s "
            "WHERE run_id = %s AND job_id = %s AND attempt = %s",
            (inserted, run_id, job.id, job.attempts),
        )
    _say(job, "effect_done")

    # Overrun jobs outlive their lease on attempt 1 only. That forces the
    # reaper to requeue a job whose handler is still running (docstring case 2
    # in conveyor.queue) while leaving later attempts able to finish inside the
    # lease. A handler that *always* outlives its lease can never be acked and
    # dies after max_attempts; that is a documented limitation, not this test.
    post = p["overrun_post"] if p["overrun"] and job.attempts == 1 else p["post"]
    time.sleep(post)
    with conn.transaction():
        conn.execute(
            "UPDATE chaos_executions SET finished_at = clock_timestamp() "
            "WHERE run_id = %s AND job_id = %s AND attempt = %s",
            (run_id, job.id, job.attempts),
        )
    _say(job, "finished")


def naive(job: Job) -> None:
    _execute(job, idempotent=False)


def idempotent(job: Job) -> None:
    _execute(job, idempotent=True)
