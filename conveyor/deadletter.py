"""Dead-letter view: jobs that exhausted their attempts, inspectable and replayable.

Usage::

    python -m conveyor.deadletter list [--limit N]
    python -m conveyor.deadletter show <id>
    python -m conveyor.deadletter replay <id> [--extra-attempts N]

The SQL view ``dead_letter`` in ``db/schema.sql`` exposes the same rows to psql.
"""

from __future__ import annotations

import argparse
import json
import sys

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect
from conveyor.queue import _JOB_COLUMNS, Job


def list_dead(conn: psycopg.Connection[DictRow], *, limit: int = 50) -> list[Job]:
    with conn.transaction():
        rows = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE status = 'dead' "
            "ORDER BY finished_at DESC, id DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [Job.from_row(r) for r in rows]


def get_dead(conn: psycopg.Connection[DictRow], job_id: int) -> Job | None:
    with conn.transaction():
        row = conn.execute(
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE id = %s AND status = 'dead'", (job_id,)
        ).fetchone()
    return Job.from_row(row) if row is not None else None


def replay(
    conn: psycopg.Connection[DictRow], job_id: int, *, extra_attempts: int | None = None
) -> bool:
    """Put a dead job back in the queue with a fresh attempt budget.

    Returns False if the job is not ``dead`` (never replay something that is
    queued, running, or already succeeded).

    ``attempts`` is deliberately *not* reset. It is the fencing token that
    ``ack``/``nack`` check, and it must stay unique over the job's whole life:
    if a zombie worker from before the job died is still holding attempt k and
    acks late, a reset would let that ack match a fresh attempt k and mark work
    it did not do as succeeded. Keeping the counter monotonic makes every old
    token permanently stale. The budget is extended instead:
    ``max_attempts = attempts + extra_attempts`` (default: the original budget).
    ``last_error`` is kept for diagnosis until the next attempt overwrites it.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                run_at = now(),
                finished_at = NULL,
                max_attempts = attempts + COALESCE(%s, max_attempts),
                updated_at = now()
            WHERE id = %s AND status = 'dead'
            """,
            (extra_attempts, job_id),
        )
        return cur.rowcount == 1


def _print_job(job: Job) -> None:
    print(f"job {job.id}  attempts {job.attempts}/{job.max_attempts}  claimed_by {job.claimed_by}")
    print(f"  key:        {job.idempotency_key}")
    print(f"  created:    {job.created_at:%Y-%m-%d %H:%M:%S%z}")
    print(f"  finished:   {job.finished_at:%Y-%m-%d %H:%M:%S%z}" if job.finished_at else "")
    print(f"  last_error: {job.last_error}")
    print(f"  payload:    {json.dumps(job.payload)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Conveyor dead-letter view")
    parser.add_argument("--dsn", default=None, help="defaults to $DATABASE_URL")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_list = sub.add_parser("list", help="most recently dead jobs")
    p_list.add_argument("--limit", type=int, default=50)
    p_show = sub.add_parser("show", help="one dead job in full")
    p_show.add_argument("id", type=int)
    p_replay = sub.add_parser("replay", help="requeue a dead job")
    p_replay.add_argument("id", type=int)
    p_replay.add_argument("--extra-attempts", type=int, default=None)
    args = parser.parse_args(argv)

    with connect(args.dsn) as conn:
        if args.cmd == "list":
            jobs = list_dead(conn, limit=args.limit)
            if not jobs:
                print("no dead jobs")
            for job in jobs:
                err = (job.last_error or "").splitlines()[0][:80] if job.last_error else ""
                print(f"{job.id:>8}  {job.attempts}/{job.max_attempts}  {err}")
            return 0
        if args.cmd == "show":
            job = get_dead(conn, args.id)
            if job is None:
                print(f"job {args.id} is not dead", file=sys.stderr)
                return 1
            _print_job(job)
            return 0
        if replay(conn, args.id, extra_attempts=args.extra_attempts):
            print(f"job {args.id} requeued")
            return 0
        print(f"job {args.id} is not dead; nothing to replay", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
