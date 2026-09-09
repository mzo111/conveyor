"""Return jobs whose visibility lease has expired to the queue.

Usage: ``python -m conveyor.reaper [--interval 5]``
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect

log = logging.getLogger(__name__)


def reap_expired(conn: psycopg.Connection[DictRow]) -> list[DictRow]:
    """Requeue every ``running`` job whose lease expired. Returns the affected rows.

    Each row has ``id``, ``attempts`` and the new ``status`` (``queued`` or
    ``dead``), so callers such as the chaos harness can log exactly which
    leases were reaped and when.

    The expired attempt already counted (``attempts`` is bumped at claim), so
    a job whose last permitted attempt timed out goes straight to ``dead``
    rather than being requeued forever.

    Race with ``ack``/``nack``: both are UPDATEs on the same row. Row locking
    serializes them, and under READ COMMITTED the loser re-checks its WHERE
    clause against the winner's committed version. If the reaper wins, the
    row is ``queued`` (or ``dead``) and the worker's ack matches zero rows and
    returns False. If the worker wins, the row is no longer ``running`` and the
    reaper matches zero rows. Exactly one wins; there is no state in which
    both apply. When the reaper wins, the job will execute again: that is the
    at-least-once window from ``conveyor.queue``, made concrete.
    """
    with conn.transaction():
        cur = conn.execute(
            """
            UPDATE jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 'dead'::job_status
                              ELSE 'queued'::job_status END,
                finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                visibility_deadline = NULL,
                last_error = 'visibility deadline expired',
                updated_at = now()
            WHERE status = 'running' AND visibility_deadline < now()
            RETURNING id, attempts, status, clock_timestamp() AS reaped_at
            """
        )
        return cur.fetchall()


def reap(conn: psycopg.Connection[DictRow]) -> int:
    """Requeue every ``running`` job whose lease expired. Returns the count."""
    return len(reap_expired(conn))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="defaults to $DATABASE_URL")
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between sweeps")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    with connect(args.dsn) as conn:
        while not stop.is_set():
            n = reap(conn)
            if n:
                log.info("reaped %d expired job(s)", n)
            stop.wait(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
