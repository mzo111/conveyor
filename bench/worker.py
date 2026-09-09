"""Timed worker process for the load test. Not part of the product.

This mirrors ``Worker.run_once`` in ``conveyor/worker.py`` and calls the same
``conveyor.queue`` functions, but times ``claim`` and ``ack`` separately, which
the real class does not expose. The loop below and the real one must stay in
step; ``bench.load`` runs a fidelity cross-check (one sweep point measured with
this module and again with ``python -m conveyor.worker``) so a divergence shows
up as a number in the report rather than as a silent lie.

Samples are written to ``--out`` as JSON when the process stops, rather than
streamed, so measurement never blocks on a pipe.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import timedelta

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect
from conveyor.queue import Job, ack, claim, extend_lease


class BenchWorker:
    def __init__(
        self,
        conn: psycopg.Connection[DictRow],
        *,
        worker_id: str,
        visibility_timeout: timedelta,
        handler_sleep: float,
        heartbeat_interval: float,
        poll_interval: float,
    ) -> None:
        self.conn = conn
        self.worker_id = worker_id
        self.visibility_timeout = visibility_timeout
        self.handler_sleep = handler_sleep
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        # Samples, in seconds. Kept as plain lists of floats: appending is cheap
        # enough not to perturb what we are measuring.
        self.claim_latencies: list[float] = []
        self.ack_latencies: list[float] = []
        self.processed = 0
        self.empty_claims = 0
        self.extends = 0
        self.lease_lost = 0
        self.ack_rejected = 0

    def stop(self) -> None:
        self._stop.set()

    def _heartbeat(self, job: Job, stop: threading.Event) -> None:
        """Mirror of ``Worker._heartbeat``, including sharing the one connection.

        Sharing matters for the measurement: psycopg serializes statements on a
        connection, so an in-flight extension delays the main thread's next
        query. That cost is part of what the heartbeat experiment measures.
        """
        while not stop.wait(self.heartbeat_interval):
            try:
                ok = extend_lease(
                    self.conn, job.id, job.attempts, visibility_timeout=self.visibility_timeout
                )
            except psycopg.Error:
                continue
            self.extends += 1
            if not ok:
                self.lease_lost += 1
                return

    def run(self) -> None:
        while not self._stop.is_set():
            t0 = time.perf_counter()
            job = claim(
                self.conn, worker_id=self.worker_id, visibility_timeout=self.visibility_timeout
            )
            self.claim_latencies.append(time.perf_counter() - t0)
            if job is None:
                self.empty_claims += 1
                self._stop.wait(self.poll_interval)
                continue

            heartbeat_stop = threading.Event()
            heartbeat: threading.Thread | None = None
            if self.heartbeat_interval > 0:
                heartbeat = threading.Thread(
                    target=self._heartbeat, args=(job, heartbeat_stop), daemon=True
                )
                heartbeat.start()

            if self.handler_sleep:
                time.sleep(self.handler_sleep)

            heartbeat_stop.set()
            if heartbeat is not None:
                heartbeat.join()

            t0 = time.perf_counter()
            ok = ack(self.conn, job.id, job.attempts)
            self.ack_latencies.append(time.perf_counter() - t0)
            if not ok:
                self.ack_rejected += 1
            self.processed += 1

    def samples(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "pid": os.getpid(),
            "processed": self.processed,
            "empty_claims": self.empty_claims,
            "extends": self.extends,
            "lease_lost": self.lease_lost,
            "ack_rejected": self.ack_rejected,
            "claim_latencies": self.claim_latencies,
            "ack_latencies": self.ack_latencies,
        }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worker-id", required=True)
    p.add_argument("--out", required=True, help="write samples here as JSON on exit")
    p.add_argument("--dsn", default=None)
    p.add_argument("--visibility-timeout", type=float, default=30.0)
    p.add_argument("--handler-sleep", type=float, default=0.0)
    p.add_argument("--heartbeat-interval", type=float, default=0.0, help="0 disables")
    p.add_argument("--poll-interval", type=float, default=0.02)
    p.add_argument(
        "--sync-commit",
        default="on",
        help="session-level synchronous_commit; 'off' is the labeled diagnostic only",
    )
    args = p.parse_args(argv)

    with connect(args.dsn) as conn:
        if args.sync_commit != "on":
            conn.execute(f"SET synchronous_commit = {args.sync_commit}")
            conn.commit()
        worker = BenchWorker(
            conn,
            worker_id=args.worker_id,
            visibility_timeout=timedelta(seconds=args.visibility_timeout),
            handler_sleep=args.handler_sleep,
            heartbeat_interval=args.heartbeat_interval,
            poll_interval=args.poll_interval,
        )
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: worker.stop())
        try:
            worker.run()
        finally:
            with open(args.out, "w") as fh:
                json.dump(worker.samples(), fh)
    return 0


if __name__ == "__main__":
    sys.exit(main())
