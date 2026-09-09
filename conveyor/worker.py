"""Worker loop: claim, execute, ack or nack, repeat.

Usage: ``python -m conveyor.worker --handler package.module:function``

The handler is called as ``handler(job)``. Returning normally acks the job;
raising nacks it with exponential backoff until ``max_attempts``, after which
the job is ``dead``.

Shutdown: SIGTERM or SIGINT sets a stop flag and nothing else. The job in
flight (if any) runs to completion and is acked or nacked as usual; then the
loop exits and the process returns 0. No other job is touched: a worker holds
at most one claimed job at a time, so there is nothing else to release.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import signal
import sys
import threading
import uuid
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import psycopg
from psycopg.rows import DictRow

from conveyor.db import connect
from conveyor.queue import Job, ack, backoff_delay, claim, extend_lease, nack

log = logging.getLogger(__name__)

Handler = Callable[[Job], Any]
Backoff = Callable[[int], timedelta]


class Worker:
    def __init__(
        self,
        conn: psycopg.Connection[DictRow],
        handler: Handler,
        *,
        worker_id: str | None = None,
        visibility_timeout: timedelta = timedelta(seconds=30),
        poll_interval: float = 0.5,
        backoff: Backoff = backoff_delay,
        heartbeat_interval: float | None = None,
    ) -> None:
        """``heartbeat_interval``: seconds between lease extensions while a handler
        runs. None = a third of the visibility timeout; 0 disables heartbeating,
        in which case a handler slower than the lease is reaped mid-run."""
        self.conn = conn
        self.handler = handler
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.visibility_timeout = visibility_timeout
        self.poll_interval = poll_interval
        self.backoff = backoff
        if heartbeat_interval is None:
            heartbeat_interval = visibility_timeout.total_seconds() / 3
        self.heartbeat_interval = heartbeat_interval
        self._stop = threading.Event()
        self.processed = 0
        self.lease_lost = False  # set by the heartbeat when an extension is rejected

    def stop(self) -> None:
        """Request a graceful stop. Safe to call from any thread or a signal handler."""
        self._stop.set()

    def install_signal_handlers(self) -> None:
        """Route SIGTERM/SIGINT to ``stop()``. Must be called from the main thread."""
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: self.stop())

    def run_once(self) -> bool:
        """Claim and process at most one job. Returns True if a job was processed."""
        job = claim(self.conn, worker_id=self.worker_id, visibility_timeout=self.visibility_timeout)
        if job is None:
            return False
        self.processed += 1
        # The claim has committed; from here until ack/nack commits, the lease is
        # the only thing keeping other workers off this job. The heartbeat keeps
        # that lease alive for as long as this process is alive and running the
        # handler; it stops before ack/nack so the two never race on the row.
        heartbeat_stop = threading.Event()
        heartbeat: threading.Thread | None = None
        self.lease_lost = False
        if self.heartbeat_interval > 0:
            heartbeat = threading.Thread(
                target=self._heartbeat, args=(job, heartbeat_stop), daemon=True
            )
            heartbeat.start()
        try:
            self.handler(job)
        except Exception as exc:  # noqa: BLE001 - any handler failure is a nack
            self._stop_heartbeat(heartbeat, heartbeat_stop)
            delay = self.backoff(job.attempts)
            error = f"{type(exc).__name__}: {exc}"
            if nack(self.conn, job.id, job.attempts, error=error, retry_in=delay):
                log.warning(
                    "job %d attempt %d/%d failed: %s (retry in %s)",
                    job.id,
                    job.attempts,
                    job.max_attempts,
                    error,
                    delay,
                )
            else:
                log.error(
                    "job %d attempt %d failed, lease lost; result discarded",
                    job.id,
                    job.attempts,
                )
            return True
        self._stop_heartbeat(heartbeat, heartbeat_stop)
        if ack(self.conn, job.id, job.attempts):
            log.info("job %d attempt %d succeeded", job.id, job.attempts)
        else:
            log.error(
                "job %d attempt %d succeeded but lease lost; it will run again",
                job.id,
                job.attempts,
            )
        return True

    def _heartbeat(self, job: Job, stop: threading.Event) -> None:
        """Extend the lease every ``heartbeat_interval`` seconds until told to stop.

        psycopg 3 connections are thread-safe, and the main thread does not
        touch ``self.conn`` while the handler runs, so sharing it is fine. A
        rejected extension means the lease is already gone (reaped or
        reclaimed); nothing can be done about the handler in flight, so we
        record it, stop heartbeating, and let ack/nack report the loss.
        """
        while not stop.wait(self.heartbeat_interval):
            try:
                ok = extend_lease(
                    self.conn, job.id, job.attempts, visibility_timeout=self.visibility_timeout
                )
            except psycopg.Error as exc:
                log.warning("job %d heartbeat failed: %s", job.id, exc)
                continue
            if not ok:
                self.lease_lost = True
                log.error("job %d attempt %d: lease lost; heartbeat stopped", job.id, job.attempts)
                return

    @staticmethod
    def _stop_heartbeat(thread: threading.Thread | None, stop: threading.Event) -> None:
        stop.set()
        if thread is not None:
            thread.join()

    def run(self) -> None:
        """Loop until ``stop()``. The stop flag is checked only between jobs."""
        log.info("%s starting", self.worker_id)
        while not self._stop.is_set():
            if not self.run_once():
                # Event.wait returns immediately once stop() is called, so a
                # signal during an idle poll does not wait out the interval.
                self._stop.wait(self.poll_interval)
        log.info("%s stopped after %d job(s)", self.worker_id, self.processed)


def load_handler(spec: str) -> Handler:
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise SystemExit(f"--handler must look like package.module:function, got {spec!r}")
    return getattr(importlib.import_module(module_name), attr)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Conveyor worker")
    parser.add_argument("--handler", required=True, help="package.module:function")
    parser.add_argument("--dsn", default=None, help="defaults to $DATABASE_URL")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--visibility-timeout", type=float, default=30.0, help="seconds")
    parser.add_argument("--poll-interval", type=float, default=0.5, help="seconds")
    parser.add_argument(
        "--backoff-base", type=float, default=1.0, help="first retry delay, seconds"
    )
    parser.add_argument("--backoff-cap", type=float, default=300.0, help="max retry delay, seconds")
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=None,
        help="seconds between lease extensions while a job runs "
        "(default: visibility-timeout / 3; 0 disables)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    handler = load_handler(args.handler)
    with connect(args.dsn) as conn:
        worker = Worker(
            conn,
            handler,
            worker_id=args.worker_id,
            visibility_timeout=timedelta(seconds=args.visibility_timeout),
            poll_interval=args.poll_interval,
            backoff=lambda attempt: backoff_delay(
                attempt, base=args.backoff_base, cap=args.backoff_cap
            ),
            heartbeat_interval=args.heartbeat_interval,
        )
        worker.install_signal_handlers()
        worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
