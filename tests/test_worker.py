"""Graceful shutdown: finish the current job, touch nothing else, exit 0."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import timedelta

from conveyor.queue import enqueue, get_job
from conveyor.worker import Worker


def test_stop_mid_job_finishes_it_and_leaves_the_rest(conn):
    first, _ = enqueue(conn, {"n": 1})
    second, _ = enqueue(conn, {"n": 2})
    started = threading.Event()

    def slow(job):
        started.set()
        time.sleep(0.5)

    worker = Worker(conn, slow, poll_interval=0.01)
    t = threading.Thread(target=worker.run)
    t.start()
    assert started.wait(5)
    assert get_job(conn, first.id).status == "running"

    worker.stop()  # arrives mid-job
    t.join(timeout=5)
    assert not t.is_alive()

    assert get_job(conn, first.id).status == "succeeded"
    assert get_job(conn, second.id).status == "queued", "stop must not claim or release others"
    assert worker.processed == 1


def test_stop_while_idle_returns_promptly(conn):
    worker = Worker(conn, lambda job: None, poll_interval=30)
    t = threading.Thread(target=worker.run)
    t.start()
    time.sleep(0.2)
    t0 = time.monotonic()
    worker.stop()
    t.join(timeout=5)
    assert not t.is_alive()
    assert time.monotonic() - t0 < 2, "stop must interrupt the poll sleep"


def test_sigterm_subprocess_finishes_job_and_exits_zero(conn, dsn):
    first, _ = enqueue(conn, {"sleep": 1.0})
    second, _ = enqueue(conn, {"sleep": 0.0})
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "conveyor.worker",
            "--handler",
            "tests.handlers:slow",
            "--worker-id",
            "sub",
            "--poll-interval",
            "0.05",
        ],
        env={**os.environ, "DATABASE_URL": dsn},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while get_job(conn, first.id).status != "running":
            assert time.monotonic() < deadline, "worker never claimed the job"
            assert proc.poll() is None, proc.stdout.read()
            time.sleep(0.05)

        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=15)
    finally:
        if proc.poll() is None:
            proc.kill()

    assert proc.returncode == 0, out
    assert get_job(conn, first.id).status == "succeeded", out
    assert get_job(conn, second.id).status == "queued", out
    assert "stopped after 1 job(s)" in out


def test_failing_handler_in_subprocess_nacks(conn, dsn):
    job, _ = enqueue(conn, {"k": "v"}, max_attempts=1)
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from conveyor.worker import Worker; from conveyor.db import connect;"
                "from tests.handlers import fail;"
                "c = connect(); w = Worker(c, fail); print(w.run_once())"
            ),
        ],
        env={**os.environ, "DATABASE_URL": dsn},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    j = get_job(conn, job.id)
    assert j.status == "dead" and j.last_error == "RuntimeError: boom {'k': 'v'}"
    assert j.visibility_deadline is None
    assert timedelta(0) <= (j.finished_at - j.claimed_at) < timedelta(seconds=10)
