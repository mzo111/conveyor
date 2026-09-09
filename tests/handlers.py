"""Handlers importable by the worker subprocess in test_worker.py."""

from __future__ import annotations

import time

from conveyor.queue import Job


def slow(job: Job) -> None:
    time.sleep(job.payload.get("sleep", 1.0))


def fail(job: Job) -> None:
    raise RuntimeError(f"boom {job.payload}")
