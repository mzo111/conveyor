"""Handlers for the product worker when it is used in the fidelity cross-check."""

from __future__ import annotations

import time

from conveyor.queue import Job

SLEEP = 0.0


def noop(job: Job) -> None:
    """Do nothing, so the measurement is of the queue and not of the work."""


def sleeper(job: Job) -> None:
    time.sleep(SLEEP)
