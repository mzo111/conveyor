"""Client SDK for the Conveyor job queue.

Enqueue, inspect and cancel jobs over a connection string. This package talks to
the queue's Postgres schema directly and imports nothing from the queue's own
modules, so the worker, reaper and lease protocol stay free to change underneath
it.

    from conveyor_client import Client

    with Client("postgresql://conveyor:conveyor@localhost:5433/conveyor") as client:
        job = client.enqueue("emails.send_welcome", {"user_id": 42})

What it does not do — and why — is documented in the README. The short version:
it produces jobs, it does not consume them.
"""

from conveyor_client.client import Client
from conveyor_client.errors import (
    ConveyorError,
    JobNotFound,
    NotCancellable,
    SchemaMismatch,
)
from conveyor_client.models import Job, JobStatus, JSONValue

__all__ = [
    "Client",
    "ConveyorError",
    "JSONValue",
    "Job",
    "JobNotFound",
    "JobStatus",
    "NotCancellable",
    "SchemaMismatch",
]

__version__ = "0.1.0"
