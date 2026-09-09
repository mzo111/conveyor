"""Exceptions raised by this SDK.

Only *this SDK's* failures are wrapped. Database failures — a dropped
connection, a full disk, a permissions error — propagate as the underlying
``psycopg.Error`` unchanged, on purpose: see the "does not retry" note in the
README. Catch ``ConveyorError`` for "the queue said no", ``psycopg.Error`` for
"the database said no".
"""

from __future__ import annotations


class ConveyorError(Exception):
    """Base class for every error this SDK raises itself."""


class JobNotFound(ConveyorError):
    """No job with this id exists.

    Note that ids are never reused, but rows are not immortal either: a job
    purged from the table is indistinguishable from one that never existed.
    """

    def __init__(self, job_id: int) -> None:
        super().__init__(f"no job with id {job_id}")
        self.job_id = job_id


class NotCancellable(ConveyorError):
    """The job exists but is past the point where cancelling means anything.

    Only a ``queued`` job can be cancelled. Once claimed, the handler is running
    outside any transaction with no channel to interrupt it, so the job will run
    to completion; ``succeeded``, ``dead`` and cancellation itself are terminal.
    (Cancelling an already-``cancelled`` job is *not* an error — see
    ``Client.cancel``.)
    """

    def __init__(self, job_id: int, status: str) -> None:
        super().__init__(
            f"job {job_id} is {status!r}, not 'queued'; only a queued job can be cancelled"
        )
        self.job_id = job_id
        self.status = status


class SchemaMismatch(ConveyorError):
    """The database does not have the shape this version of the SDK expects.

    This is the seam that keeps the queue's internals free to move. The SDK
    never migrates the database it is pointed at; it fails here instead, with
    the fix in the message.
    """
