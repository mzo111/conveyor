"""The client: a connection string in, three verbs out."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from types import TracebackType
from typing import Any

import psycopg
from psycopg.rows import DictRow, dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from conveyor_client import _sql
from conveyor_client.errors import JobNotFound, NotCancellable, SchemaMismatch
from conveyor_client.models import Job, JSONValue, wrap

__all__ = ["Client"]

_MIGRATION = "db/migrations/001_add_cancelled_status.sql"


@contextmanager
def _schema_errors() -> Iterator[None]:
    """Turn schema drift into an actionable error instead of a psycopg traceback.

    The SDK is a client of a database it does not own and never migrates. When
    the two disagree, saying so plainly — with the fix — is the most useful
    thing it can do.
    """
    try:
        yield
    except psycopg.errors.UndefinedTable as exc:
        raise SchemaMismatch(
            f"the 'jobs' table does not exist in this database: {exc}. "
            "Apply db/schema.sql before using conveyor-client."
        ) from exc
    except psycopg.errors.UndefinedColumn as exc:
        raise SchemaMismatch(
            f"the 'jobs' table is missing a column conveyor-client expects: {exc}. "
            "The database schema and this SDK version have diverged."
        ) from exc
    except psycopg.errors.InvalidTextRepresentation as exc:
        # The only enum literal the SDK writes is 'cancelled', and a database
        # created before it was added rejects it here rather than at connect.
        if "job_status" in str(exc):
            raise SchemaMismatch(
                f"this database's job_status enum has no 'cancelled' value. Apply {_MIGRATION}."
            ) from exc
        raise


class Client:
    """A connection-pooled handle on a Conveyor queue.

    Thread-safe and meant to be shared: build one per process, hand it to
    whatever needs to enqueue, and close it on the way out. Each call borrows a
    connection from the pool for exactly one transaction and returns it.

        >>> with Client("postgresql://conveyor:conveyor@localhost:5433/conveyor") as client:
        ...     job = client.enqueue("emails.send_welcome", {"user_id": 42})

    ``max_size`` bounds how many Postgres backends this process can hold at once;
    size it against the database's ``max_connections`` and the number of
    processes sharing it, not against your thread count. Threads queue for a
    connection rather than opening more.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        connect_timeout: float = 10.0,
        application_name: str = "conveyor-client",
    ) -> None:
        """Open the pool. Raises if the database is unreachable within ``connect_timeout``.

        Connecting eagerly is a deliberate choice: a typo in the DSN should fail
        where the DSN is written, not later inside whatever code path happens to
        enqueue first.
        """
        # The annotation carries what the `kwargs` below establish at runtime: a
        # type checker cannot see row_factory through that dict, and without it
        # every fetchone() in this module would statically be a tuple.
        self._pool: ConnectionPool[psycopg.Connection[DictRow]] = ConnectionPool(
            conninfo=dsn,
            min_size=min_size,
            max_size=max_size,
            kwargs={
                "row_factory": dict_row,
                "application_name": application_name,
                "connect_timeout": int(connect_timeout),
            },
            # Hand out a connection only after checking it is alive. A producer
            # can sit idle for hours between enqueues, long enough for an idle
            # timeout or a database restart to have quietly killed the socket.
            check=ConnectionPool.check_connection,
            open=False,
            name=application_name,
        )
        self._pool.open(wait=True, timeout=connect_timeout)

    def enqueue(
        self,
        handler: str,
        payload: JSONValue,
        *,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
        run_at: datetime | None = None,
    ) -> Job:
        """Put a job on the queue and return it.

        ``handler`` is a routing string stored alongside the payload; see
        :mod:`conveyor_client.models` for the exact shape written to the column.
        ``payload`` is any JSON-serialisable value.

        With an ``idempotency_key``, enqueueing is idempotent for the lifetime of
        the row: a second call with the same key returns the existing job and
        creates nothing, whatever state that job is now in — including
        ``succeeded``, ``dead`` and ``cancelled``. This is a guarantee about
        *enqueueing*, not about execution; delivery is at-least-once, so one job
        can still run more than once.

        ``max_attempts`` and ``run_at`` fall back to the queue's own defaults when
        omitted. ``run_at`` must be timezone-aware.
        """
        if not handler:
            raise ValueError("handler must be a non-empty string")
        if idempotency_key is not None and not idempotency_key:
            raise ValueError(
                "idempotency_key must be non-empty; an empty string is a real key "
                "and every caller passing it would collide on the same job"
            )
        if max_attempts is not None and max_attempts < 1:
            raise ValueError(f"max_attempts must be at least 1, got {max_attempts}")
        if run_at is not None and run_at.utcoffset() is None:
            raise ValueError(
                "run_at must be timezone-aware; a naive datetime would be silently "
                "reinterpreted in the database session's time zone"
            )

        params: dict[str, Any] = {
            "idempotency_key": idempotency_key,
            "payload": Jsonb(wrap(handler, payload)),
            "max_attempts": max_attempts,
            "run_at": run_at,
        }
        optional: Sequence[str] = [
            column for column in _sql.OPTIONAL_INSERT_COLUMNS if params[column] is not None
        ]

        with _schema_errors(), self._pool.connection() as conn:
            row = conn.execute(_sql.insert(optional), params).fetchone()
            if row is not None:
                return Job._from_row(row)
            # ON CONFLICT DO NOTHING swallowed the insert, so a job with this key
            # already exists. It cannot have been a generated key (those are
            # random UUIDs), so idempotency_key is not None here.
            row = conn.execute(_sql.SELECT_BY_KEY, params).fetchone()
            if row is None:  # pragma: no cover - the row would have to be deleted mid-call
                raise RuntimeError(
                    f"enqueue conflicted on idempotency_key {idempotency_key!r} but no such "
                    "job exists; it was deleted concurrently"
                )
            return Job._from_row(row)

    def get(self, job_id: int) -> Job:
        """Fetch a job by id. Raises :class:`JobNotFound` if there is no such row.

        The result is a snapshot. A ``queued`` job may already be running by the
        time you read the return value.
        """
        with _schema_errors(), self._pool.connection() as conn:
            row = conn.execute(_sql.SELECT_BY_ID, {"id": job_id}).fetchone()
        if row is None:
            raise JobNotFound(job_id)
        return Job._from_row(row)

    def cancel(self, job_id: int) -> Job:
        """Cancel a queued job so it is never claimed. Returns the cancelled row.

        Only a ``queued`` job can be cancelled. A job already claimed by a worker
        runs to completion: the handler executes outside any transaction and
        there is no channel to interrupt it. If a worker claims the job at the
        same moment you cancel it, exactly one of the two wins — the row lock
        decides — and cancelling the loser raises :class:`NotCancellable`.

        Cancelling an already-cancelled job is not an error; it returns the row.
        A cancellation you have to retry after a network blip should not blow up
        on the second try.

        Raises :class:`JobNotFound` if no such job exists, :class:`NotCancellable`
        if it is running, succeeded or dead.
        """
        with _schema_errors(), self._pool.connection() as conn:
            row = conn.execute(_sql.CANCEL, {"id": job_id}).fetchone()
            if row is not None:
                return Job._from_row(row)
            # Zero rows updated: the job is not queued. One extra read — only on
            # this path — to say which of the three reasons it was.
            row = conn.execute(_sql.SELECT_BY_ID, {"id": job_id}).fetchone()
        if row is None:
            raise JobNotFound(job_id)
        job = Job._from_row(row)
        if job.status == "cancelled":
            return job
        raise NotCancellable(job_id, job.status)

    def close(self) -> None:
        """Close the pool and every connection in it. Idempotent."""
        self._pool.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
