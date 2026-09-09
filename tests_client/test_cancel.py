"""Cancel: the state transition, what it excludes, and what it refuses."""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from conveyor_client import Client, JobNotFound, NotCancellable


def test_cancels_a_queued_job(client: Client) -> None:
    job = client.enqueue("h", {})
    cancelled = client.cancel(job.id)

    assert cancelled.status == "cancelled"
    assert cancelled.finished_at is not None
    assert cancelled.id == job.id
    assert client.get(job.id).status == "cancelled"


def test_a_cancelled_job_is_no_longer_claimable(client: Client, raw: psycopg.Connection) -> None:
    """The real guarantee: it drops out of the predicate conveyor's claim uses."""
    keep = client.enqueue("h", {"keep": True})
    drop = client.enqueue("h", {"drop": True})
    client.cancel(drop.id)

    claimable = raw.execute(
        "SELECT id FROM jobs WHERE status = 'queued' AND run_at <= now() "
        "ORDER BY run_at, id FOR UPDATE SKIP LOCKED"
    ).fetchall()
    assert [r["id"] for r in claimable] == [keep.id]


def test_cancelling_preserves_last_error(client: Client, raw: psycopg.Connection) -> None:
    """A job cancelled after a few failures keeps the diagnosis of why it failed."""
    job = client.enqueue("h", {})
    raw.execute("UPDATE jobs SET last_error = 'RuntimeError: boom' WHERE id = %s", (job.id,))

    assert client.cancel(job.id).last_error == "RuntimeError: boom"


def test_cancel_is_idempotent(client: Client) -> None:
    job = client.enqueue("h", {})
    first = client.cancel(job.id)
    second = client.cancel(job.id)

    assert second.status == "cancelled"
    assert second.finished_at == first.finished_at, "the second call must not rewrite the row"


def test_cancelling_a_scheduled_job(client: Client) -> None:
    """The main reason cancel exists: calling off work queued for later."""
    job = client.enqueue("h", {}, run_at=datetime(2999, 1, 1, tzinfo=UTC))
    assert client.cancel(job.id).status == "cancelled"


def test_missing_job(client: Client) -> None:
    with pytest.raises(JobNotFound):
        client.cancel(999_999)


@pytest.mark.parametrize("status", ["running", "succeeded", "dead"])
def test_refuses_anything_not_queued(client: Client, raw: psycopg.Connection, status: str) -> None:
    job = client.enqueue("h", {})
    # 'running' has to satisfy the schema's CHECK that a lease exists iff running.
    deadline = "now() + interval '30 seconds'" if status == "running" else "NULL"
    raw.execute(
        f"UPDATE jobs SET status = %s, visibility_deadline = {deadline} WHERE id = %s",
        (status, job.id),
    )

    with pytest.raises(NotCancellable) as excinfo:
        client.cancel(job.id)
    assert excinfo.value.status == status
    assert client.get(job.id).status == status, "a refused cancel must change nothing"


def test_cancelling_does_not_dead_letter(client: Client, raw: psycopg.Connection) -> None:
    """Cancelled is not dead: it stays out of the dead_letter view, so it is not
    something `python -m conveyor.deadletter replay` will hand back to a worker."""
    job = client.enqueue("h", {})
    client.cancel(job.id)

    assert raw.execute("SELECT count(*) AS n FROM dead_letter").fetchone()["n"] == 0
