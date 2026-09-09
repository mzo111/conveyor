"""Enqueue: the envelope on disk, the queue's defaults, idempotency, validation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from conveyor_client import Client


def test_writes_the_documented_envelope(client: Client, raw: psycopg.Connection) -> None:
    """The payload column holds exactly {"handler": ..., "payload": ...}, nothing else."""
    job = client.enqueue("emails.send_welcome", {"user_id": 42})

    row = raw.execute("SELECT payload FROM jobs WHERE id = %s", (job.id,)).fetchone()
    assert row is not None
    assert row["payload"] == {"handler": "emails.send_welcome", "payload": {"user_id": 42}}

    assert job.handler == "emails.send_welcome"
    assert job.payload == {"user_id": 42}
    assert job.status == "queued"


@pytest.mark.parametrize(
    "payload",
    [{"a": 1}, [1, 2, 3], "a string", 7, 1.5, True, None, {"nested": {"deep": [{"x": None}]}}],
)
def test_round_trips_any_json_value(client: Client, payload: object) -> None:
    job = client.enqueue("h", payload)  # type: ignore[arg-type]
    assert client.get(job.id).payload == payload


def test_omitted_options_take_the_databases_defaults(
    client: Client, raw: psycopg.Connection
) -> None:
    """max_attempts and run_at are left out of the INSERT, not restated by the SDK.

    The point is that the SDK never hard-codes the queue's defaults, so this
    asserts against the column defaults rather than against the number 5.
    """
    default = raw.execute(
        "SELECT column_default FROM information_schema.columns "
        "WHERE table_name = 'jobs' AND column_name = 'max_attempts'"
    ).fetchone()
    assert default is not None

    job = client.enqueue("h", {})
    assert str(job.max_attempts) == default["column_default"].split("::")[0]
    assert job.run_at <= datetime.now(UTC)
    assert job.attempts == 0
    assert job.last_error is None
    assert job.finished_at is None


def test_generates_a_unique_key_when_none_is_given(client: Client) -> None:
    keys = {client.enqueue("h", {"i": i}).idempotency_key for i in range(5)}
    assert len(keys) == 5


def test_explicit_max_attempts_and_future_run_at(client: Client, raw: psycopg.Connection) -> None:
    later = datetime.now(UTC) + timedelta(hours=1)
    job = client.enqueue("h", {}, max_attempts=2, run_at=later)

    assert job.max_attempts == 2
    assert job.run_at == later

    # A job scheduled for the future is queued but not claimable: it does not
    # match the predicate conveyor/queue.py's claim uses.
    claimable = raw.execute(
        "SELECT id FROM jobs WHERE status = 'queued' AND run_at <= now() "
        "ORDER BY run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
    ).fetchall()
    assert claimable == []


def test_same_key_returns_the_same_job_and_creates_nothing(
    client: Client, raw: psycopg.Connection
) -> None:
    first = client.enqueue("h", {"v": 1}, idempotency_key="order-7")
    second = client.enqueue("h", {"v": 2}, idempotency_key="order-7")

    assert second.id == first.id
    assert second.payload == {"v": 1}, "the second enqueue must not overwrite the first"
    assert raw.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1


def test_dedup_survives_the_job_finishing(client: Client, raw: psycopg.Connection) -> None:
    """Dedup scope is the row's lifetime, not the job's queued window."""
    first = client.enqueue("h", {}, idempotency_key="once")
    raw.execute(
        "UPDATE jobs SET status = 'succeeded', finished_at = now() WHERE id = %s", (first.id,)
    )

    again = client.enqueue("h", {}, idempotency_key="once")
    assert again.id == first.id and again.status == "succeeded"


def test_concurrent_enqueues_of_one_key_produce_one_row(
    client: Client, raw: psycopg.Connection
) -> None:
    """Eight threads, one key, one row: the unique index serialises the race.

    This exercises the pool and the ON CONFLICT / re-SELECT path together, which
    is the only place the SDK issues two statements for one call.
    """
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(client.enqueue, "h", {"n": n}, idempotency_key="k") for n in range(8)
        ]
        jobs = [f.result() for f in futures]

    assert len({job.id for job in jobs}) == 1
    assert raw.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"idempotency_key": ""}, "non-empty"),
        ({"max_attempts": 0}, "at least 1"),
        ({"run_at": datetime(2030, 1, 1)}, "timezone-aware"),
    ],
)
def test_rejects_bad_arguments_before_touching_the_database(
    client: Client, raw: psycopg.Connection, kwargs: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        client.enqueue("h", {}, **kwargs)
    assert raw.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0


def test_rejects_an_empty_handler(client: Client) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        client.enqueue("", {})
