"""Get: round trip, absence, and rows this SDK did not write."""

from __future__ import annotations

import json

import psycopg
import pytest

from conveyor_client import Client, JobNotFound


def test_round_trip(client: Client) -> None:
    job = client.enqueue("reports.build", {"quarter": "Q3"}, idempotency_key="r-1")
    fetched = client.get(job.id)

    assert fetched == job


def test_missing_job(client: Client) -> None:
    with pytest.raises(JobNotFound) as excinfo:
        client.get(999_999)
    assert excinfo.value.job_id == 999_999


def test_a_row_without_an_envelope_reads_back_raw(client: Client, raw: psycopg.Connection) -> None:
    """This is how jobs enqueued by conveyor.queue.enqueue come back.

    Written here with plain SQL rather than by importing the queue, which is the
    same thing from the database's point of view and keeps this suite free of
    the internals.
    """
    row = raw.execute(
        "INSERT INTO jobs (payload) VALUES (%s) RETURNING id", (json.dumps({"sleep": 1.0}),)
    ).fetchone()

    job = client.get(row["id"])
    assert job.handler is None
    assert job.payload == {"sleep": 1.0}


def test_a_payload_shaped_like_an_envelope_is_unwrapped(
    client: Client, raw: psycopg.Connection
) -> None:
    """Documented ambiguity: the SDK cannot tell this from one of its own.

    It is self-consistent — this is byte for byte what enqueue("x", {"y": 1})
    writes — so the round trip still holds. Pinned as a test so the behaviour is
    a decision rather than a surprise.
    """
    row = raw.execute(
        "INSERT INTO jobs (payload) VALUES (%s) RETURNING id",
        (json.dumps({"handler": "x", "payload": {"y": 1}}),),
    ).fetchone()

    job = client.get(row["id"])
    assert job.handler == "x" and job.payload == {"y": 1}


def test_a_near_miss_envelope_is_left_alone(client: Client, raw: psycopg.Connection) -> None:
    """Extra keys, or a non-string handler, mean it is not an envelope."""
    extra = raw.execute(
        "INSERT INTO jobs (payload) VALUES (%s) RETURNING id",
        (json.dumps({"handler": "x", "payload": {}, "extra": 1}),),
    ).fetchone()
    numeric = raw.execute(
        "INSERT INTO jobs (payload) VALUES (%s) RETURNING id",
        (json.dumps({"handler": 7, "payload": {}}),),
    ).fetchone()

    assert client.get(extra["id"]).handler is None
    assert client.get(extra["id"]).payload == {"handler": "x", "payload": {}, "extra": 1}
    assert client.get(numeric["id"]).handler is None


def test_is_terminal(client: Client, raw: psycopg.Connection) -> None:
    job = client.enqueue("h", {})
    assert not client.get(job.id).is_terminal

    for status in ("succeeded", "dead", "cancelled"):
        raw.execute("UPDATE jobs SET status = %s WHERE id = %s", (status, job.id))
        assert client.get(job.id).is_terminal, status
