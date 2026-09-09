"""Where conveyor_client and the worker meet: the payload envelope.

The SDK stores a handler string inside the payload column, because the jobs
table has no handler column and nothing in this repository routes per job. That
makes a claim worth pinning down: a job enqueued through the SDK runs fine on the
unmodified worker, but the handler is handed the *envelope*, not the caller's
payload. These tests are the reason the README ships an unwrap adapter rather
than pretending the two sides already agree.

This file lives in tests/ rather than tests_client/ precisely because it imports
both sides. tests_client/ stays free of conveyor imports; see
tests_client/test_independence.py.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from conveyor.queue import claim, enqueue, get_job
from conveyor.worker import Worker
from conveyor_client import Client, NotCancellable

PAYLOAD = {"user_id": 42}


@pytest.fixture
def client(dsn, conn):
    """A Client on the same database as the `conn` fixture, which truncated it."""
    with Client(dsn, max_size=2) as client:
        yield client


def test_a_job_from_the_sdk_runs_on_the_unmodified_worker(client, conn):
    job = client.enqueue("emails.send_welcome", PAYLOAD)
    seen = []

    Worker(conn, seen.append, poll_interval=0.01).run_once()

    assert client.get(job.id).status == "succeeded"
    assert len(seen) == 1
    # The documented catch: the handler gets the envelope, not PAYLOAD.
    assert seen[0].payload == {"handler": "emails.send_welcome", "payload": PAYLOAD}


def test_the_readme_adapter_unwraps_it(client, conn):
    """The three lines the README tells you to write, exercised."""

    def unwrap(fn):
        return lambda job: fn(job.payload["payload"])

    seen = []
    job = client.enqueue("emails.send_welcome", PAYLOAD)

    Worker(conn, unwrap(seen.append), poll_interval=0.01).run_once()

    assert seen == [PAYLOAD]
    assert client.get(job.id).status == "succeeded"


def test_the_sdk_reads_jobs_the_queue_enqueued(client, conn):
    """The other direction: a bare payload comes back with handler None."""
    job, created = enqueue(conn, {"sleep": 0.1})
    assert created

    fetched = client.get(job.id)
    assert fetched.handler is None
    assert fetched.payload == {"sleep": 0.1}
    assert fetched.idempotency_key == job.idempotency_key


def test_the_two_enqueue_paths_share_one_idempotency_namespace(client, conn):
    """Same key through either door yields one job. The unique index is the guarantee."""
    core, created = enqueue(conn, {"via": "core"}, idempotency_key="shared")
    assert created

    via_sdk = client.enqueue("h", {"via": "sdk"}, idempotency_key="shared")

    assert via_sdk.id == core.id
    assert via_sdk.handler is None and via_sdk.payload == {"via": "core"}


def test_cancelling_beats_a_worker_that_has_not_claimed_yet(client, conn):
    job = client.enqueue("h", {})
    client.cancel(job.id)

    assert Worker(conn, lambda j: None, poll_interval=0.01).run_once() is False, (
        "a cancelled job must not be claimable"
    )
    assert get_job(conn, job.id).status == "cancelled"


def test_cancelling_loses_cleanly_once_a_worker_has_claimed(client, conn):
    """Claim and cancel both match status='queued'; exactly one wins."""
    job = client.enqueue("h", {})

    running = claim(conn, worker_id="w1", visibility_timeout=timedelta(seconds=30))
    assert running is not None and running.id == job.id

    with pytest.raises(NotCancellable) as excinfo:
        client.cancel(job.id)
    assert excinfo.value.status == "running"
    assert get_job(conn, job.id).status == "running", "the losing cancel changed nothing"


def test_a_cancelled_job_is_not_a_dead_letter(client, conn):
    from conveyor.deadletter import list_dead, replay

    job = client.enqueue("h", {})
    client.cancel(job.id)

    assert list_dead(conn) == []
    assert replay(conn, job.id) is False, "cancelled is not replayable as a dead letter"


def test_a_scheduled_job_can_be_cancelled_before_it_comes_due(client, conn):
    job = client.enqueue("h", {}, run_at=datetime.now(UTC) + timedelta(hours=1))

    assert Worker(conn, lambda j: None, poll_interval=0.01).run_once() is False
    assert client.cancel(job.id).status == "cancelled"
