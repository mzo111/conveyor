"""Every statement this SDK sends, in one place.

Keeping the SQL here rather than inline makes the SDK's entire dependency on the
queue's schema readable in one screen: three tables' worth of coupling reduced to
one column list and four statements. If ``db/schema.sql`` changes, this is the
file that has to agree with it.

Note which columns are *absent* from ``JOB_COLUMNS``: ``visibility_deadline``,
``claimed_by`` and ``claimed_at``. Those are lease bookkeeping owned by the
worker and the reaper, and they are the part of the design most likely to change.
A producer has no business reading them, so the SDK does not select them and does
not break when they move.
"""

from __future__ import annotations

from collections.abc import Sequence

JOB_COLUMNS = (
    "id, idempotency_key, payload, status, attempts, max_attempts, "
    "run_at, last_error, created_at, updated_at, finished_at"
)

# Columns the caller may leave to the database. They are omitted from the INSERT
# entirely rather than passed as COALESCE(%s, <default>): restating the queue's
# defaults here would let the two drift apart silently, and "None means whatever
# the queue's default is" is the honest reading of an optional argument.
OPTIONAL_INSERT_COLUMNS = ("max_attempts", "run_at")


def insert(optional: Sequence[str]) -> str:
    """Build the enqueue statement, including only the optionals actually given.

    Idempotency works exactly as it does for the queue itself: the unique index
    on ``idempotency_key`` turns a duplicate into ``DO NOTHING``, and the caller
    follows up with :data:`SELECT_BY_KEY`. Two clients inserting the same key at
    once are serialised by that index — the loser blocks until the winner
    commits, then takes the DO NOTHING branch and sees the winner's row.
    """
    columns = ["idempotency_key", "payload", *optional]
    values = [
        # A caller who supplies no key still needs one: the column is NOT NULL
        # and uniquely indexed, so give it the same random default the table
        # would have used.
        "COALESCE(%(idempotency_key)s, gen_random_uuid()::text)",
        "%(payload)s",
        *(f"%({column})s" for column in optional),
    ]
    return (
        f"INSERT INTO jobs ({', '.join(columns)}) VALUES ({', '.join(values)}) "
        f"ON CONFLICT (idempotency_key) DO NOTHING RETURNING {JOB_COLUMNS}"
    )


SELECT_BY_ID = f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %(id)s"

SELECT_BY_KEY = f"SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = %(idempotency_key)s"

# Cancellation is one statement, and its WHERE clause is the whole concurrency
# argument. `claim` in conveyor/queue.py also matches on status = 'queued', so
# both are UPDATEs contending for the same row lock: the loser re-checks its
# predicate against the winner's committed row under READ COMMITTED and matches
# nothing. Either the job is cancelled and never runs, or it was claimed and runs
# to completion. There is no interleaving in which both apply.
#
# last_error is deliberately left alone: a cancelled job that had already failed
# a few times keeps the diagnosis of why. The status says it was cancelled.
CANCEL = (
    "UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now() "
    f"WHERE id = %(id)s AND status = 'queued' RETURNING {JOB_COLUMNS}"
)
