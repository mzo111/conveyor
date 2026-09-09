-- Conveyor job queue schema. Postgres 16.
--
-- A job moves queued -> running -> (succeeded | queued again | dead).
-- A `queued` job may also be cancelled outright: queued -> cancelled, a
-- terminal state written only by the client SDK (`conveyor_client`). It is
-- deliberately distinct from `dead`, which means "exhausted its attempts", so
-- the dead_letter view and `deadletter replay` stay about failures. A job that
-- has been claimed cannot be cancelled: the handler runs outside any
-- transaction and there is nothing to interrupt.
-- `running` is a *lease*: the claiming worker owns the job only until
-- `visibility_deadline`. If it has not acked or nacked by then, the reaper
-- puts the job back in `queued` (or `dead` if attempts are exhausted).

CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'dead', 'cancelled');

CREATE TABLE jobs (
    id                  BIGSERIAL PRIMARY KEY,
    -- Caller-supplied dedup key. Defaults to a random UUID so callers that do
    -- not care about idempotency still satisfy the unique index.
    idempotency_key     TEXT        NOT NULL DEFAULT gen_random_uuid()::text,
    payload             JSONB       NOT NULL,
    status              job_status  NOT NULL DEFAULT 'queued',
    -- Incremented at claim time, not at failure time, so an attempt that dies
    -- silently (lease expiry) still counts toward max_attempts.
    attempts            INT         NOT NULL DEFAULT 0,
    max_attempts        INT         NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    -- Earliest time the job may be claimed. Retry backoff pushes this forward.
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Lease expiry. Non-null iff status = 'running'.
    visibility_deadline TIMESTAMPTZ,
    claimed_by          TEXT,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    CHECK ((status = 'running') = (visibility_deadline IS NOT NULL))
);

-- Idempotency. This unique index *is* the guarantee that enqueueing the same
-- key twice yields one row: the second INSERT hits ON CONFLICT DO NOTHING.
-- It also serves the "return the existing job" lookup after a conflict.
CREATE UNIQUE INDEX jobs_idempotency_key_uq ON jobs (idempotency_key);

-- Claim path. The claim query is
--     WHERE status = 'queued' AND run_at <= now() ORDER BY run_at, id LIMIT 1
--     FOR UPDATE SKIP LOCKED
-- This partial index matches that predicate and ordering exactly, so the
-- planner walks the index in order and stops at the first unlocked row: no
-- sort, no heap scan over finished jobs. It is partial because the claimable
-- set is tiny compared with the historical table; a full index on `status`
-- would be low-cardinality dead weight that every finished job keeps growing.
CREATE INDEX jobs_claim_idx ON jobs (run_at, id) WHERE status = 'queued';

-- Reaper path. The reaper query is
--     WHERE status = 'running' AND visibility_deadline < now()
-- Partial on running jobs so the range scan touches only in-flight leases.
CREATE INDEX jobs_lease_idx ON jobs (visibility_deadline) WHERE status = 'running';

-- Dead-letter view: jobs that exhausted their attempts. Inspect with
-- `SELECT * FROM dead_letter` or `python -m conveyor.deadletter list`;
-- replay with `python -m conveyor.deadletter replay <id>`.
CREATE VIEW dead_letter AS
SELECT id, idempotency_key, payload, attempts, max_attempts, last_error, claimed_by,
       created_at, finished_at
FROM jobs
WHERE status = 'dead'
ORDER BY finished_at DESC;
