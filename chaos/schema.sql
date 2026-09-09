-- Durable side-effect ledger for the chaos harness. Not part of the product
-- schema. Every assertion the harness makes is computed from these rows plus
-- the jobs table, never from the harness's own memory of what it did.
--
-- Timestamps use clock_timestamp() (statement time), not now() (transaction
-- start), so the timeline reflects when each statement actually ran.

-- One row per handler invocation (= one claim = one attempt). Written in
-- three separate commits (started, effect, finished) so a SIGKILL between any
-- two of them is visible after the fact.
CREATE TABLE IF NOT EXISTS chaos_executions (
    run_id          TEXT        NOT NULL,
    job_id          BIGINT      NOT NULL,
    attempt         INT         NOT NULL,
    worker          TEXT        NOT NULL,
    pid             INT         NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    effect_at       TIMESTAMPTZ,
    effect_inserted BOOLEAN,
    finished_at     TIMESTAMPTZ,
    PRIMARY KEY (run_id, job_id, attempt)
);

-- The side effect itself. Append-only, no uniqueness: a naive handler that
-- runs twice leaves two rows, and that is exactly what we want to be able to
-- see.
CREATE TABLE IF NOT EXISTS chaos_effects (
    id          BIGSERIAL   PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    job_id      BIGINT      NOT NULL,
    attempt     INT         NOT NULL,
    worker      TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS chaos_effects_job_idx ON chaos_effects (run_id, job_id);

-- The idempotency mechanism used by the idempotent handler: a unique key
-- inserted in the same transaction as the effect. Same shape as the
-- idempotency_key on jobs.
CREATE TABLE IF NOT EXISTS chaos_effect_keys (
    run_id  TEXT   NOT NULL,
    job_id  BIGINT NOT NULL,
    PRIMARY KEY (run_id, job_id)
);
