-- Add the 'cancelled' status, required by conveyor_client.Client.cancel().
--
-- db/schema.sql already declares it, but the Compose volume runs schema.sql only
-- on first initialisation, so a database created before this change needs the
-- value added in place. Tests and CI build the schema from scratch and do not
-- need this file.
--
-- Safe to run repeatedly. Adding a value to an enum does not rewrite the table
-- and takes no lock on it: every existing predicate in conveyor/ matches a
-- status by equality ('queued', 'running', 'dead'), so nothing starts or stops
-- matching because a sixth value exists.
--
--     psql "$DATABASE_URL" -f db/migrations/001_add_cancelled_status.sql
--
-- ALTER TYPE ... ADD VALUE cannot run inside a transaction block that later uses
-- the new value, so run this on its own (psql autocommits it by default).

ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'cancelled';
