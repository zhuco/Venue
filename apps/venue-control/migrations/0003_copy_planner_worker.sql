-- The planner freezes the exact capital input and pure target result in the same transaction that
-- creates the immutable TEST delivery job. This table is coordination evidence, never mutation
-- authority.
CREATE TABLE IF NOT EXISTS venue_copy_plans (
    job_id TEXT PRIMARY KEY REFERENCES venue_copy_jobs(job_id),
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    source_event_sequence BIGINT NOT NULL UNIQUE,
    capital_snapshot_json JSONB NOT NULL,
    target_exposure_json JSONB NOT NULL,
    plan_digest BYTEA NOT NULL CHECK (octet_length(plan_digest) = 32),
    planned_at_ms BIGINT NOT NULL CHECK (planned_at_ms > 0)
);

CREATE INDEX IF NOT EXISTS venue_copy_plans_scope
    ON venue_copy_plans (venue, mode, trading_account_id, source_event_sequence);
