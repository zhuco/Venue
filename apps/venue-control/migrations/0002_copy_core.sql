-- Copy TEST coordination is intentionally separate from account mutation authority. Every lease
-- in this migration is constrained to observer/delivery work and cannot represent a writer.
CREATE TABLE IF NOT EXISTS venue_copy_observer_scopes (
    observer_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS venue_copy_leader_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    observer_id TEXT NOT NULL REFERENCES venue_copy_observer_scopes(observer_id),
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > observed_at_ms),
    snapshot_digest BYTEA NOT NULL CHECK (octet_length(snapshot_digest) = 32),
    snapshot_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS venue_copy_leader_intents (
    intent_id TEXT PRIMARY KEY,
    observer_id TEXT NOT NULL REFERENCES venue_copy_observer_scopes(observer_id),
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL REFERENCES venue_copy_leader_snapshots(snapshot_id),
    intent_digest BYTEA NOT NULL CHECK (octet_length(intent_digest) = 32),
    intent_json JSONB NOT NULL,
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    stored_at_ms BIGINT NOT NULL CHECK (stored_at_ms >= observed_at_ms)
);

CREATE TABLE IF NOT EXISTS venue_copy_observer_outbox (
    event_sequence BIGSERIAL PRIMARY KEY,
    observer_id TEXT NOT NULL REFERENCES venue_copy_observer_scopes(observer_id),
    intent_id TEXT NOT NULL UNIQUE REFERENCES venue_copy_leader_intents(intent_id),
    event_digest BYTEA NOT NULL CHECK (octet_length(event_digest) = 32),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0)
);

CREATE INDEX IF NOT EXISTS venue_copy_observer_outbox_scope
    ON venue_copy_observer_outbox (observer_id, event_sequence);

CREATE TABLE IF NOT EXISTS venue_copy_observer_leases (
    observer_id TEXT PRIMARY KEY REFERENCES venue_copy_observer_scopes(observer_id),
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    lease_kind TEXT NOT NULL DEFAULT 'COPY_TEST_OBSERVER'
        CHECK (lease_kind = 'COPY_TEST_OBSERVER'),
    mutation_authority BOOLEAN NOT NULL DEFAULT FALSE CHECK (mutation_authority = FALSE),
    holder_id TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    acquired_at_ms BIGINT NOT NULL CHECK (acquired_at_ms > 0),
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > acquired_at_ms)
);

CREATE TABLE IF NOT EXISTS venue_copy_observer_cursors (
    observer_id TEXT PRIMARY KEY REFERENCES venue_copy_observer_scopes(observer_id),
    last_event_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_event_sequence >= 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms > 0)
);

CREATE TABLE IF NOT EXISTS venue_copy_jobs (
    job_id TEXT PRIMARY KEY,
    observer_id TEXT NOT NULL REFERENCES venue_copy_observer_scopes(observer_id),
    source_event_sequence BIGINT NOT NULL UNIQUE
        REFERENCES venue_copy_observer_outbox(event_sequence),
    intent_id TEXT NOT NULL UNIQUE REFERENCES venue_copy_leader_intents(intent_id),
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    follower_binding_id TEXT NOT NULL,
    manifest_json JSONB NOT NULL,
    job_json JSONB NOT NULL,
    job_digest BYTEA NOT NULL CHECK (octet_length(job_digest) = 32),
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0),
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > created_at_ms)
);

CREATE INDEX IF NOT EXISTS venue_copy_jobs_delivery_scope
    ON venue_copy_jobs (venue, mode, trading_account_id, created_at_ms, job_id);

CREATE TABLE IF NOT EXISTS venue_copy_observer_inbox (
    observer_id TEXT NOT NULL REFERENCES venue_copy_observer_scopes(observer_id),
    event_sequence BIGINT NOT NULL REFERENCES venue_copy_observer_outbox(event_sequence),
    event_digest BYTEA NOT NULL CHECK (octet_length(event_digest) = 32),
    job_id TEXT NOT NULL UNIQUE REFERENCES venue_copy_jobs(job_id),
    consumed_at_ms BIGINT NOT NULL CHECK (consumed_at_ms > 0),
    PRIMARY KEY (observer_id, event_sequence)
);

CREATE TABLE IF NOT EXISTS venue_copy_delivery_outbox (
    job_id TEXT PRIMARY KEY REFERENCES venue_copy_jobs(job_id),
    delivery_state TEXT NOT NULL
        CHECK (delivery_state IN ('pending', 'claimed', 'reconciliation_required', 'settled')),
    claimed_by TEXT,
    claim_epoch BIGINT NOT NULL DEFAULT 0 CHECK (claim_epoch >= 0),
    claimed_at_ms BIGINT,
    claim_expires_at_ms BIGINT,
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms > 0),
    CONSTRAINT venue_copy_delivery_claim_fields CHECK (
        (delivery_state = 'pending'
            AND claimed_by IS NULL AND claim_epoch = 0
            AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL)
        OR (delivery_state IN ('claimed', 'reconciliation_required', 'settled')
            AND claimed_by IS NOT NULL AND claim_epoch > 0
            AND claimed_at_ms > 0 AND claim_expires_at_ms > claimed_at_ms)
    )
);

-- This is a coordinator inbox, not the account Actor inbox and not a mutation authorization.
CREATE TABLE IF NOT EXISTS venue_copy_delivery_inbox (
    job_id TEXT PRIMARY KEY REFERENCES venue_copy_jobs(job_id),
    consumer_id TEXT NOT NULL,
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
    job_digest BYTEA NOT NULL CHECK (octet_length(job_digest) = 32),
    inbox_state TEXT NOT NULL CHECK (inbox_state IN ('claimed', 'receipt_recorded')),
    claimed_at_ms BIGINT NOT NULL CHECK (claimed_at_ms > 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms >= claimed_at_ms)
);

CREATE TABLE IF NOT EXISTS venue_copy_delivery_receipts (
    job_id TEXT NOT NULL REFERENCES venue_copy_jobs(job_id),
    receipt_sequence BIGINT NOT NULL CHECK (receipt_sequence > 0),
    status TEXT NOT NULL CHECK (status IN ('applied', 'unknown', 'reconciled', 'rejected')),
    receipt_json JSONB NOT NULL,
    persisted_at_ms BIGINT NOT NULL CHECK (persisted_at_ms > 0),
    PRIMARY KEY (job_id, receipt_sequence)
);

CREATE TABLE IF NOT EXISTS venue_copy_receipt_outbox (
    job_id TEXT NOT NULL,
    receipt_sequence BIGINT NOT NULL,
    projected BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0),
    PRIMARY KEY (job_id, receipt_sequence),
    FOREIGN KEY (job_id, receipt_sequence)
        REFERENCES venue_copy_delivery_receipts(job_id, receipt_sequence)
);

CREATE TABLE IF NOT EXISTS venue_copy_projection_inbox (
    job_id TEXT NOT NULL,
    receipt_sequence BIGINT NOT NULL,
    projection_digest BYTEA NOT NULL CHECK (octet_length(projection_digest) = 32),
    projected_at_ms BIGINT NOT NULL CHECK (projected_at_ms > 0),
    PRIMARY KEY (job_id, receipt_sequence),
    FOREIGN KEY (job_id, receipt_sequence)
        REFERENCES venue_copy_receipt_outbox(job_id, receipt_sequence)
);

CREATE TABLE IF NOT EXISTS venue_copy_ledger (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    follower_binding_id TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL CHECK (ledger_sequence > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    job_id TEXT NOT NULL,
    receipt_sequence BIGINT NOT NULL CHECK (receipt_sequence > 0),
    fact_digest BYTEA NOT NULL CHECK (octet_length(fact_digest) = 32),
    entry_json JSONB NOT NULL,
    projected_at_ms BIGINT NOT NULL CHECK (projected_at_ms > 0),
    PRIMARY KEY (venue, mode, trading_account_id, follower_binding_id, ledger_sequence),
    FOREIGN KEY (job_id, receipt_sequence)
        REFERENCES venue_copy_receipt_outbox(job_id, receipt_sequence)
);

CREATE TABLE IF NOT EXISTS venue_copy_drift_projections (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'TEST' CHECK (mode = 'TEST'),
    trading_account_id TEXT NOT NULL,
    follower_binding_id TEXT NOT NULL,
    position_generation BIGINT NOT NULL CHECK (position_generation > 0),
    source_job_id TEXT NOT NULL REFERENCES venue_copy_jobs(job_id),
    receipt_sequence BIGINT NOT NULL CHECK (receipt_sequence > 0),
    projection_json JSONB NOT NULL,
    projected_at_ms BIGINT NOT NULL CHECK (projected_at_ms > 0),
    PRIMARY KEY (venue, mode, trading_account_id, follower_binding_id)
);
