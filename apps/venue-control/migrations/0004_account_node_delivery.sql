-- Control-to-account-node delivery custody. These leases coordinate database delivery only.
-- They cannot represent gateway capability, a writer, WAL durability, or dispatch authority.
CREATE TABLE IF NOT EXISTS venue_account_deliveries (
    delivery_id TEXT PRIMARY KEY,
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('control_command', 'test_copy_semantic_job')),
    source_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('TEST', 'LIVE')),
    trading_account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    config_epoch BIGINT NOT NULL CHECK (config_epoch > 0),
    payload_json JSONB NOT NULL,
    delivery_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (delivery_state IN (
            'pending', 'claimed', 'acknowledged', 'reconciliation_required', 'settled'
        )),
    lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
    leased_by TEXT,
    lease_purpose TEXT CHECK (lease_purpose IN ('install', 'reconcile_only')),
    leased_at_ms BIGINT,
    lease_expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms > 0),
    UNIQUE (source_kind, source_id),
    CONSTRAINT venue_account_copy_test_only CHECK (
        source_kind <> 'test_copy_semantic_job' OR mode = 'TEST'
    ),
    CONSTRAINT venue_account_delivery_lease_fields CHECK (
        (lease_epoch = 0 AND leased_by IS NULL AND lease_purpose IS NULL
            AND leased_at_ms IS NULL AND lease_expires_at_ms IS NULL)
        OR (lease_epoch > 0 AND leased_by IS NOT NULL AND lease_purpose IS NOT NULL
            AND leased_at_ms > 0 AND lease_expires_at_ms > leased_at_ms)
    )
);

CREATE INDEX IF NOT EXISTS venue_account_deliveries_claim_scope
    ON venue_account_deliveries (
        venue, mode, trading_account_id, symbol, instance_id, config_epoch,
        delivery_state, created_at_ms, delivery_id
    );

CREATE TABLE IF NOT EXISTS venue_account_delivery_claims (
    delivery_id TEXT NOT NULL REFERENCES venue_account_deliveries(delivery_id),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    node_id TEXT NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('install', 'reconcile_only')),
    leased_at_ms BIGINT NOT NULL CHECK (leased_at_ms > 0),
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > leased_at_ms),
    claim_json JSONB NOT NULL,
    PRIMARY KEY (delivery_id, lease_epoch)
);

CREATE TABLE IF NOT EXISTS venue_account_delivery_acks (
    delivery_id TEXT PRIMARY KEY REFERENCES venue_account_deliveries(delivery_id),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    acknowledged_ms BIGINT NOT NULL CHECK (acknowledged_ms > 0),
    durable_inbox_digest BYTEA NOT NULL CHECK (octet_length(durable_inbox_digest) = 32),
    ack_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS venue_account_delivery_receipts (
    delivery_id TEXT NOT NULL REFERENCES venue_account_deliveries(delivery_id),
    receipt_id TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    receipt_state TEXT NOT NULL
        CHECK (receipt_state IN ('applied', 'rejected', 'unknown', 'reconciled')),
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    receipt_json JSONB NOT NULL,
    PRIMARY KEY (delivery_id, receipt_id),
    UNIQUE (delivery_id, lease_epoch, receipt_state)
);
