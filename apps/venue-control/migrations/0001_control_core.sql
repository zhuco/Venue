CREATE TABLE IF NOT EXISTS venue_control_snapshots (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    generated_ms BIGINT NOT NULL CHECK (generated_ms > 0),
    snapshot_json JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS venue_control_strategy_scopes (
    instance_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('TEST', 'LIVE')),
    trading_account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    config_epoch BIGINT NOT NULL CHECK (config_epoch > 0),
    snapshot_generated_ms BIGINT NOT NULL CHECK (snapshot_generated_ms > 0)
);

CREATE INDEX IF NOT EXISTS venue_control_strategy_account_scope
    ON venue_control_strategy_scopes (venue, mode, trading_account_id);

CREATE TABLE IF NOT EXISTS venue_control_events (
    event_sequence BIGSERIAL PRIMARY KEY,
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    event_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS venue_control_command_inbox (
    request_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('TEST', 'LIVE')),
    trading_account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    config_epoch BIGINT NOT NULL CHECK (config_epoch > 0),
    action TEXT NOT NULL CHECK (action IN ('PAUSE', 'RESUME', 'STOP', 'FLATTEN')),
    command_state TEXT NOT NULL CHECK (command_state IN ('accepted', 'applied', 'rejected', 'unknown')),
    command_json JSONB NOT NULL,
    receipt_json JSONB NOT NULL,
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms > 0)
);

CREATE TABLE IF NOT EXISTS venue_control_command_outbox (
    request_id TEXT PRIMARY KEY REFERENCES venue_control_command_inbox(request_id),
    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('pending', 'claimed', 'settled')),
    claimed_by TEXT,
    claimed_ms BIGINT,
    CONSTRAINT venue_control_claim_fields CHECK (
        (delivery_state = 'pending' AND claimed_by IS NULL AND claimed_ms IS NULL)
        OR (delivery_state IN ('claimed', 'settled') AND claimed_by IS NOT NULL AND claimed_ms > 0)
    )
);
