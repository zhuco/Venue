CREATE TABLE IF NOT EXISTS venue_account_node_projection_inbox (
    venue TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode = 'LIVE'),
    trading_account_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_generation BIGINT NOT NULL CHECK (node_generation > 0),
    projection_sequence BIGINT NOT NULL CHECK (projection_sequence > 0),
    projection_digest BYTEA NOT NULL CHECK (octet_length(projection_digest) = 32),
    envelope_json JSONB NOT NULL,
    PRIMARY KEY (venue, mode, trading_account_id, node_id)
);
