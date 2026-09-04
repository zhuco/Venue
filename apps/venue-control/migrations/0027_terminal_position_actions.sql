-- Two ordinary durable commands implement a reversal. Only the confirmed close can release
-- its opposite-side opening; restarting never replays a Sending command.
CREATE TABLE IF NOT EXISTS venue_terminal_position_commands (
    command_id TEXT PRIMARY KEY REFERENCES venue_binance_commands(command_id),
    reverse_parent_id TEXT UNIQUE REFERENCES venue_binance_commands(command_id),
    released BOOLEAN NOT NULL DEFAULT TRUE,
    prepared_json JSONB,
    settlement_json JSONB,
    CHECK (reverse_parent_id IS NULL OR reverse_parent_id <> command_id),
    CHECK (prepared_json IS NULL OR jsonb_typeof(prepared_json) = 'object'),
    CHECK (settlement_json IS NULL OR jsonb_typeof(settlement_json) = 'object')
);
