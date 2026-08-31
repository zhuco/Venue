CREATE TABLE IF NOT EXISTS venue_users (
    user_id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_ms BIGINT NOT NULL CHECK (created_ms > 0)
);

CREATE TABLE IF NOT EXISTS venue_user_trading_accounts (
    trading_account_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    venue TEXT NOT NULL CHECK (venue = 'binance'),
    exchange_identity_hash BYTEA NOT NULL,
    UNIQUE (venue, exchange_identity_hash)
);

CREATE TABLE IF NOT EXISTS venue_api_credentials (
    credential_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    label TEXT NOT NULL,
    key_fingerprint BYTEA NOT NULL UNIQUE,
    masked_key TEXT NOT NULL,
    encrypted_credentials BYTEA NOT NULL,
    trading_account_id TEXT REFERENCES venue_user_trading_accounts(trading_account_id),
    verification_json JSONB NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    created_ms BIGINT NOT NULL,
    deleted_ms BIGINT
);
CREATE INDEX IF NOT EXISTS venue_api_credentials_user ON venue_api_credentials(user_id);

CREATE TABLE IF NOT EXISTS venue_user_sessions (
    token_hash BYTEA PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    expires_ms BIGINT NOT NULL,
    selected_credential_id TEXT REFERENCES venue_api_credentials(credential_id)
);
CREATE INDEX IF NOT EXISTS venue_user_sessions_user ON venue_user_sessions(user_id);

CREATE TABLE IF NOT EXISTS venue_account_rate_limits (
    bucket TEXT PRIMARY KEY,
    window_ms BIGINT NOT NULL,
    attempts INTEGER NOT NULL
);
