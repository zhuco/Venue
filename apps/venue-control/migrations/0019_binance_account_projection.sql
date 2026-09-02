-- Secret-free, user-scoped Binance private account read model. Only the singleton Executor writes
-- projections; Control can read them after checking the credential owner.
CREATE TABLE IF NOT EXISTS venue_binance_projection_subscriptions (
    credential_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL,
    trading_account_id TEXT NOT NULL,
    symbols JSONB NOT NULL CHECK (jsonb_typeof(symbols) = 'array'),
    requested_ms BIGINT NOT NULL CHECK (requested_ms > 0),
    expires_ms BIGINT NOT NULL CHECK (expires_ms > requested_ms),
    FOREIGN KEY (credential_id, owner_user_id, trading_account_id)
        REFERENCES venue_api_credentials(credential_id, user_id, trading_account_id),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id)
);
CREATE INDEX IF NOT EXISTS venue_binance_projection_subscriptions_active
    ON venue_binance_projection_subscriptions (expires_ms, credential_id);

CREATE TABLE IF NOT EXISTS venue_binance_account_projections (
    credential_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL,
    trading_account_id TEXT NOT NULL,
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    persisted_ms BIGINT NOT NULL CHECK (persisted_ms >= observed_ms),
    private_generation BIGINT NOT NULL CHECK (private_generation > 0),
    projection_json JSONB NOT NULL CHECK (jsonb_typeof(projection_json) = 'object'),
    FOREIGN KEY (credential_id, owner_user_id, trading_account_id)
        REFERENCES venue_api_credentials(credential_id, user_id, trading_account_id),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id),
    UNIQUE (trading_account_id, owner_user_id)
);

CREATE TABLE IF NOT EXISTS venue_binance_account_fills (
    trading_account_id TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    native_trade_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    occurred_ms BIGINT,
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    fill_json JSONB NOT NULL CHECK (jsonb_typeof(fill_json) = 'object'),
    PRIMARY KEY (trading_account_id, symbol, native_trade_id),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id)
);
CREATE INDEX IF NOT EXISTS venue_binance_account_fills_recent
    ON venue_binance_account_fills (trading_account_id, observed_ms DESC);

CREATE TABLE IF NOT EXISTS venue_binance_position_history (
    trading_account_id TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    position_json JSONB NOT NULL CHECK (jsonb_typeof(position_json) = 'object'),
    PRIMARY KEY (trading_account_id, symbol, position_side, observed_ms),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id)
);
CREATE INDEX IF NOT EXISTS venue_binance_position_history_recent
    ON venue_binance_position_history (trading_account_id, observed_ms DESC);

CREATE TABLE IF NOT EXISTS venue_binance_order_observations (
    trading_account_id TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    order_json JSONB NOT NULL CHECK (jsonb_typeof(order_json) = 'object'),
    PRIMARY KEY (trading_account_id, client_order_id, observed_ms),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id)
);
CREATE INDEX IF NOT EXISTS venue_binance_order_observations_recent
    ON venue_binance_order_observations (trading_account_id, observed_ms DESC);
