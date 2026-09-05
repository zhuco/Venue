CREATE TABLE venue_support_martingale_instances (
    instance_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    trading_account_id TEXT NOT NULL REFERENCES venue_user_trading_accounts(trading_account_id),
    credential_id TEXT NOT NULL REFERENCES venue_api_credentials(credential_id),
    reference_venue TEXT NOT NULL CHECK (reference_venue='binance'),
    execution_venue TEXT NOT NULL CHECK (execution_venue IN ('bitget','bybit','gate','okx','hyperliquid')),
    config JSONB NOT NULL CHECK (jsonb_typeof(config)='object'),
    lifecycle TEXT NOT NULL DEFAULT 'stopped' CHECK (lifecycle IN ('stopped','running','entry_paused','increase_paused','draining')),
    health TEXT NOT NULL DEFAULT 'healthy' CHECK (health IN ('healthy','needs_attention','unavailable')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    reserved_budget NUMERIC NOT NULL DEFAULT 0 CHECK (reserved_budget>=0),
    created_ms BIGINT NOT NULL CHECK (created_ms>0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms>0),
    UNIQUE(trading_account_id, instance_id)
);
CREATE UNIQUE INDEX venue_support_martingale_active_account ON venue_support_martingale_instances(trading_account_id) WHERE lifecycle IN ('running','entry_paused','increase_paused','draining');
CREATE TABLE venue_support_martingale_symbol_states (
    instance_id TEXT NOT NULL REFERENCES venue_support_martingale_instances(instance_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    cycle_id TEXT,
    layer INTEGER NOT NULL DEFAULT 0 CHECK(layer>=0),
    average_price NUMERIC,
    quantity NUMERIC NOT NULL DEFAULT 0 CHECK(quantity>=0),
    invested NUMERIC NOT NULL DEFAULT 0 CHECK(invested>=0),
    take_profit_price NUMERIC,
    net_pnl NUMERIC,
    status TEXT NOT NULL DEFAULT 'idle',
    last_support_lower NUMERIC,
    last_support_upper NUMERIC,
    decision_sequence BIGINT NOT NULL DEFAULT 0 CHECK(decision_sequence>=0),
    pending_command_id TEXT,
    take_profit_client_id TEXT,
    health_reason TEXT,
    cooldown_until_ms BIGINT,
    updated_ms BIGINT NOT NULL CHECK(updated_ms>0),
    PRIMARY KEY(instance_id,symbol)
);
CREATE TABLE venue_support_martingale_commands (
    command_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES venue_support_martingale_instances(instance_id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('entry','add','tp','cancel_tp')),
    cycle_id TEXT,
    support_id TEXT,
    request_id TEXT NOT NULL,
    requested_notional NUMERIC NOT NULL CHECK(requested_notional>=0),
    observed_fill NUMERIC NOT NULL DEFAULT 0 CHECK(observed_fill>=0),
    ledger_settled BOOLEAN NOT NULL DEFAULT FALSE,
    terminal BOOLEAN NOT NULL DEFAULT FALSE,
    created_ms BIGINT NOT NULL CHECK(created_ms>0),
    updated_ms BIGINT NOT NULL CHECK(updated_ms>0),
    UNIQUE(instance_id,request_id),
    UNIQUE(instance_id,symbol,cycle_id,support_id,kind) DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX venue_support_martingale_commands_pending ON venue_support_martingale_commands(instance_id,symbol) WHERE NOT terminal;
CREATE TABLE venue_support_martingale_requests (
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    request_id TEXT NOT NULL,
    instance_id TEXT NOT NULL REFERENCES venue_support_martingale_instances(instance_id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    resulting_revision BIGINT NOT NULL CHECK(resulting_revision>0),
    created_ms BIGINT NOT NULL CHECK(created_ms>0),
    PRIMARY KEY(owner_user_id,request_id)
);
