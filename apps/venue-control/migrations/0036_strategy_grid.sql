ALTER TABLE venue_api_credentials ADD COLUMN strategy_limits JSONB;
ALTER TABLE venue_api_credentials ADD CONSTRAINT venue_strategy_limits_object
    CHECK (strategy_limits IS NULL OR jsonb_typeof(strategy_limits)='object');

ALTER TABLE venue_binance_commands ADD COLUMN strategy_sequence BIGINT;
WITH ordered AS (
    SELECT command_id, ROW_NUMBER() OVER (PARTITION BY trading_account_id ORDER BY created_ms,command_id) AS sequence
    FROM venue_binance_commands WHERE command_origin='strategy'
)
UPDATE venue_binance_commands c SET strategy_sequence=o.sequence FROM ordered o WHERE c.command_id=o.command_id;
CREATE UNIQUE INDEX venue_strategy_sequence_unique ON venue_binance_commands(trading_account_id,strategy_sequence)
    WHERE command_origin='strategy';
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_strategy_sequence_shape CHECK
    ((command_origin='strategy' AND strategy_sequence IS NOT NULL AND strategy_sequence>0) OR (command_origin<>'strategy' AND strategy_sequence IS NULL));

CREATE TABLE venue_strategy_grids (
    instance_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    trading_account_id TEXT NOT NULL REFERENCES venue_user_trading_accounts(trading_account_id),
    credential_id TEXT NOT NULL REFERENCES venue_api_credentials(credential_id),
    venue TEXT NOT NULL CHECK (venue IN ('bitget','bybit','gate','okx','hyperliquid')),
    symbol TEXT NOT NULL,
    config JSONB NOT NULL CHECK (jsonb_typeof(config)='object'),
    lifecycle TEXT NOT NULL DEFAULT 'paused' CHECK (lifecycle IN ('running','pausing','paused','stopping','stopped','resetting')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    plan_sequence BIGINT NOT NULL DEFAULT 0 CHECK (plan_sequence>=0),
    convergence_pending_since_ms BIGINT CHECK (convergence_pending_since_ms>0),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures>=0),
    last_failed_strategy_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_failed_strategy_sequence>=0),
    rolling_anchor JSONB,
    desired_orders JSONB NOT NULL DEFAULT '[]',
    blocked_reason TEXT,
    created_ms BIGINT NOT NULL CHECK(created_ms>0),
    updated_ms BIGINT NOT NULL CHECK(updated_ms>0),
    UNIQUE(trading_account_id,symbol)
);

CREATE TABLE venue_strategy_grid_orders (
    client_order_id TEXT PRIMARY KEY REFERENCES venue_binance_commands(client_order_id),
    instance_id TEXT NOT NULL REFERENCES venue_strategy_grids(instance_id),
    revision BIGINT NOT NULL,
    intent JSONB NOT NULL CHECK(jsonb_typeof(intent)='object'),
    observed_filled NUMERIC NOT NULL DEFAULT 0 CHECK(observed_filled>=0),
    terminal BOOLEAN NOT NULL DEFAULT FALSE,
    created_ms BIGINT NOT NULL
);
CREATE INDEX venue_strategy_grid_orders_instance ON venue_strategy_grid_orders(instance_id,revision);

-- Grid lifecycle does not introduce a second writer lease. Admission shares the same account
-- transaction lock as the command ledger and old-scope allocation.
CREATE FUNCTION venue_validate_strategy_grid() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || NEW.trading_account_id,0));
    IF NOT EXISTS (SELECT 1 FROM venue_api_credentials c JOIN venue_user_trading_accounts a
        ON a.trading_account_id=c.trading_account_id AND a.user_id=c.user_id AND a.venue=c.venue
        WHERE c.credential_id=NEW.credential_id AND c.user_id=NEW.owner_user_id
        AND c.trading_account_id=NEW.trading_account_id AND c.venue=NEW.venue
        AND c.deleted_ms IS NULL AND c.verification_json->>'strategy_execution'='true'
        AND c.verification_json->>'verification'='verified')
       OR EXISTS (SELECT 1 FROM venue_control_strategy_scopes s WHERE s.trading_account_id=NEW.trading_account_id)
       OR (TG_OP='INSERT' AND (SELECT count(*) FROM venue_strategy_grids g
           WHERE g.trading_account_id=NEW.trading_account_id)>=20)
    THEN RAISE EXCEPTION 'strategy grid account admission rejected'; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER venue_strategy_grid_admission BEFORE INSERT OR UPDATE OF credential_id,trading_account_id,venue,owner_user_id
    ON venue_strategy_grids FOR EACH ROW EXECUTE FUNCTION venue_validate_strategy_grid();
