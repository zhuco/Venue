-- Market sources are durable authenticated order facts. One source order is copied once even
-- when Binance reports multiple TRADE events for the same MARKET order.
CREATE TABLE IF NOT EXISTS venue_kol_source_market_orders (
    leader_trading_account_id TEXT NOT NULL,
    kol_user_id TEXT NOT NULL REFERENCES venue_kol_profiles(kol_user_id),
    native_symbol TEXT NOT NULL,
    native_order_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    order_side TEXT NOT NULL CHECK (order_side IN ('buy','sell')),
    position_side TEXT NOT NULL CHECK (position_side IN ('long','short')),
    original_quantity TEXT NOT NULL CHECK (original_quantity::numeric > 0),
    reference_price TEXT NOT NULL CHECK (reference_price::numeric > 0),
    occurred_ms BIGINT NOT NULL CHECK (occurred_ms > 0),
    observed_ms BIGINT NOT NULL CHECK (observed_ms >= occurred_ms),
    PRIMARY KEY (leader_trading_account_id,native_symbol,native_order_id)
);

ALTER TABLE venue_order_mirrors
    ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'limit'
        CHECK (source_kind IN ('limit','market','stop'));

ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS trigger_price TEXT,
    ADD COLUMN IF NOT EXISTS working_type TEXT;

ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_kind;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_shape;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_price;
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_kind CHECK(
    command_origin='strategy' OR (
        order_kind IN ('market','limit_post_only','limit_gtc','cancel_exact','stop_market','cancel_algo_exact')
        AND (order_kind<>'limit_gtc' OR (command_origin='copy' AND mirror_order_id IS NOT NULL))
        AND (order_kind NOT IN ('stop_market','cancel_algo_exact') OR command_origin='copy')
    )
);
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_shape CHECK(
    command_origin='strategy' OR (
        (command_phase='cancel' AND order_kind IN ('cancel_exact','cancel_algo_exact')
          AND position_side IS NULL AND order_side IS NULL AND requested_quantity IS NULL
          AND (selected_native_order_id IS NOT NULL OR target_client_order_id IS NOT NULL))
        OR (command_phase IN ('open','close')
          AND order_kind IN ('market','limit_post_only','limit_gtc','stop_market')
          AND position_side IS NOT NULL AND order_side IS NOT NULL AND requested_quantity IS NOT NULL
          AND selected_native_order_id IS NULL AND target_client_order_id IS NULL)
    )
);
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_price CHECK(
    command_origin='strategy' OR (
        ((order_kind IN ('limit_post_only','limit_gtc')) = (limit_price IS NOT NULL))
        AND ((order_kind='stop_market') = (trigger_price IS NOT NULL AND working_type IS NOT NULL))
        AND (order_kind='stop_market' OR (trigger_price IS NULL AND working_type IS NULL))
    )
);
