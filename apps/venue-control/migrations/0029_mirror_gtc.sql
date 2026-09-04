-- GTC is a genuine mirror order type, distinct from the historical pre-0020 scaffold.
ALTER TABLE venue_order_mirrors ADD COLUMN IF NOT EXISTS cancel_attempts INTEGER NOT NULL DEFAULT 0 CHECK(cancel_attempts BETWEEN 0 AND 8);
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_kind;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_shape;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_price;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_order_kind_v2;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_shape_v2;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_shape_v3;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_limit_price_v2;
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_kind CHECK(
    order_kind IN ('market','limit_post_only','limit_gtc','cancel_exact')
    AND (order_kind<>'limit_gtc' OR (command_origin='copy' AND mirror_order_id IS NOT NULL))
);
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_shape CHECK(
    (command_phase='cancel' AND order_kind='cancel_exact' AND position_side IS NULL
      AND order_side IS NULL AND requested_quantity IS NULL
      AND (selected_native_order_id IS NOT NULL OR target_client_order_id IS NOT NULL))
    OR (command_phase IN ('open','close') AND order_kind IN ('market','limit_post_only','limit_gtc')
      AND position_side IS NOT NULL AND order_side IS NOT NULL AND requested_quantity IS NOT NULL
      AND selected_native_order_id IS NULL AND target_client_order_id IS NULL)
);
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_price CHECK(
    (order_kind IN ('limit_post_only','limit_gtc')) = (limit_price IS NOT NULL)
);
