-- Existing installations used the scaffold name limit_gtc even though the product default and
-- executor contract are maker-only. Replace only order-kind checks and preserve every row.
DO $$
DECLARE constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'venue_binance_commands'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%order_kind%'
    LOOP
        EXECUTE format('ALTER TABLE venue_binance_commands DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;

UPDATE venue_binance_commands
SET order_kind = 'limit_post_only'
WHERE order_kind = 'limit_gtc';

ALTER TABLE venue_binance_commands
    ADD CONSTRAINT venue_binance_commands_order_kind_v2 CHECK (
        order_kind IN ('market', 'limit_post_only', 'cancel_exact')
    ),
    ADD CONSTRAINT venue_binance_commands_shape_v2 CHECK (
        (command_phase = 'cancel' AND order_kind = 'cancel_exact'
            AND position_side IS NULL AND order_side IS NULL
            AND requested_quantity IS NULL AND selected_native_order_id IS NOT NULL)
        OR (command_phase IN ('open', 'close')
            AND order_kind IN ('market', 'limit_post_only')
            AND position_side IS NOT NULL AND order_side IS NOT NULL
            AND requested_quantity IS NOT NULL AND selected_native_order_id IS NULL)
    ),
    ADD CONSTRAINT venue_binance_commands_limit_price_v2 CHECK (
        (order_kind = 'limit_post_only') = (limit_price IS NOT NULL)
    );
