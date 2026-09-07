CREATE TABLE IF NOT EXISTS venue_terminal_replacements (
    command_id TEXT PRIMARY KEY REFERENCES venue_binance_commands(command_id),
    cancel_command_id TEXT NOT NULL UNIQUE REFERENCES venue_binance_commands(command_id),
    original_quantity TEXT NOT NULL CHECK (original_quantity::numeric > 0),
    remaining_quantity TEXT CHECK (remaining_quantity::numeric >= 0),
    released BOOLEAN NOT NULL DEFAULT FALSE
);

-- Preserve Copy's mirror binding; terminal GTC is admitted only by cancel-and-replace.
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_mirror_kind;
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_mirror_kind CHECK (
    command_origin='strategy' OR (
        order_kind IN ('market','limit_post_only','limit_gtc','cancel_exact','stop_market','cancel_algo_exact')
        AND (order_kind<>'limit_gtc' OR (command_origin='copy' AND mirror_order_id IS NOT NULL)
            OR (command_origin='terminal' AND rule_version='terminal-replace-v1'))
        AND (order_kind NOT IN ('stop_market','cancel_algo_exact') OR command_origin='copy')
    )
);

ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_origin_shape_v3;
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_origin_shape_v3 CHECK (
    command_origin='strategy'
    OR (command_origin='copy' AND request_id IS NULL AND relation_id IS NOT NULL
        AND relation_revision IS NOT NULL AND target_revision IS NOT NULL
        AND grid_instance_id IS NULL AND grid_config_revision IS NULL
        AND grid_plan_revision IS NULL AND grid_semantic_key IS NULL AND target_client_order_id IS NULL)
    OR (command_origin='terminal' AND request_id IS NOT NULL AND relation_id IS NULL
        AND relation_revision IS NULL AND target_revision IS NULL
        AND grid_instance_id IS NULL AND grid_config_revision IS NULL
        AND grid_plan_revision IS NULL AND grid_semantic_key IS NULL
        AND (target_client_order_id IS NULL OR (rule_version='terminal-replace-v1'
            AND command_phase='cancel' AND selected_native_order_id IS NOT NULL)))
    OR (command_origin='grid' AND request_id IS NULL AND relation_id IS NULL
        AND relation_revision IS NULL AND target_revision IS NULL
        AND grid_instance_id IS NOT NULL AND grid_config_revision IS NOT NULL
        AND grid_plan_revision IS NOT NULL AND grid_semantic_key IS NOT NULL AND source_digest IS NOT NULL)
);
