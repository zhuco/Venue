-- Binance Grid joins the existing PostgreSQL command ledger and singleton Executor.  The
-- durable boundary is intentionally limited to configuration, one rolling anchor, physical
-- order ownership, fill allocation and commands; no Actor/checkpoint/WAL lease is introduced.
CREATE TABLE IF NOT EXISTS venue_binance_grid_instances (
    instance_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    trading_account_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    create_request_id TEXT NOT NULL,
    create_request_digest BYTEA NOT NULL CHECK (octet_length(create_request_digest) = 32),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    instance_state TEXT NOT NULL CHECK (instance_state IN (
        'draft', 'start_pending', 'running', 'paused', 'stop_pending', 'stopped',
        'blocked', 'reset_required', 'needs_attention'
    )),
    revision BIGINT NOT NULL CHECK (revision > 0),
    current_config_revision BIGINT NOT NULL CHECK (current_config_revision > 0),
    plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
    desired_digest BYTEA CHECK (desired_digest IS NULL OR octet_length(desired_digest) = 32),
    dirty BOOLEAN NOT NULL,
    convergence_started_ms BIGINT CHECK (convergence_started_ms > 0),
    consecutive_failures SMALLINT NOT NULL CHECK (consecutive_failures BETWEEN 0 AND 100),
    last_facts_ms BIGINT CHECK (last_facts_ms > 0),
    attention_code TEXT CHECK (
        attention_code IS NULL OR char_length(btrim(attention_code)) BETWEEN 1 AND 64
    ),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= created_ms),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id),
    FOREIGN KEY (credential_id, owner_user_id, trading_account_id)
        REFERENCES venue_api_credentials(credential_id, user_id, trading_account_id),
    UNIQUE (owner_user_id, create_request_id),
    UNIQUE (instance_id, owner_user_id, trading_account_id, credential_id),
    UNIQUE (instance_id, trading_account_id),
    CHECK (
        (instance_state IN ('blocked', 'reset_required', 'needs_attention'))
        = (attention_code IS NOT NULL)
    ),
    CHECK (dirty OR convergence_started_ms IS NULL),
    CHECK (dirty OR consecutive_failures = 0),
    CHECK (last_facts_ms IS NULL OR last_facts_ms <= updated_ms),
    CHECK (convergence_started_ms IS NULL OR convergence_started_ms <= updated_ms)
);
CREATE INDEX IF NOT EXISTS venue_binance_grid_instances_dispatch
    ON venue_binance_grid_instances (instance_state, updated_ms, instance_id);
CREATE INDEX IF NOT EXISTS venue_binance_grid_instances_account
    ON venue_binance_grid_instances (trading_account_id, instance_state, instance_id);

CREATE TABLE IF NOT EXISTS venue_binance_grid_config_revisions (
    instance_id TEXT NOT NULL REFERENCES venue_binance_grid_instances(instance_id),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    request_id TEXT NOT NULL,
    config_json JSONB NOT NULL CHECK (jsonb_typeof(config_json) = 'object'),
    config_digest BYTEA NOT NULL CHECK (octet_length(config_digest) = 32),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    PRIMARY KEY (instance_id, config_revision),
    UNIQUE (instance_id, request_id)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_grid_instance_current_config_fk'
          AND conrelid = 'venue_binance_grid_instances'::regclass
    ) THEN
        ALTER TABLE venue_binance_grid_instances
            ADD CONSTRAINT venue_grid_instance_current_config_fk
            FOREIGN KEY (instance_id, current_config_revision)
            REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS venue_binance_grid_anchors (
    instance_id TEXT PRIMARY KEY REFERENCES venue_binance_grid_instances(instance_id),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    anchor_revision BIGINT NOT NULL CHECK (anchor_revision > 0),
    instrument_generation BIGINT NOT NULL CHECK (instrument_generation > 0),
    anchor_price TEXT NOT NULL CHECK (char_length(btrim(anchor_price)) > 0),
    price_step TEXT NOT NULL CHECK (char_length(btrim(price_step)) > 0),
    grid_quantity TEXT NOT NULL CHECK (char_length(btrim(grid_quantity)) > 0),
    source_native_trade_id TEXT CHECK (
        source_native_trade_id IS NULL
        OR char_length(btrim(source_native_trade_id)) BETWEEN 1 AND 128
    ),
    observed_ms BIGINT NOT NULL CHECK (observed_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= observed_ms),
    FOREIGN KEY (instance_id, config_revision)
        REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision)
);

-- This is the current desired surface, not an append-only command/event log. Replacing a plan
-- atomically replaces these rows so restart reconciliation can distinguish keep/cancel/place.
CREATE TABLE IF NOT EXISTS venue_binance_grid_desired_orders (
    instance_id TEXT NOT NULL REFERENCES venue_binance_grid_instances(instance_id),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
    desired_digest BYTEA NOT NULL CHECK (octet_length(desired_digest) = 32),
    semantic_key TEXT NOT NULL CHECK (char_length(btrim(semantic_key)) BETWEEN 1 AND 160),
    client_order_id TEXT NOT NULL UNIQUE CHECK (char_length(client_order_id) BETWEEN 1 AND 36),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    order_role TEXT NOT NULL CHECK (order_role IN ('open', 'close')),
    grid_level SMALLINT NOT NULL CHECK (grid_level BETWEEN 1 AND 50),
    order_sequence BIGINT NOT NULL CHECK (order_sequence > 0),
    order_side TEXT NOT NULL CHECK (order_side IN ('buy', 'sell')),
    quantity TEXT NOT NULL CHECK (char_length(btrim(quantity)) > 0),
    limit_price TEXT NOT NULL CHECK (char_length(btrim(limit_price)) > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms > 0),
    PRIMARY KEY (instance_id, semantic_key),
    UNIQUE (instance_id, client_order_id),
    FOREIGN KEY (instance_id, config_revision)
        REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision),
    CHECK (
        (position_side = 'long' AND order_role = 'open' AND order_side = 'buy')
        OR (position_side = 'long' AND order_role = 'close' AND order_side = 'sell')
        OR (position_side = 'short' AND order_role = 'open' AND order_side = 'sell')
        OR (position_side = 'short' AND order_role = 'close' AND order_side = 'buy')
    )
);
CREATE INDEX IF NOT EXISTS venue_binance_grid_desired_plan
    ON venue_binance_grid_desired_orders (instance_id, plan_revision, semantic_key);

-- Lifecycle requests are a small idempotency receipt, not an event log or strategy checkpoint.
CREATE TABLE IF NOT EXISTS venue_binance_grid_lifecycle_requests (
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    request_id TEXT NOT NULL,
    instance_id TEXT NOT NULL REFERENCES venue_binance_grid_instances(instance_id),
    action TEXT NOT NULL CHECK (action IN ('start', 'pause', 'resume', 'stop', 'reset')),
    request_digest BYTEA NOT NULL CHECK (octet_length(request_digest) = 32),
    resulting_revision BIGINT NOT NULL CHECK (resulting_revision > 0),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    PRIMARY KEY (owner_user_id, request_id),
    UNIQUE (instance_id, request_id)
);

ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS grid_instance_id TEXT,
    ADD COLUMN IF NOT EXISTS grid_config_revision BIGINT,
    ADD COLUMN IF NOT EXISTS grid_plan_revision BIGINT,
    ADD COLUMN IF NOT EXISTS grid_semantic_key TEXT,
    ADD COLUMN IF NOT EXISTS target_client_order_id TEXT;

-- 0020 deliberately rewrote only order-kind checks.  Replace the origin and shape checks here
-- without rewriting any historical copy or terminal row.
ALTER TABLE venue_binance_commands
    DROP CONSTRAINT IF EXISTS venue_binance_commands_origin_v3,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_origin_shape_v3,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_shape_v2,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_shape_v3,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_grid_fields_v3,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_cancel_target_v3;

DO $$
DECLARE constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'venue_binance_commands'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%command_origin%'
    LOOP
        EXECUTE format(
            'ALTER TABLE venue_binance_commands DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END
$$;

ALTER TABLE venue_binance_commands
    ADD CONSTRAINT venue_binance_commands_origin_v3 CHECK (
        command_origin IN ('copy', 'terminal', 'grid')
    ),
    ADD CONSTRAINT venue_binance_commands_origin_shape_v3 CHECK (
        (command_origin = 'copy' AND request_id IS NULL AND relation_id IS NOT NULL
            AND relation_revision IS NOT NULL AND target_revision IS NOT NULL
            AND grid_instance_id IS NULL AND grid_config_revision IS NULL
            AND grid_plan_revision IS NULL AND grid_semantic_key IS NULL
            AND target_client_order_id IS NULL)
        OR (command_origin = 'terminal' AND request_id IS NOT NULL AND relation_id IS NULL
            AND relation_revision IS NULL AND target_revision IS NULL
            AND grid_instance_id IS NULL AND grid_config_revision IS NULL
            AND grid_plan_revision IS NULL AND grid_semantic_key IS NULL
            AND target_client_order_id IS NULL)
        OR (command_origin = 'grid' AND request_id IS NULL AND relation_id IS NULL
            AND relation_revision IS NULL AND target_revision IS NULL
            AND grid_instance_id IS NOT NULL AND grid_config_revision IS NOT NULL
            AND grid_plan_revision IS NOT NULL AND grid_semantic_key IS NOT NULL
            AND source_digest IS NOT NULL)
    ),
    ADD CONSTRAINT venue_binance_commands_shape_v3 CHECK (
        (command_phase = 'cancel' AND order_kind = 'cancel_exact'
            AND position_side IS NULL AND order_side IS NULL
            AND requested_quantity IS NULL
            AND (selected_native_order_id IS NOT NULL OR target_client_order_id IS NOT NULL))
        OR (command_phase IN ('open', 'close')
            AND order_kind IN ('market', 'limit_post_only')
            AND position_side IS NOT NULL AND order_side IS NOT NULL
            AND requested_quantity IS NOT NULL AND selected_native_order_id IS NULL
            AND target_client_order_id IS NULL)
    ),
    ADD CONSTRAINT venue_binance_commands_grid_fields_v3 CHECK (
        (command_origin <> 'grid')
        OR (grid_config_revision > 0 AND grid_plan_revision > 0
            AND char_length(btrim(grid_semantic_key)) BETWEEN 1 AND 160
            AND ((command_phase = 'cancel' AND target_client_order_id IS NOT NULL
                    AND selected_native_order_id IS NOT NULL)
                OR (command_phase IN ('open', 'close')
                    AND order_kind IN ('market', 'limit_post_only')
                    AND target_client_order_id IS NULL)))
    ),
    ADD CONSTRAINT venue_binance_commands_cancel_target_v3 CHECK (
        target_client_order_id IS NULL OR target_client_order_id <> client_order_id
    );

CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_commands_grid_identity
    ON venue_binance_commands (
        grid_instance_id, grid_config_revision, grid_plan_revision, grid_semantic_key
    ) WHERE command_origin = 'grid';
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_commands_grid_owner_candidate
    ON venue_binance_commands (
        command_id, grid_instance_id, trading_account_id, client_order_id, symbol
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_commands_grid_instance_fk'
          AND conrelid = 'venue_binance_commands'::regclass
    ) THEN
        ALTER TABLE venue_binance_commands
            ADD CONSTRAINT venue_binance_commands_grid_instance_fk
            FOREIGN KEY (grid_instance_id, owner_user_id, trading_account_id, credential_id)
            REFERENCES venue_binance_grid_instances(
                instance_id, owner_user_id, trading_account_id, credential_id
            );
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_commands_grid_config_fk'
          AND conrelid = 'venue_binance_commands'::regclass
    ) THEN
        ALTER TABLE venue_binance_commands
            ADD CONSTRAINT venue_binance_commands_grid_config_fk
            FOREIGN KEY (grid_instance_id, grid_config_revision)
            REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision);
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS venue_binance_grid_order_owners (
    trading_account_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
    semantic_key TEXT NOT NULL CHECK (char_length(btrim(semantic_key)) BETWEEN 1 AND 160),
    place_command_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    order_role TEXT NOT NULL CHECK (order_role IN ('open', 'close')),
    grid_level SMALLINT NOT NULL CHECK (grid_level BETWEEN 1 AND 50),
    order_sequence BIGINT NOT NULL CHECK (order_sequence > 0),
    order_side TEXT NOT NULL CHECK (order_side IN ('buy', 'sell')),
    quantity TEXT NOT NULL CHECK (char_length(btrim(quantity)) > 0),
    filled_quantity TEXT NOT NULL CHECK (char_length(btrim(filled_quantity)) > 0),
    limit_price TEXT NOT NULL CHECK (char_length(btrim(limit_price)) > 0),
    native_order_id TEXT CHECK (
        native_order_id IS NULL OR char_length(btrim(native_order_id)) BETWEEN 1 AND 128
    ),
    ownership_source TEXT NOT NULL CHECK (ownership_source = 'executor'),
    order_state TEXT NOT NULL CHECK (order_state IN ('working', 'terminal')),
    ownership_digest BYTEA NOT NULL CHECK (octet_length(ownership_digest) = 32),
    first_seen_ms BIGINT NOT NULL CHECK (first_seen_ms > 0),
    last_seen_ms BIGINT NOT NULL CHECK (last_seen_ms >= first_seen_ms),
    PRIMARY KEY (trading_account_id, client_order_id),
    FOREIGN KEY (instance_id, trading_account_id)
        REFERENCES venue_binance_grid_instances(instance_id, trading_account_id),
    FOREIGN KEY (instance_id, config_revision)
        REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision),
    FOREIGN KEY (place_command_id, instance_id, trading_account_id, client_order_id, symbol)
        REFERENCES venue_binance_commands(
            command_id, grid_instance_id, trading_account_id, client_order_id, symbol
        ),
    UNIQUE (instance_id, trading_account_id, client_order_id),
    UNIQUE (instance_id, config_revision, plan_revision, semantic_key),
    UNIQUE (
        instance_id, trading_account_id, client_order_id, symbol, position_side, order_role
    ),
    CHECK (
        (position_side = 'long' AND order_role = 'open' AND order_side = 'buy')
        OR (position_side = 'long' AND order_role = 'close' AND order_side = 'sell')
        OR (position_side = 'short' AND order_role = 'open' AND order_side = 'sell')
        OR (position_side = 'short' AND order_role = 'close' AND order_side = 'buy')
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_grid_order_native_identity
    ON venue_binance_grid_order_owners (trading_account_id, symbol, native_order_id)
    WHERE native_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS venue_binance_grid_order_instance_state
    ON venue_binance_grid_order_owners (instance_id, order_state, last_seen_ms);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_commands_grid_cancel_target_fk'
          AND conrelid = 'venue_binance_commands'::regclass
    ) THEN
        ALTER TABLE venue_binance_commands
            ADD CONSTRAINT venue_binance_commands_grid_cancel_target_fk
            FOREIGN KEY (grid_instance_id, trading_account_id, target_client_order_id)
            REFERENCES venue_binance_grid_order_owners(
                instance_id, trading_account_id, client_order_id
            );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS venue_binance_grid_fill_allocations (
    trading_account_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    native_trade_id TEXT NOT NULL CHECK (
        char_length(btrim(native_trade_id)) BETWEEN 1 AND 128
    ),
    instance_id TEXT NOT NULL,
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    client_order_id TEXT NOT NULL,
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    order_role TEXT NOT NULL CHECK (order_role IN ('open', 'close')),
    quantity TEXT NOT NULL CHECK (char_length(btrim(quantity)) > 0),
    price TEXT NOT NULL CHECK (char_length(btrim(price)) > 0),
    maker BOOLEAN,
    occurred_ms BIGINT CHECK (occurred_ms > 0),
    observed_ms BIGINT NOT NULL CHECK (
        observed_ms > 0 AND (occurred_ms IS NULL OR observed_ms >= occurred_ms)
    ),
    allocation_digest BYTEA NOT NULL CHECK (octet_length(allocation_digest) = 32),
    PRIMARY KEY (trading_account_id, symbol, native_trade_id),
    FOREIGN KEY (instance_id, config_revision)
        REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision),
    FOREIGN KEY (
        instance_id, trading_account_id, client_order_id, symbol, position_side, order_role
    )
        REFERENCES venue_binance_grid_order_owners(
            instance_id, trading_account_id, client_order_id, symbol, position_side, order_role
        )
);
CREATE INDEX IF NOT EXISTS venue_binance_grid_fills_instance
    ON venue_binance_grid_fill_allocations (instance_id, observed_ms, native_trade_id);
