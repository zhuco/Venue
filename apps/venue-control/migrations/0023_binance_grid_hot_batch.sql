-- Prepare the shared Binance command ledger for an atomic Grid plan batch. Non-Grid commands
-- retain NULL batch fields; historical Grid commands receive deterministic one-command batches.
ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS grid_batch_id TEXT,
    ADD COLUMN IF NOT EXISTS dispatch_sequence BIGINT;

CREATE TABLE IF NOT EXISTS venue_binance_grid_mutation_batches (
    batch_id TEXT PRIMARY KEY CHECK (
        batch_id = btrim(batch_id) AND char_length(batch_id) BETWEEN 1 AND 64
    ),
    instance_id TEXT NOT NULL REFERENCES venue_binance_grid_instances(instance_id),
    expected_instance_revision BIGINT NOT NULL CHECK (expected_instance_revision > 0),
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
    desired_digest BYTEA NOT NULL CHECK (octet_length(desired_digest) = 32),
    batch_digest BYTEA NOT NULL CHECK (octet_length(batch_digest) = 32),
    command_count SMALLINT NOT NULL CHECK (command_count BETWEEN 0 AND 16),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    FOREIGN KEY (instance_id, config_revision)
        REFERENCES venue_binance_grid_config_revisions(instance_id, config_revision),
    UNIQUE (batch_id, instance_id, config_revision, plan_revision)
);

-- A plan batch binds the signed projection and instrument generation used by its planner.  Older
-- one-command compatibility receipts have no such context.  `source_event_received_ms` is set
-- only by the authenticated private-stream hot path and is the durable origin for end-to-end
-- event-to-send telemetry; a local command timestamp must never stand in for it.
ALTER TABLE venue_binance_grid_mutation_batches
    ADD COLUMN IF NOT EXISTS private_generation BIGINT,
    ADD COLUMN IF NOT EXISTS private_observed_ms BIGINT,
    ADD COLUMN IF NOT EXISTS instrument_generation BIGINT,
    ADD COLUMN IF NOT EXISTS source_event_received_ms BIGINT;

ALTER TABLE venue_binance_grid_mutation_batches
    DROP CONSTRAINT IF EXISTS venue_binance_grid_mutation_batches_facts_v1;

ALTER TABLE venue_binance_grid_mutation_batches
    ADD CONSTRAINT venue_binance_grid_mutation_batches_facts_v1 CHECK (
        (
            private_generation IS NULL
            AND private_observed_ms IS NULL
            AND instrument_generation IS NULL
            AND source_event_received_ms IS NULL
        )
        OR (
            private_generation IS NOT NULL
            AND private_generation > 0
            AND private_observed_ms IS NOT NULL
            AND private_observed_ms > 0
            AND instrument_generation IS NOT NULL
            AND instrument_generation > 0
            AND (
                source_event_received_ms IS NULL
                OR source_event_received_ms >= private_observed_ms
            )
        )
    );
CREATE INDEX IF NOT EXISTS venue_binance_grid_mutation_batches_instance
    ON venue_binance_grid_mutation_batches (
        instance_id, config_revision, plan_revision, created_ms, batch_id
    );

-- Existing Grid rows predate atomic batching. Preserve them without reinterpretation by giving
-- each row an isolated receipt whose digests come from its already durable command/instance facts.
INSERT INTO venue_binance_grid_mutation_batches (
    batch_id, instance_id, expected_instance_revision, config_revision, plan_revision,
    desired_digest, batch_digest, command_count, created_ms
)
SELECT
    c.command_id,
    c.grid_instance_id,
    i.revision,
    c.grid_config_revision,
    c.grid_plan_revision,
    COALESCE(i.desired_digest, c.source_digest),
    c.source_digest,
    1,
    c.created_ms
FROM venue_binance_commands AS c
JOIN venue_binance_grid_instances AS i
  ON i.instance_id = c.grid_instance_id
WHERE c.command_origin = 'grid'
  AND c.grid_batch_id IS NULL
ON CONFLICT (batch_id) DO NOTHING;

UPDATE venue_binance_commands
SET grid_batch_id = command_id,
    dispatch_sequence = 1
WHERE command_origin = 'grid'
  AND grid_batch_id IS NULL;

ALTER TABLE venue_binance_commands
    DROP CONSTRAINT IF EXISTS venue_binance_commands_grid_batch_v1;

ALTER TABLE venue_binance_commands
    ADD CONSTRAINT venue_binance_commands_grid_batch_v1 CHECK (
        (
            command_origin = 'grid'
            AND grid_batch_id IS NOT NULL
            AND grid_batch_id = btrim(grid_batch_id)
            AND char_length(grid_batch_id) BETWEEN 1 AND 64
            AND dispatch_sequence IS NOT NULL
            AND dispatch_sequence BETWEEN 1 AND 16
        )
        OR (
            command_origin <> 'grid'
            AND grid_batch_id IS NULL
            AND dispatch_sequence IS NULL
        )
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_commands_grid_batch_fk'
          AND conrelid = 'venue_binance_commands'::regclass
    ) THEN
        ALTER TABLE venue_binance_commands
            ADD CONSTRAINT venue_binance_commands_grid_batch_fk
            FOREIGN KEY (
                grid_batch_id, grid_instance_id, grid_config_revision, grid_plan_revision
            ) REFERENCES venue_binance_grid_mutation_batches (
                batch_id, instance_id, config_revision, plan_revision
            ) DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- A sequence identifies exactly one mutation within an account-local batch. The batch writer is
-- responsible for assigning every place before any cancel; the Executor consumes this order.
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_commands_grid_batch_sequence
    ON venue_binance_commands (trading_account_id, grid_batch_id, dispatch_sequence)
    WHERE grid_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS venue_binance_commands_grid_batch_dispatch
    ON venue_binance_commands (
        command_state, trading_account_id, created_ms, grid_batch_id,
        dispatch_sequence, command_id
    )
    WHERE command_origin = 'grid' AND grid_batch_id IS NOT NULL;

-- Signed snapshot fills remain valid with every stream-only column NULL. Authenticated private
-- stream fills persist both the socket connection generation and the signed baseline generation;
-- neither may stand in for the other when proving a continuous suffix.
ALTER TABLE venue_binance_account_fills
    ADD COLUMN IF NOT EXISTS stream_private_generation BIGINT,
    ADD COLUMN IF NOT EXISTS baseline_private_generation BIGINT,
    ADD COLUMN IF NOT EXISTS original_quantity TEXT,
    ADD COLUMN IF NOT EXISTS cumulative_filled_quantity TEXT,
    ADD COLUMN IF NOT EXISTS order_state TEXT,
    ADD COLUMN IF NOT EXISTS client_order_id TEXT;

ALTER TABLE venue_binance_account_fills
    DROP CONSTRAINT IF EXISTS venue_binance_account_fills_stream_context_v1;

ALTER TABLE venue_binance_account_fills
    ADD CONSTRAINT venue_binance_account_fills_stream_context_v1 CHECK (
        (
            stream_private_generation IS NULL
            AND baseline_private_generation IS NULL
            AND original_quantity IS NULL
            AND cumulative_filled_quantity IS NULL
            AND order_state IS NULL
            AND client_order_id IS NULL
        )
        OR (
            stream_private_generation IS NOT NULL
            AND stream_private_generation > 0
            AND baseline_private_generation IS NOT NULL
            AND baseline_private_generation > 0
            AND original_quantity IS NOT NULL
            AND cumulative_filled_quantity IS NOT NULL
            AND order_state IS NOT NULL
            AND order_state IN ('partially_filled', 'filled')
            AND client_order_id IS NOT NULL
            AND client_order_id = btrim(client_order_id)
            AND char_length(client_order_id) BETWEEN 1 AND 36
            AND CASE
                WHEN original_quantity ~ '^[0-9]+([.][0-9]+)?$'
                 AND cumulative_filled_quantity ~ '^[0-9]+([.][0-9]+)?$'
                THEN original_quantity::NUMERIC > 0
                 AND cumulative_filled_quantity::NUMERIC > 0
                 AND cumulative_filled_quantity::NUMERIC <= original_quantity::NUMERIC
                 AND (
                    (order_state = 'filled'
                        AND cumulative_filled_quantity::NUMERIC = original_quantity::NUMERIC)
                    OR (order_state = 'partially_filled'
                        AND cumulative_filled_quantity::NUMERIC < original_quantity::NUMERIC)
                 )
                ELSE FALSE
            END
        )
    );

CREATE INDEX IF NOT EXISTS venue_binance_account_fills_stream_generation
    ON venue_binance_account_fills (
        trading_account_id, stream_private_generation, baseline_private_generation,
        observed_ms, native_trade_id
    )
    WHERE stream_private_generation IS NOT NULL;
