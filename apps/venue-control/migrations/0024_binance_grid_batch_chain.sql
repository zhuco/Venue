-- Chain consecutive Grid plan batches to the exact projected desired surface they consumed.
-- This lets authenticated fills keep planning while an earlier physical batch is in flight,
-- without treating not-yet-accepted Place orders as signed exchange evidence.
ALTER TABLE venue_binance_grid_mutation_batches
    ADD COLUMN IF NOT EXISTS input_desired_digest BYTEA,
    ADD COLUMN IF NOT EXISTS predecessor_batch_id TEXT;

ALTER TABLE venue_binance_grid_mutation_batches
    DROP CONSTRAINT IF EXISTS venue_binance_grid_mutation_batches_chain_v1;

ALTER TABLE venue_binance_grid_mutation_batches
    ADD CONSTRAINT venue_binance_grid_mutation_batches_chain_v1 CHECK (
        (input_desired_digest IS NULL OR octet_length(input_desired_digest) = 32)
        AND (predecessor_batch_id IS NULL OR (
            predecessor_batch_id = btrim(predecessor_batch_id)
            AND char_length(predecessor_batch_id) BETWEEN 1 AND 64
            AND predecessor_batch_id <> batch_id
        ))
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_grid_mutation_batches_predecessor_fk'
          AND conrelid = 'venue_binance_grid_mutation_batches'::regclass
    ) THEN
        ALTER TABLE venue_binance_grid_mutation_batches
            ADD CONSTRAINT venue_binance_grid_mutation_batches_predecessor_fk
            FOREIGN KEY (predecessor_batch_id)
            REFERENCES venue_binance_grid_mutation_batches(batch_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

-- One predecessor can have only one successor. Together with the instance-row CAS this makes
-- the projected batch history a branch-free chain, including zero-command receipts.
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_grid_mutation_batches_successor
    ON venue_binance_grid_mutation_batches (predecessor_batch_id)
    WHERE predecessor_batch_id IS NOT NULL;

ALTER TABLE venue_binance_grid_instances
    ADD COLUMN IF NOT EXISTS grid_tail_batch_id TEXT;

-- Existing receipts predate the chain. Anchor each instance at its latest durable receipt; new
-- batches chain from that point but historical siblings are not reinterpreted.
UPDATE venue_binance_grid_instances AS instance
SET grid_tail_batch_id = (
    SELECT batch.batch_id
    FROM venue_binance_grid_mutation_batches AS batch
    WHERE batch.instance_id = instance.instance_id
    ORDER BY batch.created_ms DESC, batch.batch_id DESC
    LIMIT 1
)
WHERE instance.grid_tail_batch_id IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_binance_grid_instances_tail_batch_fk'
          AND conrelid = 'venue_binance_grid_instances'::regclass
    ) THEN
        ALTER TABLE venue_binance_grid_instances
            ADD CONSTRAINT venue_binance_grid_instances_tail_batch_fk
            FOREIGN KEY (grid_tail_batch_id)
            REFERENCES venue_binance_grid_mutation_batches(batch_id)
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS venue_binance_grid_mutation_batches_predecessor_dispatch
    ON venue_binance_grid_mutation_batches (predecessor_batch_id, batch_id);
