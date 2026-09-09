-- Heartbeats may advance observation clocks without changing any authenticated account fact.
-- Hash everything else, including health, generation, positions, orders, fills and assets.
CREATE OR REPLACE FUNCTION venue_mm_projection_digest(value JSONB) RETURNS BYTEA
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT sha256(convert_to(jsonb_set(value, '{projection}',
        (value->'projection') - 'observed_ms' - 'persisted_ms')::text, 'UTF8'))
$$;

ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS inventory_mm_projection_digest BYTEA
    CHECK (inventory_mm_projection_digest IS NULL OR
        (command_origin='inventory_mm' AND octet_length(inventory_mm_projection_digest)=32));

-- Existing commands deliberately keep NULL and retain the original exact-observation gate.
