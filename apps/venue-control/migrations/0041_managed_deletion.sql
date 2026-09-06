-- Deletion is immediately visible to the owner while physical credential erasure waits for
-- every possibly-sent child command to reach a terminal, reconciled state.
ALTER TABLE venue_managed_credentials
    ADD COLUMN IF NOT EXISTS delete_requested_ms BIGINT
        CHECK (delete_requested_ms IS NULL OR delete_requested_ms > 0);

CREATE INDEX IF NOT EXISTS venue_managed_credentials_delete_requested
    ON venue_managed_credentials(delete_requested_ms)
    WHERE delete_requested_ms IS NOT NULL;

-- Migration 0040 introduced this table after the production runtime-role grant pass.
-- Installations without these optional roles keep their owner-based privileges.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='venue_control_api') THEN
        GRANT SELECT,INSERT,UPDATE,DELETE ON venue_kol_source_market_orders TO venue_control_api;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='venue_binance_executor') THEN
        GRANT SELECT,INSERT,UPDATE,DELETE ON venue_kol_source_market_orders TO venue_binance_executor;
    END IF;
END $$;
