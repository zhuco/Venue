-- Preserve the original unknown command and its bounded signed stop reconciliation in place.
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS mirror_stop_readback JSONB;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS venue_binance_commands_check7;
ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_binance_commands_cancelled_sending
    CHECK (command_state <> 'cancelled' OR sending_ms IS NULL OR
        (mirror_stop_readback IS NOT NULL AND command_origin='copy' AND command_phase='open'
         AND order_kind IN ('limit_gtc','limit_post_only') AND native_order_id IS NULL
         AND accepted_ms IS NULL AND mirror_order_id IS NOT NULL));

CREATE OR REPLACE FUNCTION venue_keep_mirror_stop_readback_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.mirror_stop_readback IS NOT NULL AND
       (NEW.mirror_stop_readback IS DISTINCT FROM OLD.mirror_stop_readback
        OR NEW.command_state <> 'cancelled' OR NEW.native_order_id IS DISTINCT FROM OLD.native_order_id
        OR NEW.client_order_id IS DISTINCT FROM OLD.client_order_id
        OR NEW.credential_id IS DISTINCT FROM OLD.credential_id
        OR NEW.trading_account_id IS DISTINCT FROM OLD.trading_account_id
        OR NEW.symbol IS DISTINCT FROM OLD.symbol
        OR NEW.sending_ms IS DISTINCT FROM OLD.sending_ms OR NEW.created_ms IS DISTINCT FROM OLD.created_ms) THEN
        RAISE EXCEPTION 'mirror stop readback is immutable';
    END IF;
    IF NEW.mirror_stop_readback IS DISTINCT FROM OLD.mirror_stop_readback AND
       (OLD.command_state <> 'reconcile_required' OR NEW.command_state <> 'cancelled'
        OR OLD.command_origin <> 'copy' OR OLD.command_phase <> 'open'
        OR OLD.order_kind NOT IN ('limit_gtc','limit_post_only')
        OR OLD.native_order_id IS NOT NULL OR NEW.native_order_id IS NOT NULL
        OR OLD.accepted_ms IS NOT NULL OR OLD.mirror_order_id IS NULL
        OR jsonb_typeof(NEW.mirror_stop_readback) IS DISTINCT FROM 'object'
        OR NEW.mirror_stop_readback->>'kind' IS DISTINCT FROM 'mirror_stop_confirmed_absent') THEN
        RAISE EXCEPTION 'mirror stop readback requires an unacknowledged opening limit drain';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS venue_mirror_stop_readback_immutable ON venue_binance_commands;
CREATE TRIGGER venue_mirror_stop_readback_immutable BEFORE UPDATE ON venue_binance_commands
    FOR EACH ROW EXECUTE FUNCTION venue_keep_mirror_stop_readback_immutable();
