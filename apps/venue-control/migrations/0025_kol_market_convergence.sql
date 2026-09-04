-- Single-order facts live with the existing durable command, never in a local recovery journal.
ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS market_baseline JSONB,
    ADD COLUMN IF NOT EXISTS signed_settlement JSONB;

CREATE OR REPLACE FUNCTION venue_keep_market_evidence_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.market_baseline IS NOT NULL AND NEW.market_baseline IS DISTINCT FROM OLD.market_baseline THEN
        RAISE EXCEPTION 'market baseline is immutable';
    END IF;
    IF OLD.signed_settlement IS NOT NULL AND NEW.signed_settlement IS DISTINCT FROM OLD.signed_settlement THEN
        RAISE EXCEPTION 'signed settlement is immutable';
    END IF;
    IF NEW.market_baseline IS DISTINCT FROM OLD.market_baseline
       AND (OLD.command_state <> 'sending' OR OLD.order_kind <> 'market'
            OR jsonb_typeof(NEW.market_baseline) <> 'object') THEN
        RAISE EXCEPTION 'market baseline requires an unsent claimed market order';
    END IF;
    IF NEW.signed_settlement IS DISTINCT FROM OLD.signed_settlement
       AND (NEW.command_state <> 'reconciled' OR OLD.order_kind <> 'market'
            OR jsonb_typeof(NEW.signed_settlement) <> 'object') THEN
        RAISE EXCEPTION 'signed settlement requires a reconciled market order';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS venue_market_evidence_immutable ON venue_binance_commands;
CREATE TRIGGER venue_market_evidence_immutable BEFORE UPDATE ON venue_binance_commands
    FOR EACH ROW EXECUTE FUNCTION venue_keep_market_evidence_immutable();

-- Earlier copyable_quantity values were scaled, not raw KOL quantities. Never reinterpret those
-- active targets after upgrade. Preserve all history and in-flight commands for reconciliation.
UPDATE venue_kol_follow_relations
    SET relation_state='needs_attention',active_slot=NULL,
        attention_code='activation_baseline_required',revision=revision+1
    WHERE relation_state='active' AND baseline_json->>'target_model' IS DISTINCT FROM '1';
UPDATE venue_binance_commands c
    SET command_state='cancelled',terminal_ms=c.updated_ms,
        sanitized_error_code='activation_baseline_required'
    FROM venue_kol_follow_relations r
    WHERE c.relation_id=r.relation_id AND c.command_origin='copy'
      AND c.command_state='pending' AND r.attention_code='activation_baseline_required'
      AND r.baseline_json->>'target_model' IS DISTINCT FROM '1';
