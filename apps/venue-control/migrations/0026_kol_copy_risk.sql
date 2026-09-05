-- Freeze the user's policy and the source price with each command, not with mutable UI state.
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS copy_risk JSONB;

UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=updated_ms,
    sanitized_error_code='copy_risk_policy_required'
    WHERE command_origin='copy' AND command_state='pending' AND copy_risk IS NULL;

CREATE OR REPLACE FUNCTION venue_keep_copy_risk_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' AND NEW.copy_risk IS DISTINCT FROM OLD.copy_risk THEN
        RAISE EXCEPTION 'copy risk policy is immutable';
    END IF;
    IF TG_OP='INSERT' AND NEW.command_origin='copy'
       AND (NEW.copy_risk IS NULL OR jsonb_typeof(NEW.copy_risk)<>'object') THEN
        RAISE EXCEPTION 'copy command requires a frozen risk policy';
    END IF;
    IF NEW.command_origin<>'copy' AND NEW.copy_risk IS NOT NULL THEN
        RAISE EXCEPTION 'copy risk policy belongs to copy commands';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS venue_copy_risk_immutable ON venue_binance_commands;
CREATE TRIGGER venue_copy_risk_immutable BEFORE INSERT OR UPDATE ON venue_binance_commands
    FOR EACH ROW EXECUTE FUNCTION venue_keep_copy_risk_immutable();
