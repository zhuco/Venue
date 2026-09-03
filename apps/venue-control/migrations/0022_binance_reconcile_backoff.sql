-- Signed reconciliation is intentionally slower than the executor's command-discovery tick.
-- The schedule is durable so a restart cannot recreate a 100 ms authenticated-read burst.
ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS reconcile_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_reconcile_ms BIGINT;

CREATE OR REPLACE FUNCTION venue_binance_command_reconcile_schedule()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.command_state IN ('accepted', 'reconcile_required') THEN
        IF TG_OP = 'INSERT' THEN
            NEW.reconcile_attempts := 0;
            NEW.next_reconcile_ms := LEAST(NEW.updated_ms, 9223372036854775307) + 500;
        ELSIF OLD.command_state IS DISTINCT FROM NEW.command_state THEN
            -- A retry update supplies a larger attempt and deadline atomically. Ordinary state
            -- transitions do not, so they start a fresh bounded reconciliation schedule.
            IF NOT (
                OLD.command_state IN ('accepted', 'reconcile_required')
                AND NEW.reconcile_attempts >= OLD.reconcile_attempts
                AND NEW.next_reconcile_ms IS NOT NULL
                AND NEW.next_reconcile_ms > COALESCE(OLD.next_reconcile_ms, 0)
            ) THEN
                NEW.reconcile_attempts := 0;
                NEW.next_reconcile_ms := LEAST(NEW.updated_ms, 9223372036854775307) + 500;
            END IF;
        ELSIF NEW.next_reconcile_ms IS NULL THEN
            NEW.reconcile_attempts := 0;
            NEW.next_reconcile_ms := LEAST(NEW.updated_ms, 9223372036854775307) + 500;
        END IF;
    ELSE
        NEW.reconcile_attempts := 0;
        NEW.next_reconcile_ms := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS venue_binance_command_reconcile_schedule_trigger
    ON venue_binance_commands;
CREATE TRIGGER venue_binance_command_reconcile_schedule_trigger
BEFORE INSERT OR UPDATE OF command_state, updated_ms, reconcile_attempts, next_reconcile_ms
ON venue_binance_commands
FOR EACH ROW EXECUTE FUNCTION venue_binance_command_reconcile_schedule();

UPDATE venue_binance_commands
SET reconcile_attempts = 0,
    next_reconcile_ms = GREATEST(
        LEAST(updated_ms, 9223372036854775307) + 500,
        (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT + 500
    )
WHERE command_state IN ('accepted', 'reconcile_required')
  AND next_reconcile_ms IS NULL;

UPDATE venue_binance_commands
SET reconcile_attempts = 0,
    next_reconcile_ms = NULL
WHERE command_state NOT IN ('accepted', 'reconcile_required')
  AND (reconcile_attempts <> 0 OR next_reconcile_ms IS NOT NULL);

ALTER TABLE venue_binance_commands
    DROP CONSTRAINT IF EXISTS venue_binance_commands_reconcile_attempts,
    DROP CONSTRAINT IF EXISTS venue_binance_commands_reconcile_schedule;

ALTER TABLE venue_binance_commands
    ADD CONSTRAINT venue_binance_commands_reconcile_attempts CHECK (
        reconcile_attempts BETWEEN 0 AND 31
    ),
    ADD CONSTRAINT venue_binance_commands_reconcile_schedule CHECK (
        (
            command_state IN ('accepted', 'reconcile_required')
            AND next_reconcile_ms IS NOT NULL
            AND next_reconcile_ms > 0
        )
        OR (
            command_state NOT IN ('accepted', 'reconcile_required')
            AND reconcile_attempts = 0
            AND next_reconcile_ms IS NULL
        )
    );

CREATE INDEX IF NOT EXISTS venue_binance_commands_reconcile_due
    ON venue_binance_commands (next_reconcile_ms, created_ms, command_id)
    WHERE command_state IN ('accepted', 'reconcile_required');
