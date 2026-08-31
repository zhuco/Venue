DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'venue_control_command_action_v2'
          AND conrelid = 'venue_control_command_inbox'::regclass
    ) THEN
        ALTER TABLE venue_control_command_inbox
            DROP CONSTRAINT IF EXISTS venue_control_command_inbox_action_check;
        ALTER TABLE venue_control_command_inbox
            ADD CONSTRAINT venue_control_command_action_v2
            CHECK (action IN ('PAUSE', 'RESUME', 'STOP', 'FLATTEN', 'TRADE'));
    END IF;
END
$$;
