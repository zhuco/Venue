-- Each instance has a durable outbox within its single account process. Its sequence is not
-- shared with sibling instances, even when both use the same configured node_id.
ALTER TABLE venue_account_node_projection_inbox ADD COLUMN IF NOT EXISTS instance_id TEXT;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM venue_account_node_projection_inbox
               WHERE coalesce(envelope_json #>> '{binding,instance_id}', '') = ''
                  OR (instance_id IS NOT NULL AND instance_id <> envelope_json #>> '{binding,instance_id}')) THEN
        RAISE EXCEPTION 'projection cursor has no consistent immutable instance binding';
    END IF;
END $$;
UPDATE venue_account_node_projection_inbox SET instance_id = envelope_json #>> '{binding,instance_id}'
WHERE instance_id IS NULL;
ALTER TABLE venue_account_node_projection_inbox ALTER COLUMN instance_id SET NOT NULL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'venue_account_node_projection_inbox'::regclass
                     AND contype = 'p' AND pg_get_constraintdef(oid) LIKE '%instance_id%') THEN
        ALTER TABLE venue_account_node_projection_inbox DROP CONSTRAINT venue_account_node_projection_inbox_pkey;
        ALTER TABLE venue_account_node_projection_inbox ADD PRIMARY KEY (venue, mode, trading_account_id, node_id, instance_id);
    END IF;
END $$;
