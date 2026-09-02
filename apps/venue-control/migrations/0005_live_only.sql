-- Runtime mode is a production binding, not a fixture selector. Existing TEST rows are never
-- relabelled as LIVE because that would turn old coordination data into production intent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM venue_control_strategy_scopes WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_control_command_inbox WHERE mode <> 'LIVE')
        OR EXISTS (
            SELECT 1
            FROM venue_control_snapshots AS snapshots
            CROSS JOIN LATERAL jsonb_path_query(
                snapshots.snapshot_json,
                '$.**.mode'
            ) AS mode_value
            WHERE mode_value <> '"LIVE"'::jsonb
        )
        OR EXISTS (
            SELECT 1
            FROM venue_control_events AS events
            CROSS JOIN LATERAL jsonb_path_query(
                events.event_json,
                '$.**.mode'
            ) AS mode_value
            WHERE mode_value <> '"LIVE"'::jsonb
        )
        OR EXISTS (SELECT 1 FROM venue_copy_observer_scopes WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_leader_snapshots WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_leader_intents WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_observer_leases WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_jobs WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_ledger WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_drift_projections WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_plans WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_account_deliveries WHERE mode <> 'LIVE')
        OR EXISTS (SELECT 1 FROM venue_copy_observer_leases
                   WHERE lease_kind <> 'COPY_OBSERVER')
        OR EXISTS (SELECT 1 FROM venue_account_deliveries
                   WHERE source_kind NOT IN ('control_command', 'copy_semantic_job'))
        OR EXISTS (SELECT 1 FROM venue_account_delivery_claims
                   WHERE claim_json #>> '{lease,schema_version}' IS DISTINCT FROM '2')
        OR EXISTS (SELECT 1 FROM venue_account_delivery_acks
                   WHERE ack_json ->> 'schema_version' IS DISTINCT FROM '2')
        OR EXISTS (SELECT 1 FROM venue_account_delivery_receipts
                   WHERE receipt_json ->> 'schema_version' IS DISTINCT FROM '2')
    THEN
        RAISE EXCEPTION
            'legacy or invalid runtime mode, source kind, or delivery schema blocks LIVE-only migration; data was not rewritten';
    END IF;
END
$$;

ALTER TABLE venue_control_strategy_scopes
    DROP CONSTRAINT IF EXISTS venue_control_strategy_scopes_mode_check,
    ADD CONSTRAINT venue_control_strategy_scopes_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_control_command_inbox
    DROP CONSTRAINT IF EXISTS venue_control_command_inbox_mode_check,
    ADD CONSTRAINT venue_control_command_inbox_mode_check CHECK (mode = 'LIVE');

ALTER TABLE venue_copy_observer_scopes
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_observer_scopes_mode_check,
    ADD CONSTRAINT venue_copy_observer_scopes_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_leader_snapshots
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_leader_snapshots_mode_check,
    ADD CONSTRAINT venue_copy_leader_snapshots_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_leader_intents
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_leader_intents_mode_check,
    ADD CONSTRAINT venue_copy_leader_intents_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_observer_leases
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    ALTER COLUMN lease_kind SET DEFAULT 'COPY_OBSERVER',
    DROP CONSTRAINT IF EXISTS venue_copy_observer_leases_mode_check,
    DROP CONSTRAINT IF EXISTS venue_copy_observer_leases_lease_kind_check,
    ADD CONSTRAINT venue_copy_observer_leases_mode_check CHECK (mode = 'LIVE'),
    ADD CONSTRAINT venue_copy_observer_leases_lease_kind_check
        CHECK (lease_kind = 'COPY_OBSERVER');
ALTER TABLE venue_copy_jobs
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_jobs_mode_check,
    ADD CONSTRAINT venue_copy_jobs_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_ledger
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_ledger_mode_check,
    ADD CONSTRAINT venue_copy_ledger_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_drift_projections
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_drift_projections_mode_check,
    ADD CONSTRAINT venue_copy_drift_projections_mode_check CHECK (mode = 'LIVE');
ALTER TABLE venue_copy_plans
    ALTER COLUMN mode SET DEFAULT 'LIVE',
    DROP CONSTRAINT IF EXISTS venue_copy_plans_mode_check,
    ADD CONSTRAINT venue_copy_plans_mode_check CHECK (mode = 'LIVE');

ALTER TABLE venue_account_deliveries
    DROP CONSTRAINT IF EXISTS venue_account_deliveries_mode_check,
    DROP CONSTRAINT IF EXISTS venue_account_deliveries_source_kind_check,
    DROP CONSTRAINT IF EXISTS venue_account_copy_test_only,
    DROP CONSTRAINT IF EXISTS venue_account_copy_live_only,
    ADD CONSTRAINT venue_account_deliveries_mode_check CHECK (mode = 'LIVE'),
    ADD CONSTRAINT venue_account_deliveries_source_kind_check
        CHECK (source_kind IN ('control_command', 'copy_semantic_job')),
    ADD CONSTRAINT venue_account_copy_live_only
        CHECK (source_kind <> 'copy_semantic_job' OR mode = 'LIVE');
