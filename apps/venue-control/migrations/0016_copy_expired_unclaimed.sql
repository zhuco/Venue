-- A Copy job can be retired only when neither delivery surface has ever issued a lease.
-- This is historical planner coordination state, never a Node/WAL/receipt result.
ALTER TABLE venue_account_deliveries
    DROP CONSTRAINT IF EXISTS venue_account_deliveries_delivery_state_check,
    ADD CONSTRAINT venue_account_deliveries_delivery_state_check CHECK (delivery_state IN (
        'pending', 'claimed', 'acknowledged', 'reconciliation_required', 'settled',
        'expired_unclaimed'
    ));

ALTER TABLE venue_account_deliveries
    DROP CONSTRAINT IF EXISTS venue_account_delivery_lease_fields,
    ADD CONSTRAINT venue_account_delivery_lease_fields CHECK (
        (lease_epoch = 0
            AND leased_by IS NULL AND lease_purpose IS NULL
            AND leased_at_ms IS NULL AND lease_expires_at_ms IS NULL)
        OR (delivery_state <> 'expired_unclaimed' AND lease_epoch > 0
            AND leased_by IS NOT NULL AND lease_purpose IS NOT NULL
            AND leased_at_ms > 0 AND lease_expires_at_ms > leased_at_ms)
    );

ALTER TABLE venue_copy_delivery_outbox
    DROP CONSTRAINT IF EXISTS venue_copy_delivery_outbox_delivery_state_check,
    ADD CONSTRAINT venue_copy_delivery_outbox_delivery_state_check CHECK (delivery_state IN (
        'pending', 'claimed', 'reconciliation_required', 'settled', 'expired_unclaimed'
    ));

ALTER TABLE venue_copy_delivery_outbox
    DROP CONSTRAINT IF EXISTS venue_copy_delivery_claim_fields,
    ADD CONSTRAINT venue_copy_delivery_claim_fields CHECK (
        (delivery_state IN ('pending', 'expired_unclaimed')
            AND claimed_by IS NULL AND claim_epoch = 0
            AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL)
        OR (delivery_state = 'claimed'
            AND claimed_by IS NOT NULL AND claim_epoch > 0
            AND claimed_at_ms > 0 AND claim_expires_at_ms > claimed_at_ms)
        OR (delivery_state IN ('reconciliation_required', 'settled')
            AND ((claimed_by IS NOT NULL AND claim_epoch > 0
                  AND claimed_at_ms > 0 AND claim_expires_at_ms > claimed_at_ms)
                 OR (claimed_by IS NULL AND claim_epoch = 0
                     AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL)))
    );

CREATE INDEX IF NOT EXISTS venue_copy_ledger_job_id ON venue_copy_ledger (job_id);
