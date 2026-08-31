-- An account-node receipt can settle an immutable Copy delivery without a Copy consumer claim.
-- The receipt projection verifies that durable proof before using these otherwise unclaimed rows.
ALTER TABLE venue_copy_delivery_outbox
    DROP CONSTRAINT IF EXISTS venue_copy_delivery_claim_fields;

ALTER TABLE venue_copy_delivery_outbox
    ADD CONSTRAINT venue_copy_delivery_claim_fields CHECK (
        (delivery_state IN ('pending', 'expired_unclaimed')
            AND claimed_by IS NULL AND claim_epoch = 0
            AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL)
        OR (delivery_state = 'claimed'
            AND claimed_by IS NOT NULL AND claim_epoch > 0
            AND claimed_at_ms > 0 AND claim_expires_at_ms > claimed_at_ms)
        OR (delivery_state IN ('reconciliation_required', 'settled')
            AND (
                (claimed_by IS NOT NULL AND claim_epoch > 0
                    AND claimed_at_ms > 0 AND claim_expires_at_ms > claimed_at_ms)
                OR (claimed_by IS NULL AND claim_epoch = 0
                    AND claimed_at_ms IS NULL AND claim_expires_at_ms IS NULL)
            ))
    );
