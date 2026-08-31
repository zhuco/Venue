-- A browser keeps this UUID across a pending/retried user action.  Historic audit rows remain
-- nullable; only new mutations require it and the partial unique index preserves their receipt.
ALTER TABLE venue_copy_relation_audit
    ADD COLUMN IF NOT EXISTS request_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS venue_copy_relation_audit_request_id
    ON venue_copy_relation_audit (request_id)
    WHERE request_id IS NOT NULL;
