-- Every newly written Copy job redundantly stores the immutable relation commitment.  Older jobs
-- intentionally remain nullable and are rejected by the v2 decoder rather than being relabelled
-- under a newer risk policy.
ALTER TABLE venue_copy_jobs
    ADD COLUMN IF NOT EXISTS relation_id TEXT,
    ADD COLUMN IF NOT EXISTS relation_revision BIGINT CHECK (relation_revision > 0),
    ADD COLUMN IF NOT EXISTS policy_digest BYTEA CHECK (octet_length(policy_digest) = 32);

CREATE INDEX IF NOT EXISTS venue_copy_jobs_relation_revision
    ON venue_copy_jobs (relation_id, relation_revision, created_at_ms);
