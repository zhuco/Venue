-- Account-node execution evidence is kept separate from semantic delivery receipt custody.
-- Only a reconciled signed-fact result may unlock ledger/drift projection for its child job.
CREATE TABLE IF NOT EXISTS venue_copy_execution_results (
    job_id TEXT NOT NULL REFERENCES venue_copy_jobs(job_id),
    delivery_digest BYTEA NOT NULL CHECK (octet_length(delivery_digest) = 32),
    position_generation BIGINT NOT NULL CHECK (position_generation > 0),
    execution_state TEXT NOT NULL CHECK (execution_state IN (
        'prepared', 'submitted', 'accepted', 'rejected', 'unknown', 'reconciled'
    )),
    result_json JSONB NOT NULL,
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    PRIMARY KEY (job_id, delivery_digest, position_generation)
);
