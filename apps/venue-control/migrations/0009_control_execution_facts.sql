-- Control is a read-model owner only: node-signed exchange evidence never conveys mutation authority.
CREATE TABLE IF NOT EXISTS venue_control_execution_facts (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton = TRUE),
    generated_ms BIGINT NOT NULL CHECK (generated_ms > 0),
    facts_json JSONB NOT NULL
);

-- Lifecycle edits are durable, idempotent revisioned audit facts, including pause and resume.
CREATE TABLE IF NOT EXISTS venue_copy_relation_audit (
    relation_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    action TEXT NOT NULL CHECK (action IN ('created', 'updated', 'paused', 'resumed')),
    policy_digest TEXT NOT NULL,
    config_json JSONB NOT NULL,
    observed_at_ms BIGINT NOT NULL CHECK (observed_at_ms > 0),
    PRIMARY KEY (relation_id, revision)
);
