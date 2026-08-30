-- Product configuration remains separate from semantic delivery and account mutation custody.
CREATE TABLE IF NOT EXISTS venue_copy_relation_configs (
    relation_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision > 0),
    leader_venue TEXT NOT NULL,
    leader_mode TEXT NOT NULL CHECK (leader_mode = 'LIVE'),
    leader_account_id TEXT NOT NULL,
    leader_instance_id TEXT NOT NULL,
    leader_symbol TEXT NOT NULL,
    follower_venue TEXT NOT NULL,
    follower_mode TEXT NOT NULL CHECK (follower_mode = 'LIVE'),
    follower_account_id TEXT NOT NULL,
    follower_instance_id TEXT NOT NULL,
    follower_symbol TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('active', 'paused')),
    config_json JSONB NOT NULL,
    created_at_ms BIGINT NOT NULL CHECK (created_at_ms > 0),
    updated_at_ms BIGINT NOT NULL CHECK (updated_at_ms > 0),
    UNIQUE (follower_venue, follower_account_id, follower_instance_id, follower_symbol)
);

CREATE INDEX IF NOT EXISTS venue_copy_relation_configs_leader
    ON venue_copy_relation_configs (leader_venue, leader_account_id, leader_instance_id, leader_symbol);
