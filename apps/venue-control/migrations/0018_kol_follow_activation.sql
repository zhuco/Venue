-- A confirmed enable request is intentionally distinct from Active. Only the singleton executor
-- may establish the signed KOL/follower baseline and then promote the relation.
CREATE TABLE IF NOT EXISTS venue_kol_activation_requests (
    relation_id TEXT PRIMARY KEY REFERENCES venue_kol_follow_relations(relation_id),
    request_id TEXT NOT NULL UNIQUE,
    relation_revision BIGINT NOT NULL CHECK (relation_revision > 0),
    request_state TEXT NOT NULL CHECK (request_state IN ('pending', 'cancelled', 'completed', 'rejected')),
    requested_ms BIGINT NOT NULL CHECK (requested_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= requested_ms),
    sanitized_reason TEXT CHECK (sanitized_reason IS NULL OR char_length(sanitized_reason) BETWEEN 1 AND 64)
);
CREATE INDEX IF NOT EXISTS venue_kol_activation_requests_dispatch
    ON venue_kol_activation_requests (request_state, requested_ms, relation_id);
