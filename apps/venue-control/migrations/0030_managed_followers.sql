ALTER TABLE venue_users ADD COLUMN IF NOT EXISTS login_enabled BOOLEAN NOT NULL DEFAULT TRUE;

-- A managed subject has no login or trading authorization. Its credentials reuse the
-- ordinary encrypted store; the KOL can only save and request read-only verification.
CREATE TABLE IF NOT EXISTS venue_kol_managed_followers (
    managed_id TEXT PRIMARY KEY,
    kol_user_id TEXT NOT NULL REFERENCES venue_kol_profiles(kol_user_id),
    follower_user_id TEXT NOT NULL UNIQUE REFERENCES venue_users(user_id),
    credential_id TEXT NOT NULL UNIQUE REFERENCES venue_api_credentials(credential_id),
    request_id TEXT NOT NULL,
    request_hash BYTEA NOT NULL CHECK (octet_length(request_hash)=32),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    CHECK (follower_user_id <> kol_user_id),
    UNIQUE (kol_user_id, request_id)
);
