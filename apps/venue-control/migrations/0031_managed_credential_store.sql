-- Frozen KOL Web used venue_kol_managed_followers with an incompatible schema.
-- Keep that table intact and refuse to hide any pre-existing managed credentials.
DO $migration$
BEGIN
    IF EXISTS (SELECT 1 FROM venue_kol_managed_followers) THEN
        RAISE EXCEPTION 'Existing managed records require a reviewed credential migration';
    END IF;
END
$migration$;

CREATE TABLE IF NOT EXISTS venue_managed_credentials (
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
