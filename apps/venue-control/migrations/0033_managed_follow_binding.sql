-- Managed binding proves the same immutable owner as the encrypted credential mapping.
CREATE UNIQUE INDEX IF NOT EXISTS venue_managed_credentials_binding
    ON venue_managed_credentials(managed_id,follower_user_id,kol_user_id);
ALTER TABLE venue_user_kol_bindings ALTER COLUMN invite_id DROP NOT NULL;
ALTER TABLE venue_user_kol_bindings ADD COLUMN IF NOT EXISTS managed_id TEXT;
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='venue_user_kol_bindings'::regclass
                  AND conname='venue_binding_source') THEN
        ALTER TABLE venue_user_kol_bindings ADD CONSTRAINT venue_binding_source
            CHECK ((invite_id IS NULL) <> (managed_id IS NULL));
        ALTER TABLE venue_user_kol_bindings ADD CONSTRAINT venue_binding_managed_owner
            FOREIGN KEY(managed_id,user_id,kol_user_id)
            REFERENCES venue_managed_credentials(managed_id,follower_user_id,kol_user_id);
    END IF;
END $$;
CREATE OR REPLACE FUNCTION venue_reject_kol_rebinding()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'KOL binding is immutable'; END IF;
    IF NEW.user_id IS DISTINCT FROM OLD.user_id OR NEW.kol_user_id IS DISTINCT FROM OLD.kol_user_id
       OR NEW.invite_id IS DISTINCT FROM OLD.invite_id OR NEW.managed_id IS DISTINCT FROM OLD.managed_id THEN
        RAISE EXCEPTION 'KOL binding is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TABLE IF NOT EXISTS venue_follow_requests (
    follower_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    request_id TEXT NOT NULL,
    actor_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    request_hash BYTEA NOT NULL CHECK(octet_length(request_hash)=32),
    response_json JSONB NOT NULL,
    created_ms BIGINT NOT NULL CHECK(created_ms>0),
    PRIMARY KEY(follower_user_id,request_id)
);
