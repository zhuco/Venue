-- A missing grant always denies leader-bot creation/start. Ordinary Grid remains independent.
CREATE TABLE IF NOT EXISTS venue_leader_bot_permissions (
    kol_user_id TEXT PRIMARY KEY REFERENCES venue_kol_profiles(kol_user_id),
    enabled BOOLEAN NOT NULL DEFAULT false,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_by TEXT NOT NULL CHECK (char_length(btrim(updated_by)) BETWEEN 1 AND 100),
    updated_ms BIGINT NOT NULL CHECK (updated_ms > 0)
);
CREATE TABLE IF NOT EXISTS venue_leader_bot_permission_audit (
    kol_user_id TEXT NOT NULL REFERENCES venue_kol_profiles(kol_user_id),
    revision BIGINT NOT NULL CHECK (revision > 0),
    enabled BOOLEAN NOT NULL,
    operator TEXT NOT NULL,
    occurred_ms BIGINT NOT NULL,
    PRIMARY KEY (kol_user_id, revision)
);
CREATE TABLE IF NOT EXISTS venue_leader_bots (
    bot_id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL UNIQUE REFERENCES venue_kol_profiles(kol_user_id),
    trading_account_id TEXT NOT NULL UNIQUE,
    credential_id TEXT NOT NULL,
    create_request_id TEXT NOT NULL,
    bot_state TEXT NOT NULL CHECK (bot_state IN ('stopped','running','draining','needs_attention')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    permission_revision BIGINT NOT NULL CHECK (permission_revision > 0),
    started_ms BIGINT,
    last_request_id TEXT,
    last_request_json JSONB,
    attention_code TEXT,
    created_ms BIGINT NOT NULL,
    updated_ms BIGINT NOT NULL,
    FOREIGN KEY (trading_account_id,owner_user_id)
        REFERENCES venue_kol_profiles(leader_trading_account_id,kol_user_id),
    FOREIGN KEY (credential_id,owner_user_id,trading_account_id)
        REFERENCES venue_api_credentials(credential_id,user_id,trading_account_id),
    CHECK (bot_state <> 'running' OR started_ms IS NOT NULL)
);
-- Source identity and follower identity remain separate. One physical child attempt is never
-- rewritten to mean a replacement order; replacements get a new sequence/clientOrderId.
CREATE TABLE IF NOT EXISTS venue_order_mirrors (
    mirror_id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL REFERENCES venue_leader_bots(bot_id),
    bot_revision BIGINT NOT NULL CHECK (bot_revision > 0),
    permission_revision BIGINT NOT NULL CHECK (permission_revision > 0),
    relation_id TEXT NOT NULL REFERENCES venue_kol_follow_relations(relation_id),
    relation_revision BIGINT NOT NULL CHECK (relation_revision > 0),
    source_order_id TEXT NOT NULL,
    source_client_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source_order_json JSONB NOT NULL,
    child_sequence BIGINT NOT NULL CHECK (child_sequence > 0),
    command_revision BIGSERIAL UNIQUE,
    child_client_order_id TEXT NOT NULL UNIQUE,
    child_native_order_id TEXT,
    child_quantity TEXT NOT NULL CHECK (child_quantity::numeric > 0),
    filled_quantity TEXT NOT NULL DEFAULT '0' CHECK (filled_quantity::numeric >= 0),
    mirror_state TEXT NOT NULL CHECK (mirror_state IN ('pending','live','cancelling','terminal','blocked')),
    attention_code TEXT,
    created_ms BIGINT NOT NULL,
    updated_ms BIGINT NOT NULL,
    UNIQUE (relation_id,relation_revision,symbol,source_order_id,child_sequence),
    UNIQUE (mirror_id,relation_id)
);
CREATE INDEX IF NOT EXISTS venue_order_mirrors_live
    ON venue_order_mirrors(bot_id,relation_id) WHERE mirror_state <> 'terminal';
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS mirror_order_id TEXT;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='venue_binance_commands'::regclass
                   AND conname='venue_command_mirror_relation') THEN
        ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_command_mirror_relation
            FOREIGN KEY (mirror_order_id,relation_id) REFERENCES venue_order_mirrors(mirror_id,relation_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='venue_binance_commands'::regclass
                   AND conname='venue_command_mirror_origin') THEN
        ALTER TABLE venue_binance_commands ADD CONSTRAINT venue_command_mirror_origin
            CHECK (mirror_order_id IS NULL OR command_origin='copy');
    END IF;
END $$;
-- Original fills and commands survive deployment. Existing relationships must explicitly pass
-- the new empty-account activation gate; their history is never recast as mirrored orders.
UPDATE venue_kol_follow_relations SET relation_state='needs_attention',active_slot=NULL,
    revision=revision+1,attention_code='order_mirror_activation_required'
    WHERE relation_state='active' AND COALESCE(baseline_json->>'target_model','')<>'2';
UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=updated_ms,
    sanitized_error_code='order_mirror_activation_required'
    WHERE command_origin='copy' AND command_state='pending' AND mirror_order_id IS NULL;
