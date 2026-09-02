-- KOL MVP ownership is built on the existing encrypted credential and account tables.
-- These candidate keys make cross-user references impossible without copying secret data.
CREATE UNIQUE INDEX IF NOT EXISTS venue_user_trading_accounts_account_owner
    ON venue_user_trading_accounts (trading_account_id, user_id);
CREATE UNIQUE INDEX IF NOT EXISTS venue_api_credentials_credential_owner_account
    ON venue_api_credentials (credential_id, user_id, trading_account_id);

CREATE TABLE IF NOT EXISTS venue_kol_profiles (
    kol_user_id TEXT PRIMARY KEY REFERENCES venue_users(user_id),
    leader_trading_account_id TEXT NOT NULL UNIQUE,
    public_name TEXT NOT NULL CHECK (
        char_length(btrim(public_name)) BETWEEN 1 AND 64
    ),
    public_title TEXT NOT NULL CHECK (
        char_length(btrim(public_title)) BETWEEN 1 AND 120
    ),
    public_description TEXT NOT NULL CHECK (
        char_length(public_description) <= 2000
    ),
    strategy_capital TEXT NOT NULL CHECK (char_length(btrim(strategy_capital)) > 0),
    profile_state TEXT NOT NULL CHECK (profile_state IN ('draft', 'enabled', 'disabled')),
    active_slot SMALLINT UNIQUE CHECK (active_slot BETWEEN 1 AND 5),
    source_cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
        jsonb_typeof(source_cursor_json) = 'object'
    ),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= created_ms),
    FOREIGN KEY (leader_trading_account_id, kol_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id),
    CHECK ((profile_state = 'enabled') = (active_slot IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS venue_kol_profiles_account_owner
    ON venue_kol_profiles (leader_trading_account_id, kol_user_id);

CREATE TABLE IF NOT EXISTS venue_kol_invites (
    invite_id TEXT PRIMARY KEY,
    kol_user_id TEXT NOT NULL REFERENCES venue_kol_profiles(kol_user_id),
    code_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(code_hash) = 32),
    invite_state TEXT NOT NULL CHECK (invite_state IN ('active', 'disabled')),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    expires_ms BIGINT CHECK (expires_ms > created_ms),
    disabled_ms BIGINT CHECK (disabled_ms >= created_ms),
    CHECK (
        (invite_state = 'active' AND disabled_ms IS NULL)
        OR (invite_state = 'disabled' AND disabled_ms IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS venue_kol_invites_one_active_per_kol
    ON venue_kol_invites (kol_user_id) WHERE invite_state = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS venue_kol_invites_invite_owner
    ON venue_kol_invites (invite_id, kol_user_id);

CREATE TABLE IF NOT EXISTS venue_user_kol_bindings (
    user_id TEXT PRIMARY KEY REFERENCES venue_users(user_id),
    kol_user_id TEXT NOT NULL REFERENCES venue_kol_profiles(kol_user_id),
    invite_id TEXT NOT NULL,
    bound_ms BIGINT NOT NULL CHECK (bound_ms > 0),
    FOREIGN KEY (invite_id, kol_user_id)
        REFERENCES venue_kol_invites(invite_id, kol_user_id),
    CHECK (user_id <> kol_user_id),
    UNIQUE (user_id, kol_user_id)
);

-- No product role receives UPDATE or DELETE authority for immutable user-to-KOL bindings.
CREATE OR REPLACE FUNCTION venue_reject_kol_rebinding()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KOL binding is immutable';
    END IF;
    IF NEW.user_id IS DISTINCT FROM OLD.user_id
        OR NEW.kol_user_id IS DISTINCT FROM OLD.kol_user_id
        OR NEW.invite_id IS DISTINCT FROM OLD.invite_id THEN
        RAISE EXCEPTION 'KOL binding is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS venue_user_kol_binding_immutable ON venue_user_kol_bindings;
CREATE TRIGGER venue_user_kol_binding_immutable
BEFORE UPDATE OR DELETE ON venue_user_kol_bindings
FOR EACH ROW EXECUTE FUNCTION venue_reject_kol_rebinding();

CREATE TABLE IF NOT EXISTS venue_kol_follow_relations (
    relation_id TEXT PRIMARY KEY,
    follower_user_id TEXT NOT NULL UNIQUE,
    kol_user_id TEXT NOT NULL,
    leader_trading_account_id TEXT NOT NULL,
    follower_trading_account_id TEXT NOT NULL UNIQUE,
    credential_id TEXT NOT NULL UNIQUE,
    relation_state TEXT NOT NULL CHECK (
        relation_state IN ('paused', 'active', 'needs_attention', 'disabled')
    ),
    active_slot SMALLINT UNIQUE CHECK (active_slot BETWEEN 1 AND 200),
    allocated_capital TEXT NOT NULL CHECK (char_length(btrim(allocated_capital)) > 0),
    multiplier TEXT NOT NULL CHECK (char_length(btrim(multiplier)) > 0),
    max_order_notional TEXT NOT NULL CHECK (char_length(btrim(max_order_notional)) > 0),
    max_total_notional TEXT NOT NULL CHECK (char_length(btrim(max_total_notional)) > 0),
    max_deviation_bps INTEGER NOT NULL CHECK (max_deviation_bps BETWEEN 0 AND 5000),
    allowed_symbols JSONB NOT NULL CHECK (jsonb_typeof(allowed_symbols) = 'array'),
    baseline_json JSONB CHECK (baseline_json IS NULL OR jsonb_typeof(baseline_json) = 'object'),
    attention_code TEXT CHECK (attention_code IS NULL OR char_length(attention_code) BETWEEN 1 AND 64),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= created_ms),
    FOREIGN KEY (follower_user_id, kol_user_id)
        REFERENCES venue_user_kol_bindings(user_id, kol_user_id),
    FOREIGN KEY (leader_trading_account_id, kol_user_id)
        REFERENCES venue_kol_profiles(leader_trading_account_id, kol_user_id),
    FOREIGN KEY (follower_trading_account_id, follower_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id),
    FOREIGN KEY (credential_id, follower_user_id, follower_trading_account_id)
        REFERENCES venue_api_credentials(credential_id, user_id, trading_account_id),
    CHECK (follower_user_id <> kol_user_id),
    CHECK (leader_trading_account_id <> follower_trading_account_id),
    CHECK ((relation_state = 'active') = (active_slot IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS venue_kol_follow_relations_kol
    ON venue_kol_follow_relations (kol_user_id, relation_state);
CREATE UNIQUE INDEX IF NOT EXISTS venue_kol_follow_relations_execution_binding
    ON venue_kol_follow_relations (
        relation_id, follower_user_id, follower_trading_account_id, credential_id
    );

CREATE TABLE IF NOT EXISTS venue_kol_source_fills (
    kol_trading_account_id TEXT NOT NULL,
    kol_user_id TEXT NOT NULL,
    native_symbol TEXT NOT NULL,
    native_trade_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    order_side TEXT NOT NULL CHECK (order_side IN ('buy', 'sell')),
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    quantity TEXT NOT NULL CHECK (char_length(btrim(quantity)) > 0),
    price TEXT NOT NULL CHECK (char_length(btrim(price)) > 0),
    occurred_ms BIGINT NOT NULL CHECK (occurred_ms > 0),
    observed_ms BIGINT NOT NULL CHECK (observed_ms >= occurred_ms),
    payload_digest BYTEA NOT NULL CHECK (octet_length(payload_digest) = 32),
    PRIMARY KEY (kol_trading_account_id, native_symbol, native_trade_id),
    FOREIGN KEY (kol_trading_account_id, kol_user_id)
        REFERENCES venue_kol_profiles(leader_trading_account_id, kol_user_id)
);

CREATE TABLE IF NOT EXISTS venue_kol_copy_targets (
    relation_id TEXT NOT NULL REFERENCES venue_kol_follow_relations(relation_id),
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    position_side TEXT NOT NULL CHECK (position_side IN ('long', 'short')),
    copyable_quantity TEXT NOT NULL CHECK (char_length(btrim(copyable_quantity)) > 0),
    target_quantity TEXT NOT NULL CHECK (char_length(btrim(target_quantity)) > 0),
    observed_quantity TEXT NOT NULL CHECK (char_length(btrim(observed_quantity)) > 0),
    target_revision BIGINT NOT NULL CHECK (target_revision > 0),
    last_native_symbol TEXT NOT NULL,
    last_native_trade_id TEXT NOT NULL,
    dirty BOOLEAN NOT NULL,
    updated_ms BIGINT NOT NULL CHECK (updated_ms > 0),
    PRIMARY KEY (relation_id, symbol, position_side)
);

CREATE TABLE IF NOT EXISTS venue_binance_commands (
    command_id TEXT PRIMARY KEY,
    command_origin TEXT NOT NULL CHECK (command_origin IN ('copy', 'terminal')),
    request_id TEXT,
    relation_id TEXT REFERENCES venue_kol_follow_relations(relation_id),
    relation_revision BIGINT CHECK (relation_revision > 0),
    target_revision BIGINT CHECK (target_revision > 0),
    owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
    trading_account_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    symbol TEXT NOT NULL CHECK (symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
    position_side TEXT CHECK (position_side IN ('long', 'short')),
    command_phase TEXT NOT NULL CHECK (command_phase IN ('open', 'close', 'cancel')),
    order_kind TEXT NOT NULL CHECK (order_kind IN ('market', 'limit_gtc', 'cancel_exact')),
    order_side TEXT CHECK (order_side IN ('buy', 'sell')),
    requested_quantity TEXT CHECK (
        requested_quantity IS NULL OR char_length(btrim(requested_quantity)) > 0
    ),
    target_quantity TEXT CHECK (
        target_quantity IS NULL OR char_length(btrim(target_quantity)) > 0
    ),
    limit_price TEXT CHECK (limit_price IS NULL OR char_length(btrim(limit_price)) > 0),
    rule_version TEXT NOT NULL CHECK (char_length(btrim(rule_version)) BETWEEN 1 AND 128),
    native_order_id TEXT,
    selected_native_order_id TEXT,
    client_order_id TEXT NOT NULL UNIQUE CHECK (
        char_length(client_order_id) BETWEEN 1 AND 36
    ),
    command_state TEXT NOT NULL CHECK (command_state IN (
        'pending', 'sending', 'accepted', 'rejected', 'reconcile_required',
        'reconciled', 'cancelled'
    )),
    source_digest BYTEA CHECK (source_digest IS NULL OR octet_length(source_digest) = 32),
    sanitized_error_code TEXT CHECK (
        sanitized_error_code IS NULL OR char_length(sanitized_error_code) BETWEEN 1 AND 64
    ),
    created_ms BIGINT NOT NULL CHECK (created_ms > 0),
    sending_ms BIGINT CHECK (sending_ms >= created_ms),
    accepted_ms BIGINT CHECK (accepted_ms >= sending_ms),
    terminal_ms BIGINT CHECK (terminal_ms >= created_ms),
    updated_ms BIGINT NOT NULL CHECK (updated_ms >= created_ms),
    FOREIGN KEY (trading_account_id, owner_user_id)
        REFERENCES venue_user_trading_accounts(trading_account_id, user_id),
    FOREIGN KEY (credential_id, owner_user_id, trading_account_id)
        REFERENCES venue_api_credentials(credential_id, user_id, trading_account_id),
    FOREIGN KEY (relation_id, owner_user_id, trading_account_id, credential_id)
        REFERENCES venue_kol_follow_relations(
            relation_id, follower_user_id, follower_trading_account_id, credential_id
        ),
    CHECK (
        (command_origin = 'copy' AND request_id IS NULL AND relation_id IS NOT NULL
            AND relation_revision IS NOT NULL AND target_revision IS NOT NULL)
        OR (command_origin = 'terminal' AND request_id IS NOT NULL AND relation_id IS NULL
            AND relation_revision IS NULL AND target_revision IS NULL)
    ),
    CHECK (
        (command_phase = 'cancel' AND order_kind = 'cancel_exact'
            AND position_side IS NULL AND order_side IS NULL
            AND requested_quantity IS NULL AND selected_native_order_id IS NOT NULL)
        OR (command_phase IN ('open', 'close') AND order_kind IN ('market', 'limit_gtc')
            AND position_side IS NOT NULL AND order_side IS NOT NULL
            AND requested_quantity IS NOT NULL AND selected_native_order_id IS NULL)
    ),
    CHECK ((order_kind = 'limit_gtc') = (limit_price IS NOT NULL)),
    CHECK (command_state <> 'cancelled' OR sending_ms IS NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_commands_copy_identity
    ON venue_binance_commands (
        relation_id, relation_revision, target_revision, trading_account_id,
        symbol, position_side, command_phase
    ) WHERE command_origin = 'copy';
CREATE UNIQUE INDEX IF NOT EXISTS venue_binance_commands_terminal_request
    ON venue_binance_commands (owner_user_id, request_id)
    WHERE command_origin = 'terminal';
CREATE INDEX IF NOT EXISTS venue_binance_commands_dispatch
    ON venue_binance_commands (command_state, created_ms, command_id);
