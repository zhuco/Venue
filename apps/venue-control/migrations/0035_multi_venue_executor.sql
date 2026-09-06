-- Extend the existing account/credential and command ledger boundaries for strategy execution on
-- all currently modelled venues.  Historical migrations remain immutable; this migration rewrites
-- only live CHECK constraints and preserves every existing Binance row.

DO $$
DECLARE constraint_name TEXT;
BEGIN
    -- The original account venue check has used generated and explicit names across installs.
    -- Drop only checks that still encode the historical Binance-only domain.
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'venue_user_trading_accounts'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%venue%'
          AND pg_get_constraintdef(oid) LIKE '%binance%'
    LOOP
        EXECUTE format('ALTER TABLE venue_user_trading_accounts DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END
$$;
ALTER TABLE venue_user_trading_accounts
    ADD CONSTRAINT venue_user_trading_accounts_venue_v2 CHECK (
        venue IN ('binance', 'bitget', 'bybit', 'gate', 'hyperliquid', 'okx')
    );
CREATE UNIQUE INDEX IF NOT EXISTS venue_user_trading_accounts_owner_venue
    ON venue_user_trading_accounts (trading_account_id, user_id, venue);

ALTER TABLE venue_api_credentials
    ADD COLUMN IF NOT EXISTS venue TEXT;
UPDATE venue_api_credentials SET venue = 'binance' WHERE venue IS NULL;
ALTER TABLE venue_api_credentials
    ALTER COLUMN venue SET NOT NULL,
    ALTER COLUMN venue SET DEFAULT 'binance',
    ADD COLUMN IF NOT EXISTS strategy_snapshot JSONB;
ALTER TABLE venue_api_credentials
    DROP CONSTRAINT IF EXISTS venue_api_credentials_venue_check;
ALTER TABLE venue_api_credentials
    ADD CONSTRAINT venue_api_credentials_venue_v2 CHECK (
        venue IN ('binance', 'bitget', 'bybit', 'gate', 'hyperliquid', 'okx')
    ),
    ADD CONSTRAINT venue_api_credentials_strategy_snapshot_object CHECK (
        strategy_snapshot IS NULL OR jsonb_typeof(strategy_snapshot) = 'object'
    );

ALTER TABLE venue_binance_commands
    ADD COLUMN IF NOT EXISTS strategy_command JSONB,
    ADD COLUMN IF NOT EXISTS strategy_venue TEXT,
    ADD COLUMN IF NOT EXISTS strategy_nonce BIGINT;

DO $$
DECLARE constraint_name TEXT;
DECLARE constraint_expression TEXT;
BEGIN
    -- Preserve every historical command/grid/mirror predicate verbatim for old origins.  The
    -- strategy branch is admitted by a wrapper, then constrained separately below.
    FOR constraint_name, constraint_expression IN
        SELECT conname, pg_get_expr(conbin, conrelid)
        FROM pg_constraint
        WHERE conrelid = 'venue_binance_commands'::regclass
          AND contype = 'c'
          AND (
              pg_get_expr(conbin, conrelid) LIKE '%command_origin%'
              OR pg_get_expr(conbin, conrelid) LIKE '%command_phase%'
              OR pg_get_expr(conbin, conrelid) LIKE '%order_kind%'
              OR pg_get_expr(conbin, conrelid) LIKE '%position_side%'
              OR pg_get_expr(conbin, conrelid) LIKE '%limit_price%'
          )
    LOOP
        EXECUTE format('ALTER TABLE venue_binance_commands DROP CONSTRAINT %I', constraint_name);
        EXECUTE format(
            'ALTER TABLE venue_binance_commands ADD CONSTRAINT %I CHECK (command_origin = ''strategy'' OR (%s))',
            constraint_name, constraint_expression
        );
    END LOOP;
END
$$;

ALTER TABLE venue_binance_commands
    ADD CONSTRAINT venue_commands_origin_multi_v1 CHECK (
        command_origin IN ('copy', 'terminal', 'grid', 'strategy')
    ),
    ADD CONSTRAINT venue_commands_strategy_fields_v1 CHECK (
        (command_origin = 'strategy'
            AND strategy_command IS NOT NULL
            AND jsonb_typeof(strategy_command) = 'object'
            AND strategy_venue IN ('bitget', 'bybit', 'gate', 'hyperliquid', 'okx')
            AND relation_id IS NULL AND request_id IS NULL
            AND grid_instance_id IS NULL AND grid_batch_id IS NULL
            AND grid_config_revision IS NULL AND grid_plan_revision IS NULL
            AND grid_semantic_key IS NULL AND dispatch_sequence IS NULL
            AND mirror_order_id IS NULL AND copy_risk IS NULL
            AND selected_native_order_id IS NULL)
        OR (command_origin <> 'strategy'
            AND strategy_command IS NULL AND strategy_venue IS NULL AND strategy_nonce IS NULL)
    ),
    ADD CONSTRAINT venue_commands_strategy_nonce_v1 CHECK (
        strategy_nonce IS NULL OR strategy_nonce > 0
    ),
    ADD CONSTRAINT venue_commands_strategy_phase_v1 CHECK (
        command_origin <> 'strategy'
        OR ((command_phase = 'cancel' AND target_client_order_id IS NOT NULL)
            OR (command_phase IN ('open', 'close') AND target_client_order_id IS NULL))
    );

CREATE UNIQUE INDEX IF NOT EXISTS venue_commands_strategy_nonce_unique
    ON venue_binance_commands (strategy_venue, trading_account_id, strategy_nonce)
    WHERE strategy_command IS NOT NULL AND strategy_nonce IS NOT NULL;
CREATE INDEX IF NOT EXISTS venue_commands_strategy_dispatch
    ON venue_binance_commands (trading_account_id, command_state, created_ms, command_id)
    WHERE strategy_command IS NOT NULL;

CREATE OR REPLACE FUNCTION venue_reject_legacy_scope_with_strategy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.mode = 'LIVE' THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended('venue-strategy-admission:' || NEW.trading_account_id, 0)
        );
        IF EXISTS (
            SELECT 1 FROM venue_api_credentials c
            WHERE c.trading_account_id=NEW.trading_account_id
              AND c.deleted_ms IS NULL
              AND c.verification_json->>'strategy_execution'='true'
        ) OR EXISTS (
            SELECT 1 FROM venue_binance_commands c
            WHERE c.trading_account_id=NEW.trading_account_id
              AND c.strategy_command IS NOT NULL
              AND c.command_state IN ('pending','sending','accepted','reconcile_required')
        ) THEN
            RAISE EXCEPTION 'legacy LIVE scope conflicts with strategy execution';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS venue_reject_legacy_scope_with_strategy_trigger
    ON venue_control_strategy_scopes;
CREATE TRIGGER venue_reject_legacy_scope_with_strategy_trigger
BEFORE INSERT OR UPDATE OF venue,mode,trading_account_id
ON venue_control_strategy_scopes
FOR EACH ROW EXECUTE FUNCTION venue_reject_legacy_scope_with_strategy();

CREATE OR REPLACE FUNCTION venue_validate_command_venue()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE credential_venue TEXT;
DECLARE account_venue TEXT;
DECLARE verified BOOLEAN;
DECLARE strategy_enabled BOOLEAN;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('venue-strategy-admission:' || NEW.trading_account_id, 0)
    );
    SELECT c.venue, a.venue,
           COALESCE(c.verification_json->>'verification' = 'verified', FALSE),
           COALESCE(c.verification_json->>'strategy_execution' = 'true', FALSE)
      INTO credential_venue, account_venue, verified, strategy_enabled
      FROM venue_api_credentials c
      JOIN venue_user_trading_accounts a
        ON a.trading_account_id = c.trading_account_id
       AND a.user_id = c.user_id
     WHERE c.credential_id = NEW.credential_id
       AND c.user_id = NEW.owner_user_id
       AND c.trading_account_id = NEW.trading_account_id
       AND c.deleted_ms IS NULL;
    IF NOT FOUND OR credential_venue IS DISTINCT FROM account_venue THEN
        RAISE EXCEPTION 'command credential and account ownership mismatch';
    END IF;
    IF NEW.strategy_command IS NULL THEN
        IF credential_venue <> 'binance' OR account_venue <> 'binance' THEN
            RAISE EXCEPTION 'legacy command requires a Binance account';
        END IF;
    ELSE
        IF NEW.command_origin <> 'strategy'
           OR NEW.strategy_venue IS DISTINCT FROM credential_venue
           OR NEW.strategy_venue IS DISTINCT FROM account_venue
           OR NEW.strategy_venue NOT IN ('bitget', 'bybit', 'gate', 'hyperliquid', 'okx')
           OR NOT verified OR NOT strategy_enabled THEN
            RAISE EXCEPTION 'strategy command venue or credential admission rejected';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS venue_validate_command_venue_trigger ON venue_binance_commands;
CREATE TRIGGER venue_validate_command_venue_trigger
BEFORE INSERT OR UPDATE OF command_origin, strategy_command, strategy_venue,
    credential_id, trading_account_id, owner_user_id
ON venue_binance_commands
FOR EACH ROW EXECUTE FUNCTION venue_validate_command_venue();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'venue_api_credentials_account_venue_fk'
          AND conrelid = 'venue_api_credentials'::regclass
    ) THEN
        ALTER TABLE venue_api_credentials
            ADD CONSTRAINT venue_api_credentials_account_venue_fk
            FOREIGN KEY (trading_account_id, user_id, venue)
            REFERENCES venue_user_trading_accounts (trading_account_id, user_id, venue);
    END IF;
END
$$;
