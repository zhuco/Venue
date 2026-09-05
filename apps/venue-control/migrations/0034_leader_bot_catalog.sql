-- A KOL may keep multiple built-in leader-bot configurations on the same verified source
-- account. Only one may be non-stopped at a time because followers do not select a bot in the
-- current private product flow; this prevents one source order from being mirrored twice.
ALTER TABLE venue_leader_bots
    DROP CONSTRAINT IF EXISTS venue_leader_bots_owner_user_id_key,
    DROP CONSTRAINT IF EXISTS venue_leader_bots_trading_account_id_key;

ALTER TABLE venue_leader_bots
    ADD COLUMN IF NOT EXISTS bot_name TEXT,
    ADD COLUMN IF NOT EXISTS bot_description TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS strategy_capital TEXT,
    ADD COLUMN IF NOT EXISTS config_revision BIGINT NOT NULL DEFAULT 1;

UPDATE venue_leader_bots b
SET bot_name = COALESCE(b.bot_name, p.public_title),
    strategy_capital = COALESCE(b.strategy_capital, p.strategy_capital)
FROM venue_kol_profiles p
WHERE p.kol_user_id = b.owner_user_id
  AND (b.bot_name IS NULL OR b.strategy_capital IS NULL);

ALTER TABLE venue_leader_bots
    ALTER COLUMN bot_name SET NOT NULL,
    ALTER COLUMN strategy_capital SET NOT NULL;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'venue_leader_bots'::regclass
          AND conname = 'venue_leader_bots_name'
    ) THEN
        ALTER TABLE venue_leader_bots ADD CONSTRAINT venue_leader_bots_name
            CHECK (char_length(btrim(bot_name)) BETWEEN 1 AND 64 AND bot_name = btrim(bot_name));
        ALTER TABLE venue_leader_bots ADD CONSTRAINT venue_leader_bots_description
            CHECK (char_length(bot_description) <= 500);
        ALTER TABLE venue_leader_bots ADD CONSTRAINT venue_leader_bots_strategy_capital
            CHECK (strategy_capital::numeric > 0);
        ALTER TABLE venue_leader_bots ADD CONSTRAINT venue_leader_bots_config_revision
            CHECK (config_revision > 0);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS venue_leader_bots_owner_create_request
    ON venue_leader_bots(owner_user_id, create_request_id);
CREATE UNIQUE INDEX IF NOT EXISTS venue_leader_bots_one_active_per_owner
    ON venue_leader_bots(owner_user_id) WHERE bot_state <> 'stopped';
CREATE INDEX IF NOT EXISTS venue_leader_bots_owner_updated
    ON venue_leader_bots(owner_user_id, updated_ms DESC, bot_id);
