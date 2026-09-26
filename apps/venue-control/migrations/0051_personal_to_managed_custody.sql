-- Retire personal custody without changing historical account IDs, owners or exchange identity.
ALTER TABLE venue_user_trading_accounts ADD COLUMN IF NOT EXISTS retired_ms BIGINT;
ALTER TABLE venue_user_trading_accounts ADD COLUMN IF NOT EXISTS successor_account_id TEXT
    REFERENCES venue_user_trading_accounts(trading_account_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE venue_user_trading_accounts ADD COLUMN IF NOT EXISTS retired_by_managed_id TEXT
    REFERENCES venue_managed_credentials(managed_id);
ALTER TABLE venue_user_trading_accounts ADD CONSTRAINT venue_account_custody_retirement CHECK (
    (retired_ms IS NULL AND successor_account_id IS NULL AND retired_by_managed_id IS NULL)
    OR (venue='binance' AND retired_ms IS NOT NULL AND retired_ms>0 AND successor_account_id IS NOT NULL
        AND successor_account_id<>trading_account_id AND retired_by_managed_id IS NOT NULL)
);
CREATE UNIQUE INDEX venue_account_active_exchange_identity
    ON venue_user_trading_accounts(venue,exchange_identity_hash) WHERE retired_ms IS NULL;
ALTER TABLE venue_user_trading_accounts
    DROP CONSTRAINT venue_user_trading_accounts_venue_exchange_identity_hash_key;
