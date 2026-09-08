-- Administrators can provision a draft KOL before its owner supplies credentials.
ALTER TABLE venue_kol_profiles ALTER COLUMN leader_trading_account_id DROP NOT NULL;
ALTER TABLE venue_kol_profiles ADD CONSTRAINT venue_kol_source_required_when_enabled
    CHECK (profile_state <> 'enabled' OR leader_trading_account_id IS NOT NULL);
