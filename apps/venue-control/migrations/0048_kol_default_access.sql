-- Existing explicit revocations remain effective; only missing initial grants are provisioned.
WITH granted AS (
    INSERT INTO venue_leader_bot_permissions(kol_user_id,enabled,revision,updated_by,updated_ms)
    SELECT kol_user_id,true,1,'kol_default_access',GREATEST(updated_ms,1)
    FROM venue_kol_profiles WHERE profile_state <> 'disabled'
    ON CONFLICT(kol_user_id) DO NOTHING
    RETURNING kol_user_id,revision,enabled,updated_by,updated_ms
)
INSERT INTO venue_leader_bot_permission_audit(kol_user_id,revision,enabled,operator,occurred_ms)
SELECT kol_user_id,revision,enabled,updated_by,updated_ms FROM granted;

-- Control may initialize only a verified, already provisioned KOL. It cannot rewrite or restore grants.
CREATE OR REPLACE FUNCTION venue_initialize_kol_permission(kol_id TEXT, observed_ms BIGINT)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
BEGIN
    IF observed_ms <= 0 OR NOT EXISTS (
        SELECT 1 FROM venue_kol_profiles p JOIN venue_api_credentials c
          ON c.user_id=p.kol_user_id AND c.trading_account_id=p.leader_trading_account_id
        WHERE p.kol_user_id=kol_id AND p.profile_state='enabled' AND c.deleted_ms IS NULL
          AND c.verification_json->>'verification'='verified'
          AND c.verification_json->>'dual_position'='true'
          AND c.verification_json->>'account_mode'='Portfolio Margin · UM'
    ) THEN RAISE EXCEPTION 'verified KOL source required'; END IF;
    WITH granted AS (
        INSERT INTO venue_leader_bot_permissions(kol_user_id,enabled,revision,updated_by,updated_ms)
        VALUES(kol_id,true,1,'kol_source_onboarding',observed_ms)
        ON CONFLICT(kol_user_id) DO NOTHING
        RETURNING kol_user_id,revision,enabled,updated_by,updated_ms
    )
    INSERT INTO venue_leader_bot_permission_audit(kol_user_id,revision,enabled,operator,occurred_ms)
    SELECT kol_user_id,revision,enabled,updated_by,updated_ms FROM granted;
END;
$$;
REVOKE ALL ON FUNCTION venue_initialize_kol_permission(TEXT,BIGINT) FROM PUBLIC;
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='venue_control_api') THEN
        GRANT EXECUTE ON FUNCTION venue_initialize_kol_permission(TEXT,BIGINT) TO venue_control_api;
    END IF;
END $$;
