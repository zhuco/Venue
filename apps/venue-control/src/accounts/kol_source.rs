use super::{AccountError, AccountService, Principal, database_error, error, ms};
use sqlx::Row;
use venue_control_protocol::{accounts::AccountErrorCode as Code, kol_source::*};

const HAS_DEPENDENTS: &str = "SELECT EXISTS(SELECT 1 FROM venue_leader_bots WHERE owner_user_id=$1) OR EXISTS(SELECT 1 FROM venue_kol_follow_relations WHERE kol_user_id=$1)";

impl AccountService {
    pub async fn own_kol_source(
        &self,
        principal: &Principal,
    ) -> Result<KolSourceSummary, AccountError> {
        let row = sqlx::query("SELECT leader_trading_account_id,revision,profile_state FROM venue_kol_profiles WHERE kol_user_id=$1")
            .bind(&principal.user.user_id).fetch_optional(&self.pool).await.map_err(database_error)?.ok_or(error(Code::Forbidden))?;
        let used: bool = sqlx::query_scalar(HAS_DEPENDENTS)
            .bind(&principal.user.user_id)
            .fetch_one(&self.pool)
            .await
            .map_err(database_error)?;
        Ok(KolSourceSummary {
            trading_account_id: row
                .try_get("leader_trading_account_id")
                .map_err(database_error)?,
            revision: u64::try_from(row.try_get::<i64, _>("revision").map_err(database_error)?)
                .map_err(|_| error(Code::Unavailable))?,
            can_change: !used
                && row
                    .try_get::<String, _>("profile_state")
                    .map_err(database_error)?
                    != "disabled",
        })
    }

    pub async fn select_kol_source(
        &self,
        principal: &Principal,
        request: KolSourceRequest,
        now: u64,
    ) -> Result<KolSourceSummary, AccountError> {
        if !venue_control_protocol::leader_bot::valid_id(&request.credential_id)
            || request.expected_revision == 0
        {
            return Err(error(Code::InvalidInput));
        }
        let expected =
            i64::try_from(request.expected_revision).map_err(|_| error(Code::InvalidInput))?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        // The same profile lock serializes bot creation, invite changes and source selection.
        let row = sqlx::query("SELECT leader_trading_account_id,revision,profile_state FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE")
            .bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::Forbidden))?;
        if row
            .try_get::<String, _>("profile_state")
            .map_err(database_error)?
            == "disabled"
        {
            return Err(error(Code::Forbidden));
        }
        let account: String = sqlx::query_scalar("SELECT c.trading_account_id FROM venue_api_credentials c JOIN venue_user_trading_accounts a ON a.trading_account_id=c.trading_account_id AND a.user_id=c.user_id WHERE c.user_id=$1 AND c.credential_id=$2 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' AND c.verification_json->>'dual_position'='true' AND c.verification_json->>'account_mode'='Portfolio Margin · UM' FOR UPDATE OF c,a")
            .bind(&principal.user.user_id).bind(&request.credential_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::VerificationRequired))?;
        if row
            .try_get::<Option<String>, _>("leader_trading_account_id")
            .map_err(database_error)?
            .as_deref()
            != Some(&account)
        {
            let used: bool = sqlx::query_scalar(HAS_DEPENDENTS)
                .bind(&principal.user.user_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(database_error)?;
            if used || row.try_get::<i64, _>("revision").map_err(database_error)? != expected {
                return Err(error(Code::Conflict));
            }
            if row
                .try_get::<String, _>("profile_state")
                .map_err(database_error)?
                == "draft"
            {
                // A draft is an administrator-provisioned identity, never a public registration role.
                sqlx::query("SELECT pg_advisory_xact_lock(470047)")
                    .execute(&mut *tx)
                    .await
                    .map_err(database_error)?;
                let slot: Option<i32> = sqlx::query_scalar("SELECT n FROM generate_series(1,5) n WHERE NOT EXISTS(SELECT 1 FROM venue_kol_profiles WHERE active_slot=n) ORDER BY n LIMIT 1")
                    .fetch_optional(&mut *tx).await.map_err(database_error)?;
                let slot = slot.ok_or(error(Code::Conflict))?;
                let equity: String = sqlx::query_scalar("SELECT verification_json->>'equity' FROM venue_api_credentials WHERE credential_id=$1")
                    .bind(&request.credential_id).fetch_one(&mut *tx).await.map_err(database_error)?;
                if equity
                    .parse::<rust_decimal::Decimal>()
                    .ok()
                    .is_none_or(|v| v <= rust_decimal::Decimal::ZERO)
                {
                    return Err(error(Code::VerificationRequired));
                }
                sqlx::query("UPDATE venue_kol_profiles SET leader_trading_account_id=$1,profile_state='enabled',active_slot=$2,strategy_capital=$3 WHERE kol_user_id=$4")
                    .bind(&account).bind(slot as i16).bind(equity).bind(&principal.user.user_id).execute(&mut *tx).await.map_err(database_error)?;
            }
            // Do not rewrite any historical relationship or bot account identity.
            sqlx::query("UPDATE venue_kol_profiles SET leader_trading_account_id=$1,revision=revision+1,updated_ms=$2 WHERE kol_user_id=$3")
                .bind(account).bind(ms(now)?).bind(&principal.user.user_id).execute(&mut *tx).await.map_err(database_error)?;
        }
        // Selecting a verified source completes initial KOL onboarding; an explicit revocation remains final.
        sqlx::query("SELECT venue_initialize_kol_permission($1,$2)")
            .bind(&principal.user.user_id)
            .bind(ms(now)?)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.own_kol_source(principal).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{
        crypto,
        test_support::{Fixture, TestResult, login, now},
    };

    #[tokio::test]
    async fn source_is_owned_single_and_revision_guarded() -> TestResult {
        let Some(f) = Fixture::create().await? else {
            return Ok(());
        };
        let time = now();
        let session = f.service.register(login("source-kol"), time).await?;
        let owner = f.service.authenticate(session.token.expose(), time).await?;
        let stranger_session = f.service.register(login("source-other"), time).await?;
        let stranger = f
            .service
            .authenticate(stranger_session.token.expose(), time)
            .await?;
        sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,public_name,public_title,public_description,strategy_capital,profile_state,created_ms,updated_ms) VALUES($1,'KOL','Title','','1','draft',$2,$2)")
            .bind(&owner.user.user_id).bind(ms(time)?).execute(&f.pool).await?;
        let mut ids = Vec::new();
        for n in 1..=2_u8 {
            let account = crypto::opaque_id()?;
            let credential = crypto::opaque_id()?;
            sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
                .bind(&account).bind(&owner.user.user_id).bind(vec![n;32]).execute(&f.pool).await?;
            sqlx::query("INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES($1,$2,'fixture',$3,'***',$3,$4,$5,$6)")
                .bind(&credential).bind(&owner.user.user_id).bind(vec![n;32]).bind(&account)
                .bind(serde_json::json!({"verification":"verified","dual_position":true,"account_mode":"Portfolio Margin · UM","equity":"123.45"})).bind(ms(time)?).execute(&f.pool).await?;
            ids.push((credential, account));
        }
        assert!(f.service.own_kol_source(&stranger).await.is_err());
        let request = KolSourceRequest {
            credential_id: ids[0].0.clone(),
            expected_revision: 1,
        };
        assert!(
            f.service
                .select_kol_source(&stranger, request.clone(), time)
                .await
                .is_err()
        );
        let first = f
            .service
            .select_kol_source(&owner, request.clone(), time)
            .await?;
        assert_eq!(first.trading_account_id.as_deref(), Some(ids[0].1.as_str()));
        assert_eq!(first.revision, 2);
        assert_eq!(
            f.service
                .select_kol_source(&owner, request, time)
                .await?
                .revision,
            2
        );
        let stale = KolSourceRequest {
            credential_id: ids[1].0.clone(),
            expected_revision: 1,
        };
        assert!(
            f.service
                .select_kol_source(&owner, stale, time)
                .await
                .is_err()
        );
        let (state, capital): (String, String) = sqlx::query_as(
            "SELECT profile_state,strategy_capital FROM venue_kol_profiles WHERE kol_user_id=$1",
        )
        .bind(&owner.user.user_id)
        .fetch_one(&f.pool)
        .await?;
        assert_eq!((state.as_str(), capital.as_str()), ("enabled", "123.45"));
        let access = f.service.leader_bots_access(&owner).await?;
        assert!(access.can_use);
        assert_eq!(access.permission_revision, 1);
        crate::leader_bot_admin::set_permission(
            &f.pool,
            &owner.user.user_id,
            false,
            1,
            "fixture",
            time,
        )
        .await?;
        f.service
            .select_kol_source(
                &owner,
                KolSourceRequest {
                    credential_id: ids[0].0.clone(),
                    expected_revision: 2,
                },
                time,
            )
            .await?;
        let access = f.service.leader_bots_access(&owner).await?;
        assert!(!access.can_use);
        assert_eq!(access.permission_revision, 2);

        sqlx::query("INSERT INTO venue_leader_bots(bot_id,owner_user_id,trading_account_id,credential_id,bot_state,permission_revision,created_ms,updated_ms,create_request_id,bot_name,bot_description,strategy_capital) VALUES($1,$2,$3,$4,'stopped',1,$5,$5,$1,'fixture','','123.45')")
            .bind(crypto::opaque_id()?).bind(&owner.user.user_id).bind(&ids[0].1).bind(&ids[0].0).bind(ms(time)?).execute(&f.pool).await?;
        assert!(!f.service.own_kol_source(&owner).await?.can_change);
        assert!(
            f.service
                .select_kol_source(
                    &owner,
                    KolSourceRequest {
                        credential_id: ids[1].0.clone(),
                        expected_revision: 2
                    },
                    time
                )
                .await
                .is_err()
        );
        f.cleanup().await
    }
}
