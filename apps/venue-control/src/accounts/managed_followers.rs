use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use sqlx::Row;
use venue_control_protocol::{
    accounts::{AccountErrorCode as Code, CredentialSummary, UserSummary},
    kol::{
        FollowLifecycleRequest, FollowRelationSummary, FollowRiskSettings,
        FollowSettingsUpsertRequest, KOL_SCHEMA_VERSION,
    },
    leader_bot::valid_id,
    managed_followers::*,
};
use zeroize::Zeroizing;

impl AccountService {
    pub async fn managed_followers(
        &self,
        principal: &Principal,
    ) -> Result<ManagedFollowers, AccountError> {
        let enabled: Option<bool> = sqlx::query_scalar(
            "SELECT profile_state='enabled' FROM venue_kol_profiles WHERE kol_user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        let rows = sqlx::query("SELECT m.managed_id,c.verification_json FROM venue_managed_credentials m JOIN venue_api_credentials c ON c.credential_id=m.credential_id AND c.user_id=m.follower_user_id WHERE m.kol_user_id=$1 AND c.deleted_ms IS NULL ORDER BY m.created_ms,m.managed_id LIMIT 200")
            .bind(&principal.user.user_id).fetch_all(&self.pool).await.map_err(database_error)?;
        let accounts = rows
            .into_iter()
            .map(|row| {
                let summary: CredentialSummary = serde_json::from_value(
                    row.try_get("verification_json").map_err(database_error)?,
                )
                .map_err(|_| error(Code::Unavailable))?;
                Ok(managed_summary(
                    row.try_get("managed_id").map_err(database_error)?,
                    summary,
                ))
            })
            .collect::<Result<_, AccountError>>()?;
        Ok(ManagedFollowers {
            can_manage: enabled == Some(true),
            accounts,
        })
    }

    pub async fn create_managed_follower(
        &self,
        principal: &Principal,
        request: ManagedFollowerCreateRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowerSummary, AccountError> {
        if !valid_id(&request.request_id) || !request.credential.valid() {
            return Err(error(Code::InvalidInput));
        }
        let payload =
            Zeroizing::new(serde_json::to_vec(&request).map_err(|_| error(Code::InvalidInput))?);
        let request_hash = crypto::fingerprint(&payload);
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        // Serialize admission and idempotent retries with KOL disablement and the 200-account cap.
        let enabled: Option<bool> = sqlx::query_scalar("SELECT profile_state='enabled' FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE")
            .bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?;
        if enabled != Some(true) {
            return Err(error(Code::Forbidden));
        }
        if let Some(row) = sqlx::query("SELECT m.managed_id,m.request_hash,c.verification_json FROM venue_managed_credentials m JOIN venue_api_credentials c ON c.credential_id=m.credential_id WHERE m.kol_user_id=$1 AND m.request_id=$2")
            .bind(&principal.user.user_id).bind(&request.request_id).fetch_optional(&mut *tx).await.map_err(database_error)? {
            if row.try_get::<Vec<u8>,_>("request_hash").map_err(database_error)? != request_hash { return Err(error(Code::Conflict)); }
            let summary = serde_json::from_value(row.try_get("verification_json").map_err(database_error)?).map_err(|_| error(Code::Unavailable))?;
            return Ok(managed_summary(row.try_get("managed_id").map_err(database_error)?, summary));
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_managed_credentials WHERE kol_user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(database_error)?;
        if count >= 200 {
            return Err(error(Code::RateLimited));
        }
        let user_id = crypto::opaque_id()?;
        let managed_id = crypto::opaque_id()?;
        sqlx::query("INSERT INTO venue_users(user_id,username,password_hash,created_ms,login_enabled) VALUES($1,$2,$3,$4,false)")
            .bind(&user_id).bind(format!("managed:{user_id}")).bind(&self.dummy_hash).bind(ms(now_ms)?)
            .execute(&mut *tx).await.map_err(database_error)?;
        let credential = self
            .insert_credential(&mut tx, &user_id, request.credential, now_ms)
            .await?;
        sqlx::query("INSERT INTO venue_managed_credentials(managed_id,kol_user_id,follower_user_id,credential_id,request_id,request_hash,created_ms) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(&managed_id).bind(&principal.user.user_id).bind(&user_id).bind(&credential.credential_id)
            .bind(&request.request_id).bind(request_hash).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        Ok(managed_summary(managed_id, credential))
    }

    pub async fn verify_managed_follower(
        &self,
        principal: &Principal,
        request: ManagedFollowerVerifyRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowerSummary, AccountError> {
        let (subject, credential_id) = self
            .managed_verification_subject(principal, &request.managed_id, now_ms)
            .await?;
        let summary = self
            .verify_credential(&subject, &credential_id, now_ms)
            .await?;
        Ok(managed_summary(request.managed_id, summary))
    }

    pub async fn upsert_managed_follow_settings(
        &self,
        principal: &Principal,
        request: ManagedFollowSettingsUpsertRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowRelationSummary, AccountError> {
        let (subject, credential_id) = self
            .managed_verification_subject(principal, &request.managed_id, now_ms)
            .await?;
        let relation = self
            .upsert_follow_settings_scoped(
                &subject,
                FollowSettingsUpsertRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: request.request_id,
                    settings: managed_settings(request.settings, credential_id),
                    expected_revision: request.expected_revision,
                },
                now_ms,
                Some((&principal.user.user_id, &request.managed_id)),
            )
            .await?;
        Ok(managed_relation_summary(request.managed_id, relation))
    }

    pub async fn managed_follow_status(
        &self,
        principal: &Principal,
        request: ManagedFollowStatusRequest,
        now_ms: u64,
    ) -> Result<Option<ManagedFollowRelationSummary>, AccountError> {
        let (subject, _) = self
            .managed_follow_subject(principal, &request.managed_id, now_ms, false)
            .await?;
        match self.follow_relation(&subject).await {
            Ok(relation) => Ok(Some(managed_relation_summary(request.managed_id, relation))),
            Err(cause) if cause.code == Code::NotFound => Ok(None),
            Err(cause) => Err(cause),
        }
    }

    pub async fn request_managed_follow_lifecycle(
        &self,
        principal: &Principal,
        request: ManagedFollowLifecycleRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowRelationSummary, AccountError> {
        let (subject, _) = self
            .managed_follow_subject(
                principal,
                &request.managed_id,
                now_ms,
                request.action == venue_control_protocol::kol::FollowLifecycleAction::Activate,
            )
            .await?;
        let relation = self
            .request_follow_lifecycle_scoped(
                &subject,
                FollowLifecycleRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: request.request_id,
                    relation_id: request.relation_id,
                    expected_revision: request.expected_revision,
                    action: request.action,
                    risk_confirmed: request.risk_confirmed,
                },
                now_ms,
                Some((&principal.user.user_id, &request.managed_id)),
            )
            .await?;
        Ok(managed_relation_summary(request.managed_id, relation))
    }

    pub(super) async fn managed_verification_subject(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
    ) -> Result<(Principal, String), AccountError> {
        self.managed_follow_subject(principal, id, now_ms, true)
            .await
    }

    async fn managed_follow_subject(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
        require_enabled: bool,
    ) -> Result<(Principal, String), AccountError> {
        if !valid_id(id) {
            return Err(error(Code::InvalidInput));
        }
        let row = sqlx::query("SELECT m.follower_user_id,m.credential_id,u.username FROM venue_managed_credentials m JOIN venue_kol_profiles p ON p.kol_user_id=m.kol_user_id JOIN venue_users u ON u.user_id=m.follower_user_id WHERE m.managed_id=$1 AND m.kol_user_id=$2 AND (NOT $3 OR p.profile_state='enabled') AND NOT u.login_enabled")
            .bind(id).bind(&principal.user.user_id).bind(require_enabled).fetch_optional(&self.pool).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
        self.rate_limit(
            &format!("managed-verify:{}", principal.user.user_id),
            20,
            now_ms,
        )
        .await?;
        // Internal subjects never become login sessions. Trading lifecycle calls recheck
        // managed ownership inside the same transaction as the relation and request audit.
        Ok((
            Principal {
                user: UserSummary {
                    user_id: row.try_get("follower_user_id").map_err(database_error)?,
                    username: row.try_get("username").map_err(database_error)?,
                },
                token_hash: Vec::new(),
                selected_credential_id: None,
            },
            row.try_get("credential_id").map_err(database_error)?,
        ))
    }
}

fn managed_settings(
    settings: ManagedFollowRiskSettings,
    credential_id: String,
) -> FollowRiskSettings {
    FollowRiskSettings {
        credential_id,
        sizing: settings.sizing,
        allocated_capital: settings.allocated_capital,
        multiplier: settings.multiplier,
        max_order_notional: settings.max_order_notional,
        max_total_notional: settings.max_total_notional,
        max_deviation_bps: settings.max_deviation_bps,
        allowed_symbols: settings.allowed_symbols,
    }
}

fn managed_relation_summary(
    managed_id: String,
    relation: FollowRelationSummary,
) -> ManagedFollowRelationSummary {
    ManagedFollowRelationSummary {
        managed_id,
        relation_id: relation.relation_id,
        state: relation.state,
        revision: relation.revision,
        settings: ManagedFollowRiskSettings {
            sizing: relation.settings.sizing,
            allocated_capital: relation.settings.allocated_capital,
            multiplier: relation.settings.multiplier,
            max_order_notional: relation.settings.max_order_notional,
            max_total_notional: relation.settings.max_total_notional,
            max_deviation_bps: relation.settings.max_deviation_bps,
            allowed_symbols: relation.settings.allowed_symbols,
        },
        activation_requested: relation.activation_requested,
    }
}

fn managed_summary(managed_id: String, summary: CredentialSummary) -> ManagedFollowerSummary {
    ManagedFollowerSummary {
        managed_id,
        label: summary.label,
        masked_key: summary.masked_key,
        verification: summary.verification,
        verified_ms: summary.verified_ms,
    }
}

#[cfg(test)]
mod tests;
