use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use sqlx::Row;
use venue_control_protocol::{
    accounts::{AccountErrorCode as Code, CredentialSummary, UserSummary},
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
        let rows = sqlx::query("SELECT m.managed_id,c.verification_json FROM venue_kol_managed_followers m JOIN venue_api_credentials c ON c.credential_id=m.credential_id AND c.user_id=m.follower_user_id WHERE m.kol_user_id=$1 AND c.deleted_ms IS NULL ORDER BY m.created_ms,m.managed_id LIMIT 200")
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
        if let Some(row) = sqlx::query("SELECT m.managed_id,m.request_hash,c.verification_json FROM venue_kol_managed_followers m JOIN venue_api_credentials c ON c.credential_id=m.credential_id WHERE m.kol_user_id=$1 AND m.request_id=$2")
            .bind(&principal.user.user_id).bind(&request.request_id).fetch_optional(&mut *tx).await.map_err(database_error)? {
            if row.try_get::<Vec<u8>,_>("request_hash").map_err(database_error)? != request_hash { return Err(error(Code::Conflict)); }
            let summary = serde_json::from_value(row.try_get("verification_json").map_err(database_error)?).map_err(|_| error(Code::Unavailable))?;
            return Ok(managed_summary(row.try_get("managed_id").map_err(database_error)?, summary));
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_kol_managed_followers WHERE kol_user_id=$1",
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
        sqlx::query("INSERT INTO venue_kol_managed_followers(managed_id,kol_user_id,follower_user_id,credential_id,request_id,request_hash,created_ms) VALUES($1,$2,$3,$4,$5,$6,$7)")
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

    async fn managed_verification_subject(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
    ) -> Result<(Principal, String), AccountError> {
        if !valid_id(id) {
            return Err(error(Code::InvalidInput));
        }
        let row = sqlx::query("SELECT m.follower_user_id,m.credential_id,u.username FROM venue_kol_managed_followers m JOIN venue_kol_profiles p ON p.kol_user_id=m.kol_user_id JOIN venue_users u ON u.user_id=m.follower_user_id WHERE m.managed_id=$1 AND m.kol_user_id=$2 AND p.profile_state='enabled' AND NOT u.login_enabled")
            .bind(id).bind(&principal.user.user_id).fetch_optional(&self.pool).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
        self.rate_limit(
            &format!("managed-verify:{}", principal.user.user_id),
            20,
            now_ms,
        )
        .await?;
        // Internal scope is used solely by the existing read-only credential probe, never
        // returned to a caller or installed as a login/session/selected trading account.
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
