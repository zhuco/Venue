use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use secrecy::SecretString;
use sqlx::Row;
use std::collections::BTreeSet;
use venue_control_protocol::{
    ControlSnapshot, HealthState, StrategyLifecycle, VenueId,
    accounts::{
        AccountErrorCode as Code, AccountOverview, ApiVerificationState as State,
        BindCredentialRequest, CredentialSummary, DeleteCredentialRequest,
    },
};
use venue_gateway_binance::{BinanceCredentials, BinanceProbeError, probe_credentials};
use zeroize::Zeroizing;

impl AccountService {
    pub async fn overview(
        &self,
        principal: &Principal,
        _now_ms: u64,
    ) -> Result<AccountOverview, AccountError> {
        let rows = sqlx::query("SELECT verification_json FROM venue_api_credentials WHERE user_id=$1 AND deleted_ms IS NULL ORDER BY created_ms,credential_id")
            .bind(&principal.user.user_id).fetch_all(&self.pool).await.map_err(database_error)?;
        let mut credentials = Vec::with_capacity(rows.len());
        for row in rows {
            let summary =
                decode_summary(row.try_get("verification_json").map_err(database_error)?)?;
            credentials.push(summary);
        }
        Ok(AccountOverview {
            user: principal.user.clone(),
            credentials,
            selected_credential_id: principal.selected_credential_id.clone(),
        })
    }

    pub async fn bind_credential(
        &self,
        principal: &Principal,
        request: BindCredentialRequest,
        now_ms: u64,
    ) -> Result<CredentialSummary, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit(&format!("bind:{}", principal.user.user_id), 10, now_ms)
            .await?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let summary = self
            .insert_credential(&mut tx, &principal.user.user_id, request, now_ms)
            .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    pub(super) async fn insert_credential(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        user_id: &str,
        request: BindCredentialRequest,
        now_ms: u64,
    ) -> Result<CredentialSummary, AccountError> {
        let id = crypto::opaque_id()?;
        let summary = CredentialSummary {
            credential_id: id.clone(),
            label: request.label.trim().to_owned(),
            venue: VenueId::Binance,
            masked_key: format!(
                "••••{}",
                request
                    .api_key
                    .expose()
                    .get(request.api_key.expose().len().saturating_sub(4)..)
                    .unwrap_or_default()
            ),
            trading_account_id: None,
            verification: State::Unverified,
            verified_ms: None,
            expires_ms: None,
            api_reachable: false,
            dual_position: false,
            account_mode: None,
            has_exposure: None,
        };
        let payload =
            Zeroizing::new(serde_json::to_vec(&request).map_err(|_| error(Code::InvalidInput))?);
        let encrypted = self
            .cipher
            .encrypt(&credential_scope(user_id, &id), &payload)?;
        sqlx::query("SELECT user_id FROM venue_users WHERE user_id=$1 FOR UPDATE")
            .bind(user_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(database_error)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_api_credentials WHERE user_id=$1 AND deleted_ms IS NULL",
        )
        .bind(user_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(database_error)?;
        if count >= 20 {
            return Err(error(Code::RateLimited));
        }
        let inserted = sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,verification_json,created_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (key_fingerprint) DO NOTHING")
            .bind(&id).bind(user_id).bind(&summary.label).bind(crypto::fingerprint(request.api_key.expose().as_bytes()))
            .bind(&summary.masked_key).bind(encrypted).bind(encode_summary(&summary)?).bind(ms(now_ms)?)
            .execute(&mut **tx).await.map_err(database_error)?;
        if inserted.rows_affected() != 1 {
            return Err(error(Code::Conflict));
        }
        Ok(summary)
    }

    pub async fn verify_credential(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
    ) -> Result<CredentialSummary, AccountError> {
        self.verify_with(principal, id, now_ms, |credentials| async move {
            probe_credentials(&credentials).await
        })
        .await
    }

    pub(super) async fn verify_with<F, Fut>(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
        probe: F,
    ) -> Result<CredentialSummary, AccountError>
    where
        F: FnOnce(BinanceCredentials) -> Fut,
        Fut: std::future::Future<
                Output = Result<venue_gateway_binance::BinanceCredentialProbe, BinanceProbeError>,
            >,
    {
        self.rate_limit(&format!("verify:{}", principal.user.user_id), 10, now_ms)
            .await?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query("SELECT encrypted_credentials,verification_json,revision FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL FOR UPDATE")
            .bind(id).bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
        let mut summary =
            decode_summary(row.try_get("verification_json").map_err(database_error)?)?;
        invalidate(&mut summary, State::Unverified);
        let revision: i64 = row.try_get("revision").map_err(database_error)?;
        let revision = revision.checked_add(1).ok_or(error(Code::Unavailable))?;
        let encrypted: Vec<u8> = row
            .try_get("encrypted_credentials")
            .map_err(database_error)?;
        sqlx::query("UPDATE venue_api_credentials SET verification_json=$1,revision=$2 WHERE credential_id=$3")
            .bind(encode_summary(&summary)?).bind(revision).bind(id).execute(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        let payload = self.cipher.decrypt(&scope(principal, id), &encrypted)?;
        let request: BindCredentialRequest =
            serde_json::from_slice(&payload).map_err(|_| error(Code::Unavailable))?;
        let credentials = BinanceCredentials::from_secrets(
            SecretString::from(request.api_key.expose().to_owned()),
            SecretString::from(request.api_secret.expose().to_owned()),
        )
        .map_err(|_| error(Code::InvalidInput))?;
        let result = probe(credentials).await;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let current: Option<i64> = sqlx::query_scalar("SELECT revision FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL FOR UPDATE")
            .bind(id).bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?;
        if current != Some(revision) {
            return Err(error(Code::Conflict));
        }
        match result {
            Ok(probe) => {
                let new_account_id = crypto::opaque_id()?;
                sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3) ON CONFLICT (venue,exchange_identity_hash) DO NOTHING")
                    .bind(&new_account_id).bind(&principal.user.user_id).bind(probe.account_identity_hash.as_slice())
                    .execute(&mut *tx).await.map_err(database_error)?;
                let account = sqlx::query("SELECT trading_account_id,user_id FROM venue_user_trading_accounts WHERE venue='binance' AND exchange_identity_hash=$1")
                    .bind(probe.account_identity_hash.as_slice()).fetch_one(&mut *tx).await.map_err(database_error)?;
                let owner: String = account.try_get("user_id").map_err(database_error)?;
                let account_id: String = account
                    .try_get("trading_account_id")
                    .map_err(database_error)?;
                if owner != principal.user.user_id
                    || summary
                        .trading_account_id
                        .as_ref()
                        .is_some_and(|id| id != &account_id)
                {
                    invalidate(&mut summary, State::AccountConflict);
                } else {
                    summary.trading_account_id = Some(account_id);
                    summary.verification = State::Verified;
                    summary.verified_ms = Some(probe.observed_ms);
                    // A successful binding remains selected across UI sessions. Runtime order
                    // admission still requires a fresh signed private projection and fails closed
                    // when Binance revokes the key, permissions, or account access.
                    summary.expires_ms = None;
                    summary.api_reachable = true;
                    summary.dual_position = true;
                    summary.account_mode = Some("Portfolio Margin · UM".into());
                    summary.has_exposure = Some(probe.has_exposure);
                }
            }
            Err(failure) => invalidate(
                &mut summary,
                match failure {
                    BinanceProbeError::Credentials => State::InvalidCredentials,
                    BinanceProbeError::Permissions => State::PermissionDenied,
                    BinanceProbeError::AccountMode => State::ModeMismatch,
                    BinanceProbeError::Unavailable | BinanceProbeError::Incomplete => {
                        State::NetworkUnavailable
                    }
                },
            ),
        }
        sqlx::query("UPDATE venue_api_credentials SET trading_account_id=$1,verification_json=$2 WHERE credential_id=$3 AND revision=$4")
            .bind(&summary.trading_account_id).bind(encode_summary(&summary)?).bind(id).bind(revision)
            .execute(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    pub async fn select_credential(
        &self,
        principal: &Principal,
        id: &str,
        now_ms: u64,
    ) -> Result<(), AccountError> {
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query("SELECT verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL FOR SHARE")
            .bind(id).bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
        if !decode_summary(row.try_get("verification_json").map_err(database_error)?)?
            .selectable(now_ms)
        {
            return Err(error(Code::VerificationRequired));
        }
        let changed = sqlx::query("UPDATE venue_user_sessions SET selected_credential_id=$1 WHERE token_hash=$2 AND expires_ms>$3")
            .bind(id).bind(&principal.token_hash).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(database_error)?;
        if changed.rows_affected() != 1 {
            return Err(error(Code::Unauthorized));
        }
        tx.commit().await.map_err(database_error)?;
        Ok(())
    }

    pub async fn delete_credential(
        &self,
        principal: &Principal,
        request: DeleteCredentialRequest,
        now_ms: u64,
    ) -> Result<(), AccountError> {
        self.delete_with(principal, request, now_ms, |credentials| async move {
            probe_credentials(&credentials).await
        })
        .await
    }

    async fn delete_with<F, Fut>(
        &self,
        principal: &Principal,
        request: DeleteCredentialRequest,
        now_ms: u64,
        probe: F,
    ) -> Result<(), AccountError>
    where
        F: FnOnce(BinanceCredentials) -> Fut,
        Fut: std::future::Future<
                Output = Result<venue_gateway_binance::BinanceCredentialProbe, BinanceProbeError>,
            >,
    {
        self.confirm_password(principal, request.password, now_ms)
            .await?;
        let id = request.credential_id;
        let overview = self.overview(principal, now_ms).await?;
        let initial = overview
            .credentials
            .into_iter()
            .find(|c| c.credential_id == id)
            .ok_or(error(Code::NotFound))?;
        // A never-verified binding has never been eligible for execution. Once bound to an
        // account, fresh signed readback and stopped local custody are required for deletion.
        if initial.trading_account_id.is_some() {
            let checked = self.verify_with(principal, &id, now_ms, probe).await?;
            if checked.verification != State::Verified || checked.has_exposure != Some(false) {
                return Err(error(Code::AccountInUse));
            }
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query("SELECT verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL FOR UPDATE")
            .bind(&id).bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::NotFound))?;
        let summary = decode_summary(row.try_get("verification_json").map_err(database_error)?)?;
        if let Some(account_id) = &summary.trading_account_id {
            // All keys for one real account share this deletion/command barrier.
            // Locking only the removed key would race with another key's commands.
            sqlx::query("SELECT trading_account_id FROM venue_user_trading_accounts WHERE trading_account_id=$1 AND user_id=$2 FOR UPDATE")
                .bind(account_id).bind(&principal.user.user_id).fetch_one(&mut *tx).await.map_err(database_error)?;
            let checked_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| u64::try_from(d.as_millis()).ok())
                .ok_or(error(Code::Unavailable))?;
            if !summary.selectable(checked_now) || summary.has_exposure != Some(false) {
                return Err(error(Code::AccountInUse));
            }
            let snapshot: Option<serde_json::Value> = sqlx::query_scalar(
                "SELECT snapshot_json FROM venue_control_snapshots WHERE singleton=TRUE FOR SHARE",
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(database_error)?;
            if let Some(value) = snapshot {
                let snapshot: ControlSnapshot =
                    serde_json::from_value(value).map_err(|_| error(Code::Unavailable))?;
                snapshot.validate().map_err(|_| error(Code::Unavailable))?;
                if snapshot.accounts.iter().any(|a| {
                    &a.trading_account_id == account_id && a.health != HealthState::Stopped
                }) || snapshot.strategies.iter().any(|s| {
                    &s.trading_account_id == account_id
                        && (s.lifecycle != StrategyLifecycle::Stopped
                            || s.open_orders != 0
                            || !s.long_quantity.is_zero()
                            || !s.short_quantity.is_zero())
                }) {
                    return Err(error(Code::AccountInUse));
                }
            }
            let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_control_command_inbox WHERE command_json->>'trading_account_id'=$1 AND receipt_json->>'state' IN ('accepted','unknown'))")
                .bind(account_id).fetch_one(&mut *tx).await.map_err(database_error)?;
            if pending {
                return Err(error(Code::AccountInUse));
            }
        }
        sqlx::query("UPDATE venue_user_sessions SET selected_credential_id=NULL WHERE selected_credential_id=$1").bind(&id)
            .execute(&mut *tx).await.map_err(database_error)?;
        // Remove encrypted material, while the independent account identity remains stable.
        sqlx::query("DELETE FROM venue_api_credentials WHERE credential_id=$1")
            .bind(&id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        Ok(())
    }

    pub async fn owned_account_ids(
        &self,
        principal: &Principal,
    ) -> Result<BTreeSet<String>, AccountError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT trading_account_id FROM venue_user_trading_accounts WHERE user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        Ok(ids.into_iter().collect())
    }

    pub async fn owns_receipt(
        &self,
        principal: &Principal,
        request_id: &str,
    ) -> Result<bool, AccountError> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_control_command_inbox c JOIN venue_user_trading_accounts a ON a.trading_account_id=c.trading_account_id WHERE a.user_id=$1 AND c.request_id=$2)")
            .bind(&principal.user.user_id).bind(request_id).fetch_one(&self.pool).await.map_err(database_error)
    }

    pub async fn authorize_command(
        &self,
        principal: &Principal,
        venue: VenueId,
        account_id: &str,
        now_ms: u64,
    ) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, AccountError> {
        if venue != VenueId::Binance {
            return Err(error(Code::Forbidden));
        }
        let id = principal
            .selected_credential_id
            .as_ref()
            .ok_or(error(Code::VerificationRequired))?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row: Option<serde_json::Value> = sqlx::query_scalar("SELECT c.verification_json FROM venue_api_credentials c JOIN venue_user_sessions s ON s.selected_credential_id=c.credential_id WHERE c.credential_id=$1 AND c.user_id=$2 AND c.trading_account_id=$3 AND c.deleted_ms IS NULL AND s.token_hash=$4 AND s.expires_ms>$5 FOR SHARE OF c,s")
            .bind(id).bind(&principal.user.user_id).bind(account_id).bind(&principal.token_hash).bind(ms(now_ms)?).fetch_optional(&mut *tx).await.map_err(database_error)?;
        let summary = decode_summary(row.ok_or(error(Code::Forbidden))?)?;
        if summary.selectable(now_ms) {
            sqlx::query("SELECT trading_account_id FROM venue_user_trading_accounts WHERE trading_account_id=$1 AND user_id=$2 FOR SHARE")
                .bind(account_id).bind(&principal.user.user_id).fetch_one(&mut *tx).await.map_err(database_error)?;
            Ok(tx)
        } else {
            Err(error(Code::VerificationRequired))
        }
    }
}

#[cfg(test)]
mod tests;

pub(crate) fn credential_scope(user_id: &str, credential_id: &str) -> String {
    format!("venue-api-v1:{user_id}:{credential_id}")
}

fn scope(principal: &Principal, id: &str) -> String {
    credential_scope(&principal.user.user_id, id)
}
pub(super) fn decode_summary(value: serde_json::Value) -> Result<CredentialSummary, AccountError> {
    serde_json::from_value(value).map_err(|_| error(Code::Unavailable))
}
fn encode_summary(value: &CredentialSummary) -> Result<serde_json::Value, AccountError> {
    serde_json::to_value(value).map_err(|_| error(Code::Unavailable))
}
fn invalidate(summary: &mut CredentialSummary, state: State) {
    summary.verification = state;
    summary.verified_ms = None;
    summary.expires_ms = None;
    summary.api_reachable = false;
    summary.dual_position = false;
    summary.account_mode = None;
    summary.has_exposure = None;
}
