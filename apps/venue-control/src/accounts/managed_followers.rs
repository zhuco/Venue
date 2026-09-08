use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use rust_decimal::Decimal;
use sqlx::{PgPool, Postgres, Row, Transaction};
use venue_control_protocol::{
    accounts::{AccountErrorCode as Code, CredentialSummary, UserSummary},
    follow_sizing::{FollowAuthorization, FollowSizing},
    kol::{
        FollowLifecycleAction, FollowLifecycleRequest, FollowLifecycleState, FollowRelationSummary,
        FollowRiskSettings, FollowSettingsUpsertRequest, KOL_SCHEMA_VERSION,
    },
    leader_bot::valid_id,
    managed_followers::*,
};
use venue_gateway_binance::{BinanceCredentials, BinanceProbeError, probe_credentials};
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
        let rows = sqlx::query("SELECT m.managed_id,c.verification_json FROM venue_managed_credentials m JOIN venue_api_credentials c ON c.credential_id=m.credential_id AND c.user_id=m.follower_user_id WHERE m.kol_user_id=$1 AND m.delete_requested_ms IS NULL AND c.deleted_ms IS NULL ORDER BY m.created_ms,m.managed_id LIMIT 200")
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
        if !valid_id(&request.request_id)
            || !request.credential.valid()
            || !request.authorization.valid()
        {
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
            "SELECT count(*) FROM venue_managed_credentials m JOIN venue_api_credentials c USING(credential_id) WHERE m.kol_user_id=$1 AND m.delete_requested_ms IS NULL AND c.deleted_ms IS NULL",
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
        super::credentials::save_follow_authorization(
            &mut tx,
            &credential.credential_id,
            request.authorization,
        )
        .await?;
        sqlx::query("INSERT INTO venue_managed_credentials(managed_id,kol_user_id,follower_user_id,credential_id,request_id,request_hash,created_ms) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(&managed_id).bind(&principal.user.user_id).bind(&user_id).bind(&credential.credential_id)
            .bind(&request.request_id).bind(request_hash).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(database_error)?;
        bind_managed_owner(
            &mut tx,
            &managed_id,
            &principal.user.user_id,
            &user_id,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(managed_summary(managed_id, credential))
    }

    pub async fn verify_managed_follower(
        &self,
        principal: &Principal,
        request: ManagedFollowerVerifyRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowerSummary, AccountError> {
        self.verify_managed_follower_with(principal, request, now_ms, |credentials| async move {
            probe_credentials(&credentials).await
        })
        .await
    }

    async fn verify_managed_follower_with<F, Fut>(
        &self,
        principal: &Principal,
        request: ManagedFollowerVerifyRequest,
        now_ms: u64,
        probe: F,
    ) -> Result<ManagedFollowerSummary, AccountError>
    where
        F: FnOnce(BinanceCredentials) -> Fut,
        Fut: std::future::Future<
                Output = Result<venue_gateway_binance::BinanceCredentialProbe, BinanceProbeError>,
            >,
    {
        let (subject, credential_id) = self
            .managed_verification_subject(principal, &request.managed_id, now_ms)
            .await?;
        let summary = self
            .verify_with(&subject, &credential_id, now_ms, probe)
            .await?;
        if summary.verification == venue_control_protocol::accounts::ApiVerificationState::Verified
        {
            let mut tx = self.pool.begin().await.map_err(database_error)?;
            bind_managed_owner(
                &mut tx,
                &request.managed_id,
                &principal.user.user_id,
                &subject.user.user_id,
                now_ms,
            )
            .await?;
            tx.commit().await.map_err(database_error)?;
            self.activate_saved_follow_authorization(
                &subject,
                &credential_id,
                &summary,
                now_ms,
                Some((&principal.user.user_id, &request.managed_id)),
            )
            .await?;
        }
        Ok(managed_summary(request.managed_id, summary))
    }

    pub async fn delete_managed_follower(
        &self,
        principal: &Principal,
        request: ManagedFollowerDeleteRequest,
        now_ms: u64,
    ) -> Result<ManagedFollowers, AccountError> {
        if !valid_id(&request.managed_id) {
            return Err(error(Code::InvalidInput));
        }
        let (subject, _) = self
            .managed_follow_subject(principal, &request.managed_id, now_ms, false)
            .await?;
        match self.follow_relation(&subject).await {
            Ok(relation)
                if relation.state != FollowLifecycleState::Disabled
                    && (relation.state != FollowLifecycleState::Paused
                        || relation.activation_requested) =>
            {
                self.request_managed_follow_lifecycle(
                    principal,
                    ManagedFollowLifecycleRequest {
                        request_id: crypto::opaque_id()?,
                        managed_id: request.managed_id.clone(),
                        relation_id: relation.relation_id,
                        expected_revision: relation.revision,
                        action: FollowLifecycleAction::Pause,
                        risk_confirmed: false,
                    },
                    now_ms,
                )
                .await?;
            }
            Ok(_) => {}
            Err(cause) if cause.code == Code::NotFound => {}
            Err(cause) => return Err(cause),
        }
        let changed = sqlx::query("UPDATE venue_managed_credentials m SET delete_requested_ms=$1 FROM venue_api_credentials c WHERE m.managed_id=$2 AND m.kol_user_id=$3 AND m.follower_user_id=$4 AND c.credential_id=m.credential_id AND c.user_id=m.follower_user_id AND m.delete_requested_ms IS NULL AND c.deleted_ms IS NULL")
            .bind(ms(now_ms)?).bind(&request.managed_id).bind(&principal.user.user_id)
            .bind(&subject.user.user_id).execute(&self.pool).await.map_err(database_error)?;
        if changed.rows_affected() != 1 {
            return Err(error(Code::NotFound));
        }
        self.managed_followers(principal).await
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
        let row = sqlx::query("SELECT m.follower_user_id,m.credential_id,u.username FROM venue_managed_credentials m JOIN venue_kol_profiles p ON p.kol_user_id=m.kol_user_id JOIN venue_users u ON u.user_id=m.follower_user_id WHERE m.managed_id=$1 AND m.kol_user_id=$2 AND m.delete_requested_ms IS NULL AND (NOT $3 OR p.profile_state='enabled') AND NOT u.login_enabled")
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

    pub(super) async fn activate_saved_follow_authorization(
        &self,
        principal: &Principal,
        credential_id: &str,
        summary: &CredentialSummary,
        now_ms: u64,
        managed: Option<(&str, &str)>,
    ) -> Result<(), AccountError> {
        // The signed probe completes after the HTTP request timestamp was captured. Use its
        // observation time so the freshly verified credential is not rejected as future-dated.
        let activation_now_ms = now_ms.max(summary.verified_ms.unwrap_or(now_ms));
        let value: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT follow_authorization_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL",
        )
        .bind(credential_id)
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .flatten();
        let Some(value) = value else {
            return Ok(());
        };
        let authorization: FollowAuthorization =
            serde_json::from_value(value).map_err(|_| error(Code::Unavailable))?;
        if !authorization.valid() {
            return Err(error(Code::Unavailable));
        }
        let has_follow_owner: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_user_kol_bindings WHERE user_id=$1)",
        )
        .bind(&principal.user.user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)?;
        if !has_follow_owner {
            return Ok(());
        }
        if summary.has_exposure != Some(false) {
            return Err(error(Code::AccountInUse));
        }
        let equity = summary.equity.ok_or(error(Code::Unavailable))?;
        if equity <= Decimal::ZERO {
            return Err(error(Code::AccountInUse));
        }
        // Keep legacy wire fields valid; new mirrors persist ExchangeAccount admission.
        let order = match authorization.sizing {
            FollowSizing::Proportional => equity,
            FollowSizing::FixedNotional { notional } => notional,
        };
        let total = order;
        let existing = match self.follow_relation(principal).await {
            Ok(value) => Some(value),
            Err(cause) if cause.code == Code::NotFound => None,
            Err(cause) => return Err(cause),
        };
        if existing.as_ref().is_some_and(|relation| {
            relation.state == venue_control_protocol::kol::FollowLifecycleState::Active
                || relation.activation_requested
        }) {
            return Ok(());
        }
        let relation = self
            .upsert_follow_settings_scoped(
                principal,
                FollowSettingsUpsertRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: crypto::opaque_id()?,
                    settings: FollowRiskSettings {
                        credential_id: credential_id.to_owned(),
                        sizing: authorization.sizing,
                        allocated_capital: equity,
                        multiplier: authorization.multiplier,
                        max_order_notional: order,
                        max_total_notional: total,
                        max_deviation_bps: 5_000,
                        allowed_symbols: Vec::new(),
                    },
                    expected_revision: existing.map(|value| value.revision),
                },
                activation_now_ms,
                managed,
            )
            .await?;
        self.request_follow_lifecycle_scoped(
            principal,
            FollowLifecycleRequest {
                schema_version: KOL_SCHEMA_VERSION,
                request_id: crypto::opaque_id()?,
                relation_id: relation.relation_id,
                expected_revision: relation.revision,
                action: venue_control_protocol::kol::FollowLifecycleAction::Activate,
                risk_confirmed: true,
            },
            activation_now_ms,
            managed,
        )
        .await?;
        Ok(())
    }
}

pub async fn run_managed_deletion_cleanup(
    pool: PgPool,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return; }
            }
            _ = interval.tick() => {}
        }
        if let Err(cause) = finalize_managed_deletions(&pool, current_ms()).await {
            tracing::warn!(?cause, "Managed credential deletion cleanup deferred");
        }
    }
}

async fn finalize_managed_deletions(pool: &PgPool, now_ms: u64) -> Result<u64, AccountError> {
    let mut tx = pool.begin().await.map_err(database_error)?;
    let rows = sqlx::query(
        "SELECT m.managed_id,m.credential_id,c.verification_json FROM venue_managed_credentials m JOIN venue_api_credentials c ON c.credential_id=m.credential_id AND c.user_id=m.follower_user_id WHERE m.delete_requested_ms IS NOT NULL AND c.deleted_ms IS NULL AND NOT EXISTS(SELECT 1 FROM venue_kol_follow_relations r WHERE r.follower_user_id=m.follower_user_id AND (r.relation_state NOT IN ('paused','disabled') OR EXISTS(SELECT 1 FROM venue_kol_activation_requests a WHERE a.relation_id=r.relation_id AND a.request_state='pending') OR EXISTS(SELECT 1 FROM venue_order_mirrors o WHERE o.relation_id=r.relation_id AND o.mirror_state NOT IN ('terminal','blocked')) OR EXISTS(SELECT 1 FROM venue_binance_commands b WHERE b.relation_id=r.relation_id AND b.command_state IN ('pending','sending','accepted','reconcile_required')))) ORDER BY m.delete_requested_ms,m.managed_id LIMIT 20 FOR UPDATE OF m,c SKIP LOCKED",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(database_error)?;
    let mut finalized = 0_u64;
    for row in rows {
        let managed_id: String = row.try_get("managed_id").map_err(database_error)?;
        let credential_id: String = row.try_get("credential_id").map_err(database_error)?;
        let mut tombstone: CredentialSummary =
            serde_json::from_value(row.try_get("verification_json").map_err(database_error)?)
                .map_err(|_| error(Code::Unavailable))?;
        super::credentials::invalidate(
            &mut tombstone,
            venue_control_protocol::accounts::ApiVerificationState::Unverified,
        );
        tombstone.masked_key = "已删除".into();
        let tombstone_key = crypto::fingerprint(
            format!("managed-deleted:{managed_id}:{credential_id}:{now_ms}").as_bytes(),
        );
        sqlx::query("UPDATE venue_api_credentials SET key_fingerprint=$1,masked_key=$2,encrypted_credentials=$3,verification_json=$4,deleted_ms=$5,revision=revision+1 WHERE credential_id=$6 AND deleted_ms IS NULL")
            .bind(tombstone_key).bind(&tombstone.masked_key).bind(Vec::<u8>::new())
            .bind(serde_json::to_value(&tombstone).map_err(|_| error(Code::Unavailable))?)
            .bind(ms(now_ms)?).bind(&credential_id).execute(&mut *tx).await.map_err(database_error)?;
        sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='disabled',active_slot=NULL,attention_code=NULL,revision=revision+1,updated_ms=$1 WHERE follower_user_id=(SELECT follower_user_id FROM venue_managed_credentials WHERE managed_id=$2) AND relation_state<>'disabled'")
            .bind(ms(now_ms)?).bind(&managed_id).execute(&mut *tx).await.map_err(database_error)?;
        finalized += 1;
    }
    tx.commit().await.map_err(database_error)?;
    Ok(finalized)
}

fn current_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| match u64::try_from(duration.as_millis()) {
            Ok(value) => value,
            Err(_) => u64::MAX,
        })
}

async fn bind_managed_owner(
    tx: &mut Transaction<'_, Postgres>,
    managed_id: &str,
    kol_user_id: &str,
    follower_user_id: &str,
    now_ms: u64,
) -> Result<(), AccountError> {
    let has_binding_source: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='venue_user_kol_bindings'::regclass AND attname='binding_source' AND NOT attisdropped)",
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(database_error)?;
    if has_binding_source {
        sqlx::query("INSERT INTO venue_user_kol_bindings(user_id,kol_user_id,managed_id,bound_ms,binding_source) VALUES($1,$2,$3,$4,'kol_managed') ON CONFLICT(user_id) DO NOTHING")
            .bind(follower_user_id).bind(kol_user_id).bind(managed_id).bind(ms(now_ms)?)
            .execute(&mut **tx).await.map_err(database_error)?;
    } else {
        sqlx::query("INSERT INTO venue_user_kol_bindings(user_id,kol_user_id,managed_id,bound_ms) VALUES($1,$2,$3,$4) ON CONFLICT(user_id) DO NOTHING")
            .bind(follower_user_id).bind(kol_user_id).bind(managed_id).bind(ms(now_ms)?)
            .execute(&mut **tx).await.map_err(database_error)?;
    }
    let matches: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_user_kol_bindings WHERE user_id=$1 AND kol_user_id=$2 AND managed_id=$3 AND invite_id IS NULL)")
        .bind(follower_user_id).bind(kol_user_id).bind(managed_id)
        .fetch_one(&mut **tx).await.map_err(database_error)?;
    if !matches {
        return Err(error(Code::Conflict));
    }
    Ok(())
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
        equity: summary.equity,
        available_margin: summary.available_margin,
        balance_observed_ms: summary.balance_observed_ms,
    }
}

#[cfg(test)]
mod tests;
