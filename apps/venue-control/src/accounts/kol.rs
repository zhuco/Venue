//! KOL invite and public-profile persistence. Invite codes are only handled as hashes, and a
//! successful registration writes the immutable binding in the same transaction as the user.

use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use rust_decimal::Decimal;
use sqlx::{Row, postgres::PgRow};
use venue_control_protocol::{
    accounts::{AccountErrorCode as Code, RegisterRequest, SessionResponse, UserSummary},
    kol::{
        FollowLifecycleAction, FollowLifecycleRequest, FollowLifecycleState, FollowRelationSummary,
        FollowRiskSettings, FollowSettingsUpsertRequest, InviteResolution, KOL_SCHEMA_VERSION,
        KolProfileState, KolProfileUpdateRequest, KolPublicProfile, TerminalAccountProjection,
        TerminalProjectionRequest,
    },
};

impl AccountService {
    pub async fn register_with_invite(
        &self,
        request: RegisterRequest,
        now_ms: u64,
    ) -> Result<SessionResponse, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit("registration", 10, now_ms).await?;
        let username = request
            .normalized_username()
            .ok_or(error(Code::InvalidInput))?;
        let invite_hash = crypto::fingerprint(
            request
                .normalized_invite_code()
                .ok_or(error(Code::InvalidInput))?
                .as_bytes(),
        );
        let user = UserSummary {
            user_id: crypto::opaque_id()?,
            username,
        };
        let password_hash = self.password_hash(request.password).await?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let invite = sqlx::query(
            "SELECT i.invite_id,i.kol_user_id FROM venue_kol_invites i \
             JOIN venue_kol_profiles p ON p.kol_user_id=i.kol_user_id \
             WHERE i.code_hash=$1 AND i.invite_state='active' AND p.profile_state='enabled' \
             AND (i.expires_ms IS NULL OR i.expires_ms>$2) FOR SHARE OF i,p",
        )
        .bind(invite_hash)
        .bind(ms(now_ms)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::InvalidInput))?;
        let inserted = sqlx::query(
            "INSERT INTO venue_users (user_id,username,password_hash,created_ms) \
             VALUES ($1,$2,$3,$4) ON CONFLICT (username) DO NOTHING",
        )
        .bind(&user.user_id)
        .bind(&user.username)
        .bind(password_hash)
        .bind(ms(now_ms)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if inserted.rows_affected() != 1 {
            return Err(error(Code::UsernameUnavailable));
        }
        sqlx::query(
            "INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) \
             VALUES ($1,$2,$3,$4)",
        )
        .bind(&user.user_id)
        .bind(
            invite
                .try_get::<String, _>("kol_user_id")
                .map_err(database_error)?,
        )
        .bind(
            invite
                .try_get::<String, _>("invite_id")
                .map_err(database_error)?,
        )
        .bind(ms(now_ms)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        let session = self
            .create_session_in_transaction(&mut tx, user, now_ms)
            .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(session)
    }

    pub async fn resolve_invite(
        &self,
        invite_code: &str,
        now_ms: u64,
    ) -> Result<InviteResolution, AccountError> {
        let code = invite_code.trim();
        if !(venue_control_protocol::accounts::MIN_INVITE_CODE_CHARS
            ..=venue_control_protocol::accounts::MAX_INVITE_CODE_CHARS)
            .contains(&code.len())
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(error(Code::NotFound));
        }
        let row = sqlx::query(
            "SELECT p.kol_user_id,p.public_name,p.public_title,p.public_description,p.revision,p.profile_state \
             FROM venue_kol_invites i JOIN venue_kol_profiles p ON p.kol_user_id=i.kol_user_id \
             WHERE i.code_hash=$1 AND i.invite_state='active' AND p.profile_state='enabled' \
             AND (i.expires_ms IS NULL OR i.expires_ms>$2)",
        )
        .bind(crypto::fingerprint(code.as_bytes()))
        .bind(ms(now_ms)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::NotFound))?;
        Ok(InviteResolution {
            schema_version: KOL_SCHEMA_VERSION,
            profile: public_profile(&row)?,
        })
    }

    pub async fn update_own_kol_profile(
        &self,
        principal: &Principal,
        request: KolProfileUpdateRequest,
        now_ms: u64,
    ) -> Result<KolPublicProfile, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        self.rate_limit(
            &format!("kol-profile:{}", principal.user.user_id),
            20,
            now_ms,
        )
        .await?;
        let expected =
            i64::try_from(request.expected_revision).map_err(|_| error(Code::InvalidInput))?;
        let next = expected.checked_add(1).ok_or(error(Code::Conflict))?;
        let row = sqlx::query(
            "UPDATE venue_kol_profiles SET public_name=$1,public_title=$2,public_description=$3, \
             revision=$4,updated_ms=$5 WHERE kol_user_id=$6 AND revision=$7 \
             RETURNING kol_user_id,public_name,public_title,public_description,revision,profile_state",
        )
        .bind(request.name.trim())
        .bind(request.title.trim())
        .bind(request.description)
        .bind(next)
        .bind(ms(now_ms)?)
        .bind(&principal.user.user_id)
        .bind(expected)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        match row {
            Some(row) => public_profile(&row),
            None => {
                let exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM venue_kol_profiles WHERE kol_user_id=$1)",
                )
                .bind(&principal.user.user_id)
                .fetch_one(&self.pool)
                .await
                .map_err(database_error)?;
                Err(error(if exists {
                    Code::Conflict
                } else {
                    Code::Forbidden
                }))
            }
        }
    }

    pub async fn own_kol_profile(
        &self,
        principal: &Principal,
    ) -> Result<KolPublicProfile, AccountError> {
        let row = sqlx::query(
            "SELECT kol_user_id,public_name,public_title,public_description,revision,profile_state \
             FROM venue_kol_profiles WHERE kol_user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::Forbidden))?;
        public_profile(&row)
    }

    pub async fn follow_relation(
        &self,
        principal: &Principal,
    ) -> Result<FollowRelationSummary, AccountError> {
        let row = sqlx::query(
            "SELECT r.relation_id,r.relation_state,r.revision,r.credential_id,r.allocated_capital, \
             r.multiplier,r.max_order_notional,r.max_total_notional,r.max_deviation_bps,r.allowed_symbols,r.sizing_json, \
             EXISTS(SELECT 1 FROM venue_kol_activation_requests a WHERE a.relation_id=r.relation_id \
             AND a.request_state='pending') AS activation_requested \
             FROM venue_kol_follow_relations r WHERE r.follower_user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::NotFound))?;
        follow_relation_summary(&row)
    }

    pub async fn upsert_follow_settings(
        &self,
        principal: &Principal,
        request: FollowSettingsUpsertRequest,
        now_ms: u64,
    ) -> Result<FollowRelationSummary, AccountError> {
        self.upsert_follow_settings_scoped(principal, request, now_ms, None)
            .await
    }

    pub(super) async fn upsert_follow_settings_scoped(
        &self,
        principal: &Principal,
        request: FollowSettingsUpsertRequest,
        now_ms: u64,
        managed: Option<(&str, &str)>,
    ) -> Result<FollowRelationSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        self.rate_limit(
            &format!("follow-settings:{}", principal.user.user_id),
            20,
            now_ms,
        )
        .await?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        super::follow_requests::lock_scope(
            &mut tx,
            principal,
            managed,
            true,
            Some(&request.settings.credential_id),
            now_ms,
        )
        .await?;
        let actor = managed
            .map(|scope| scope.0)
            .unwrap_or(&principal.user.user_id);
        let hash = super::follow_requests::digest("settings", &request)?;
        if let Some(prior) =
            super::follow_requests::replay(&mut tx, principal, actor, &request.request_id, &hash)
                .await?
        {
            return Ok(prior);
        }
        let existing = sqlx::query(
            "SELECT relation_id,relation_state,revision FROM venue_kol_follow_relations \
             WHERE follower_user_id=$1 FOR UPDATE",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if let Some(ref row) = existing {
            let relation_id: String = row.try_get("relation_id").map_err(database_error)?;
            let unresolved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_order_mirrors WHERE relation_id=$1 AND mirror_state NOT IN ('terminal','blocked')) OR EXISTS(SELECT 1 FROM venue_binance_commands WHERE relation_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
                .bind(relation_id).fetch_one(&mut *tx).await.map_err(database_error)?;
            if unresolved {
                return Err(error(Code::AccountInUse));
            }
        }
        let credential =
            verified_empty_credential(&mut tx, principal, &request.settings, now_ms).await?;
        let binding = sqlx::query(
            "SELECT b.kol_user_id,p.leader_trading_account_id FROM venue_user_kol_bindings b \
             JOIN venue_kol_profiles p ON p.kol_user_id=b.kol_user_id \
             WHERE b.user_id=$1 AND p.profile_state='enabled'",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::Forbidden))?;
        let leader_account: String = binding
            .try_get("leader_trading_account_id")
            .map_err(database_error)?;
        let follower_account: String = credential
            .try_get("trading_account_id")
            .map_err(database_error)?;
        if leader_account == follower_account {
            return Err(error(Code::Conflict));
        }
        let allowed_symbols = serde_json::to_value(
            request
                .settings
                .allowed_symbols
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| error(Code::InvalidInput))?;
        let row = match existing {
            None => {
                if request.expected_revision.is_some() {
                    return Err(error(Code::Conflict));
                }
                let relation_id = crypto::opaque_id()?;
                sqlx::query(
                    "INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms,sizing_json) \
                     VALUES ($1,$2,$3,$4,$5,$6,'paused',$7,$8,$9,$10,$11,$12,1,$13,$13,$14)",
                )
                .bind(&relation_id)
                .bind(&principal.user.user_id)
                .bind(binding.try_get::<String, _>("kol_user_id").map_err(database_error)?)
                .bind(&leader_account)
                .bind(&follower_account)
                .bind(&request.settings.credential_id)
                .bind(decimal_text(request.settings.allocated_capital))
                .bind(decimal_text(request.settings.multiplier))
                .bind(decimal_text(request.settings.max_order_notional))
                .bind(decimal_text(request.settings.max_total_notional))
                .bind(i32::try_from(request.settings.max_deviation_bps).map_err(|_| error(Code::InvalidInput))?)
                .bind(allowed_symbols)
                .bind(ms(now_ms)?)
                .bind(serde_json::to_value(request.settings.sizing).map_err(|_| error(Code::InvalidInput))?)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
                query_follow_relation(&mut tx, &relation_id).await?
            }
            Some(existing) => {
                let state: String = existing.try_get("relation_state").map_err(database_error)?;
                let revision: i64 = existing.try_get("revision").map_err(database_error)?;
                let expected = request
                    .expected_revision
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or(error(Code::Conflict))?;
                if state != "paused" || revision != expected {
                    return Err(error(Code::Conflict));
                }
                let next = revision.checked_add(1).ok_or(error(Code::Unavailable))?;
                let relation_id: String =
                    existing.try_get("relation_id").map_err(database_error)?;
                sqlx::query(
                    "UPDATE venue_kol_follow_relations SET kol_user_id=$1,leader_trading_account_id=$2, \
                     follower_trading_account_id=$3,credential_id=$4,allocated_capital=$5,multiplier=$6, \
                     max_order_notional=$7,max_total_notional=$8,max_deviation_bps=$9,allowed_symbols=$10, \
                     revision=$11,updated_ms=$12,sizing_json=$14 WHERE relation_id=$13",
                )
                .bind(binding.try_get::<String, _>("kol_user_id").map_err(database_error)?)
                .bind(&leader_account)
                .bind(&follower_account)
                .bind(&request.settings.credential_id)
                .bind(decimal_text(request.settings.allocated_capital))
                .bind(decimal_text(request.settings.multiplier))
                .bind(decimal_text(request.settings.max_order_notional))
                .bind(decimal_text(request.settings.max_total_notional))
                .bind(i32::try_from(request.settings.max_deviation_bps).map_err(|_| error(Code::InvalidInput))?)
                .bind(allowed_symbols)
                .bind(next)
                .bind(ms(now_ms)?)
                .bind(&relation_id)
                .bind(serde_json::to_value(request.settings.sizing).map_err(|_| error(Code::InvalidInput))?)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
                crate::kol_executor::cancel_pending_copy_commands(
                    &mut tx,
                    &relation_id,
                    ms(now_ms)?,
                    "settings_changed",
                )
                .await
                .map_err(database_error)?;
                sqlx::query("UPDATE venue_kol_activation_requests SET request_state='cancelled',sanitized_reason='settings_changed',updated_ms=$1 WHERE relation_id=$2 AND request_state='pending'")
                    .bind(ms(now_ms)?)
                    .bind(&relation_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(database_error)?;
                query_follow_relation(&mut tx, &relation_id).await?
            }
        };
        let summary = follow_relation_summary(&row)?;
        super::follow_requests::save(
            &mut tx,
            principal,
            actor,
            &request.request_id,
            &hash,
            &summary,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    pub async fn request_follow_lifecycle(
        &self,
        principal: &Principal,
        request: FollowLifecycleRequest,
        now_ms: u64,
    ) -> Result<FollowRelationSummary, AccountError> {
        self.request_follow_lifecycle_scoped(principal, request, now_ms, None)
            .await
    }

    pub(super) async fn request_follow_lifecycle_scoped(
        &self,
        principal: &Principal,
        request: FollowLifecycleRequest,
        now_ms: u64,
        managed: Option<(&str, &str)>,
    ) -> Result<FollowRelationSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        self.rate_limit(
            &format!("follow-lifecycle:{}", principal.user.user_id),
            20,
            now_ms,
        )
        .await?;
        let expected =
            i64::try_from(request.expected_revision).map_err(|_| error(Code::InvalidInput))?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        super::follow_requests::lock_scope(
            &mut tx,
            principal,
            managed,
            request.action == FollowLifecycleAction::Activate,
            None,
            now_ms,
        )
        .await?;
        let actor = managed
            .map(|scope| scope.0)
            .unwrap_or(&principal.user.user_id);
        let hash = super::follow_requests::digest("lifecycle", &request)?;
        if let Some(prior) =
            super::follow_requests::replay(&mut tx, principal, actor, &request.request_id, &hash)
                .await?
        {
            return Ok(prior);
        }
        let row = sqlx::query(
            "SELECT relation_id,relation_state,revision,credential_id,allocated_capital,multiplier, \
             max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,sizing_json \
             FROM venue_kol_follow_relations WHERE relation_id=$1 AND follower_user_id=$2 FOR UPDATE",
        )
        .bind(&request.relation_id)
        .bind(&principal.user.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::NotFound))?;
        let revision: i64 = row.try_get("revision").map_err(database_error)?;
        if revision != expected {
            return Err(error(Code::Conflict));
        }
        let relation_id: String = row.try_get("relation_id").map_err(database_error)?;
        match request.action {
            FollowLifecycleAction::Activate => {
                let settings = follow_relation_settings(&row)?;
                let _ = verified_empty_credential(&mut tx, principal, &settings, now_ms).await?;
                let state: String = row.try_get("relation_state").map_err(database_error)?;
                if state != "paused" {
                    return Err(error(Code::Conflict));
                }
                sqlx::query(
                    "INSERT INTO venue_kol_activation_requests (relation_id,request_id,relation_revision,request_state,requested_ms,updated_ms) \
                     VALUES ($1,$2,$3,'pending',$4,$4) ON CONFLICT (relation_id) DO UPDATE \
                     SET request_id=EXCLUDED.request_id,relation_revision=EXCLUDED.relation_revision, \
                     request_state='pending',requested_ms=EXCLUDED.requested_ms,updated_ms=EXCLUDED.updated_ms \
                     WHERE venue_kol_activation_requests.request_state <> 'pending'",
                )
                .bind(&relation_id)
                .bind(&request.request_id)
                .bind(revision)
                .bind(ms(now_ms)?)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
            }
            FollowLifecycleAction::Pause => {
                let next = revision.checked_add(1).ok_or(error(Code::Unavailable))?;
                sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='paused',active_slot=NULL,revision=$1,updated_ms=$2 WHERE relation_id=$3")
                    .bind(next)
                    .bind(ms(now_ms)?)
                    .bind(&relation_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(database_error)?;
                crate::kol_executor::cancel_pending_copy_commands(
                    &mut tx,
                    &relation_id,
                    ms(now_ms)?,
                    "follow_paused",
                )
                .await
                .map_err(database_error)?;
                sqlx::query("UPDATE venue_kol_activation_requests SET request_state='cancelled',sanitized_reason='paused',updated_ms=$1 WHERE relation_id=$2 AND request_state='pending'")
                    .bind(ms(now_ms)?)
                    .bind(&relation_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(database_error)?;
            }
        }
        let summary =
            follow_relation_summary(&query_follow_relation(&mut tx, &relation_id).await?)?;
        super::follow_requests::save(
            &mut tx,
            principal,
            actor,
            &request.request_id,
            &hash,
            &summary,
            now_ms,
        )
        .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }

    pub async fn terminal_account_projection(
        &self,
        principal: &Principal,
        request: TerminalProjectionRequest,
        now_ms: u64,
    ) -> Result<Option<TerminalAccountProjection>, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let store =
            crate::private_projection::BinancePrivateProjectionStore::new(self.pool.clone());
        store
            .subscribe(
                &principal.user.user_id,
                &request.credential_id,
                &request.symbols,
                now_ms,
            )
            .await
            .map_err(projection_error)?;
        store
            .load_owned(&principal.user.user_id, &request.credential_id)
            .await
            .map_err(projection_error)
    }
}

fn projection_error(
    error_value: crate::private_projection::PrivateProjectionError,
) -> AccountError {
    use crate::private_projection::PrivateProjectionError;
    match error_value {
        PrivateProjectionError::Invalid => error(Code::InvalidInput),
        PrivateProjectionError::Forbidden => error(Code::VerificationRequired),
        PrivateProjectionError::Unavailable => error(Code::Unavailable),
    }
}

async fn verified_empty_credential(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal: &Principal,
    settings: &FollowRiskSettings,
    now_ms: u64,
) -> Result<PgRow, AccountError> {
    let row = sqlx::query("SELECT trading_account_id,verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL FOR SHARE")
        .bind(&settings.credential_id)
        .bind(&principal.user.user_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::NotFound))?;
    let summary = super::credentials::decode_summary(
        row.try_get("verification_json").map_err(database_error)?,
    )?;
    if !summary.selectable(now_ms) {
        return Err(error(Code::VerificationRequired));
    }
    if summary.has_exposure != Some(false) {
        return Err(error(Code::AccountInUse));
    }
    if row
        .try_get::<Option<String>, _>("trading_account_id")
        .map_err(database_error)?
        .is_none()
    {
        return Err(error(Code::VerificationRequired));
    }
    Ok(row)
}

async fn query_follow_relation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    relation_id: &str,
) -> Result<PgRow, AccountError> {
    sqlx::query(
        "SELECT r.relation_id,r.relation_state,r.revision,r.credential_id,r.allocated_capital, \
         r.multiplier,r.max_order_notional,r.max_total_notional,r.max_deviation_bps,r.allowed_symbols,r.sizing_json, \
         EXISTS(SELECT 1 FROM venue_kol_activation_requests a WHERE a.relation_id=r.relation_id \
         AND a.request_state='pending') AS activation_requested \
         FROM venue_kol_follow_relations r WHERE r.relation_id=$1",
    )
    .bind(relation_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(database_error)
}

fn follow_relation_summary(row: &PgRow) -> Result<FollowRelationSummary, AccountError> {
    let state = match row
        .try_get::<String, _>("relation_state")
        .map_err(database_error)?
        .as_str()
    {
        "paused" => FollowLifecycleState::Paused,
        "active" => FollowLifecycleState::Active,
        "needs_attention" => FollowLifecycleState::NeedsAttention,
        "disabled" => FollowLifecycleState::Disabled,
        _ => return Err(error(Code::Unavailable)),
    };
    let revision: i64 = row.try_get("revision").map_err(database_error)?;
    Ok(FollowRelationSummary {
        relation_id: row.try_get("relation_id").map_err(database_error)?,
        state,
        revision: u64::try_from(revision).map_err(|_| error(Code::Unavailable))?,
        settings: follow_relation_settings(row)?,
        activation_requested: row
            .try_get("activation_requested")
            .map_err(database_error)?,
    })
}

fn follow_relation_settings(row: &PgRow) -> Result<FollowRiskSettings, AccountError> {
    let allowed: Vec<String> =
        serde_json::from_value(row.try_get("allowed_symbols").map_err(database_error)?)
            .map_err(|_| error(Code::Unavailable))?;
    let allowed_symbols = allowed
        .into_iter()
        .map(|symbol| symbol.parse().map_err(|_| error(Code::Unavailable)))
        .collect::<Result<Vec<_>, _>>()?;
    let decimal = |column| -> Result<Decimal, AccountError> {
        row.try_get::<String, _>(column)
            .map_err(database_error)?
            .parse()
            .map_err(|_| error(Code::Unavailable))
    };
    Ok(FollowRiskSettings {
        sizing: serde_json::from_value(row.try_get("sizing_json").map_err(database_error)?)
            .map_err(|_| error(Code::Unavailable))?,
        credential_id: row.try_get("credential_id").map_err(database_error)?,
        allocated_capital: decimal("allocated_capital")?,
        multiplier: decimal("multiplier")?,
        max_order_notional: decimal("max_order_notional")?,
        max_total_notional: decimal("max_total_notional")?,
        max_deviation_bps: u32::try_from(
            row.try_get::<i32, _>("max_deviation_bps")
                .map_err(database_error)?,
        )
        .map_err(|_| error(Code::Unavailable))?,
        allowed_symbols,
    })
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn public_profile(row: &PgRow) -> Result<KolPublicProfile, AccountError> {
    let state: String = row.try_get("profile_state").map_err(database_error)?;
    let state = match state.as_str() {
        "draft" => KolProfileState::Draft,
        "enabled" => KolProfileState::Enabled,
        "disabled" => KolProfileState::Disabled,
        _ => return Err(error(Code::Unavailable)),
    };
    let revision: i64 = row.try_get("revision").map_err(database_error)?;
    Ok(KolPublicProfile {
        kol_id: row.try_get("kol_user_id").map_err(database_error)?,
        name: row.try_get("public_name").map_err(database_error)?,
        title: row.try_get("public_title").map_err(database_error)?,
        description: row.try_get("public_description").map_err(database_error)?,
        state,
        revision: u64::try_from(revision).map_err(|_| error(Code::Unavailable))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::test_support::{Fixture, TestResult, login, now};
    use crate::{BinanceCommandLedger, BinanceCommandLedgerError};
    use venue_control_protocol::accounts::{
        AccountErrorCode, BindCredentialRequest, RegisterRequest, SecretValue,
    };
    use venue_gateway_binance::BinanceCredentialProbe;

    const KOL_ACCOUNT: &str = "00000000-0000-4000-8000-000000000102";
    const INVITE: &str = "Safe_Kol_Invite_Code_00001";

    #[tokio::test]
    async fn registration_binds_the_resolved_invite_once_and_profiles_are_owner_scoped()
    -> TestResult {
        let Some(fixture) = Fixture::create().await? else {
            return Ok(());
        };
        let timestamp = now();
        let kol_session = fixture.service.register(login("kol"), timestamp).await?;
        let kol_user_id = kol_session.user.user_id.clone();
        sqlx::query(
            "INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)",
        )
        .bind(KOL_ACCOUNT)
        .bind(&kol_user_id)
        .bind(vec![9_u8; 32])
        .execute(&fixture.pool)
        .await?;
        sqlx::query(
            "INSERT INTO venue_kol_profiles (kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES ($1,$2,'KOL','Title','Description','100','enabled',1,$3,$3)",
        )
        .bind(&kol_user_id)
        .bind(KOL_ACCOUNT)
        .bind(i64::try_from(timestamp)?)
        .execute(&fixture.pool)
        .await?;
        sqlx::query(
            "INSERT INTO venue_kol_invites (invite_id,kol_user_id,code_hash,invite_state,created_ms) VALUES ('00000000-0000-4000-8000-000000000103',$1,$2,'active',$3)",
        )
        .bind(&kol_user_id)
        .bind(crypto::fingerprint(INVITE.as_bytes()))
        .bind(i64::try_from(timestamp)?)
        .execute(&fixture.pool)
        .await?;

        let resolved = fixture.service.resolve_invite(INVITE, timestamp).await?;
        assert_eq!(resolved.profile.kol_id, kol_user_id);
        let registration = RegisterRequest {
            username: "follower".into(),
            password: SecretValue::new("a sufficiently long password".into()),
            invite_code: INVITE.into(),
        };
        let follower = fixture
            .service
            .register_with_invite(registration.clone(), timestamp)
            .await?;
        let bound: String =
            sqlx::query_scalar("SELECT kol_user_id FROM venue_user_kol_bindings WHERE user_id=$1")
                .bind(&follower.user.user_id)
                .fetch_one(&fixture.pool)
                .await?;
        assert_eq!(bound, kol_user_id);
        assert_eq!(
            fixture
                .service
                .register_with_invite(registration, timestamp)
                .await
                .err()
                .map(|error| error.code),
            Some(AccountErrorCode::UsernameUnavailable)
        );

        let kol_principal = fixture
            .service
            .authenticate(kol_session.token.expose(), timestamp)
            .await?;
        let follower_principal = fixture
            .service
            .authenticate(follower.token.expose(), timestamp)
            .await?;
        let credential = fixture
            .service
            .bind_credential(
                &follower_principal,
                BindCredentialRequest {
                    label: "follow".into(),
                    api_key: SecretValue::new("A".repeat(32)),
                    api_secret: SecretValue::new("B".repeat(32)),
                },
                timestamp,
            )
            .await?;
        let verified = fixture
            .service
            .verify_with(
                &follower_principal,
                &credential.credential_id,
                timestamp,
                |_| async {
                    Ok(BinanceCredentialProbe {
                        account_identity_hash: [29; 32],
                        observed_ms: timestamp,
                        has_exposure: false,
                    })
                },
            )
            .await?;
        let settings = FollowRiskSettings {
            sizing: Default::default(),
            credential_id: credential.credential_id.clone(),
            allocated_capital: Decimal::new(100, 0),
            multiplier: Decimal::ONE,
            max_order_notional: Decimal::new(20, 0),
            max_total_notional: Decimal::new(100, 0),
            max_deviation_bps: 100,
            allowed_symbols: vec!["BTC/USDT".parse()?],
        };
        let relation = fixture
            .service
            .upsert_follow_settings(
                &follower_principal,
                FollowSettingsUpsertRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000105".into(),
                    settings,
                    expected_revision: None,
                },
                timestamp,
            )
            .await?;
        assert_eq!(relation.state, FollowLifecycleState::Paused);
        let requested = fixture
            .service
            .request_follow_lifecycle(
                &follower_principal,
                FollowLifecycleRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000106".into(),
                    relation_id: relation.relation_id.clone(),
                    expected_revision: relation.revision,
                    action: FollowLifecycleAction::Activate,
                    risk_confirmed: true,
                },
                timestamp,
            )
            .await?;
        assert!(requested.activation_requested);
        let paused = fixture
            .service
            .request_follow_lifecycle(
                &follower_principal,
                FollowLifecycleRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000107".into(),
                    relation_id: requested.relation_id,
                    expected_revision: requested.revision,
                    action: FollowLifecycleAction::Pause,
                    risk_confirmed: false,
                },
                timestamp,
            )
            .await?;
        assert!(!paused.activation_requested);
        assert_eq!(paused.revision, relation.revision + 1);
        sqlx::query("UPDATE venue_kol_follow_relations SET relation_state='active',active_slot=1,baseline_json='{\"target_model\":1,\"baseline_ms\":1}'::jsonb WHERE relation_id=$1")
            .bind(&relation.relation_id).execute(&fixture.pool).await?;
        for (command_id, target_revision) in [
            ("00000000-0000-4000-8000-000000000108", 1_i64),
            ("00000000-0000-4000-8000-000000000109", 2_i64),
        ] {
            sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk) VALUES ($1,'copy',$2,$8,$3,$4,$5,$6,'BTC/USDT','long','open','market','buy','0.001','0.001','fixture',$1,'pending',$7,$7,$9)")
                .bind(command_id)
                .bind(&relation.relation_id)
                .bind(target_revision)
                .bind(&follower.user.user_id)
                .bind(verified.trading_account_id.as_ref().ok_or("account missing")?)
                .bind(&credential.credential_id)
                .bind(i64::try_from(timestamp)?)
                .bind(i64::try_from(paused.revision)?)
                .bind(serde_json::json!({"max_order_notional":"20","max_total_notional":"100",
                    "max_deviation_bps":100,"source_price":"10000","source_occurred_ms":timestamp}))
                .execute(&fixture.pool)
                .await?;
        }
        let ledger = BinanceCommandLedger::new(fixture.pool.clone());
        let first = ledger
            .claim_next(
                verified
                    .trading_account_id
                    .as_deref()
                    .ok_or("account missing")?,
                timestamp,
            )
            .await?
            .ok_or("pending command missing")?;
        assert_eq!(
            first.client_order_id,
            "00000000-0000-4000-8000-000000000108"
        );
        assert!(
            ledger
                .claim_next(
                    verified
                        .trading_account_id
                        .as_deref()
                        .ok_or("account missing")?,
                    timestamp
                )
                .await?
                .is_none()
        );
        sqlx::query("INSERT INTO venue_kol_copy_targets (relation_id,symbol,position_side,copyable_quantity,target_quantity,observed_quantity,target_revision,last_native_symbol,last_native_trade_id,dirty,updated_ms) VALUES ($1,'BTC/USDT','long','0.01','0.001','0',1,'BTCUSDT','fixture',false,$2)")
            .bind(&relation.relation_id).bind(i64::try_from(timestamp)?).execute(&fixture.pool).await?;
        crate::executor_store::PgExecutorStore::new(fixture.pool.clone())
            .persist_market_baseline(
                &first.command_id,
                &crate::executor_exchange::MarketBaseline {
                    before_quantity: Decimal::ZERO,
                    order_quantity: Decimal::new(1, 3),
                    observed_ms: timestamp - 1,
                    valid_until_ms: timestamp + 1_000,
                },
            )
            .await?;
        ledger
            .settle(
                &first.command_id,
                venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired,
                timestamp,
                Some("timeout"),
            )
            .await?;
        assert!(
            ledger
                .claim_next(
                    verified
                        .trading_account_id
                        .as_deref()
                        .ok_or("account missing")?,
                    timestamp
                )
                .await?
                .is_none()
        );
        ledger
            .settle_with_execution(
                &first.command_id,
                venue_control_protocol::kol::ExecutorCommandState::Reconciled,
                timestamp,
                None,
                Some("fixture-native-1"),
                Some(&crate::executor_exchange::MarketSettlement {
                    executed_quantity: Decimal::new(1, 3),
                    position_quantity: Decimal::new(1, 3),
                    observed_ms: timestamp,
                }),
            )
            .await?;
        let second = ledger
            .claim_next(
                verified
                    .trading_account_id
                    .as_deref()
                    .ok_or("account missing")?,
                timestamp,
            )
            .await?
            .ok_or("second command missing")?;
        assert_eq!(
            second.client_order_id,
            "00000000-0000-4000-8000-000000000109"
        );
        let pending_id = "00000000-0000-4000-8000-000000000110";
        sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk) SELECT $1,'copy',relation_id,relation_revision,3,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,$1,'pending',created_ms,updated_ms,copy_risk FROM venue_binance_commands WHERE command_id=$2")
            .bind(pending_id).bind(&second.command_id).execute(&fixture.pool).await?;
        fixture
            .service
            .request_follow_lifecycle(
                &follower_principal,
                FollowLifecycleRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000111".into(),
                    relation_id: relation.relation_id.clone(),
                    expected_revision: paused.revision,
                    action: FollowLifecycleAction::Pause,
                    risk_confirmed: false,
                },
                timestamp + 1,
            )
            .await?;
        let pending_state: String = sqlx::query_scalar(
            "SELECT command_state FROM venue_binance_commands WHERE command_id=$1",
        )
        .bind(pending_id)
        .fetch_one(&fixture.pool)
        .await?;
        let sent_state: String = sqlx::query_scalar(
            "SELECT command_state FROM venue_binance_commands WHERE command_id=$1",
        )
        .bind(&second.command_id)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(pending_state, "cancelled");
        assert_eq!(sent_state, "sending");
        assert_eq!(
            ledger
                .settle(
                    &second.command_id,
                    venue_control_protocol::kol::ExecutorCommandState::Pending,
                    timestamp,
                    None
                )
                .await
                .err(),
            Some(BinanceCommandLedgerError::Conflict)
        );
        let update = KolProfileUpdateRequest {
            schema_version: KOL_SCHEMA_VERSION,
            request_id: "00000000-0000-4000-8000-000000000104".into(),
            name: "Updated".into(),
            title: "Updated title".into(),
            description: "Updated description".into(),
            expected_revision: 1,
        };
        assert_eq!(
            fixture
                .service
                .update_own_kol_profile(&follower_principal, update.clone(), timestamp)
                .await
                .err()
                .map(|error| error.code),
            Some(AccountErrorCode::Forbidden)
        );
        let stale = update.clone();
        let updated = fixture
            .service
            .update_own_kol_profile(&kol_principal, update, timestamp)
            .await?;
        assert_eq!(updated.name, "Updated");
        assert_eq!(updated.revision, 2);
        assert_eq!(
            fixture
                .service
                .update_own_kol_profile(&kol_principal, stale, timestamp)
                .await
                .err()
                .map(|error| error.code),
            Some(AccountErrorCode::Conflict)
        );
        fixture.cleanup().await
    }
}
