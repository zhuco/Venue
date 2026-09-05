use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use sqlx::Row;
use venue_control_protocol::{accounts::AccountErrorCode as Code, leader_bot::*};

impl AccountService {
    pub async fn own_mirror_orders(
        &self,
        principal: &Principal,
    ) -> Result<Vec<MirrorOrderSummary>, AccountError> {
        let rows=sqlx::query("SELECT m.* FROM venue_order_mirrors m JOIN venue_kol_follow_relations r ON r.relation_id=m.relation_id WHERE r.follower_user_id=$1 ORDER BY m.created_ms DESC,m.mirror_id LIMIT 500")
            .bind(&principal.user.user_id).fetch_all(&self.pool).await.map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(MirrorOrderSummary {
                    mirror_id: row.try_get("mirror_id").map_err(database_error)?,
                    symbol: row
                        .try_get::<String, _>("symbol")
                        .map_err(database_error)?
                        .parse()
                        .map_err(|_| error(Code::Unavailable))?,
                    source_order_id: row.try_get("source_order_id").map_err(database_error)?,
                    child_client_order_id: row
                        .try_get("child_client_order_id")
                        .map_err(database_error)?,
                    state: row.try_get("mirror_state").map_err(database_error)?,
                    requested_quantity: row.try_get("child_quantity").map_err(database_error)?,
                    filled_quantity: row.try_get("filled_quantity").map_err(database_error)?,
                    attention_code: row.try_get("attention_code").map_err(database_error)?,
                })
            })
            .collect()
    }
    pub async fn leader_bot_access(
        &self,
        principal: &Principal,
    ) -> Result<LeaderBotAccess, AccountError> {
        let permission = sqlx::query("SELECT g.enabled,g.revision,p.profile_state FROM venue_leader_bot_permissions g JOIN venue_kol_profiles p ON p.kol_user_id=g.kol_user_id WHERE g.kol_user_id=$1")
            .bind(&principal.user.user_id).fetch_optional(&self.pool).await.map_err(database_error)?;
        let (can_use, permission_revision) = if let Some(row) = permission {
            (
                row.try_get::<bool, _>("enabled").map_err(database_error)?
                    && row
                        .try_get::<String, _>("profile_state")
                        .map_err(database_error)?
                        == "enabled",
                u64::try_from(row.try_get::<i64, _>("revision").map_err(database_error)?)
                    .map_err(|_| error(Code::Unavailable))?,
            )
        } else {
            (false, 0)
        };
        let row = sqlx::query("SELECT b.*,(SELECT count(*) FROM venue_kol_follow_relations r WHERE r.kol_user_id=b.owner_user_id AND r.relation_state='active') AS followers,(SELECT count(*) FROM venue_order_mirrors m WHERE m.bot_id=b.bot_id AND m.mirror_state NOT IN ('terminal','blocked')) AS orders FROM venue_leader_bots b WHERE b.owner_user_id=$1")
            .bind(&principal.user.user_id).fetch_optional(&self.pool).await.map_err(database_error)?;
        let bot = row
            .map(|row| -> Result<LeaderBotSummary, AccountError> {
                let state = match row
                    .try_get::<String, _>("bot_state")
                    .map_err(database_error)?
                    .as_str()
                {
                    "stopped" => LeaderBotState::Stopped,
                    "running" => LeaderBotState::Running,
                    "draining" => LeaderBotState::Draining,
                    "needs_attention" => LeaderBotState::NeedsAttention,
                    _ => return Err(error(Code::Unavailable)),
                };
                Ok(LeaderBotSummary {
                    bot_id: row.try_get("bot_id").map_err(database_error)?,
                    trading_account_id: row
                        .try_get("trading_account_id")
                        .map_err(database_error)?,
                    credential_id: row.try_get("credential_id").map_err(database_error)?,
                    state,
                    revision: u64::try_from(
                        row.try_get::<i64, _>("revision").map_err(database_error)?,
                    )
                    .map_err(|_| error(Code::Unavailable))?,
                    active_followers: u32::try_from(
                        row.try_get::<i64, _>("followers").map_err(database_error)?,
                    )
                    .map_err(|_| error(Code::Unavailable))?,
                    pending_orders: u32::try_from(
                        row.try_get::<i64, _>("orders").map_err(database_error)?,
                    )
                    .map_err(|_| error(Code::Unavailable))?,
                    attention_code: row.try_get("attention_code").map_err(database_error)?,
                })
            })
            .transpose()?;
        Ok(LeaderBotAccess {
            schema_version: LEADER_BOT_SCHEMA_VERSION,
            can_use,
            permission_revision,
            bot,
        })
    }

    pub async fn create_leader_bot(
        &self,
        principal: &Principal,
        request: LeaderBotCreateRequest,
        now_ms: u64,
    ) -> Result<LeaderBotAccess, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit(
            &format!("leader-bot:{}", principal.user.user_id),
            30,
            now_ms,
        )
        .await?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let grant = authorized_grant(&mut tx, &principal.user.user_id).await?;
        let credential = sqlx::query("SELECT c.trading_account_id FROM venue_api_credentials c JOIN venue_kol_profiles p ON p.kol_user_id=c.user_id AND p.leader_trading_account_id=c.trading_account_id WHERE c.credential_id=$1 AND c.user_id=$2 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified'")
            .bind(&request.credential_id).bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::VerificationRequired))?;
        let account: String = credential
            .try_get("trading_account_id")
            .map_err(database_error)?;
        let existing = sqlx::query("SELECT credential_id,create_request_id FROM venue_leader_bots WHERE owner_user_id=$1 FOR UPDATE")
            .bind(&principal.user.user_id).fetch_optional(&mut *tx).await.map_err(database_error)?;
        if let Some(row) = existing {
            if row
                .try_get::<String, _>("credential_id")
                .map_err(database_error)?
                != request.credential_id
                || row
                    .try_get::<String, _>("create_request_id")
                    .map_err(database_error)?
                    != request.request_id
            {
                return Err(error(Code::Conflict));
            }
        } else {
            sqlx::query("INSERT INTO venue_leader_bots (bot_id,owner_user_id,trading_account_id,credential_id,create_request_id,bot_state,permission_revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,'stopped',$6,$7,$7)")
                .bind(crypto::opaque_id()?).bind(&principal.user.user_id).bind(account).bind(&request.credential_id)
                .bind(request.request_id).bind(grant).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(database_error)?;
        }
        tx.commit().await.map_err(database_error)?;
        self.leader_bot_access(principal).await
    }

    pub async fn request_leader_bot_lifecycle(
        &self,
        principal: &Principal,
        request: LeaderBotLifecycleRequest,
        now_ms: u64,
    ) -> Result<LeaderBotAccess, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        // Grants and starts share the profile lock. Stop remains available after revocation.
        sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE")
            .bind(&principal.user.user_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(database_error)?
            .ok_or(error(Code::Forbidden))?;
        let grant = if request.action == LeaderBotAction::Start {
            Some(authorized_grant(&mut tx, &principal.user.user_id).await?)
        } else {
            None
        };
        let row = sqlx::query(
            "SELECT * FROM venue_leader_bots WHERE bot_id=$1 AND owner_user_id=$2 FOR UPDATE",
        )
        .bind(&request.bot_id)
        .bind(&principal.user.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::NotFound))?;
        let request_json = serde_json::to_value(&request).map_err(|_| error(Code::InvalidInput))?;
        if row
            .try_get::<Option<String>, _>("last_request_id")
            .map_err(database_error)?
            .as_deref()
            == Some(&request.request_id)
        {
            if row
                .try_get::<Option<serde_json::Value>, _>("last_request_json")
                .map_err(database_error)?
                != Some(request_json)
            {
                return Err(error(Code::Conflict));
            }
            tx.commit().await.map_err(database_error)?;
            return self.leader_bot_access(principal).await;
        }
        if row.try_get::<i64, _>("revision").map_err(database_error)?
            != ms(request.expected_revision)?
        {
            return Err(error(Code::Conflict));
        }
        let account: String = row.try_get("trading_account_id").map_err(database_error)?;
        if request.action == LeaderBotAction::Start {
            if row
                .try_get::<String, _>("bot_state")
                .map_err(database_error)?
                != "stopped"
            {
                return Err(error(Code::Conflict));
            }
            let invalid: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM venue_api_credentials c WHERE c.credential_id=$1 AND c.user_id=$2 AND c.trading_account_id=$3 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified') OR EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE venue='binance' AND mode='LIVE' AND trading_account_id=$3) OR EXISTS(SELECT 1 FROM venue_kol_follow_relations WHERE follower_trading_account_id=$3 AND relation_state='active') OR EXISTS(SELECT 1 FROM venue_order_mirrors WHERE bot_id=$4 AND mirror_state NOT IN ('terminal','blocked')) OR EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id WHERE m.bot_id=$4 AND c.command_state IN ('pending','sending','accepted','reconcile_required'))")
                .bind(row.try_get::<String,_>("credential_id").map_err(database_error)?).bind(&principal.user.user_id).bind(account).bind(&request.bot_id)
                .fetch_one(&mut *tx).await.map_err(database_error)?;
            if invalid {
                return Err(error(Code::VerificationRequired));
            }
        }
        let start = request.action == LeaderBotAction::Start;
        if !start {
            // An explicit stop can retry definitively rejected cleanup; unresolved commands
            // remain fenced and retain their original identities and readback schedules.
            sqlx::query("UPDATE venue_order_mirrors SET cancel_attempts=0 WHERE bot_id=$1 AND mirror_state='cancelling'")
                .bind(&request.bot_id).execute(&mut *tx).await.map_err(database_error)?;
        }
        sqlx::query("UPDATE venue_leader_bots SET bot_state=$1,revision=revision+1,permission_revision=COALESCE($2,permission_revision),started_ms=CASE WHEN $3 THEN $4 ELSE started_ms END,updated_ms=$4,last_request_id=$5,last_request_json=$6,attention_code=NULL WHERE bot_id=$7")
            .bind(if start {"running"} else {"draining"}).bind(grant).bind(start).bind(ms(now_ms)?).bind(request.request_id).bind(request_json).bind(request.bot_id)
            .execute(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.leader_bot_access(principal).await
    }
}

async fn authorized_grant(
    connection: &mut sqlx::PgConnection,
    user: &str,
) -> Result<i64, AccountError> {
    // Administrators take this lock before changing the grant. Read the grant in a subsequent
    // statement so a waiter sees the committed revocation without permission-table UPDATE rights.
    sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 AND profile_state='enabled' FOR UPDATE")
        .bind(user).fetch_optional(&mut *connection).await.map_err(database_error)?.ok_or(error(Code::Forbidden))?;
    sqlx::query_scalar(
        "SELECT revision FROM venue_leader_bot_permissions WHERE kol_user_id=$1 AND enabled",
    )
    .bind(user)
    .fetch_optional(connection)
    .await
    .map_err(database_error)?
    .ok_or(error(Code::Forbidden))
}
