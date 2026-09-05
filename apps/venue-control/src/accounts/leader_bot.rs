use super::{AccountError, AccountService, Principal, crypto, database_error, error, ms};
use rust_decimal::Decimal;
use sqlx::{PgConnection, Row, postgres::PgRow};
use venue_control_protocol::{
    accounts::AccountErrorCode as Code, kol::KolProfileState, leader_bot::*,
};

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

    pub async fn leader_bots_access(
        &self,
        principal: &Principal,
    ) -> Result<LeaderBotsAccess, AccountError> {
        let (profile_state, can_use, permission_revision) =
            access_header(&self.pool, &principal.user.user_id).await?;
        let rows = sqlx::query(
            "SELECT b.*,
             CASE WHEN b.bot_state<>'stopped' THEN
               (SELECT count(*) FROM venue_kol_follow_relations r
                WHERE r.kol_user_id=b.owner_user_id AND r.relation_state='active')
             ELSE 0 END AS followers,
             (SELECT count(*) FROM venue_order_mirrors m
              WHERE m.bot_id=b.bot_id AND m.mirror_state NOT IN ('terminal','blocked')) AS orders
             FROM venue_leader_bots b WHERE b.owner_user_id=$1
             ORDER BY (b.bot_state<>'stopped') DESC,b.updated_ms DESC,b.bot_id
             LIMIT $2",
        )
        .bind(&principal.user.user_id)
        .bind(i64::from(MAX_LEADER_BOTS_PER_KOL) + 1)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        if rows.len() > MAX_LEADER_BOTS_PER_KOL as usize {
            return Err(error(Code::Unavailable));
        }
        let bots = rows.iter().map(list_item).collect::<Result<Vec<_>, _>>()?;
        let access = LeaderBotsAccess {
            schema_version: LEADER_BOTS_SCHEMA_VERSION,
            profile_state,
            can_use,
            permission_revision,
            bots,
        };
        if !access.valid() {
            return Err(error(Code::Unavailable));
        }
        Ok(access)
    }

    pub async fn leader_bot_access(
        &self,
        principal: &Principal,
    ) -> Result<LeaderBotAccess, AccountError> {
        let access = self.leader_bots_access(principal).await?;
        Ok(legacy_access(&access))
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
        let capital: String = sqlx::query_scalar(
            "SELECT strategy_capital FROM venue_kol_profiles WHERE kol_user_id=$1",
        )
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::Forbidden))?;
        let strategy_capital = capital.parse().map_err(|_| error(Code::Unavailable))?;
        self.create_configured_leader_bot(
            principal,
            LeaderBotConfiguredCreateRequest {
                schema_version: LEADER_BOTS_SCHEMA_VERSION,
                request_id: request.request_id,
                credential_id: request.credential_id,
                config: LeaderBotConfig {
                    name: "KOL 带单".into(),
                    description: String::new(),
                    strategy_capital,
                },
            },
            now_ms,
        )
        .await?;
        self.leader_bot_access(principal).await
    }

    pub async fn create_configured_leader_bot(
        &self,
        principal: &Principal,
        request: LeaderBotConfiguredCreateRequest,
        now_ms: u64,
    ) -> Result<LeaderBotsAccess, AccountError> {
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
        let account =
            verified_leader_credential(&mut tx, &principal.user.user_id, &request.credential_id)
                .await?;
        let existing = sqlx::query(
            "SELECT credential_id,bot_name,bot_description,strategy_capital
             FROM venue_leader_bots
             WHERE owner_user_id=$1 AND create_request_id=$2 FOR UPDATE",
        )
        .bind(&principal.user.user_id)
        .bind(&request.request_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if let Some(row) = existing {
            if row
                .try_get::<String, _>("credential_id")
                .map_err(database_error)?
                != request.credential_id
                || row
                    .try_get::<String, _>("bot_name")
                    .map_err(database_error)?
                    != request.config.name
                || row
                    .try_get::<String, _>("bot_description")
                    .map_err(database_error)?
                    != request.config.description
                || row
                    .try_get::<String, _>("strategy_capital")
                    .map_err(database_error)?
                    != decimal_text(request.config.strategy_capital)
            {
                return Err(error(Code::Conflict));
            }
        } else {
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM venue_leader_bots WHERE owner_user_id=$1")
                    .bind(&principal.user.user_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(database_error)?;
            if count >= i64::from(MAX_LEADER_BOTS_PER_KOL) {
                return Err(error(Code::Conflict));
            }
            sqlx::query(
                "INSERT INTO venue_leader_bots
                 (bot_id,owner_user_id,trading_account_id,credential_id,create_request_id,
                  bot_name,bot_description,strategy_capital,bot_state,permission_revision,
                  created_ms,updated_ms)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'stopped',$9,$10,$10)",
            )
            .bind(crypto::opaque_id()?)
            .bind(&principal.user.user_id)
            .bind(account)
            .bind(&request.credential_id)
            .bind(&request.request_id)
            .bind(&request.config.name)
            .bind(&request.config.description)
            .bind(decimal_text(request.config.strategy_capital))
            .bind(grant)
            .bind(ms(now_ms)?)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        }
        tx.commit().await.map_err(database_error)?;
        self.leader_bots_access(principal).await
    }

    pub async fn update_leader_bot(
        &self,
        principal: &Principal,
        request: LeaderBotUpdateRequest,
        now_ms: u64,
    ) -> Result<LeaderBotsAccess, AccountError> {
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
        let _ = authorized_grant(&mut tx, &principal.user.user_id).await?;
        let account =
            verified_leader_credential(&mut tx, &principal.user.user_id, &request.credential_id)
                .await?;
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
            return self.leader_bots_access(principal).await;
        }
        if row.try_get::<i64, _>("revision").map_err(database_error)?
            != ms(request.expected_revision)?
            || row
                .try_get::<String, _>("bot_state")
                .map_err(database_error)?
                != "stopped"
        {
            return Err(error(Code::Conflict));
        }
        if unresolved_bot_work(&mut tx, &request.bot_id).await? {
            return Err(error(Code::AccountInUse));
        }
        sqlx::query(
            "UPDATE venue_leader_bots
             SET trading_account_id=$1,credential_id=$2,bot_name=$3,bot_description=$4,
                 strategy_capital=$5,config_revision=config_revision+1,revision=revision+1,
                 updated_ms=$6,last_request_id=$7,last_request_json=$8,attention_code=NULL
             WHERE bot_id=$9",
        )
        .bind(account)
        .bind(&request.credential_id)
        .bind(&request.config.name)
        .bind(&request.config.description)
        .bind(decimal_text(request.config.strategy_capital))
        .bind(ms(now_ms)?)
        .bind(&request.request_id)
        .bind(request_json)
        .bind(&request.bot_id)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.leader_bots_access(principal).await
    }

    pub async fn request_leader_bot_lifecycle(
        &self,
        principal: &Principal,
        request: LeaderBotLifecycleRequest,
        now_ms: u64,
    ) -> Result<LeaderBotAccess, AccountError> {
        self.request_leader_bots_lifecycle(principal, request, now_ms)
            .await?;
        self.leader_bot_access(principal).await
    }

    pub async fn request_leader_bots_lifecycle(
        &self,
        principal: &Principal,
        request: LeaderBotLifecycleRequest,
        now_ms: u64,
    ) -> Result<LeaderBotsAccess, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        // Grants, starts and sibling instances share the profile lock. Stop remains available
        // after revocation so already-created child orders can still be drained.
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
            return self.leader_bots_access(principal).await;
        }
        if row.try_get::<i64, _>("revision").map_err(database_error)?
            != ms(request.expected_revision)?
        {
            return Err(error(Code::Conflict));
        }
        let state: String = row.try_get("bot_state").map_err(database_error)?;
        let account: String = row.try_get("trading_account_id").map_err(database_error)?;
        let start = request.action == LeaderBotAction::Start;
        if start {
            if state != "stopped" {
                return Err(error(Code::Conflict));
            }
            let sibling_active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM venue_leader_bots
                 WHERE owner_user_id=$1 AND bot_id<>$2 AND bot_state<>'stopped')",
            )
            .bind(&principal.user.user_id)
            .bind(&request.bot_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(database_error)?;
            if sibling_active {
                return Err(error(Code::AccountInUse));
            }
            let invalid: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM venue_api_credentials c WHERE c.credential_id=$1 AND c.user_id=$2 AND c.trading_account_id=$3 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified') OR EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE venue='binance' AND mode='LIVE' AND trading_account_id=$3) OR EXISTS(SELECT 1 FROM venue_kol_follow_relations WHERE follower_trading_account_id=$3 AND relation_state='active') OR EXISTS(SELECT 1 FROM venue_order_mirrors WHERE bot_id=$4 AND mirror_state NOT IN ('terminal','blocked')) OR EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id WHERE m.bot_id=$4 AND c.command_state IN ('pending','sending','accepted','reconcile_required'))")
                .bind(row.try_get::<String,_>("credential_id").map_err(database_error)?).bind(&principal.user.user_id).bind(account).bind(&request.bot_id)
                .fetch_one(&mut *tx).await.map_err(database_error)?;
            if invalid {
                return Err(error(Code::VerificationRequired));
            }
        } else if !matches!(state.as_str(), "running" | "draining" | "needs_attention") {
            return Err(error(Code::Conflict));
        } else {
            sqlx::query("UPDATE venue_order_mirrors SET cancel_attempts=0 WHERE bot_id=$1 AND mirror_state='cancelling'")
                .bind(&request.bot_id).execute(&mut *tx).await.map_err(database_error)?;
        }
        sqlx::query("UPDATE venue_leader_bots SET bot_state=$1,revision=revision+1,permission_revision=COALESCE($2,permission_revision),started_ms=CASE WHEN $3 THEN $4 ELSE started_ms END,updated_ms=$4,last_request_id=$5,last_request_json=$6,attention_code=NULL WHERE bot_id=$7")
            .bind(if start {"running"} else {"draining"}).bind(grant).bind(start).bind(ms(now_ms)?).bind(request.request_id).bind(request_json).bind(request.bot_id)
            .execute(&mut *tx).await.map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.leader_bots_access(principal).await
    }
}

async fn access_header(
    pool: &sqlx::PgPool,
    user: &str,
) -> Result<(Option<KolProfileState>, bool, u64), AccountError> {
    let profile = sqlx::query("SELECT p.profile_state,COALESCE(g.enabled,false) AS enabled,COALESCE(g.revision,0) AS revision FROM venue_kol_profiles p LEFT JOIN venue_leader_bot_permissions g ON g.kol_user_id=p.kol_user_id WHERE p.kol_user_id=$1")
        .bind(user).fetch_optional(pool).await.map_err(database_error)?;
    let Some(row) = profile else {
        return Ok((None, false, 0));
    };
    let state = match row
        .try_get::<String, _>("profile_state")
        .map_err(database_error)?
        .as_str()
    {
        "draft" => KolProfileState::Draft,
        "enabled" => KolProfileState::Enabled,
        "disabled" => KolProfileState::Disabled,
        _ => return Err(error(Code::Unavailable)),
    };
    let enabled = row.try_get::<bool, _>("enabled").map_err(database_error)?;
    let revision = u64::try_from(row.try_get::<i64, _>("revision").map_err(database_error)?)
        .map_err(|_| error(Code::Unavailable))?;
    Ok((
        Some(state),
        enabled && state == KolProfileState::Enabled,
        revision,
    ))
}

fn list_item(row: &PgRow) -> Result<LeaderBotListItem, AccountError> {
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
    let number = |column| -> Result<u64, AccountError> {
        u64::try_from(row.try_get::<i64, _>(column).map_err(database_error)?)
            .map_err(|_| error(Code::Unavailable))
    };
    let count = |column| -> Result<u32, AccountError> {
        u32::try_from(row.try_get::<i64, _>(column).map_err(database_error)?)
            .map_err(|_| error(Code::Unavailable))
    };
    Ok(LeaderBotListItem {
        bot_id: row.try_get("bot_id").map_err(database_error)?,
        trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
        credential_id: row.try_get("credential_id").map_err(database_error)?,
        config: LeaderBotConfig {
            name: row.try_get("bot_name").map_err(database_error)?,
            description: row.try_get("bot_description").map_err(database_error)?,
            strategy_capital: row
                .try_get::<String, _>("strategy_capital")
                .map_err(database_error)?
                .parse()
                .map_err(|_| error(Code::Unavailable))?,
        },
        state,
        revision: number("revision")?,
        config_revision: number("config_revision")?,
        active_followers: count("followers")?,
        pending_orders: count("orders")?,
        attention_code: row.try_get("attention_code").map_err(database_error)?,
        created_ms: number("created_ms")?,
        updated_ms: number("updated_ms")?,
    })
}

fn legacy_access(access: &LeaderBotsAccess) -> LeaderBotAccess {
    LeaderBotAccess {
        schema_version: LEADER_BOT_SCHEMA_VERSION,
        profile_state: access.profile_state,
        can_use: access.can_use,
        permission_revision: access.permission_revision,
        bot: access.bots.first().map(|bot| LeaderBotSummary {
            bot_id: bot.bot_id.clone(),
            trading_account_id: bot.trading_account_id.clone(),
            credential_id: bot.credential_id.clone(),
            state: bot.state,
            revision: bot.revision,
            active_followers: bot.active_followers,
            pending_orders: bot.pending_orders,
            attention_code: bot.attention_code.clone(),
        }),
    }
}

async fn verified_leader_credential(
    connection: &mut PgConnection,
    user: &str,
    credential_id: &str,
) -> Result<String, AccountError> {
    sqlx::query_scalar("SELECT c.trading_account_id FROM venue_api_credentials c JOIN venue_kol_profiles p ON p.kol_user_id=c.user_id AND p.leader_trading_account_id=c.trading_account_id WHERE c.credential_id=$1 AND c.user_id=$2 AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified'")
        .bind(credential_id).bind(user).fetch_optional(connection).await.map_err(database_error)?.ok_or(error(Code::VerificationRequired))
}

async fn unresolved_bot_work(
    connection: &mut PgConnection,
    bot_id: &str,
) -> Result<bool, AccountError> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_order_mirrors WHERE bot_id=$1 AND mirror_state NOT IN ('terminal','blocked')) OR EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_order_mirrors m ON m.mirror_id=c.mirror_order_id WHERE m.bot_id=$1 AND c.command_state IN ('pending','sending','accepted','reconcile_required'))")
        .bind(bot_id).fetch_one(connection).await.map_err(database_error)
}

async fn authorized_grant(connection: &mut PgConnection, user: &str) -> Result<i64, AccountError> {
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

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}
