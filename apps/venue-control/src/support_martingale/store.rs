use crate::multi_venue_store::{MultiVenueStore, MultiVenueStoreError, StrategyEnqueueResult};
use sqlx::{PgPool, Row};
use venue_control_protocol::support_martingale::{
    SupportMartingaleAction, SupportMartingaleConfig, SupportMartingaleCreateRequest,
    SupportMartingaleHealth, SupportMartingaleInstance, SupportMartingaleLifecycle,
    SupportMartingaleLifecycleRequest, SupportMartingaleListItem, SupportMartingaleSymbolState,
};
use venue_domain::{ExecutionCommand, OrderPurpose, Symbol};
use venue_gateway_api::{GatewayMode, VenueId};

#[derive(Clone)]
pub struct SupportMartingaleStore {
    pub(crate) pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupportMartingaleStoreError {
    #[error("support martingale input is invalid")]
    Invalid,
    #[error("support martingale state conflicts with current account state")]
    Conflict,
    #[error("support martingale storage is unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupportMartingaleCommandKind {
    Entry,
    Add,
    TakeProfit,
    CancelTakeProfit,
}
impl SupportMartingaleCommandKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::Add => "add",
            Self::TakeProfit => "tp",
            Self::CancelTakeProfit => "cancel_tp",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SupportMartingaleRuntimeSymbolState {
    pub symbol: Symbol,
    pub cycle_id: Option<String>,
    pub last_support_lower: Option<rust_decimal::Decimal>,
    pub last_support_upper: Option<rust_decimal::Decimal>,
    pub decision_sequence: u64,
    pub pending_command_id: Option<String>,
    pub take_profit_client_id: Option<String>,
    pub health_reason: Option<String>,
    pub cooldown_until_ms: Option<u64>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SupportMartingaleRuntimeState {
    pub instance_id: String,
    pub owner_user_id: String,
    pub trading_account_id: String,
    pub revision: u64,
    pub lifecycle: SupportMartingaleLifecycle,
    pub reserved_budget: rust_decimal::Decimal,
    pub symbols: Vec<SupportMartingaleRuntimeSymbolState>,
}

impl SupportMartingaleStore {
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn create(
        &self,
        owner: &str,
        request: SupportMartingaleCreateRequest,
        now_ms: u64,
    ) -> Result<String, SupportMartingaleStoreError> {
        request
            .validate()
            .map_err(|_| SupportMartingaleStoreError::Invalid)?;
        if owner.trim().is_empty() || now_ms == 0 {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let row = sqlx::query("SELECT trading_account_id,venue,verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL").bind(&request.credential_id).bind(owner).fetch_one(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Conflict)?;
        let account: String = row
            .try_get("trading_account_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let venue: String = row
            .try_get("venue")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let verified: serde_json::Value = row
            .try_get("verification_json")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if venue != request.config.execution_venue.as_str()
            || verified
                .get("verification")
                .and_then(serde_json::Value::as_str)
                != Some("verified")
            || verified
                .get("strategy_execution")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        lock_account(&mut tx, &account).await?;
        let id = format!("sm-{}", request.request_id);
        let config = serde_json::to_value(&request.config)
            .map_err(|_| SupportMartingaleStoreError::Invalid)?;
        if let Some(existing) = sqlx::query(
            "SELECT owner_user_id,credential_id,config FROM venue_support_martingale_instances WHERE instance_id=$1",
        )
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
        {
            let same = existing
                .try_get::<String, _>("owner_user_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                == owner
                && existing
                    .try_get::<String, _>("credential_id")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                    == request.credential_id
                && existing
                    .try_get::<serde_json::Value, _>("config")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                    == config;
            tx.rollback()
                .await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            return if same {
                Ok(id)
            } else {
                Err(SupportMartingaleStoreError::Conflict)
            };
        }
        sqlx::query("INSERT INTO venue_support_martingale_instances(instance_id,owner_user_id,trading_account_id,credential_id,reference_venue,execution_venue,config,lifecycle,revision,reserved_budget,created_ms,updated_ms) VALUES($1,$2,$3,$4,$5,$6,$7,'stopped',1,0,$8,$8)").bind(&id).bind(owner).bind(&account).bind(&request.credential_id).bind(request.config.reference_venue.as_str()).bind(request.config.execution_venue.as_str()).bind(config).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Conflict)?;
        for symbol in &request.config.symbols {
            sqlx::query("INSERT INTO venue_support_martingale_symbol_states(instance_id,symbol,status,layer,quantity,invested,updated_ms) VALUES($1,$2,'idle',0,0,0,$3)").bind(&id).bind(symbol.to_string()).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Conflict)?;
        }
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        Ok(id)
    }
    pub async fn list(
        &self,
        owner: &str,
    ) -> Result<Vec<SupportMartingaleListItem>, SupportMartingaleStoreError> {
        let rows = sqlx::query("SELECT instance_id,execution_venue,trading_account_id,lifecycle,health,revision,(SELECT count(*) FROM venue_support_martingale_symbol_states s WHERE s.instance_id=i.instance_id) AS symbol_count,reserved_budget::text AS reserved_budget FROM venue_support_martingale_instances i WHERE owner_user_id=$1 ORDER BY instance_id").bind(owner).fetch_all(&self.pool).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        rows.into_iter()
            .map(|r| {
                Ok(SupportMartingaleListItem {
                    instance_id: r
                        .try_get("instance_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    execution_venue: parse_venue(
                        &r.try_get::<String, _>("execution_venue")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    trading_account_id: r
                        .try_get("trading_account_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    lifecycle: parse_lifecycle(
                        &r.try_get::<String, _>("lifecycle")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    health: parse_health(
                        &r.try_get::<String, _>("health")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    revision: i64_to_u64(
                        r.try_get("revision")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    symbol_count: i64_to_u32(
                        r.try_get("symbol_count")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    reserved_budget: decimal(
                        &r.try_get::<String, _>("reserved_budget")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                })
            })
            .collect()
    }
    pub async fn get(
        &self,
        owner: &str,
        instance_id: &str,
    ) -> Result<SupportMartingaleInstance, SupportMartingaleStoreError> {
        let r = sqlx::query("SELECT instance_id,owner_user_id,credential_id,trading_account_id,execution_venue,config::text AS config,lifecycle,health,revision::text AS revision,reserved_budget::text AS reserved_budget FROM venue_support_martingale_instances WHERE owner_user_id=$1 AND instance_id=$2").bind(owner).bind(instance_id).fetch_one(&self.pool).await.map_err(|_| SupportMartingaleStoreError::Conflict)?;
        let config: SupportMartingaleConfig = serde_json::from_str(
            &r.try_get::<String, _>("config")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )
        .map_err(|_| SupportMartingaleStoreError::Conflict)?;
        let states = sqlx::query("SELECT symbol,cycle_id,layer::text AS layer,average_price::text AS average_price,quantity::text AS quantity,invested::text AS invested,take_profit_price::text AS take_profit_price,net_pnl::text AS net_pnl,status FROM venue_support_martingale_symbol_states WHERE instance_id=$1 ORDER BY symbol").bind(instance_id).fetch_all(&self.pool).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let symbols = states
            .into_iter()
            .map(|s| {
                Ok(SupportMartingaleSymbolState {
                    symbol: s
                        .try_get::<String, _>("symbol")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                        .parse()
                        .map_err(|_| SupportMartingaleStoreError::Conflict)?,
                    cycle_id: s
                        .try_get("cycle_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    layer: s
                        .try_get::<String, _>("layer")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                        .parse()
                        .map_err(|_| SupportMartingaleStoreError::Conflict)?,
                    average_price: opt_decimal(
                        &s.try_get::<Option<String>, _>("average_price")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    quantity: decimal(
                        &s.try_get::<String, _>("quantity")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    invested: decimal(
                        &s.try_get::<String, _>("invested")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    take_profit_price: opt_decimal(
                        &s.try_get::<Option<String>, _>("take_profit_price")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    net_pnl: opt_decimal(
                        &s.try_get::<Option<String>, _>("net_pnl")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    status: s
                        .try_get("status")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SupportMartingaleInstance {
            instance_id: r
                .try_get("instance_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            owner_user_id: r
                .try_get("owner_user_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            credential_id: r
                .try_get("credential_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            trading_account_id: r
                .try_get("trading_account_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            execution_venue: parse_venue(
                &r.try_get::<String, _>("execution_venue")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?,
            mode: GatewayMode::Live,
            config,
            lifecycle: parse_lifecycle(
                &r.try_get::<String, _>("lifecycle")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?,
            health: parse_health(
                &r.try_get::<String, _>("health")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?,
            revision: r
                .try_get::<String, _>("revision")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                .parse()
                .map_err(|_| SupportMartingaleStoreError::Conflict)?,
            reserved_budget: decimal(
                &r.try_get::<String, _>("reserved_budget")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?,
            symbols,
        })
    }
    pub async fn lifecycle(
        &self,
        owner: &str,
        request: SupportMartingaleLifecycleRequest,
        now_ms: u64,
    ) -> Result<u64, SupportMartingaleStoreError> {
        request
            .validate()
            .map_err(|_| SupportMartingaleStoreError::Invalid)?;
        let record = self.get(owner, &request.instance_id).await?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        if let Some(existing) = sqlx::query("SELECT resulting_revision FROM venue_support_martingale_requests WHERE owner_user_id=$1 AND request_id=$2 AND instance_id=$3")
            .bind(owner).bind(&request.request_id).bind(&request.instance_id).fetch_optional(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)? {
            let revision: i64 = existing.try_get("resulting_revision").map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            tx.rollback().await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            return i64_to_u64(revision);
        }
        if matches!(
            request.action,
            SupportMartingaleAction::Start | SupportMartingaleAction::Resume
        ) {
            let conflict: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_strategy_grids WHERE trading_account_id=$1 AND lifecycle IN ('running','pausing','paused','stopping','resetting')) OR EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE trading_account_id=$1) OR EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
                .bind(&record.trading_account_id)
                .fetch_one(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            if conflict {
                return Err(SupportMartingaleStoreError::Conflict);
            }
        }
        let next = match (request.action, record.lifecycle) {
            (
                SupportMartingaleAction::Start | SupportMartingaleAction::Resume,
                SupportMartingaleLifecycle::Stopped
                | SupportMartingaleLifecycle::EntryPaused
                | SupportMartingaleLifecycle::IncreasePaused,
            ) => SupportMartingaleLifecycle::Running,
            (SupportMartingaleAction::PauseEntry, SupportMartingaleLifecycle::Running) => {
                SupportMartingaleLifecycle::EntryPaused
            }
            (
                SupportMartingaleAction::PauseIncrease,
                SupportMartingaleLifecycle::Running | SupportMartingaleLifecycle::EntryPaused,
            ) => SupportMartingaleLifecycle::IncreasePaused,
            (SupportMartingaleAction::Drain, _) => SupportMartingaleLifecycle::Draining,
            _ => return Err(SupportMartingaleStoreError::Conflict),
        };
        let updated = sqlx::query("UPDATE venue_support_martingale_instances SET lifecycle=$1,revision=revision+1,updated_ms=$2 WHERE instance_id=$3 AND owner_user_id=$4 AND revision=$5").bind(lifecycle_str(next)).bind(ms(now_ms)?).bind(&request.instance_id).bind(owner).bind(i64::try_from(request.expected_revision).map_err(|_| SupportMartingaleStoreError::Invalid)?).execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let resulting_revision = request.expected_revision + 1;
        sqlx::query("INSERT INTO venue_support_martingale_requests(owner_user_id,request_id,instance_id,action,resulting_revision,created_ms) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(owner).bind(&request.request_id).bind(&request.instance_id).bind(lifecycle_str(next)).bind(i64::try_from(resulting_revision).map_err(|_| SupportMartingaleStoreError::Invalid)?).bind(ms(now_ms)?).execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Conflict)?;
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        Ok(resulting_revision)
    }
}
async fn lock_account(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: &str,
) -> Result<(), SupportMartingaleStoreError> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || $1,0))",
    )
    .bind(account)
    .execute(&mut **tx)
    .await
    .map(|_| ())
    .map_err(|_| SupportMartingaleStoreError::Unavailable)
}
fn ms(v: u64) -> Result<i64, SupportMartingaleStoreError> {
    i64::try_from(v).map_err(|_| SupportMartingaleStoreError::Invalid)
}
fn i64_to_u64(v: i64) -> Result<u64, SupportMartingaleStoreError> {
    v.try_into()
        .map_err(|_| SupportMartingaleStoreError::Conflict)
}
fn i64_to_u32(v: i64) -> Result<u32, SupportMartingaleStoreError> {
    v.try_into()
        .map_err(|_| SupportMartingaleStoreError::Conflict)
}
fn i64_to_u16(v: i64) -> Result<u16, SupportMartingaleStoreError> {
    v.try_into()
        .map_err(|_| SupportMartingaleStoreError::Conflict)
}
fn parse_venue(v: &str) -> Result<VenueId, SupportMartingaleStoreError> {
    v.parse().map_err(|_| SupportMartingaleStoreError::Conflict)
}
fn parse_lifecycle(v: &str) -> Result<SupportMartingaleLifecycle, SupportMartingaleStoreError> {
    Ok(match v {
        "stopped" => SupportMartingaleLifecycle::Stopped,
        "running" => SupportMartingaleLifecycle::Running,
        "entry_paused" => SupportMartingaleLifecycle::EntryPaused,
        "increase_paused" => SupportMartingaleLifecycle::IncreasePaused,
        "draining" => SupportMartingaleLifecycle::Draining,
        _ => return Err(SupportMartingaleStoreError::Conflict),
    })
}
fn lifecycle_str(v: SupportMartingaleLifecycle) -> &'static str {
    match v {
        SupportMartingaleLifecycle::Stopped => "stopped",
        SupportMartingaleLifecycle::Running => "running",
        SupportMartingaleLifecycle::EntryPaused => "entry_paused",
        SupportMartingaleLifecycle::IncreasePaused => "increase_paused",
        SupportMartingaleLifecycle::Draining => "draining",
    }
}
fn parse_health(v: &str) -> Result<SupportMartingaleHealth, SupportMartingaleStoreError> {
    Ok(match v {
        "healthy" => SupportMartingaleHealth::Healthy,
        "needs_attention" => SupportMartingaleHealth::NeedsAttention,
        "unavailable" => SupportMartingaleHealth::Unavailable,
        _ => return Err(SupportMartingaleStoreError::Conflict),
    })
}
fn decimal(value: &str) -> Result<rust_decimal::Decimal, SupportMartingaleStoreError> {
    value
        .parse()
        .map_err(|_| SupportMartingaleStoreError::Conflict)
}
fn opt_decimal(
    value: &Option<String>,
) -> Result<Option<rust_decimal::Decimal>, SupportMartingaleStoreError> {
    value.as_deref().map(decimal).transpose()
}

impl SupportMartingaleStore {
    pub async fn active_accounts(&self) -> Result<Vec<String>, SupportMartingaleStoreError> {
        sqlx::query_scalar("SELECT DISTINCT trading_account_id FROM venue_support_martingale_instances WHERE lifecycle IN ('running','entry_paused','increase_paused','draining') ORDER BY trading_account_id").fetch_all(&self.pool).await.map_err(|_| SupportMartingaleStoreError::Unavailable)
    }
    pub async fn active_instances(
        &self,
        account: Option<&str>,
    ) -> Result<Vec<String>, SupportMartingaleStoreError> {
        let result = match account {
            Some(account) => sqlx::query_scalar("SELECT instance_id FROM venue_support_martingale_instances WHERE trading_account_id=$1 AND lifecycle IN ('running','entry_paused','increase_paused','draining') ORDER BY instance_id").bind(account).fetch_all(&self.pool).await,
            None => sqlx::query_scalar("SELECT instance_id FROM venue_support_martingale_instances WHERE lifecycle IN ('running','entry_paused','increase_paused','draining') ORDER BY instance_id").fetch_all(&self.pool).await,
        };
        result.map_err(|_| SupportMartingaleStoreError::Unavailable)
    }
    pub async fn load_runtime_state(
        &self,
        owner: &str,
        instance_id: &str,
    ) -> Result<SupportMartingaleRuntimeState, SupportMartingaleStoreError> {
        let instance = self.get(owner, instance_id).await?;
        let rows = sqlx::query("SELECT symbol,cycle_id,last_support_lower::text AS last_support_lower,last_support_upper::text AS last_support_upper,decision_sequence,pending_command_id,take_profit_client_id,health_reason,cooldown_until_ms FROM venue_support_martingale_symbol_states WHERE instance_id=$1 ORDER BY symbol").bind(instance_id).fetch_all(&self.pool).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let symbols = rows
            .into_iter()
            .map(|row| {
                Ok(SupportMartingaleRuntimeSymbolState {
                    symbol: row
                        .try_get::<String, _>("symbol")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                        .parse()
                        .map_err(|_| SupportMartingaleStoreError::Conflict)?,
                    cycle_id: row
                        .try_get("cycle_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    last_support_lower: opt_decimal(
                        &row.try_get::<Option<String>, _>("last_support_lower")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    last_support_upper: opt_decimal(
                        &row.try_get::<Option<String>, _>("last_support_upper")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    decision_sequence: i64_to_u64(
                        row.try_get("decision_sequence")
                            .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    )?,
                    pending_command_id: row
                        .try_get("pending_command_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    take_profit_client_id: row
                        .try_get("take_profit_client_id")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    health_reason: row
                        .try_get("health_reason")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
                    cooldown_until_ms: row
                        .try_get::<Option<i64>, _>("cooldown_until_ms")
                        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                        .map(i64_to_u64)
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SupportMartingaleRuntimeState {
            instance_id: instance.instance_id,
            owner_user_id: instance.owner_user_id,
            trading_account_id: instance.trading_account_id,
            revision: instance.revision,
            lifecycle: instance.lifecycle,
            reserved_budget: instance.reserved_budget,
            symbols,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_command(
        &self,
        owner: &str,
        instance_id: &str,
        symbol: &Symbol,
        kind: SupportMartingaleCommandKind,
        cycle_id: Option<&str>,
        support_id: Option<&str>,
        support_bounds: Option<(rust_decimal::Decimal, rust_decimal::Decimal)>,
        request_id: &str,
        requested_notional: rust_decimal::Decimal,
        command: ExecutionCommand,
        now_ms: u64,
    ) -> Result<StrategyEnqueueResult, SupportMartingaleStoreError> {
        use rust_decimal::Decimal;
        let increasing = matches!(
            kind,
            SupportMartingaleCommandKind::Entry | SupportMartingaleCommandKind::Add
        );
        if owner.trim().is_empty()
            || instance_id.trim().is_empty()
            || request_id.trim().is_empty()
            || now_ms == 0
            || (increasing && requested_notional <= Decimal::ZERO)
            || (!increasing && !requested_notional.is_zero())
        {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let command_owner = command.mutation_owner();
        let purpose_ok = match kind {
            SupportMartingaleCommandKind::Entry | SupportMartingaleCommandKind::Add => {
                command_owner.purpose == OrderPurpose::Entry
            }
            SupportMartingaleCommandKind::TakeProfit
            | SupportMartingaleCommandKind::CancelTakeProfit => {
                command_owner.purpose == OrderPurpose::TakeProfit
            }
        };
        if command_owner.strategy_instance_id != instance_id
            || command_owner.symbol != *symbol
            || !purpose_ok
        {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let account: String = sqlx::query_scalar(
            "SELECT trading_account_id FROM venue_support_martingale_instances WHERE instance_id=$1 AND owner_user_id=$2",
        )
        .bind(instance_id)
        .bind(owner)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
        .ok_or(SupportMartingaleStoreError::Conflict)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        lock_account(&mut tx, &account).await?;
        let row = sqlx::query("SELECT credential_id,execution_venue,config,lifecycle,reserved_budget::text AS reserved_budget FROM venue_support_martingale_instances WHERE instance_id=$1 AND owner_user_id=$2 AND trading_account_id=$3 FOR UPDATE")
            .bind(instance_id).bind(owner).bind(&account).fetch_one(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let credential: String = row
            .try_get("credential_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let venue: String = row
            .try_get("execution_venue")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if command_owner.account != account || command_owner.exchange != venue {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let has_grid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_strategy_grids WHERE trading_account_id=$1 AND lifecycle IN ('running','pausing','paused','stopping','resetting'))")
            .bind(&account).fetch_one(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if has_grid {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let lifecycle = parse_lifecycle(
            &row.try_get::<String, _>("lifecycle")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )?;
        let allowed = match kind {
            SupportMartingaleCommandKind::Entry => lifecycle == SupportMartingaleLifecycle::Running,
            SupportMartingaleCommandKind::Add => matches!(
                lifecycle,
                SupportMartingaleLifecycle::Running | SupportMartingaleLifecycle::EntryPaused
            ),
            SupportMartingaleCommandKind::TakeProfit
            | SupportMartingaleCommandKind::CancelTakeProfit => {
                lifecycle != SupportMartingaleLifecycle::Stopped
            }
        };
        if !allowed {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let state = sqlx::query("SELECT pending_command_id,decision_sequence,layer,quantity::text AS quantity,take_profit_client_id,last_support_lower::text AS last_support_lower FROM venue_support_martingale_symbol_states WHERE instance_id=$1 AND symbol=$2 FOR UPDATE")
            .bind(instance_id).bind(symbol.to_string()).fetch_one(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if state
            .try_get::<Option<String>, _>("pending_command_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?
            .is_some()
        {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let quantity = decimal(
            &state
                .try_get::<String, _>("quantity")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )?;
        let layer = i64_to_u16(i64::from(
            state
                .try_get::<i32, _>("layer")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        ))?;
        let config: SupportMartingaleConfig = serde_json::from_value(
            row.try_get("config")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )
        .map_err(|_| SupportMartingaleStoreError::Conflict)?;
        match kind {
            SupportMartingaleCommandKind::Entry if !quantity.is_zero() || layer != 0 => {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            SupportMartingaleCommandKind::Add
                if quantity <= Decimal::ZERO || layer >= config.max_entries =>
            {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            SupportMartingaleCommandKind::TakeProfit if quantity <= Decimal::ZERO => {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            SupportMartingaleCommandKind::CancelTakeProfit
                if state
                    .try_get::<Option<String>, _>("take_profit_client_id")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?
                    .is_none() =>
            {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            _ => {}
        }
        if increasing {
            let support = support_id
                .filter(|value| !value.trim().is_empty())
                .ok_or(SupportMartingaleStoreError::Invalid)?;
            let (lower, upper) = support_bounds
                .filter(|(lower, upper)| *lower > Decimal::ZERO && *upper >= *lower)
                .ok_or(SupportMartingaleStoreError::Invalid)?;
            let duplicate: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_support_martingale_commands WHERE instance_id=$1 AND symbol=$2 AND kind IN ('entry','add') AND support_id=$3)")
                .bind(instance_id).bind(symbol.to_string()).bind(support).fetch_one(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            let previous = opt_decimal(
                &state
                    .try_get::<Option<String>, _>("last_support_lower")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?;
            if duplicate || previous.is_some_and(|value| upper >= value) {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            if kind == SupportMartingaleCommandKind::Entry {
                let active: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_support_martingale_symbol_states WHERE instance_id=$1 AND quantity>0")
                    .bind(instance_id).fetch_one(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
                if active < 0
                    || u16::try_from(active)
                        .ok()
                        .is_none_or(|value| value >= config.max_active_positions)
                {
                    return Err(SupportMartingaleStoreError::Conflict);
                }
            }
            let reserved = decimal(
                &row.try_get::<String, _>("reserved_budget")
                    .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
            )?;
            let invested_text: String = sqlx::query_scalar("SELECT COALESCE(sum(invested),0)::text FROM venue_support_martingale_symbol_states WHERE instance_id=$1")
                .bind(instance_id).fetch_one(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            let invested = decimal(&invested_text)?;
            if invested
                .checked_add(reserved)
                .and_then(|value| value.checked_add(requested_notional))
                .is_none_or(|value| value > config.total_budget)
            {
                return Err(SupportMartingaleStoreError::Conflict);
            }
            let _ = (lower, upper);
        }
        let result = MultiVenueStore::new(self.pool.clone())
            .enqueue_in(&mut tx, owner, &credential, command.clone(), now_ms)
            .await
            .map_err(map_multi_error)?;
        let command_id = command.command_id().as_str().to_owned();
        let next_sequence = state
            .try_get::<i64, _>("decision_sequence")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?
            .checked_add(1)
            .ok_or(SupportMartingaleStoreError::Invalid)?;
        sqlx::query("INSERT INTO venue_support_martingale_commands(command_id,instance_id,symbol,kind,cycle_id,support_id,request_id,requested_notional,created_ms,updated_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9,$9)")
            .bind(&command_id).bind(instance_id).bind(symbol.to_string()).bind(kind.as_str()).bind(cycle_id).bind(support_id)
            .bind(request_id).bind(requested_notional.to_string()).bind(ms(now_ms)?).execute(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Conflict)?;
        let tp_client_id = command.native_client_id().map(|id| id.as_str().to_owned());
        let (support_lower, support_upper) = support_bounds.unzip();
        sqlx::query("UPDATE venue_support_martingale_symbol_states SET pending_command_id=$1,decision_sequence=$2,status=$3,cycle_id=COALESCE(cycle_id,$10),take_profit_client_id=CASE WHEN $3='tp_pending' THEN $7 ELSE take_profit_client_id END,last_support_lower=COALESCE($8::text::numeric,last_support_lower),last_support_upper=COALESCE($9::text::numeric,last_support_upper),updated_ms=$4 WHERE instance_id=$5 AND symbol=$6")
            .bind(&command_id).bind(next_sequence).bind(match kind { SupportMartingaleCommandKind::Entry=>"entry_pending",SupportMartingaleCommandKind::Add=>"add_pending",SupportMartingaleCommandKind::TakeProfit=>"tp_pending",SupportMartingaleCommandKind::CancelTakeProfit=>"cancel_tp_pending" })
            .bind(ms(now_ms)?).bind(instance_id).bind(symbol.to_string()).bind(tp_client_id)
            .bind(support_lower.map(|value| value.to_string())).bind(support_upper.map(|value| value.to_string()))
            .bind(cycle_id)
            .execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if increasing {
            sqlx::query("UPDATE venue_support_martingale_instances SET reserved_budget=reserved_budget+$1::text::numeric,revision=revision+1,updated_ms=$2 WHERE instance_id=$3 AND owner_user_id=$4")
                .bind(requested_notional.to_string()).bind(ms(now_ms)?).bind(instance_id).bind(owner).execute(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        Ok(result)
    }

    pub async fn settle_command(
        &self,
        owner: &str,
        command_id: &str,
        observed_fill: rust_decimal::Decimal,
        order_terminal: bool,
        now_ms: u64,
    ) -> Result<(), SupportMartingaleStoreError> {
        if owner.trim().is_empty()
            || command_id.trim().is_empty()
            || observed_fill < rust_decimal::Decimal::ZERO
            || now_ms == 0
        {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let row = sqlx::query("SELECT c.instance_id,c.symbol,c.kind,c.requested_notional::text AS requested_notional,c.observed_fill::text AS prior_fill,c.ledger_settled,c.terminal,i.trading_account_id FROM venue_support_martingale_commands c JOIN venue_support_martingale_instances i USING(instance_id) WHERE c.command_id=$1 AND i.owner_user_id=$2 FOR UPDATE").bind(command_id).bind(owner).fetch_optional(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?.ok_or(SupportMartingaleStoreError::Conflict)?;
        let instance_id: String = row
            .try_get("instance_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let symbol: String = row
            .try_get("symbol")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let account: String = row
            .try_get("trading_account_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        lock_account(&mut tx, &account).await?;
        let prior = decimal(
            &row.try_get::<String, _>("prior_fill")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )?;
        if row
            .try_get::<bool, _>("ledger_settled")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?
        {
            let prior_terminal = row
                .try_get::<bool, _>("terminal")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            return if prior == observed_fill && prior_terminal == order_terminal {
                Ok(())
            } else {
                Err(SupportMartingaleStoreError::Conflict)
            };
        }
        if observed_fill < prior {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let kind: String = row
            .try_get("kind")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        sqlx::query("UPDATE venue_support_martingale_commands SET observed_fill=$1::text::numeric,ledger_settled=TRUE,terminal=$2,updated_ms=$3 WHERE command_id=$4")
            .bind(observed_fill.to_string()).bind(order_terminal).bind(ms(now_ms)?).bind(command_id)
            .execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let next_status = match kind.as_str() {
            "entry" | "add" if observed_fill > rust_decimal::Decimal::ZERO => "holding",
            "entry" => "idle",
            "add" => "holding",
            "tp" if observed_fill > rust_decimal::Decimal::ZERO => "exit_only",
            "tp" => "holding",
            "cancel_tp" => "add_ready",
            _ => return Err(SupportMartingaleStoreError::Conflict),
        };
        sqlx::query("UPDATE venue_support_martingale_symbol_states SET pending_command_id=NULL,status=$1,layer=CASE WHEN $2 AND $3::text::numeric>0 THEN layer+1 ELSE layer END,take_profit_client_id=CASE WHEN $4 OR ($5='tp' AND $6) THEN NULL ELSE take_profit_client_id END,updated_ms=$7 WHERE instance_id=$8 AND symbol=$9 AND pending_command_id=$10")
            .bind(next_status).bind(matches!(kind.as_str(), "entry"|"add")).bind(observed_fill.to_string())
            .bind(kind=="cancel_tp").bind(&kind).bind(order_terminal).bind(ms(now_ms)?).bind(&instance_id).bind(&symbol).bind(command_id)
            .execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if matches!(kind.as_str(), "entry" | "add") {
            let requested: String = row
                .try_get("requested_notional")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            sqlx::query("UPDATE venue_support_martingale_instances SET reserved_budget=GREATEST(0,reserved_budget-$1::text::numeric),revision=revision+1,updated_ms=$2 WHERE instance_id=$3 AND owner_user_id=$4")
                .bind(requested).bind(ms(now_ms)?).bind(&instance_id).bind(owner).execute(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)
    }

    pub async fn consumed_supports(
        &self,
        instance_id: &str,
        symbol: &Symbol,
        cycle_id: Option<&str>,
    ) -> Result<std::collections::BTreeSet<String>, SupportMartingaleStoreError> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT support_id FROM venue_support_martingale_commands WHERE instance_id=$1 AND symbol=$2 AND cycle_id IS NOT DISTINCT FROM $3 AND kind IN ('entry','add') AND support_id IS NOT NULL ORDER BY support_id",
        )
        .bind(instance_id)
        .bind(symbol.to_string())
        .bind(cycle_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        Ok(rows.into_iter().collect())
    }

    pub async fn observe_take_profit(
        &self,
        owner: &str,
        command_id: &str,
        observed_fill: rust_decimal::Decimal,
        terminal: bool,
        now_ms: u64,
    ) -> Result<(), SupportMartingaleStoreError> {
        if owner.trim().is_empty()
            || command_id.trim().is_empty()
            || observed_fill < rust_decimal::Decimal::ZERO
            || now_ms == 0
        {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let row = sqlx::query("SELECT c.instance_id,c.symbol,c.observed_fill::text AS prior_fill,c.terminal,i.trading_account_id FROM venue_support_martingale_commands c JOIN venue_support_martingale_instances i USING(instance_id) WHERE c.command_id=$1 AND c.kind='tp' AND c.ledger_settled AND i.owner_user_id=$2 FOR UPDATE")
            .bind(command_id).bind(owner).fetch_one(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let account: String = row
            .try_get("trading_account_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        lock_account(&mut tx, &account).await?;
        let prior = decimal(
            &row.try_get::<String, _>("prior_fill")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )?;
        let prior_terminal: bool = row
            .try_get("terminal")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if observed_fill < prior || (prior_terminal && !terminal) {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        sqlx::query("UPDATE venue_support_martingale_commands SET observed_fill=$1::text::numeric,terminal=$2,updated_ms=$3 WHERE command_id=$4")
            .bind(observed_fill.to_string()).bind(terminal).bind(ms(now_ms)?).bind(command_id)
            .execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if observed_fill > rust_decimal::Decimal::ZERO {
            let instance_id: String = row
                .try_get("instance_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            let symbol: String = row
                .try_get("symbol")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            sqlx::query("UPDATE venue_support_martingale_symbol_states SET status='exit_only',updated_ms=$1 WHERE instance_id=$2 AND symbol=$3")
                .bind(ms(now_ms)?).bind(instance_id).bind(symbol).execute(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        if terminal {
            let instance_id: String = row
                .try_get("instance_id")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            let symbol: String = row
                .try_get("symbol")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
            sqlx::query("UPDATE venue_support_martingale_symbol_states SET take_profit_client_id=NULL,take_profit_price=NULL,status=CASE WHEN $1::text::numeric>0 THEN 'exit_only' WHEN status='add_ready' THEN 'add_ready' ELSE 'take_profit_blocked' END,updated_ms=$2 WHERE instance_id=$3 AND symbol=$4")
                .bind(observed_fill.to_string()).bind(ms(now_ms)?).bind(instance_id).bind(symbol)
                .execute(&mut *tx).await.map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn sync_symbol_facts(
        &self,
        owner: &str,
        instance_id: &str,
        symbol: &Symbol,
        quantity: rust_decimal::Decimal,
        average_price: Option<rust_decimal::Decimal>,
        invested: rust_decimal::Decimal,
        take_profit_price: Option<rust_decimal::Decimal>,
        net_pnl: Option<rust_decimal::Decimal>,
        now_ms: u64,
    ) -> Result<(), SupportMartingaleStoreError> {
        use rust_decimal::Decimal;
        if owner.trim().is_empty()
            || instance_id.trim().is_empty()
            || quantity < Decimal::ZERO
            || invested < Decimal::ZERO
            || now_ms == 0
            || (quantity > Decimal::ZERO
                && average_price.is_none_or(|value| value <= Decimal::ZERO))
        {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let account: String = sqlx::query_scalar(
            "SELECT trading_account_id FROM venue_support_martingale_instances WHERE instance_id=$1 AND owner_user_id=$2",
        )
        .bind(instance_id)
        .bind(owner)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| SupportMartingaleStoreError::Unavailable)?
        .ok_or(SupportMartingaleStoreError::Conflict)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        lock_account(&mut tx, &account).await?;
        let state = sqlx::query("SELECT pending_command_id,status,quantity::text AS quantity,cooldown_until_ms FROM venue_support_martingale_symbol_states WHERE instance_id=$1 AND symbol=$2 FOR UPDATE")
            .bind(instance_id).bind(symbol.to_string()).fetch_one(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if state
            .try_get::<Option<String>, _>("pending_command_id")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?
            .is_some()
        {
            return Err(SupportMartingaleStoreError::Conflict);
        }
        let status: String = state
            .try_get("status")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        let prior_quantity = decimal(
            &state
                .try_get::<String, _>("quantity")
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?,
        )?;
        let ended_cycle = prior_quantity > Decimal::ZERO && quantity.is_zero();
        let existing_cooldown = state
            .try_get::<Option<i64>, _>("cooldown_until_ms")
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?
            .map(i64_to_u64)
            .transpose()?;
        let cooldown = if ended_cycle {
            Some(now_ms.saturating_add(1_800_000))
        } else {
            existing_cooldown.filter(|until| now_ms < *until)
        };
        let next_status = if quantity.is_zero() && cooldown.is_some() {
            "cooldown"
        } else if quantity.is_zero() {
            "idle"
        } else if status == "exit_only" {
            "exit_only"
        } else if status == "add_ready" && take_profit_price.is_none() {
            "add_ready"
        } else if take_profit_price.is_some() {
            "holding"
        } else {
            "take_profit_blocked"
        };
        sqlx::query("UPDATE venue_support_martingale_symbol_states SET quantity=$1::text::numeric,average_price=$2::text::numeric,invested=$3::text::numeric,take_profit_price=$4::text::numeric,net_pnl=$5::text::numeric,status=$6,cooldown_until_ms=$7,cycle_id=CASE WHEN $8 THEN NULL ELSE cycle_id END,layer=CASE WHEN $8 THEN 0 ELSE layer END,last_support_lower=CASE WHEN $8 THEN NULL ELSE last_support_lower END,last_support_upper=CASE WHEN $8 THEN NULL ELSE last_support_upper END,take_profit_client_id=CASE WHEN $8 THEN NULL ELSE take_profit_client_id END,updated_ms=$9 WHERE instance_id=$10 AND symbol=$11")
            .bind(quantity.to_string()).bind(average_price.map(|value| value.to_string()))
            .bind(invested.to_string()).bind(take_profit_price.map(|value| value.to_string()))
            .bind(net_pnl.map(|value| value.to_string())).bind(next_status)
            .bind(cooldown.map(ms).transpose()?).bind(ended_cycle).bind(ms(now_ms)?)
            .bind(instance_id).bind(symbol.to_string()).execute(&mut *tx).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if quantity.is_zero() {
            sqlx::query("UPDATE venue_support_martingale_instances SET lifecycle='stopped',updated_ms=$1 WHERE instance_id=$2 AND owner_user_id=$3 AND lifecycle='draining' AND NOT EXISTS(SELECT 1 FROM venue_support_martingale_symbol_states WHERE instance_id=$2 AND (quantity>0 OR pending_command_id IS NOT NULL OR take_profit_client_id IS NOT NULL))")
                .bind(ms(now_ms)?).bind(instance_id).bind(owner).execute(&mut *tx).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        tx.commit()
            .await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)
    }

    pub async fn mark_health(
        &self,
        instance_id: &str,
        health: SupportMartingaleHealth,
        reason: Option<&str>,
        now_ms: u64,
    ) -> Result<(), SupportMartingaleStoreError> {
        if instance_id.trim().is_empty()
            || now_ms == 0
            || reason.is_some_and(|value| value.trim().is_empty())
        {
            return Err(SupportMartingaleStoreError::Invalid);
        }
        let value = match health {
            SupportMartingaleHealth::Healthy => "healthy",
            SupportMartingaleHealth::NeedsAttention => "needs_attention",
            SupportMartingaleHealth::Unavailable => "unavailable",
        };
        sqlx::query("UPDATE venue_support_martingale_instances SET health=$1,updated_ms=$2 WHERE instance_id=$3")
            .bind(value).bind(ms(now_ms)?).bind(instance_id).execute(&self.pool).await
            .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        if health == SupportMartingaleHealth::Healthy {
            sqlx::query("UPDATE venue_support_martingale_symbol_states SET health_reason=NULL,updated_ms=$1 WHERE instance_id=$2")
                .bind(ms(now_ms)?).bind(instance_id).execute(&self.pool).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        } else if let Some(reason) = reason {
            sqlx::query("UPDATE venue_support_martingale_symbol_states SET health_reason=$1,updated_ms=$2 WHERE instance_id=$3")
                .bind(reason).bind(ms(now_ms)?).bind(instance_id).execute(&self.pool).await
                .map_err(|_| SupportMartingaleStoreError::Unavailable)?;
        }
        Ok(())
    }
}

fn map_multi_error(value: MultiVenueStoreError) -> SupportMartingaleStoreError {
    match value {
        MultiVenueStoreError::Invalid => SupportMartingaleStoreError::Invalid,
        MultiVenueStoreError::Conflict => SupportMartingaleStoreError::Conflict,
        MultiVenueStoreError::Unavailable => SupportMartingaleStoreError::Unavailable,
    }
}
