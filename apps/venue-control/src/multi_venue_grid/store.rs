use crate::multi_venue_store::{MultiVenueStore, MultiVenueStoreError as Error};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::str::FromStr;
use venue_domain::{CommandId, ExecutionCommand, Symbol};
use venue_gateway_api::VenueId;
use venue_strategies::hedged_grid::{
    GridOrderIntent, GridPlannerConfig, GridPosition, GridRollingAnchor,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyGridConfig {
    pub planner: GridPlannerConfig,
    /// Hyperliquid requires an explicit direction; the other venues use both Hedge legs.
    pub net_direction: Option<GridPosition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyGridRecord {
    pub instance_id: String,
    pub owner_user_id: String,
    pub credential_id: String,
    pub trading_account_id: String,
    pub venue: VenueId,
    pub symbol: Symbol,
    pub config: StrategyGridConfig,
    pub lifecycle: String,
    pub revision: u64,
    pub plan_sequence: u64,
    pub rolling_anchor: Option<GridRollingAnchor>,
    pub blocked_reason: Option<String>,
    pub convergence_pending_since_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub last_failed_strategy_sequence: u64,
}

#[derive(Clone)]
pub(crate) struct GridOrderRow {
    pub command: ExecutionCommand,
    pub native_id: Option<String>,
    pub intent: GridOrderIntent,
    pub observed_filled: Decimal,
    pub ledger_state: String,
}

#[derive(Clone)]
pub struct StrategyGridStore {
    pub(crate) pool: PgPool,
}

impl StrategyGridStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        owner: &str,
        credential: &str,
        config: StrategyGridConfig,
        now: u64,
    ) -> Result<String, Error> {
        let id = &config.planner.instance_id;
        CommandId::new(id).map_err(|_| Error::Invalid)?;
        config.planner.validate().map_err(|_| Error::Invalid)?;
        let max_levels = if config.net_direction.is_some() { 8 } else { 4 };
        if config.planner.revision != 1
            || config.planner.grid_count > max_levels
            || config.planner.maximum_grid_notional.value < config.planner.order_notional.value
        {
            return Err(Error::Invalid);
        }
        let row=sqlx::query("SELECT trading_account_id,venue,strategy_limits FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL")
            .bind(credential).bind(owner).fetch_one(&self.pool).await.map_err(|_| Error::Conflict)?;
        let account: String = row
            .try_get("trading_account_id")
            .map_err(|_| Error::Conflict)?;
        let venue: String = row.try_get("venue").map_err(|_| Error::Conflict)?;
        if (venue == "hyperliquid") != config.net_direction.is_some() {
            return Err(Error::Invalid);
        }
        let raw: Option<serde_json::Value> = row
            .try_get("strategy_limits")
            .map_err(|_| Error::Conflict)?;
        let limits: crate::multi_venue_risk::StrategyRiskLimits =
            serde_json::from_value(raw.ok_or(Error::Conflict)?).map_err(|_| Error::Conflict)?;
        if !limits.validate()
            || config.planner.maximum_grid_notional.value > limits.max_symbol_notional
            || config.planner.order_notional.value > limits.max_order_notional
        {
            return Err(Error::Conflict);
        }
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &account).await?;
        let admitted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND trading_account_id=$3 AND venue=$4 AND deleted_ms IS NULL)",
        )
        .bind(credential)
        .bind(owner)
        .bind(&account)
        .bind(&venue)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        let grid_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_strategy_grids WHERE trading_account_id=$1",
        )
        .bind(&account)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        if !admitted || grid_count >= 20 {
            return Err(Error::Conflict);
        }
        sqlx::query("INSERT INTO venue_strategy_grids(instance_id,owner_user_id,trading_account_id,credential_id,venue,symbol,config,created_ms,updated_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$8)")
            .bind(id).bind(owner).bind(account).bind(credential).bind(venue).bind(config.planner.symbol.to_string())
            .bind(serde_json::to_value(&config).map_err(|_| Error::Invalid)?).bind(ms(now)?)
            .execute(&mut *tx).await.map_err(|_| Error::Conflict)?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(id.clone())
    }

    pub async fn get(&self, owner: &str, id: &str) -> Result<StrategyGridRecord, Error> {
        let row = sqlx::query(
            "SELECT * FROM venue_strategy_grids WHERE instance_id=$1 AND owner_user_id=$2",
        )
        .bind(id)
        .bind(owner)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| Error::Conflict)?;
        decode(row)
    }

    pub async fn lifecycle(
        &self,
        owner: &str,
        id: &str,
        action: &str,
        now: u64,
    ) -> Result<(), Error> {
        let record = self.get(owner, id).await?;
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        let current: String = sqlx::query_scalar(
            "SELECT lifecycle FROM venue_strategy_grids WHERE instance_id=$1 FOR UPDATE",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| Error::Unavailable)?;
        let next = match (action, current.as_str()) {
            ("start" | "resume", "paused" | "stopped") => "running",
            ("pause", "running" | "resetting") => "pausing",
            ("stop", _) => "stopping",
            ("reset", "running" | "paused") => "resetting",
            _ => return Err(Error::Conflict),
        };
        if matches!(action, "start" | "resume" | "reset") {
            let support_active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM venue_support_martingale_instances WHERE trading_account_id=$1 AND lifecycle IN ('running','entry_paused','increase_paused','draining'))",
            )
            .bind(&record.trading_account_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| Error::Unavailable)?;
            if support_active {
                return Err(Error::Conflict);
            }
        }
        // An unsent entry may be cancelled locally. Sent or uncertain commands keep their original
        // identity and finish reconciliation before any cancellation or new strategy planning.
        if next != "running" {
            sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1,updated_ms=$1 WHERE trading_account_id=$2 AND strategy_command->'payload'->'owner'->>'strategy_instance_id'=$3 AND command_state='pending' AND command_phase<>'cancel'")
                .bind(ms(now)?).bind(&record.trading_account_id).bind(id).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        }
        let clears_gate = matches!(action, "start" | "resume" | "reset");
        sqlx::query("UPDATE venue_strategy_grids SET lifecycle=$1,blocked_reason=NULL,convergence_pending_since_ms=CASE WHEN $2 THEN NULL ELSE convergence_pending_since_ms END,consecutive_failures=CASE WHEN $2 THEN 0 ELSE consecutive_failures END,last_failed_strategy_sequence=CASE WHEN $2 THEN COALESCE((SELECT MAX(c.strategy_sequence) FROM venue_binance_commands c WHERE c.trading_account_id=venue_strategy_grids.trading_account_id AND c.strategy_command->'payload'->'owner'->>'strategy_instance_id'=venue_strategy_grids.instance_id),last_failed_strategy_sequence) ELSE last_failed_strategy_sequence END,updated_ms=$3 WHERE instance_id=$4")
            .bind(next).bind(clears_gate).bind(ms(now)?).bind(id).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)
    }

    pub(crate) async fn active(&self, account: &str) -> Result<Vec<StrategyGridRecord>, Error> {
        sqlx::query("SELECT * FROM venue_strategy_grids WHERE trading_account_id=$1 AND lifecycle IN ('running','pausing','stopping','resetting') ORDER BY instance_id LIMIT 20")
            .bind(account).fetch_all(&self.pool).await.map_err(|_| Error::Unavailable)?.into_iter().map(decode).collect()
    }

    pub(crate) async fn orders(&self, id: &str) -> Result<Vec<GridOrderRow>, Error> {
        let rows=sqlx::query("SELECT o.intent,o.observed_filled::text AS observed_filled,c.strategy_command,c.native_order_id,c.command_state FROM venue_strategy_grid_orders o JOIN venue_binance_commands c USING(client_order_id) WHERE o.instance_id=$1 AND NOT o.terminal ORDER BY o.created_ms,o.client_order_id")
            .bind(id).fetch_all(&self.pool).await.map_err(|_| Error::Unavailable)?;
        rows.into_iter()
            .map(|r| {
                Ok(GridOrderRow {
                    command: serde_json::from_value(
                        r.try_get("strategy_command").map_err(|_| Error::Conflict)?,
                    )
                    .map_err(|_| Error::Conflict)?,
                    native_id: r.try_get("native_order_id").map_err(|_| Error::Conflict)?,
                    intent: serde_json::from_value(
                        r.try_get("intent").map_err(|_| Error::Conflict)?,
                    )
                    .map_err(|_| Error::Conflict)?,
                    observed_filled: Decimal::from_str(
                        r.try_get::<String, _>("observed_filled")
                            .map_err(|_| Error::Conflict)?
                            .as_str(),
                    )
                    .map_err(|_| Error::Conflict)?,
                    ledger_state: r.try_get("command_state").map_err(|_| Error::Conflict)?,
                })
            })
            .collect()
    }

    pub(crate) async fn note_failure(
        &self,
        record: &StrategyGridRecord,
        reason: &str,
        now: u64,
    ) -> Result<bool, Error> {
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        let current=sqlx::query("SELECT revision,plan_sequence,lifecycle,convergence_pending_since_ms,consecutive_failures FROM venue_strategy_grids WHERE instance_id=$1 FOR UPDATE")
            .bind(&record.instance_id).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if current.try_get::<i64, _>("revision").ok() != Some(ms(record.revision)?)
            || current.try_get::<i64, _>("plan_sequence").ok() != Some(ms(record.plan_sequence)?)
            || current.try_get::<String, _>("lifecycle").ok().as_deref()
                != Some(record.lifecycle.as_str())
        {
            return Err(Error::Conflict);
        }
        if !matches!(record.lifecycle.as_str(), "running" | "resetting") {
            sqlx::query("UPDATE venue_strategy_grids SET blocked_reason=$1,updated_ms=$2 WHERE instance_id=$3")
                .bind(reason).bind(ms(now)?).bind(&record.instance_id).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(false);
        }
        let transition = super::progress::advance(
            decode_progress(&current)?,
            super::progress::ProgressEvent::Failures(1),
            &record.config.planner.reset_policy,
            now,
        )
        .ok_or(Error::Invalid)?;
        let lifecycle = if transition.pause {
            "pausing"
        } else {
            record.lifecycle.as_str()
        };
        let blocked_reason = if transition.pause {
            if transition.timed_out {
                "convergence_timeout"
            } else {
                "convergence_failure_threshold"
            }
        } else {
            reason
        };
        sqlx::query("UPDATE venue_strategy_grids SET lifecycle=$1,convergence_pending_since_ms=$2,consecutive_failures=$3,blocked_reason=$4,updated_ms=$5 WHERE instance_id=$6")
            .bind(lifecycle).bind(transition.progress.pending_since_ms.map(ms).transpose()?)
            .bind(i32::try_from(transition.progress.consecutive_failures).map_err(|_| Error::Invalid)?)
            .bind(blocked_reason).bind(ms(now)?).bind(&record.instance_id)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if transition.pause {
            cancel_pending_non_cancel(
                &mut tx,
                &record.trading_account_id,
                &record.instance_id,
                now,
            )
            .await?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(transition.pause)
    }

    pub(crate) async fn note_account_pending(&self, account: &str, now: u64) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, account).await?;
        let rows=sqlx::query("SELECT g.instance_id,g.config,g.convergence_pending_since_ms,g.consecutive_failures FROM venue_strategy_grids g WHERE g.trading_account_id=$1 AND g.lifecycle IN ('running','resetting') AND EXISTS(SELECT 1 FROM venue_binance_commands c WHERE c.trading_account_id=g.trading_account_id AND c.command_state IN ('pending','sending','accepted','reconcile_required') AND c.strategy_command->'payload'->'owner'->>'strategy_instance_id'=g.instance_id) ORDER BY g.instance_id FOR UPDATE")
            .bind(account).fetch_all(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        for row in rows {
            let config: StrategyGridConfig =
                serde_json::from_value(row.try_get("config").map_err(|_| Error::Conflict)?)
                    .map_err(|_| Error::Conflict)?;
            let transition = super::progress::advance(
                decode_progress(&row)?,
                super::progress::ProgressEvent::Pending,
                &config.planner.reset_policy,
                now,
            )
            .ok_or(Error::Invalid)?;
            let id: String = row.try_get("instance_id").map_err(|_| Error::Conflict)?;
            sqlx::query("UPDATE venue_strategy_grids SET lifecycle=CASE WHEN $1 THEN 'pausing' ELSE lifecycle END,convergence_pending_since_ms=$2,consecutive_failures=$3,blocked_reason=CASE WHEN $1 THEN 'convergence_timeout' ELSE blocked_reason END,updated_ms=$4 WHERE instance_id=$5")
                .bind(transition.pause).bind(transition.progress.pending_since_ms.map(ms).transpose()?)
                .bind(i32::try_from(transition.progress.consecutive_failures).map_err(|_| Error::Invalid)?)
                .bind(ms(now)?).bind(&id).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            if transition.pause {
                cancel_pending_non_cancel(&mut tx, account, &id, now).await?;
            }
        }
        tx.commit().await.map_err(|_| Error::Unavailable)
    }

    pub(crate) async fn note_new_rejections(
        &self,
        record: &StrategyGridRecord,
        now: u64,
    ) -> Result<(bool, bool), Error> {
        if !matches!(record.lifecycle.as_str(), "running" | "resetting") {
            return Ok((false, false));
        }
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        let current=sqlx::query("SELECT revision,plan_sequence,lifecycle,convergence_pending_since_ms,consecutive_failures,last_failed_strategy_sequence FROM venue_strategy_grids WHERE instance_id=$1 FOR UPDATE")
            .bind(&record.instance_id).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if current.try_get::<i64, _>("revision").ok() != Some(ms(record.revision)?)
            || current.try_get::<i64, _>("plan_sequence").ok() != Some(ms(record.plan_sequence)?)
            || current.try_get::<String, _>("lifecycle").ok().as_deref()
                != Some(record.lifecycle.as_str())
        {
            return Err(Error::Conflict);
        }
        let last = current
            .try_get::<i64, _>("last_failed_strategy_sequence")
            .map_err(|_| Error::Conflict)?;
        let failures=sqlx::query("SELECT COALESCE(MAX(strategy_sequence),0) AS last_sequence,count(*) AS failure_count FROM venue_binance_commands WHERE trading_account_id=$1 AND strategy_command->'payload'->'owner'->>'strategy_instance_id'=$2 AND command_state='rejected' AND strategy_sequence>$3")
            .bind(&record.trading_account_id).bind(&record.instance_id).bind(last)
            .fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        let count = failures
            .try_get::<i64, _>("failure_count")
            .map_err(|_| Error::Conflict)?;
        if count == 0 {
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok((false, false));
        }
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        let transition = super::progress::advance(
            decode_progress(&current)?,
            super::progress::ProgressEvent::Failures(count),
            &record.config.planner.reset_policy,
            now,
        )
        .ok_or(Error::Invalid)?;
        let last_sequence = failures
            .try_get::<i64, _>("last_sequence")
            .map_err(|_| Error::Conflict)?;
        sqlx::query("UPDATE venue_strategy_grids SET lifecycle=CASE WHEN $1 THEN 'pausing' ELSE lifecycle END,convergence_pending_since_ms=$2,consecutive_failures=$3,last_failed_strategy_sequence=$4,blocked_reason=CASE WHEN $1 THEN CASE WHEN $5 THEN 'convergence_timeout' ELSE 'convergence_failure_threshold' END ELSE 'strategy_command_rejected' END,updated_ms=$6 WHERE instance_id=$7")
            .bind(transition.pause).bind(transition.progress.pending_since_ms.map(ms).transpose()?)
            .bind(i32::try_from(transition.progress.consecutive_failures).map_err(|_| Error::Invalid)?)
            .bind(last_sequence).bind(transition.timed_out).bind(ms(now)?).bind(&record.instance_id)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if transition.pause {
            cancel_pending_non_cancel(
                &mut tx,
                &record.trading_account_id,
                &record.instance_id,
                now,
            )
            .await?;
        }
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok((true, transition.pause))
    }

    pub(crate) async fn apply(
        &self,
        record: &StrategyGridRecord,
        plan: super::planner::GridWork,
        now: u64,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(|_| Error::Unavailable)?;
        lock_account(&mut tx, &record.trading_account_id).await?;
        let current=sqlx::query("SELECT revision,plan_sequence,lifecycle FROM venue_strategy_grids WHERE instance_id=$1 FOR UPDATE")
            .bind(&record.instance_id).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if current.try_get::<i64, _>("revision").ok() != Some(ms(record.revision)?)
            || current.try_get::<i64, _>("plan_sequence").ok() != Some(ms(record.plan_sequence)?)
            || current.try_get::<String, _>("lifecycle").ok().as_deref()
                != Some(record.lifecycle.as_str())
        {
            return Err(Error::Conflict);
        }
        let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(&record.trading_account_id).fetch_one(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        if pending {
            return Err(Error::Conflict);
        }
        let has_commands = !plan.commands.is_empty();
        let next_lifecycle = plan
            .lifecycle
            .as_deref()
            .unwrap_or(record.lifecycle.as_str())
            .to_owned();
        let stable = !has_commands && record.lifecycle == "running" && next_lifecycle == "running";
        // Only signed terminal observations let the planner finish resetting. Rebuilding
        // the new surface must not inherit the elapsed cancellation deadline.
        let reset_drained =
            !has_commands && record.lifecycle == "resetting" && next_lifecycle == "running";
        let progress = if stable
            || reset_drained
            || (has_commands && matches!(next_lifecycle.as_str(), "running" | "resetting"))
        {
            Some(
                super::progress::advance(
                    super::progress::ConvergenceProgress {
                        pending_since_ms: record.convergence_pending_since_ms,
                        consecutive_failures: record.consecutive_failures,
                    },
                    if reset_drained {
                        super::progress::ProgressEvent::ResetDrained
                    } else if stable {
                        super::progress::ProgressEvent::Converged
                    } else {
                        super::progress::ProgressEvent::Pending
                    },
                    &record.config.planner.reset_policy,
                    now,
                )
                .ok_or(Error::Invalid)?,
            )
        } else {
            None
        };
        if progress.is_some_and(|transition| transition.pause) {
            let transition = progress.ok_or(Error::Invalid)?;
            sqlx::query("UPDATE venue_strategy_grids SET lifecycle='pausing',convergence_pending_since_ms=$1,consecutive_failures=$2,blocked_reason=CASE WHEN $3 THEN 'convergence_timeout' ELSE 'convergence_failure_threshold' END,updated_ms=$4 WHERE instance_id=$5")
                .bind(transition.progress.pending_since_ms.map(ms).transpose()?)
                .bind(i32::try_from(transition.progress.consecutive_failures).map_err(|_| Error::Invalid)?)
                .bind(transition.timed_out).bind(ms(now)?).bind(&record.instance_id)
                .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            tx.commit().await.map_err(|_| Error::Unavailable)?;
            return Ok(());
        }
        let commands = MultiVenueStore::new(self.pool.clone());
        for (command, intent) in plan.commands {
            let client = command.native_client_id().map(|v| v.as_str().to_owned());
            commands
                .enqueue_in(
                    &mut tx,
                    &record.owner_user_id,
                    &record.credential_id,
                    command,
                    now,
                )
                .await?;
            if let (Some(client), Some(intent)) = (client, intent) {
                sqlx::query("INSERT INTO venue_strategy_grid_orders(client_order_id,instance_id,revision,intent,created_ms) VALUES($1,$2,$3,$4,$5)")
                    .bind(client).bind(&record.instance_id).bind(ms(record.revision)?).bind(serde_json::to_value(intent).map_err(|_| Error::Invalid)?).bind(ms(now)?)
                    .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
            }
        }
        for (client, filled, terminal) in plan.observations {
            sqlx::query("UPDATE venue_strategy_grid_orders SET observed_filled=$1::text::numeric,terminal=$2 WHERE client_order_id=$3 AND instance_id=$4 AND observed_filled<=$1::text::numeric")
                .bind(filled.normalize().to_string()).bind(terminal).bind(client).bind(&record.instance_id).execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        }
        let revision = record
            .revision
            .checked_add(u64::from(plan.new_revision))
            .ok_or(Error::Invalid)?;
        let mut config = record.config.clone();
        config.planner.revision = revision;
        let pending_since = progress
            .map(|value| value.progress.pending_since_ms)
            .unwrap_or(record.convergence_pending_since_ms)
            .map(ms)
            .transpose()?;
        let failures = progress
            .map(|value| value.progress.consecutive_failures)
            .unwrap_or(record.consecutive_failures);
        let blocked_reason = if matches!(
            next_lifecycle.as_str(),
            "pausing" | "paused" | "stopping" | "stopped"
        ) {
            record.blocked_reason.clone()
        } else {
            None
        };
        sqlx::query("UPDATE venue_strategy_grids SET lifecycle=$1,revision=$2,plan_sequence=plan_sequence+1,rolling_anchor=$3,desired_orders=$4,config=$5,blocked_reason=$6,convergence_pending_since_ms=$7,consecutive_failures=$8,updated_ms=$9 WHERE instance_id=$10")
            .bind(next_lifecycle).bind(ms(revision)?)
            .bind(plan.anchor.map(serde_json::to_value).transpose().map_err(|_| Error::Invalid)?)
            .bind(serde_json::to_value(plan.desired).map_err(|_| Error::Invalid)?).bind(serde_json::to_value(config).map_err(|_| Error::Invalid)?)
            .bind(blocked_reason).bind(pending_since).bind(i32::try_from(failures).map_err(|_| Error::Invalid)?)
            .bind(ms(now)?).bind(&record.instance_id)
            .execute(&mut *tx).await.map_err(|_| Error::Unavailable)?;
        tx.commit().await.map_err(|_| Error::Unavailable)
    }
}

pub(crate) async fn lock_account(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: &str,
) -> Result<(), Error> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || $1,0))",
    )
    .bind(account)
    .execute(&mut **tx)
    .await
    .map_err(|_| Error::Unavailable)?;
    Ok(())
}

async fn cancel_pending_non_cancel(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account: &str,
    instance_id: &str,
    now: u64,
) -> Result<(), Error> {
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1,updated_ms=$1 WHERE trading_account_id=$2 AND strategy_command->'payload'->'owner'->>'strategy_instance_id'=$3 AND command_state='pending' AND command_phase<>'cancel'")
        .bind(ms(now)?).bind(account).bind(instance_id).execute(&mut **tx).await.map_err(|_| Error::Unavailable)?;
    Ok(())
}

fn decode_progress(
    r: &sqlx::postgres::PgRow,
) -> Result<super::progress::ConvergenceProgress, Error> {
    let pending_since_ms = r
        .try_get::<Option<i64>, _>("convergence_pending_since_ms")
        .map_err(|_| Error::Conflict)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| Error::Conflict)?;
    let consecutive_failures = r
        .try_get::<i32, _>("consecutive_failures")
        .ok()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(Error::Conflict)?;
    Ok(super::progress::ConvergenceProgress {
        pending_since_ms,
        consecutive_failures,
    })
}

fn ms(v: u64) -> Result<i64, Error> {
    i64::try_from(v).map_err(|_| Error::Invalid)
}
fn decode(r: sqlx::postgres::PgRow) -> Result<StrategyGridRecord, Error> {
    Ok(StrategyGridRecord {
        instance_id: r.try_get("instance_id").map_err(|_| Error::Conflict)?,
        owner_user_id: r.try_get("owner_user_id").map_err(|_| Error::Conflict)?,
        credential_id: r.try_get("credential_id").map_err(|_| Error::Conflict)?,
        trading_account_id: r
            .try_get("trading_account_id")
            .map_err(|_| Error::Conflict)?,
        venue: r
            .try_get::<String, _>("venue")
            .map_err(|_| Error::Conflict)?
            .parse()
            .map_err(|_| Error::Conflict)?,
        symbol: r
            .try_get::<String, _>("symbol")
            .map_err(|_| Error::Conflict)?
            .parse()
            .map_err(|_| Error::Conflict)?,
        config: serde_json::from_value(r.try_get("config").map_err(|_| Error::Conflict)?)
            .map_err(|_| Error::Conflict)?,
        lifecycle: r.try_get("lifecycle").map_err(|_| Error::Conflict)?,
        revision: r
            .try_get::<i64, _>("revision")
            .ok()
            .and_then(|v| u64::try_from(v).ok())
            .ok_or(Error::Conflict)?,
        plan_sequence: r
            .try_get::<i64, _>("plan_sequence")
            .ok()
            .and_then(|v| u64::try_from(v).ok())
            .ok_or(Error::Conflict)?,
        rolling_anchor: r
            .try_get::<Option<serde_json::Value>, _>("rolling_anchor")
            .map_err(|_| Error::Conflict)?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| Error::Conflict)?,
        blocked_reason: r.try_get("blocked_reason").map_err(|_| Error::Conflict)?,
        convergence_pending_since_ms: r
            .try_get::<Option<i64>, _>("convergence_pending_since_ms")
            .map_err(|_| Error::Conflict)?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| Error::Conflict)?,
        consecutive_failures: r
            .try_get::<i32, _>("consecutive_failures")
            .ok()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(Error::Conflict)?,
        last_failed_strategy_sequence: r
            .try_get::<i64, _>("last_failed_strategy_sequence")
            .ok()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(Error::Conflict)?,
    })
}
