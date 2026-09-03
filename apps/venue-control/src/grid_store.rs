//! PostgreSQL boundary for Binance Grid configuration, convergence and owned-order facts.
//!
//! Planning remains pure and execution remains in the singleton Binance Executor. This store does
//! not grant dispatch authority and deliberately has no local journal, Actor or writer lease.

use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use venue_control_protocol::grid::{
    GRID_SCHEMA_VERSION, GridAnchor, GridConfig, GridConfigUpdateRequest,
    GridInstanceCreateRequest, GridInstanceState, GridInstanceSummary, GridLifecycleAction,
    GridLifecycleRequest, GridOrderRole, GridOrderSemanticKey, MAX_GRID_CONSECUTIVE_FAILURES,
};
use venue_control_protocol::kol::{ExecutorCommandPhase, ExecutorCommandState, ExecutorOrderKind};
use venue_domain::{OrderSide, PositionSide, Symbol, is_canonical_trading_account_id};

use crate::{
    executor_store::{account_queue_has_capacity, lock_account_command_queue},
    kol_executor::BinanceCommandLedgerError,
};

#[path = "grid_store/types.rs"]
mod types;
pub use types::*;
#[path = "grid_store/hot_batch.rs"]
mod hot_batch;
#[path = "grid_store/reads.rs"]
mod reads;
pub use hot_batch::*;
#[path = "grid_store/surface.rs"]
mod surface;
#[cfg(test)]
#[path = "grid_store/tests.rs"]
mod tests;

pub const MIGRATION_0021: &str = include_str!("../migrations/0021_binance_grid.sql");

#[derive(Clone)]
pub struct BinanceGridStore {
    pool: PgPool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GridStoreError {
    #[error("grid input is invalid")]
    Invalid,
    #[error("grid owner or verified credential was rejected")]
    Forbidden,
    #[error("grid revision or durable identity conflicts")]
    Conflict,
    #[error("grid storage is unavailable")]
    Unavailable,
    #[error("grid durable state is corrupt")]
    Corrupt,
}

impl BinanceGridStore {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_instance(
        &self,
        owner_user_id: &str,
        trading_account_id: &str,
        instance_id: &str,
        request: &GridInstanceCreateRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        request.validate().map_err(|_| GridStoreError::Invalid)?;
        validate_ids(&[owner_user_id, trading_account_id, instance_id])?;
        if now_ms == 0 {
            return Err(GridStoreError::Invalid);
        }
        let request_digest = digest(request)?;
        let config_json = serde_json::to_value(&request.config).map_err(invalid_json)?;
        let config_digest = digest(&request.config)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let inserted = sqlx::query(
            "INSERT INTO venue_binance_grid_instances \
             (instance_id,owner_user_id,trading_account_id,credential_id,create_request_id,\
              create_request_digest,symbol,instance_state,revision,current_config_revision,\
              plan_revision,desired_digest,dirty,convergence_started_ms,consecutive_failures,\
              last_facts_ms,attention_code,created_ms,updated_ms) \
             SELECT $1,$2,$3,c.credential_id,$4,$5,$6,'draft',1,1,1,NULL,FALSE,NULL,0,NULL,NULL,$7,$7 \
             FROM venue_api_credentials c \
             WHERE c.credential_id=$8 AND c.user_id=$2 AND c.trading_account_id=$3 \
               AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' \
             ON CONFLICT DO NOTHING",
        )
        .bind(instance_id)
        .bind(owner_user_id)
        .bind(trading_account_id)
        .bind(&request.request_id)
        .bind(request_digest.as_slice())
        .bind(request.symbol.to_string())
        .bind(ms(now_ms)?)
        .bind(&request.credential_id)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if inserted.rows_affected() == 0 {
            let durable: Option<Vec<u8>> = sqlx::query_scalar(
                "SELECT create_request_digest FROM venue_binance_grid_instances \
                 WHERE owner_user_id=$1 AND create_request_id=$2 FOR SHARE",
            )
            .bind(owner_user_id)
            .bind(&request.request_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(database_error)?;
            if durable.as_deref() != Some(request_digest.as_slice()) {
                let instance_exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM venue_binance_grid_instances WHERE instance_id=$1)",
                )
                .bind(instance_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(database_error)?;
                return Err(if instance_exists || durable.is_some() {
                    GridStoreError::Conflict
                } else {
                    GridStoreError::Forbidden
                });
            }
        } else {
            sqlx::query(
                "INSERT INTO venue_binance_grid_config_revisions \
                 (instance_id,config_revision,request_id,config_json,config_digest,created_ms) \
                 VALUES ($1,1,$2,$3,$4,$5)",
            )
            .bind(instance_id)
            .bind(&request.request_id)
            .bind(config_json)
            .bind(config_digest.as_slice())
            .bind(ms(now_ms)?)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(owner_user_id, instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn load_owned(
        &self,
        owner_user_id: &str,
        instance_id: &str,
    ) -> Result<Option<GridInstanceSummary>, GridStoreError> {
        validate_ids(&[owner_user_id, instance_id])?;
        let statement = summary_select("WHERE i.instance_id=$1 AND i.owner_user_id=$2");
        let row = sqlx::query(&statement)
            .bind(instance_id)
            .bind(owner_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(database_error)?;
        row.as_ref().map(decode_summary).transpose()
    }

    pub async fn list_owned(
        &self,
        owner_user_id: &str,
    ) -> Result<Vec<GridInstanceSummary>, GridStoreError> {
        validate_ids(&[owner_user_id])?;
        let statement =
            summary_select("WHERE i.owner_user_id=$1 ORDER BY i.created_ms,i.instance_id");
        let rows = sqlx::query(&statement)
            .bind(owner_user_id)
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        rows.iter().map(decode_summary).collect()
    }

    pub async fn list_runtime_instances(&self) -> Result<Vec<GridRuntimeRecord>, GridStoreError> {
        let statement = summary_select(
            "WHERE i.instance_state IN ('start_pending','running','paused','stop_pending',\
             'blocked','reset_required','needs_attention') ORDER BY i.updated_ms,i.instance_id",
        );
        let rows = sqlx::query(&statement)
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        rows.iter()
            .map(|row| {
                Ok(GridRuntimeRecord {
                    owner_user_id: row.try_get("owner_user_id").map_err(corrupt_row)?,
                    instance: decode_summary(row)?,
                    tail_batch_id: row.try_get("grid_tail_batch_id").map_err(corrupt_row)?,
                })
            })
            .collect()
    }

    pub async fn update_config(
        &self,
        owner_user_id: &str,
        request: &GridConfigUpdateRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        request.validate().map_err(|_| GridStoreError::Invalid)?;
        validate_ids(&[owner_user_id])?;
        if now_ms == 0 {
            return Err(GridStoreError::Invalid);
        }
        let config_json = serde_json::to_value(&request.config).map_err(invalid_json)?;
        let config_digest = digest(&request.config)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let replay: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT r.config_digest FROM venue_binance_grid_config_revisions r \
             JOIN venue_binance_grid_instances i ON i.instance_id=r.instance_id \
             WHERE r.instance_id=$1 AND r.request_id=$2 AND i.owner_user_id=$3 FOR SHARE",
        )
        .bind(&request.instance_id)
        .bind(&request.request_id)
        .bind(owner_user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if let Some(replay) = replay {
            if replay.as_slice() != config_digest.as_slice() {
                return Err(GridStoreError::Conflict);
            }
            tx.commit().await.map_err(database_error)?;
            return self
                .load_owned(owner_user_id, &request.instance_id)
                .await?
                .ok_or(GridStoreError::Corrupt);
        }
        let row = sqlx::query(
            "SELECT revision,current_config_revision,instance_state FROM venue_binance_grid_instances \
             WHERE instance_id=$1 AND owner_user_id=$2 FOR UPDATE",
        )
        .bind(&request.instance_id)
        .bind(owner_user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let revision = unsigned(row.try_get("revision").map_err(corrupt_row)?)?;
        let config_revision = unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let state = decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?;
        if revision != request.expected_revision
            || matches!(
                state,
                GridInstanceState::StartPending | GridInstanceState::StopPending
            )
        {
            return Err(GridStoreError::Conflict);
        }
        let next_config = config_revision
            .checked_add(1)
            .ok_or(GridStoreError::Conflict)?;
        let next_revision = revision.checked_add(1).ok_or(GridStoreError::Conflict)?;
        cancel_pending_risk_commands(&mut tx, &request.instance_id, now_ms).await?;
        let inserted = sqlx::query(
            "INSERT INTO venue_binance_grid_config_revisions \
             (instance_id,config_revision,request_id,config_json,config_digest,created_ms) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&request.instance_id)
        .bind(integer(next_config)?)
        .bind(&request.request_id)
        .bind(config_json)
        .bind(config_digest.as_slice())
        .bind(ms(now_ms)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
            .bind(&request.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
            .bind(&request.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        let is_operational = matches!(
            state,
            GridInstanceState::Running
                | GridInstanceState::Blocked
                | GridInstanceState::ResetRequired
                | GridInstanceState::NeedsAttention
        );
        let updated = sqlx::query(
            "UPDATE venue_binance_grid_instances SET current_config_revision=$1,revision=$2,\
             desired_digest=NULL,dirty=$3,convergence_started_ms=$4,consecutive_failures=0,\
             updated_ms=$5 WHERE instance_id=$6 AND revision=$7",
        )
        .bind(integer(next_config)?)
        .bind(integer(next_revision)?)
        .bind(is_operational)
        .bind(if is_operational {
            Some(ms(now_ms)?)
        } else {
            None
        })
        .bind(ms(now_ms)?)
        .bind(&request.instance_id)
        .bind(integer(revision)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if inserted.rows_affected() != 1 || updated.rows_affected() != 1 {
            return Err(GridStoreError::Conflict);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(owner_user_id, &request.instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn request_lifecycle(
        &self,
        owner_user_id: &str,
        request: &GridLifecycleRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        request.validate().map_err(|_| GridStoreError::Invalid)?;
        validate_ids(&[owner_user_id])?;
        if now_ms == 0 {
            return Err(GridStoreError::Invalid);
        }
        let request_digest = digest(request)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let replay: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT request_digest FROM venue_binance_grid_lifecycle_requests \
             WHERE owner_user_id=$1 AND request_id=$2 FOR SHARE",
        )
        .bind(owner_user_id)
        .bind(&request.request_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if let Some(replay) = replay {
            if replay.as_slice() != request_digest.as_slice() {
                return Err(GridStoreError::Conflict);
            }
            tx.commit().await.map_err(database_error)?;
            return self
                .load_owned(owner_user_id, &request.instance_id)
                .await?
                .ok_or(GridStoreError::Corrupt);
        }
        let row = sqlx::query(
            "SELECT revision,current_config_revision,instance_state,trading_account_id \
             FROM venue_binance_grid_instances \
             WHERE instance_id=$1 AND owner_user_id=$2 FOR UPDATE",
        )
        .bind(&request.instance_id)
        .bind(owner_user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let revision = unsigned(row.try_get("revision").map_err(corrupt_row)?)?;
        let current_config_revision = unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let state = decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?;
        let trading_account_id: String = row.try_get("trading_account_id").map_err(corrupt_row)?;
        if revision != request.expected_revision {
            return Err(GridStoreError::Conflict);
        }
        let (next_state, dirty, attention) = lifecycle_transition(state, request.action)?;
        if matches!(
            request.action,
            GridLifecycleAction::Start | GridLifecycleAction::Resume
        ) {
            let legacy_owned: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes \
                 WHERE venue='binance' AND mode='LIVE' AND trading_account_id=$1)",
            )
            .bind(&trading_account_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(database_error)?;
            if legacy_owned {
                return Err(GridStoreError::Conflict);
            }
        }
        if matches!(
            request.action,
            GridLifecycleAction::Pause | GridLifecycleAction::Stop | GridLifecycleAction::Reset
        ) {
            cancel_pending_risk_commands(&mut tx, &request.instance_id, now_ms).await?;
        }
        let next_revision = revision.checked_add(1).ok_or(GridStoreError::Conflict)?;
        let next_config_revision = if request.action == GridLifecycleAction::Reset {
            let synthetic_request_id = synthetic_config_request_id(
                "manual-reset",
                &request.instance_id,
                &request.request_id,
                current_config_revision,
            );
            clone_config_revision(
                &mut tx,
                &request.instance_id,
                current_config_revision,
                &synthetic_request_id,
                now_ms,
            )
            .await?
        } else {
            current_config_revision
        };
        let empty_digest = empty_desired_digest();
        let desired_override = match request.action {
            GridLifecycleAction::Pause | GridLifecycleAction::Stop | GridLifecycleAction::Reset => {
                Some(empty_digest.as_slice())
            }
            GridLifecycleAction::Start | GridLifecycleAction::Resume => None,
        };
        sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
            .bind(&request.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        if matches!(
            request.action,
            GridLifecycleAction::Pause | GridLifecycleAction::Reset
        ) {
            sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
                .bind(&request.instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        let reset_failures = !dirty
            || matches!(
                request.action,
                GridLifecycleAction::Start | GridLifecycleAction::Reset
            );
        sqlx::query(
            "UPDATE venue_binance_grid_instances SET instance_state=$1,revision=$2,\
             current_config_revision=$3,dirty=$4,\
             desired_digest=$8,\
             convergence_started_ms=$5,\
             consecutive_failures=CASE WHEN $9 THEN 0 ELSE consecutive_failures END,\
             attention_code=$6,updated_ms=$7 WHERE instance_id=$10 AND revision=$11",
        )
        .bind(state_name(next_state))
        .bind(integer(next_revision)?)
        .bind(integer(next_config_revision)?)
        .bind(dirty)
        .bind(if dirty { Some(ms(now_ms)?) } else { None })
        .bind(attention)
        .bind(ms(now_ms)?)
        .bind(desired_override)
        .bind(reset_failures)
        .bind(&request.instance_id)
        .bind(integer(revision)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        sqlx::query(
            "INSERT INTO venue_binance_grid_lifecycle_requests \
             (owner_user_id,request_id,instance_id,action,request_digest,resulting_revision,created_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(owner_user_id)
        .bind(&request.request_id)
        .bind(&request.instance_id)
        .bind(action_name(request.action))
        .bind(request_digest.as_slice())
        .bind(integer(next_revision)?)
        .bind(ms(now_ms)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.load_owned(owner_user_id, &request.instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn update_convergence(
        &self,
        update: &GridConvergenceUpdate,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        validate_ids(&[&update.instance_id])?;
        if update.expected_instance_revision == 0
            || !convergence_state_allows_update(update.expected_state)
            || (update.expected_state == GridInstanceState::Paused && update.dirty)
            || update.expected_plan_revision == 0
            || update.next_plan_revision != update.expected_plan_revision
            || update.last_facts_ms == 0
            || update.last_facts_ms > now_ms
            || update.consecutive_failures > MAX_GRID_CONSECUTIVE_FAILURES
            || (!update.dirty && update.consecutive_failures != 0)
        {
            return Err(GridStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query(
            "SELECT i.owner_user_id,i.revision,i.current_config_revision,i.plan_revision,\
             i.instance_state,i.attention_code,i.convergence_started_ms,i.desired_digest,\
             c.config_json \
             FROM venue_binance_grid_instances i \
             JOIN venue_binance_grid_config_revisions c ON c.instance_id=i.instance_id \
              AND c.config_revision=i.current_config_revision \
             WHERE i.instance_id=$1 FOR UPDATE",
        )
        .bind(&update.instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let owner_user_id: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
        let instance_revision = unsigned(row.try_get("revision").map_err(corrupt_row)?)?;
        let config_revision = unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let plan_revision = unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)?;
        let state = decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?;
        let durable_desired = row
            .try_get::<Option<Vec<u8>>, _>("desired_digest")
            .map_err(corrupt_row)?;
        if !convergence_cas_matches(update, instance_revision, state)
            || plan_revision != update.expected_plan_revision
            || durable_desired.as_deref() != Some(update.desired_digest.as_slice())
        {
            return Err(GridStoreError::Conflict);
        }
        let config: GridConfig =
            serde_json::from_value(row.try_get("config_json").map_err(corrupt_row)?)
                .map_err(|_| GridStoreError::Corrupt)?;
        config.validate().map_err(|_| GridStoreError::Corrupt)?;
        let prior_started = row
            .try_get::<Option<i64>, _>("convergence_started_ms")
            .map_err(corrupt_row)?
            .map(unsigned)
            .transpose()?;
        let started = if update.dirty {
            if update.next_plan_revision != update.expected_plan_revision {
                now_ms
            } else {
                prior_started.unwrap_or(now_ms)
            }
        } else {
            0
        };
        let timeout = update.dirty
            && now_ms.saturating_sub(started) > config.reset_policy.convergence_timeout_ms;
        let failures = update.dirty
            && update.consecutive_failures >= config.reset_policy.max_consecutive_failures;
        let reset_required = timeout || failures;
        let entering_reset = reset_required && state != GridInstanceState::ResetRequired;
        let next_state = if reset_required {
            GridInstanceState::ResetRequired
        } else if !update.dirty
            && matches!(
                state,
                GridInstanceState::StartPending | GridInstanceState::Blocked
            )
        {
            GridInstanceState::Running
        } else {
            state
        };
        let attention: Option<String> = if reset_required {
            Some(
                if failures {
                    "consecutive_failures"
                } else {
                    "convergence_timeout"
                }
                .to_owned(),
            )
        } else if matches!(
            next_state,
            GridInstanceState::Blocked
                | GridInstanceState::ResetRequired
                | GridInstanceState::NeedsAttention
        ) {
            row.try_get::<Option<String>, _>("attention_code")
                .map_err(corrupt_row)?
        } else {
            None
        };
        if entering_reset {
            cancel_pending_risk_commands(&mut tx, &update.instance_id, now_ms).await?;
        }
        let next_config_revision = if entering_reset {
            let trigger = attention.as_deref().ok_or(GridStoreError::Corrupt)?;
            let synthetic_request_id = synthetic_config_request_id(
                "automatic-reset",
                &update.instance_id,
                trigger,
                config_revision,
            );
            clone_config_revision(
                &mut tx,
                &update.instance_id,
                config_revision,
                &synthetic_request_id,
                now_ms,
            )
            .await?
        } else {
            config_revision
        };
        let persisted_desired = if reset_required {
            sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
                .bind(&update.instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
            sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
                .bind(&update.instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
            empty_desired_digest()
        } else {
            update.desired_digest
        };
        let updated = sqlx::query(
            "UPDATE venue_binance_grid_instances SET plan_revision=$1,current_config_revision=$2,\
             desired_digest=$3,dirty=$4,convergence_started_ms=$5,consecutive_failures=$6,\
             last_facts_ms=$7,instance_state=$8,attention_code=$9,revision=revision+1,\
             updated_ms=$10 WHERE instance_id=$11 AND revision=$12 AND plan_revision=$13 \
              AND instance_state=$14",
        )
        .bind(integer(update.next_plan_revision)?)
        .bind(integer(next_config_revision)?)
        .bind(persisted_desired.as_slice())
        .bind(update.dirty)
        .bind(if update.dirty {
            Some(ms(started)?)
        } else {
            None
        })
        .bind(
            i16::try_from(if reset_required {
                0
            } else {
                update.consecutive_failures
            })
            .map_err(|_| GridStoreError::Invalid)?,
        )
        .bind(ms(update.last_facts_ms)?)
        .bind(state_name(next_state))
        .bind(attention)
        .bind(ms(now_ms)?)
        .bind(&update.instance_id)
        .bind(integer(update.expected_instance_revision)?)
        .bind(integer(update.expected_plan_revision)?)
        .bind(state_name(update.expected_state))
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(GridStoreError::Conflict);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(&owner_user_id, &update.instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn settle_runtime_state(
        &self,
        instance_id: &str,
        expected: GridInstanceState,
        next: GridInstanceState,
        attention_code: Option<&str>,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, GridStoreError> {
        validate_ids(&[instance_id])?;
        if now_ms == 0
            || !runtime_transition_allowed(expected, next)
            || attention_code_valid(next, attention_code).is_err()
        {
            return Err(GridStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let locked = sqlx::query(
            "SELECT owner_user_id,revision,current_config_revision,instance_state \
             FROM venue_binance_grid_instances WHERE instance_id=$1 FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Forbidden)?;
        let owner: String = locked.try_get("owner_user_id").map_err(corrupt_row)?;
        let revision = unsigned(locked.try_get("revision").map_err(corrupt_row)?)?;
        let current_config_revision = unsigned(
            locked
                .try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?;
        let durable_state = decode_state(locked.try_get("instance_state").map_err(corrupt_row)?)?;
        if durable_state != expected {
            return Err(GridStoreError::Conflict);
        }
        let next_config_revision = if next == GridInstanceState::ResetRequired {
            let trigger = attention_code.ok_or(GridStoreError::Invalid)?;
            let synthetic_request_id = synthetic_config_request_id(
                "runtime-reset",
                instance_id,
                trigger,
                current_config_revision,
            );
            clone_config_revision(
                &mut tx,
                instance_id,
                current_config_revision,
                &synthetic_request_id,
                now_ms,
            )
            .await?
        } else {
            current_config_revision
        };
        let clears_desired = matches!(
            next,
            GridInstanceState::ResetRequired | GridInstanceState::Stopped
        );
        if next == GridInstanceState::ResetRequired {
            cancel_pending_risk_commands(&mut tx, instance_id, now_ms).await?;
        }
        if clears_desired {
            sqlx::query("DELETE FROM venue_binance_grid_desired_orders WHERE instance_id=$1")
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        if next == GridInstanceState::ResetRequired {
            sqlx::query("DELETE FROM venue_binance_grid_anchors WHERE instance_id=$1")
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        let row = sqlx::query(
            "UPDATE venue_binance_grid_instances SET instance_state=$1,attention_code=$2,\
             desired_digest=CASE WHEN $3 THEN $4 ELSE desired_digest END,\
             dirty=CASE WHEN $1='stopped' THEN FALSE WHEN $1='reset_required' THEN TRUE ELSE dirty END,\
             convergence_started_ms=CASE WHEN $1='stopped' THEN NULL \
              WHEN $1='reset_required' THEN COALESCE(convergence_started_ms,$5) ELSE convergence_started_ms END,\
             consecutive_failures=CASE WHEN $1 IN ('stopped','reset_required') \
              THEN 0 ELSE consecutive_failures END,current_config_revision=$6,\
             revision=revision+1,updated_ms=$5 WHERE instance_id=$7 AND revision=$8 \
              AND current_config_revision=$9 AND instance_state=$10 RETURNING owner_user_id",
        )
        .bind(state_name(next))
        .bind(attention_code)
        .bind(clears_desired)
        .bind(empty_desired_digest().as_slice())
        .bind(ms(now_ms)?)
        .bind(integer(next_config_revision)?)
        .bind(instance_id)
        .bind(integer(revision)?)
        .bind(integer(current_config_revision)?)
        .bind(state_name(expected))
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Conflict)?;
        let returned_owner: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
        if returned_owner != owner {
            return Err(GridStoreError::Corrupt);
        }
        tx.commit().await.map_err(database_error)?;
        self.load_owned(&owner, instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)
    }

    pub async fn enqueue_command(
        &self,
        command: &GridLedgerCommand,
        now_ms: u64,
    ) -> Result<GridLedgerCommandRecord, GridStoreError> {
        validate_command(command, now_ms)?;
        if !bounded(&command.command_id, 1, 64) {
            return Err(GridStoreError::Invalid);
        }
        let (phase, kind, position_side, order_side, quantity, limit_price, target) =
            command_columns(&command.intent)?;
        let allowed_states = if target.is_some() {
            "('running','paused','stop_pending','blocked','reset_required','needs_attention')"
        } else {
            "('running')"
        };
        let query = format!(
            "INSERT INTO venue_binance_commands \
             (command_id,command_origin,request_id,relation_id,relation_revision,target_revision,\
              owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,\
              order_kind,order_side,requested_quantity,limit_price,rule_version,client_order_id,\
              command_state,source_digest,created_ms,updated_ms,grid_instance_id,\
              grid_config_revision,grid_plan_revision,grid_semantic_key,selected_native_order_id,\
               target_client_order_id,grid_batch_id,dispatch_sequence) \
             SELECT $1,'grid',NULL,NULL,NULL,NULL,i.owner_user_id,i.trading_account_id,\
              i.credential_id,i.symbol,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$11,$11,\
               i.instance_id,$12,$13,$14,(SELECT owned.native_order_id \
                FROM venue_binance_grid_order_owners owned \
                WHERE owned.instance_id=i.instance_id \
                  AND owned.trading_account_id=i.trading_account_id \
                   AND owned.client_order_id=$15),$15,$1,1 \
             FROM venue_binance_grid_instances i \
             WHERE i.instance_id=$16 AND i.current_config_revision=$12 \
               AND i.plan_revision=$13 AND i.instance_state IN {allowed_states} \
               AND ($15::text IS NULL OR EXISTS(SELECT 1 \
                 FROM venue_binance_grid_order_owners owned \
                 WHERE owned.instance_id=i.instance_id \
                   AND owned.trading_account_id=i.trading_account_id \
                   AND owned.client_order_id=$15 AND owned.native_order_id IS NOT NULL)) \
               AND NOT EXISTS (SELECT 1 FROM venue_control_strategy_scopes legacy \
                 WHERE legacy.venue='binance' AND legacy.mode='LIVE' \
                   AND legacy.trading_account_id=i.trading_account_id) \
             ON CONFLICT DO NOTHING"
        );
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let instance_row = sqlx::query(
            "SELECT owner_user_id,trading_account_id,credential_id,revision \
             FROM venue_binance_grid_instances WHERE instance_id=$1 \
              AND current_config_revision=$2 AND plan_revision=$3 FOR UPDATE",
        )
        .bind(&command.instance_id)
        .bind(integer(command.config_revision)?)
        .bind(integer(command.plan_revision)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Conflict)?;
        let expected_instance_revision: i64 =
            instance_row.try_get("revision").map_err(corrupt_row)?;
        let owner_user_id: String = instance_row.try_get("owner_user_id").map_err(corrupt_row)?;
        let trading_account_id: String = instance_row
            .try_get("trading_account_id")
            .map_err(corrupt_row)?;
        let credential_id: String = instance_row.try_get("credential_id").map_err(corrupt_row)?;
        let existing_receipt = sqlx::query(
            "SELECT instance_id,config_revision,plan_revision,desired_digest,batch_digest,\
              command_count FROM venue_binance_grid_mutation_batches WHERE batch_id=$1 FOR SHARE",
        )
        .bind(&command.command_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        let receipt_existed = existing_receipt.is_some();
        if !receipt_existed {
            let queue_depth = lock_account_command_queue(
                &mut *tx,
                &owner_user_id,
                &trading_account_id,
                &credential_id,
            )
            .await
            .map_err(grid_command_admission_error)?;
            if !account_queue_has_capacity(queue_depth, 1) {
                return Err(GridStoreError::Conflict);
            }
            sqlx::query(
                "INSERT INTO venue_binance_grid_mutation_batches \
                 (batch_id,instance_id,expected_instance_revision,config_revision,plan_revision,\
                   desired_digest,batch_digest,command_count,created_ms) \
                  VALUES ($1,$2,$3,$4,$5,$6,$6,1,$7) ON CONFLICT DO NOTHING",
            )
            .bind(&command.command_id)
            .bind(&command.instance_id)
            .bind(expected_instance_revision)
            .bind(integer(command.config_revision)?)
            .bind(integer(command.plan_revision)?)
            .bind(command.source_digest.as_slice())
            .bind(ms(now_ms)?)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        }
        let receipt = match existing_receipt {
            Some(receipt) => receipt,
            None => sqlx::query(
                "SELECT instance_id,config_revision,plan_revision,desired_digest,batch_digest,\
                 command_count FROM venue_binance_grid_mutation_batches \
                 WHERE batch_id=$1 FOR SHARE",
            )
            .bind(&command.command_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(database_error)?,
        };
        if receipt
            .try_get::<String, _>("instance_id")
            .map_err(corrupt_row)?
            != command.instance_id
            || unsigned(receipt.try_get("config_revision").map_err(corrupt_row)?)?
                != command.config_revision
            || unsigned(receipt.try_get("plan_revision").map_err(corrupt_row)?)?
                != command.plan_revision
            || bytes_digest(receipt.try_get("desired_digest").map_err(corrupt_row)?)?
                != command.source_digest
            || bytes_digest(receipt.try_get("batch_digest").map_err(corrupt_row)?)?
                != command.source_digest
            || receipt
                .try_get::<i16, _>("command_count")
                .map_err(corrupt_row)?
                != 1
        {
            return Err(GridStoreError::Conflict);
        }
        if receipt_existed {
            let row = load_enqueued_grid_command(&mut tx, command)
                .await?
                .ok_or(GridStoreError::Conflict)?;
            let record = verify_enqueued_grid_command(&row, command)?;
            tx.commit().await.map_err(database_error)?;
            return Ok(record);
        }
        if let GridCommandIntent::LimitPostOnly {
            key,
            quantity,
            limit_price,
        } = &command.intent
        {
            let desired = sqlx::query_scalar::<_, i32>(
                "SELECT 1 FROM venue_binance_grid_instances i \
                 JOIN venue_binance_grid_desired_orders d ON d.instance_id=i.instance_id \
                 WHERE i.instance_id=$1 AND i.current_config_revision=$2 AND i.plan_revision=$3 \
                   AND d.config_revision=$2 AND d.plan_revision=$3 AND d.semantic_key=$4 \
                   AND d.client_order_id=$5 AND d.position_side=$6 AND d.order_role=$7 \
                   AND d.grid_level=$8 AND d.order_sequence=$9 AND d.quantity=$10 \
                   AND d.limit_price=$11 AND d.desired_digest=$12 FOR SHARE OF i,d",
            )
            .bind(&command.instance_id)
            .bind(integer(command.config_revision)?)
            .bind(integer(command.plan_revision)?)
            .bind(&command.semantic_key)
            .bind(&command.client_order_id)
            .bind(position_side_name(key.position_side))
            .bind(role_name(key.role))
            .bind(i16::try_from(key.level).map_err(|_| GridStoreError::Invalid)?)
            .bind(integer(key.sequence)?)
            .bind(decimal_text(*quantity))
            .bind(decimal_text(*limit_price))
            .bind(command.source_digest.as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(database_error)?;
            if desired.is_none() {
                return Err(GridStoreError::Conflict);
            }
        }
        sqlx::query(&query)
            .bind(&command.command_id)
            .bind(position_side)
            .bind(phase)
            .bind(kind)
            .bind(order_side)
            .bind(quantity)
            .bind(limit_price)
            .bind(&command.rule_version)
            .bind(&command.client_order_id)
            .bind(command.source_digest.as_slice())
            .bind(ms(now_ms)?)
            .bind(integer(command.config_revision)?)
            .bind(integer(command.plan_revision)?)
            .bind(&command.semantic_key)
            .bind(target)
            .bind(&command.instance_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        let row = load_enqueued_grid_command(&mut tx, command)
            .await?
            .ok_or(GridStoreError::Conflict)?;
        let record = verify_enqueued_grid_command(&row, command)?;
        tx.commit().await.map_err(database_error)?;
        Ok(record)
    }

    pub async fn record_order_ownership(
        &self,
        ownership: &GridOrderOwnership,
    ) -> Result<(), GridStoreError> {
        validate_ownership(ownership)?;
        let ownership_digest = ownership_identity_digest(ownership)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        let existing = sqlx::query(
            "SELECT ownership_digest,filled_quantity,native_order_id,first_seen_ms,last_seen_ms \
             FROM venue_binance_grid_order_owners WHERE trading_account_id=$1 \
             AND client_order_id=$2 FOR UPDATE",
        )
        .bind(&ownership.trading_account_id)
        .bind(&ownership.client_order_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(database_error)?;
        if let Some(row) = existing {
            let durable_digest: Vec<u8> = row.try_get("ownership_digest").map_err(corrupt_row)?;
            let filled = decimal(row.try_get("filled_quantity").map_err(corrupt_row)?)?;
            let native: Option<String> = row.try_get("native_order_id").map_err(corrupt_row)?;
            let first_seen = unsigned(row.try_get("first_seen_ms").map_err(corrupt_row)?)?;
            let last_seen = unsigned(row.try_get("last_seen_ms").map_err(corrupt_row)?)?;
            if durable_digest.as_slice() != ownership_digest.as_slice()
                || ownership.filled_quantity < filled
                || ownership.first_seen_ms != first_seen
                || ownership.last_seen_ms < last_seen
                || native
                    .as_deref()
                    .is_some_and(|value| ownership.native_order_id.as_deref() != Some(value))
            {
                return Err(GridStoreError::Conflict);
            }
            sqlx::query(
                "UPDATE venue_binance_grid_order_owners SET filled_quantity=$1,\
                 native_order_id=COALESCE(native_order_id,$2),order_state=$3,last_seen_ms=$4 \
                 WHERE trading_account_id=$5 AND client_order_id=$6",
            )
            .bind(decimal_text(ownership.filled_quantity))
            .bind(&ownership.native_order_id)
            .bind(owned_state_name(ownership.state))
            .bind(ms(ownership.last_seen_ms)?)
            .bind(&ownership.trading_account_id)
            .bind(&ownership.client_order_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        } else {
            sqlx::query(
                "INSERT INTO venue_binance_grid_order_owners \
                 (trading_account_id,client_order_id,instance_id,config_revision,plan_revision,\
                  semantic_key,place_command_id,symbol,position_side,order_role,grid_level,\
                  order_sequence,order_side,quantity,filled_quantity,limit_price,native_order_id,\
                  ownership_source,order_state,ownership_digest,first_seen_ms,last_seen_ms) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,\
                         'executor',$18,$19,$20,$21)",
            )
            .bind(&ownership.trading_account_id)
            .bind(&ownership.client_order_id)
            .bind(&ownership.instance_id)
            .bind(integer(ownership.config_revision)?)
            .bind(integer(ownership.plan_revision)?)
            .bind(ownership.key.encoded())
            .bind(&ownership.place_command_id)
            .bind(ownership.symbol.to_string())
            .bind(position_side_name(ownership.key.position_side))
            .bind(role_name(ownership.key.role))
            .bind(i16::try_from(ownership.key.level).map_err(|_| GridStoreError::Invalid)?)
            .bind(integer(ownership.key.sequence)?)
            .bind(order_side_name(ownership.key.order_side()))
            .bind(decimal_text(ownership.quantity))
            .bind(decimal_text(ownership.filled_quantity))
            .bind(decimal_text(ownership.limit_price))
            .bind(&ownership.native_order_id)
            .bind(owned_state_name(ownership.state))
            .bind(ownership_digest.as_slice())
            .bind(ms(ownership.first_seen_ms)?)
            .bind(ms(ownership.last_seen_ms)?)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        }
        tx.commit().await.map_err(database_error)?;
        Ok(())
    }

    pub async fn load_owned_orders(
        &self,
        instance_id: &str,
    ) -> Result<Vec<GridOrderOwnership>, GridStoreError> {
        validate_ids(&[instance_id])?;
        let rows = sqlx::query(
            "SELECT instance_id,trading_account_id,config_revision,plan_revision,place_command_id,\
             client_order_id,symbol,position_side,order_role,grid_level,order_sequence,quantity,\
             filled_quantity,limit_price,native_order_id,order_state,first_seen_ms,last_seen_ms \
             FROM venue_binance_grid_order_owners WHERE instance_id=$1 \
             ORDER BY first_seen_ms,client_order_id",
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.iter().map(decode_ownership).collect()
    }

    pub async fn record_fill_allocation(
        &self,
        fill: &GridFillAllocation,
    ) -> Result<bool, GridStoreError> {
        validate_fill(fill)?;
        let allocation_digest = digest(fill)?;
        let inserted = sqlx::query(
            "INSERT INTO venue_binance_grid_fill_allocations \
             (trading_account_id,symbol,native_trade_id,instance_id,config_revision,\
              client_order_id,position_side,order_role,quantity,price,maker,occurred_ms,\
              observed_ms,allocation_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,\
              $12,$13,$14) ON CONFLICT DO NOTHING",
        )
        .bind(&fill.trading_account_id)
        .bind(fill.symbol.to_string())
        .bind(&fill.native_trade_id)
        .bind(&fill.instance_id)
        .bind(integer(fill.config_revision)?)
        .bind(&fill.client_order_id)
        .bind(position_side_name(fill.position_side))
        .bind(role_name(fill.role))
        .bind(decimal_text(fill.quantity))
        .bind(decimal_text(fill.price))
        .bind(fill.maker)
        .bind(fill.occurred_ms.map(ms).transpose()?)
        .bind(ms(fill.observed_ms)?)
        .bind(allocation_digest.as_slice())
        .execute(&self.pool)
        .await
        .map_err(database_error)?;
        if inserted.rows_affected() == 0 {
            let durable: Option<Vec<u8>> = sqlx::query_scalar(
                "SELECT allocation_digest FROM venue_binance_grid_fill_allocations \
                 WHERE trading_account_id=$1 AND symbol=$2 AND native_trade_id=$3",
            )
            .bind(&fill.trading_account_id)
            .bind(fill.symbol.to_string())
            .bind(&fill.native_trade_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(database_error)?;
            if durable.as_deref() != Some(allocation_digest.as_slice()) {
                return Err(GridStoreError::Conflict);
            }
        }
        Ok(inserted.rows_affected() == 1)
    }
}

async fn load_enqueued_grid_command(
    tx: &mut Transaction<'_, Postgres>,
    command: &GridLedgerCommand,
) -> Result<Option<sqlx::postgres::PgRow>, GridStoreError> {
    sqlx::query(
        "SELECT command_id,client_order_id,grid_instance_id,grid_config_revision,\
         grid_plan_revision,grid_semantic_key,owner_user_id,trading_account_id,credential_id,\
         symbol,source_digest FROM venue_binance_commands WHERE grid_instance_id=$1 \
         AND grid_config_revision=$2 AND grid_plan_revision=$3 AND grid_semantic_key=$4 \
         AND command_origin='grid' FOR SHARE",
    )
    .bind(&command.instance_id)
    .bind(integer(command.config_revision)?)
    .bind(integer(command.plan_revision)?)
    .bind(&command.semantic_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)
}

fn verify_enqueued_grid_command(
    row: &sqlx::postgres::PgRow,
    command: &GridLedgerCommand,
) -> Result<GridLedgerCommandRecord, GridStoreError> {
    let durable_digest: Vec<u8> = row.try_get("source_digest").map_err(corrupt_row)?;
    if durable_digest.as_slice() != command.source_digest.as_slice()
        || row
            .try_get::<String, _>("command_id")
            .map_err(corrupt_row)?
            != command.command_id
        || row
            .try_get::<String, _>("client_order_id")
            .map_err(corrupt_row)?
            != command.client_order_id
    {
        return Err(GridStoreError::Conflict);
    }
    decode_command_record(row)
}

fn grid_command_admission_error(error: BinanceCommandLedgerError) -> GridStoreError {
    match error {
        BinanceCommandLedgerError::Conflict => GridStoreError::Conflict,
        BinanceCommandLedgerError::Unavailable => GridStoreError::Unavailable,
    }
}

fn summary_select(suffix: &str) -> String {
    format!(
        "SELECT i.owner_user_id,i.instance_id,i.credential_id,i.trading_account_id,i.symbol,i.instance_state,\
         i.revision,i.current_config_revision,i.plan_revision,i.desired_digest,i.dirty,\
         i.convergence_started_ms,i.consecutive_failures,i.last_facts_ms,i.attention_code,\
         i.grid_tail_batch_id,\
         i.created_ms,i.updated_ms,c.config_json,a.anchor_revision,a.instrument_generation,\
         a.anchor_price,a.price_step,a.grid_quantity,a.source_native_trade_id,\
         a.observed_ms AS anchor_observed_ms \
         FROM venue_binance_grid_instances i \
         JOIN venue_binance_grid_config_revisions c ON c.instance_id=i.instance_id \
          AND c.config_revision=i.current_config_revision \
         LEFT JOIN venue_binance_grid_anchors a ON a.instance_id=i.instance_id \
          AND a.config_revision=i.current_config_revision {suffix}"
    )
}

async fn cancel_pending_risk_commands(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: &str,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    sqlx::query(
        "WITH cancelled AS (UPDATE venue_binance_commands SET command_state='cancelled',\
         sanitized_error_code='lifecycle_fenced',terminal_ms=$1,updated_ms=$1 \
         WHERE command_origin='grid' AND grid_instance_id=$2 AND command_state='pending' \
           AND command_phase<>'cancel' RETURNING command_id) \
         UPDATE venue_binance_grid_order_owners owner SET order_state='terminal',\
         last_seen_ms=GREATEST(owner.last_seen_ms,$1) FROM cancelled \
         WHERE owner.place_command_id=cancelled.command_id AND owner.native_order_id IS NULL \
           AND owner.order_state='working'",
    )
    .bind(ms(now_ms)?)
    .bind(instance_id)
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn clone_config_revision(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: &str,
    current_config_revision: u64,
    synthetic_request_id: &str,
    now_ms: u64,
) -> Result<u64, GridStoreError> {
    validate_ids(&[instance_id, synthetic_request_id])?;
    let next_config_revision = current_config_revision
        .checked_add(1)
        .ok_or(GridStoreError::Conflict)?;
    let inserted = sqlx::query(
        "INSERT INTO venue_binance_grid_config_revisions \
         (instance_id,config_revision,request_id,config_json,config_digest,created_ms) \
         SELECT instance_id,$1,$2,config_json,config_digest,$3 \
         FROM venue_binance_grid_config_revisions \
         WHERE instance_id=$4 AND config_revision=$5",
    )
    .bind(integer(next_config_revision)?)
    .bind(synthetic_request_id)
    .bind(ms(now_ms)?)
    .bind(instance_id)
    .bind(integer(current_config_revision)?)
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    if inserted.rows_affected() != 1 {
        return Err(GridStoreError::Corrupt);
    }
    Ok(next_config_revision)
}

fn synthetic_config_request_id(
    scope: &str,
    instance_id: &str,
    trigger: &str,
    config_revision: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"venue-grid-config-clone-v1");
    hasher.update([0]);
    hasher.update(scope.as_bytes());
    hasher.update([0]);
    hasher.update(instance_id.as_bytes());
    hasher.update([0]);
    hasher.update(trigger.as_bytes());
    hasher.update([0]);
    hasher.update(config_revision.to_be_bytes());
    let encoded = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!(
        "{}-{}-{}-{}-{}",
        &encoded[0..8],
        &encoded[8..12],
        &encoded[12..16],
        &encoded[16..20],
        &encoded[20..32]
    )
}

fn decode_summary(row: &sqlx::postgres::PgRow) -> Result<GridInstanceSummary, GridStoreError> {
    let config: GridConfig =
        serde_json::from_value(row.try_get("config_json").map_err(corrupt_row)?)
            .map_err(|_| GridStoreError::Corrupt)?;
    let anchor_revision: Option<i64> = row.try_get("anchor_revision").map_err(corrupt_row)?;
    let anchor = anchor_revision
        .map(|revision| {
            Ok(GridAnchor {
                revision: unsigned(revision)?,
                instrument_generation: unsigned(
                    row.try_get::<Option<i64>, _>("instrument_generation")
                        .map_err(corrupt_row)?
                        .ok_or(GridStoreError::Corrupt)?,
                )?,
                price: decimal(
                    row.try_get::<Option<String>, _>("anchor_price")
                        .map_err(corrupt_row)?
                        .ok_or(GridStoreError::Corrupt)?,
                )?,
                price_step: decimal(
                    row.try_get::<Option<String>, _>("price_step")
                        .map_err(corrupt_row)?
                        .ok_or(GridStoreError::Corrupt)?,
                )?,
                grid_quantity: decimal(
                    row.try_get::<Option<String>, _>("grid_quantity")
                        .map_err(corrupt_row)?
                        .ok_or(GridStoreError::Corrupt)?,
                )?,
                source_native_trade_id: row
                    .try_get("source_native_trade_id")
                    .map_err(corrupt_row)?,
                observed_ms: unsigned(
                    row.try_get::<Option<i64>, _>("anchor_observed_ms")
                        .map_err(corrupt_row)?
                        .ok_or(GridStoreError::Corrupt)?,
                )?,
            })
        })
        .transpose()?;
    let desired: Option<Vec<u8>> = row.try_get("desired_digest").map_err(corrupt_row)?;
    let summary = GridInstanceSummary {
        schema_version: GRID_SCHEMA_VERSION,
        instance_id: row.try_get("instance_id").map_err(corrupt_row)?,
        credential_id: row.try_get("credential_id").map_err(corrupt_row)?,
        trading_account_id: row.try_get("trading_account_id").map_err(corrupt_row)?,
        symbol: row
            .try_get::<String, _>("symbol")
            .map_err(corrupt_row)?
            .parse()
            .map_err(|_| GridStoreError::Corrupt)?,
        state: decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?,
        revision: unsigned(row.try_get("revision").map_err(corrupt_row)?)?,
        config_revision: unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )?,
        plan_revision: unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)?,
        config,
        anchor,
        desired_digest: desired.map(hex_digest).transpose()?,
        dirty: row.try_get("dirty").map_err(corrupt_row)?,
        convergence_started_ms: row
            .try_get::<Option<i64>, _>("convergence_started_ms")
            .map_err(corrupt_row)?
            .map(unsigned)
            .transpose()?,
        consecutive_failures: u16::try_from(
            row.try_get::<i16, _>("consecutive_failures")
                .map_err(corrupt_row)?,
        )
        .map_err(|_| GridStoreError::Corrupt)?,
        last_facts_ms: row
            .try_get::<Option<i64>, _>("last_facts_ms")
            .map_err(corrupt_row)?
            .map(unsigned)
            .transpose()?,
        attention_code: row.try_get("attention_code").map_err(corrupt_row)?,
        created_ms: unsigned(row.try_get("created_ms").map_err(corrupt_row)?)?,
        updated_ms: unsigned(row.try_get("updated_ms").map_err(corrupt_row)?)?,
    };
    summary.validate().map_err(|_| GridStoreError::Corrupt)?;
    Ok(summary)
}

fn lifecycle_transition(
    state: GridInstanceState,
    action: GridLifecycleAction,
) -> Result<(GridInstanceState, bool, Option<&'static str>), GridStoreError> {
    match (state, action) {
        (GridInstanceState::Draft | GridInstanceState::Stopped, GridLifecycleAction::Start)
        | (GridInstanceState::Paused, GridLifecycleAction::Resume) => {
            Ok((GridInstanceState::StartPending, true, None))
        }
        (
            GridInstanceState::Running
            | GridInstanceState::StartPending
            | GridInstanceState::Blocked
            | GridInstanceState::ResetRequired
            | GridInstanceState::NeedsAttention,
            GridLifecycleAction::Pause,
        ) => Ok((GridInstanceState::Paused, true, None)),
        (GridInstanceState::Draft, GridLifecycleAction::Stop) => {
            Ok((GridInstanceState::Stopped, false, None))
        }
        (
            GridInstanceState::Running
            | GridInstanceState::StartPending
            | GridInstanceState::Paused
            | GridInstanceState::Blocked
            | GridInstanceState::ResetRequired
            | GridInstanceState::NeedsAttention,
            GridLifecycleAction::Stop,
        ) => Ok((GridInstanceState::StopPending, true, None)),
        (
            GridInstanceState::Running
            | GridInstanceState::Paused
            | GridInstanceState::Blocked
            | GridInstanceState::NeedsAttention,
            GridLifecycleAction::Reset,
        ) => Ok((
            GridInstanceState::ResetRequired,
            true,
            Some("manual_reset_requested"),
        )),
        _ => Err(GridStoreError::Conflict),
    }
}

fn runtime_transition_allowed(from: GridInstanceState, to: GridInstanceState) -> bool {
    matches!(
        (from, to),
        (GridInstanceState::StartPending, GridInstanceState::Running)
            | (GridInstanceState::StartPending, GridInstanceState::Blocked)
            | (
                GridInstanceState::StartPending,
                GridInstanceState::ResetRequired | GridInstanceState::NeedsAttention
            )
            | (GridInstanceState::Running, GridInstanceState::Blocked)
            | (
                GridInstanceState::Running,
                GridInstanceState::ResetRequired | GridInstanceState::NeedsAttention
            )
            | (GridInstanceState::Blocked, GridInstanceState::Running)
            | (
                GridInstanceState::Blocked,
                GridInstanceState::ResetRequired | GridInstanceState::NeedsAttention
            )
            | (
                GridInstanceState::ResetRequired,
                GridInstanceState::Running
                    | GridInstanceState::Blocked
                    | GridInstanceState::NeedsAttention
            )
            | (
                GridInstanceState::StopPending,
                GridInstanceState::Stopped | GridInstanceState::NeedsAttention
            )
            | (GridInstanceState::Paused, GridInstanceState::NeedsAttention)
    )
}

const fn convergence_state_allows_update(state: GridInstanceState) -> bool {
    matches!(
        state,
        GridInstanceState::StartPending
            | GridInstanceState::Running
            | GridInstanceState::Paused
            | GridInstanceState::Blocked
    )
}

fn convergence_cas_matches(
    update: &GridConvergenceUpdate,
    instance_revision: u64,
    state: GridInstanceState,
) -> bool {
    convergence_state_allows_update(state)
        && update.expected_instance_revision == instance_revision
        && update.expected_state == state
}

fn attention_code_valid(
    state: GridInstanceState,
    attention_code: Option<&str>,
) -> Result<(), GridStoreError> {
    let required = matches!(
        state,
        GridInstanceState::Blocked
            | GridInstanceState::ResetRequired
            | GridInstanceState::NeedsAttention
    );
    if required != attention_code.is_some()
        || attention_code.is_some_and(|value| !bounded(value, 1, 64))
    {
        return Err(GridStoreError::Invalid);
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn command_columns(
    intent: &GridCommandIntent,
) -> Result<
    (
        &'static str,
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        Option<String>,
        Option<String>,
        Option<&str>,
    ),
    GridStoreError,
> {
    match intent {
        GridCommandIntent::LimitPostOnly {
            key,
            quantity,
            limit_price,
        } => Ok((
            role_name(key.role),
            "limit_post_only",
            Some(position_side_name(key.position_side)),
            Some(order_side_name(key.order_side())),
            Some(decimal_text(*quantity)),
            Some(decimal_text(*limit_price)),
            None,
        )),
        GridCommandIntent::Market {
            position_side,
            role,
            quantity,
        } => Ok((
            role_name(*role),
            "market",
            Some(position_side_name(*position_side)),
            Some(order_side_name(side_for(*position_side, *role)?)),
            Some(decimal_text(*quantity)),
            None,
            None,
        )),
        GridCommandIntent::Cancel {
            target_client_order_id,
        } => Ok((
            "cancel",
            "cancel_exact",
            None,
            None,
            None,
            None,
            Some(target_client_order_id),
        )),
    }
}

fn validate_command(command: &GridLedgerCommand, now_ms: u64) -> Result<(), GridStoreError> {
    validate_ids(&[&command.instance_id])?;
    if now_ms == 0
        || command.config_revision == 0
        || command.plan_revision == 0
        || !bounded(&command.command_id, 1, 128)
        || !bounded(&command.client_order_id, 1, 36)
        || !bounded(&command.semantic_key, 1, 160)
        || !bounded(&command.rule_version, 1, 128)
    {
        return Err(GridStoreError::Invalid);
    }
    match &command.intent {
        GridCommandIntent::LimitPostOnly {
            key,
            quantity,
            limit_price,
        } => {
            key.validate().map_err(|_| GridStoreError::Invalid)?;
            if command.semantic_key != key.encoded()
                || !positive(*quantity)
                || !positive(*limit_price)
            {
                return Err(GridStoreError::Invalid);
            }
        }
        GridCommandIntent::Market {
            position_side,
            quantity,
            ..
        } => {
            if *position_side == PositionSide::Net || !positive(*quantity) {
                return Err(GridStoreError::Invalid);
            }
        }
        GridCommandIntent::Cancel {
            target_client_order_id,
        } => {
            if !bounded(target_client_order_id, 1, 36)
                || target_client_order_id == &command.client_order_id
            {
                return Err(GridStoreError::Invalid);
            }
        }
    }
    Ok(())
}

fn validate_desired_orders(orders: &[GridDesiredOrder]) -> Result<(), GridStoreError> {
    if orders.len() > MAX_GRID_DESIRED_ORDERS {
        return Err(GridStoreError::Invalid);
    }
    let mut semantic_keys = BTreeSet::new();
    let mut client_order_ids = BTreeSet::new();
    for order in orders {
        order.key.validate().map_err(|_| GridStoreError::Invalid)?;
        if !bounded(&order.client_order_id, 1, 36)
            || !positive(order.quantity)
            || !positive(order.limit_price)
            || !semantic_keys.insert(order.key.encoded())
            || !client_order_ids.insert(&order.client_order_id)
        {
            return Err(GridStoreError::Invalid);
        }
    }
    Ok(())
}

fn validate_ownership(ownership: &GridOrderOwnership) -> Result<(), GridStoreError> {
    validate_ids(&[&ownership.instance_id, &ownership.trading_account_id])?;
    ownership
        .key
        .validate()
        .map_err(|_| GridStoreError::Invalid)?;
    if ownership.config_revision == 0
        || ownership.plan_revision == 0
        || !bounded(&ownership.place_command_id, 1, 128)
        || !bounded(&ownership.client_order_id, 1, 36)
        || !positive(ownership.quantity)
        || ownership.filled_quantity < Decimal::ZERO
        || ownership.filled_quantity > ownership.quantity
        || !positive(ownership.limit_price)
        || ownership
            .native_order_id
            .as_deref()
            .is_some_and(|value| !bounded(value, 1, 128))
        || ownership.first_seen_ms == 0
        || ownership.last_seen_ms < ownership.first_seen_ms
    {
        return Err(GridStoreError::Invalid);
    }
    Ok(())
}

fn validate_fill(fill: &GridFillAllocation) -> Result<(), GridStoreError> {
    validate_ids(&[&fill.instance_id, &fill.trading_account_id])?;
    if fill.position_side == PositionSide::Net
        || fill.config_revision == 0
        || !bounded(&fill.client_order_id, 1, 36)
        || !bounded(&fill.native_trade_id, 1, 128)
        || !positive(fill.quantity)
        || !positive(fill.price)
        || fill.observed_ms == 0
        || fill
            .occurred_ms
            .is_some_and(|value| value > fill.observed_ms)
    {
        return Err(GridStoreError::Invalid);
    }
    Ok(())
}

#[derive(Serialize)]
struct OwnershipIdentity<'a> {
    instance_id: &'a str,
    trading_account_id: &'a str,
    config_revision: u64,
    plan_revision: u64,
    key: &'a GridOrderSemanticKey,
    place_command_id: &'a str,
    client_order_id: &'a str,
    symbol: &'a Symbol,
    quantity: String,
    limit_price: String,
}

fn ownership_identity_digest(ownership: &GridOrderOwnership) -> Result<[u8; 32], GridStoreError> {
    digest(&OwnershipIdentity {
        instance_id: &ownership.instance_id,
        trading_account_id: &ownership.trading_account_id,
        config_revision: ownership.config_revision,
        plan_revision: ownership.plan_revision,
        key: &ownership.key,
        place_command_id: &ownership.place_command_id,
        client_order_id: &ownership.client_order_id,
        symbol: &ownership.symbol,
        quantity: decimal_text(ownership.quantity),
        limit_price: decimal_text(ownership.limit_price),
    })
}

fn decode_command_record(
    row: &sqlx::postgres::PgRow,
) -> Result<GridLedgerCommandRecord, GridStoreError> {
    Ok(GridLedgerCommandRecord {
        command_id: row.try_get("command_id").map_err(corrupt_row)?,
        client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
        instance_id: row.try_get("grid_instance_id").map_err(corrupt_row)?,
        config_revision: unsigned(row.try_get("grid_config_revision").map_err(corrupt_row)?)?,
        plan_revision: unsigned(row.try_get("grid_plan_revision").map_err(corrupt_row)?)?,
        semantic_key: row.try_get("grid_semantic_key").map_err(corrupt_row)?,
        owner_user_id: row.try_get("owner_user_id").map_err(corrupt_row)?,
        trading_account_id: row.try_get("trading_account_id").map_err(corrupt_row)?,
        credential_id: row.try_get("credential_id").map_err(corrupt_row)?,
        symbol: row
            .try_get::<String, _>("symbol")
            .map_err(corrupt_row)?
            .parse()
            .map_err(|_| GridStoreError::Corrupt)?,
    })
}

fn decode_grid_command_status(
    row: &sqlx::postgres::PgRow,
) -> Result<GridCommandStatus, GridStoreError> {
    Ok(GridCommandStatus {
        command_id: row.try_get("command_id").map_err(corrupt_row)?,
        client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
        semantic_key: row.try_get("grid_semantic_key").map_err(corrupt_row)?,
        phase: decode_command_phase(row.try_get("command_phase").map_err(corrupt_row)?)?,
        order_kind: decode_order_kind(row.try_get("order_kind").map_err(corrupt_row)?)?,
        state: decode_command_state(row.try_get("command_state").map_err(corrupt_row)?)?,
        native_order_id: row.try_get("native_order_id").map_err(corrupt_row)?,
        selected_native_order_id: row
            .try_get("selected_native_order_id")
            .map_err(corrupt_row)?,
        target_client_order_id: row.try_get("target_client_order_id").map_err(corrupt_row)?,
        sanitized_error_code: row.try_get("sanitized_error_code").map_err(corrupt_row)?,
        updated_ms: unsigned(row.try_get("updated_ms").map_err(corrupt_row)?)?,
    })
}

fn decode_ownership(row: &sqlx::postgres::PgRow) -> Result<GridOrderOwnership, GridStoreError> {
    let position_side = decode_position_side(row.try_get("position_side").map_err(corrupt_row)?)?;
    let role = decode_role(row.try_get("order_role").map_err(corrupt_row)?)?;
    let ownership = GridOrderOwnership {
        instance_id: row.try_get("instance_id").map_err(corrupt_row)?,
        trading_account_id: row.try_get("trading_account_id").map_err(corrupt_row)?,
        config_revision: unsigned(row.try_get("config_revision").map_err(corrupt_row)?)?,
        plan_revision: unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)?,
        key: GridOrderSemanticKey {
            position_side,
            role,
            level: u16::try_from(row.try_get::<i16, _>("grid_level").map_err(corrupt_row)?)
                .map_err(|_| GridStoreError::Corrupt)?,
            sequence: unsigned(row.try_get("order_sequence").map_err(corrupt_row)?)?,
        },
        place_command_id: row.try_get("place_command_id").map_err(corrupt_row)?,
        client_order_id: row.try_get("client_order_id").map_err(corrupt_row)?,
        symbol: row
            .try_get::<String, _>("symbol")
            .map_err(corrupt_row)?
            .parse()
            .map_err(|_| GridStoreError::Corrupt)?,
        quantity: decimal(row.try_get("quantity").map_err(corrupt_row)?)?,
        filled_quantity: decimal(row.try_get("filled_quantity").map_err(corrupt_row)?)?,
        limit_price: decimal(row.try_get("limit_price").map_err(corrupt_row)?)?,
        native_order_id: row.try_get("native_order_id").map_err(corrupt_row)?,
        state: decode_owned_state(row.try_get("order_state").map_err(corrupt_row)?)?,
        first_seen_ms: unsigned(row.try_get("first_seen_ms").map_err(corrupt_row)?)?,
        last_seen_ms: unsigned(row.try_get("last_seen_ms").map_err(corrupt_row)?)?,
    };
    validate_ownership(&ownership).map_err(|_| GridStoreError::Corrupt)?;
    Ok(ownership)
}

fn decode_state(value: String) -> Result<GridInstanceState, GridStoreError> {
    match value.as_str() {
        "draft" => Ok(GridInstanceState::Draft),
        "start_pending" => Ok(GridInstanceState::StartPending),
        "running" => Ok(GridInstanceState::Running),
        "paused" => Ok(GridInstanceState::Paused),
        "stop_pending" => Ok(GridInstanceState::StopPending),
        "stopped" => Ok(GridInstanceState::Stopped),
        "blocked" => Ok(GridInstanceState::Blocked),
        "reset_required" => Ok(GridInstanceState::ResetRequired),
        "needs_attention" => Ok(GridInstanceState::NeedsAttention),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_position_side(value: String) -> Result<PositionSide, GridStoreError> {
    match value.as_str() {
        "long" => Ok(PositionSide::Long),
        "short" => Ok(PositionSide::Short),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_role(value: String) -> Result<GridOrderRole, GridStoreError> {
    match value.as_str() {
        "open" => Ok(GridOrderRole::Open),
        "close" => Ok(GridOrderRole::Close),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_owned_state(value: String) -> Result<GridOwnedOrderState, GridStoreError> {
    match value.as_str() {
        "working" => Ok(GridOwnedOrderState::Working),
        "terminal" => Ok(GridOwnedOrderState::Terminal),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_command_phase(value: String) -> Result<ExecutorCommandPhase, GridStoreError> {
    match value.as_str() {
        "open" => Ok(ExecutorCommandPhase::Open),
        "close" => Ok(ExecutorCommandPhase::Close),
        "cancel" => Ok(ExecutorCommandPhase::Cancel),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_order_kind(value: String) -> Result<ExecutorOrderKind, GridStoreError> {
    match value.as_str() {
        "market" => Ok(ExecutorOrderKind::Market),
        "limit_post_only" => Ok(ExecutorOrderKind::LimitPostOnly),
        "cancel_exact" => Ok(ExecutorOrderKind::CancelExact),
        _ => Err(GridStoreError::Corrupt),
    }
}

fn decode_command_state(value: String) -> Result<ExecutorCommandState, GridStoreError> {
    match value.as_str() {
        "pending" => Ok(ExecutorCommandState::Pending),
        "sending" => Ok(ExecutorCommandState::Sending),
        "accepted" => Ok(ExecutorCommandState::Accepted),
        "rejected" => Ok(ExecutorCommandState::Rejected),
        "reconcile_required" => Ok(ExecutorCommandState::ReconcileRequired),
        "reconciled" => Ok(ExecutorCommandState::Reconciled),
        "cancelled" => Ok(ExecutorCommandState::Cancelled),
        _ => Err(GridStoreError::Corrupt),
    }
}

const fn state_name(value: GridInstanceState) -> &'static str {
    match value {
        GridInstanceState::Draft => "draft",
        GridInstanceState::StartPending => "start_pending",
        GridInstanceState::Running => "running",
        GridInstanceState::Paused => "paused",
        GridInstanceState::StopPending => "stop_pending",
        GridInstanceState::Stopped => "stopped",
        GridInstanceState::Blocked => "blocked",
        GridInstanceState::ResetRequired => "reset_required",
        GridInstanceState::NeedsAttention => "needs_attention",
    }
}

const fn action_name(value: GridLifecycleAction) -> &'static str {
    match value {
        GridLifecycleAction::Start => "start",
        GridLifecycleAction::Pause => "pause",
        GridLifecycleAction::Resume => "resume",
        GridLifecycleAction::Stop => "stop",
        GridLifecycleAction::Reset => "reset",
    }
}

const fn role_name(value: GridOrderRole) -> &'static str {
    match value {
        GridOrderRole::Open => "open",
        GridOrderRole::Close => "close",
    }
}

const fn owned_state_name(value: GridOwnedOrderState) -> &'static str {
    match value {
        GridOwnedOrderState::Working => "working",
        GridOwnedOrderState::Terminal => "terminal",
    }
}

const fn position_side_name(value: PositionSide) -> &'static str {
    match value {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => "net",
    }
}

const fn order_side_name(value: OrderSide) -> &'static str {
    match value {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn side_for(side: PositionSide, role: GridOrderRole) -> Result<OrderSide, GridStoreError> {
    match (side, role) {
        (PositionSide::Long, GridOrderRole::Open) | (PositionSide::Short, GridOrderRole::Close) => {
            Ok(OrderSide::Buy)
        }
        (PositionSide::Long, GridOrderRole::Close) | (PositionSide::Short, GridOrderRole::Open) => {
            Ok(OrderSide::Sell)
        }
        (PositionSide::Net, _) => Err(GridStoreError::Invalid),
    }
}

fn validate_ids(values: &[&str]) -> Result<(), GridStoreError> {
    if values
        .iter()
        .any(|value| !is_canonical_trading_account_id(value))
    {
        return Err(GridStoreError::Invalid);
    }
    Ok(())
}

fn positive(value: Decimal) -> bool {
    value.is_sign_positive() && !value.is_zero()
}

fn bounded(value: &str, minimum: usize, maximum: usize) -> bool {
    let trimmed = value.trim();
    (minimum..=maximum).contains(&trimmed.chars().count()) && !value.chars().any(char::is_control)
}

fn digest(value: &impl Serialize) -> Result<[u8; 32], GridStoreError> {
    let encoded = serde_json::to_vec(value).map_err(invalid_json)?;
    Ok(Sha256::digest(encoded).into())
}

fn hex_digest(value: Vec<u8>) -> Result<String, GridStoreError> {
    Ok(bytes_digest(value)?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>())
}

fn bytes_digest(value: Vec<u8>) -> Result<[u8; 32], GridStoreError> {
    value.try_into().map_err(|_| GridStoreError::Corrupt)
}

fn empty_desired_digest() -> [u8; 32] {
    Sha256::digest(b"venue-grid-empty-desired-v1").into()
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn decimal(value: String) -> Result<Decimal, GridStoreError> {
    value.parse().map_err(|_| GridStoreError::Corrupt)
}

fn integer(value: u64) -> Result<i64, GridStoreError> {
    i64::try_from(value).map_err(|_| GridStoreError::Invalid)
}

fn ms(value: u64) -> Result<i64, GridStoreError> {
    integer(value)
}

fn unsigned(value: i64) -> Result<u64, GridStoreError> {
    u64::try_from(value).map_err(|_| GridStoreError::Corrupt)
}

fn invalid_json(_: serde_json::Error) -> GridStoreError {
    GridStoreError::Invalid
}

fn corrupt_row(_: sqlx::Error) -> GridStoreError {
    GridStoreError::Corrupt
}

fn database_error(error: sqlx::Error) -> GridStoreError {
    if error.as_database_error().is_some_and(|database| {
        matches!(
            database.code().as_deref(),
            Some("23503" | "23505" | "23514")
        )
    }) {
        GridStoreError::Conflict
    } else {
        GridStoreError::Unavailable
    }
}
