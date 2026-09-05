//! PostgreSQL command storage for strategy execution on the non-Binance venues.
//!
//! This module owns durable strategy command identity and account serialization only.  It does
//! not decrypt credentials, select an adapter, or perform an exchange request.

use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use venue_control_protocol::VenueId;
use venue_domain::{ExecutionCommand, OrderPurpose, PositionSide};
use venue_execution::validate_durable_command;
use venue_gateway_api::{GatewayBinding, GatewayMode};

/// A full sixteen-order grid may cancel its old surface and enqueue sixteen replacements.
/// This bound is independent of Binance KOL's existing sixteen-command queue.
pub const MAX_STRATEGY_QUEUE_DEPTH: usize = 32;

const MAX_RECONCILE_ATTEMPTS: i32 = 31;

#[derive(Clone)]
pub struct MultiVenueStore {
    pool: PgPool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyClaim {
    pub command: ExecutionCommand,
    pub owner_user_id: String,
    pub credential_id: String,
    pub venue: VenueId,
    pub nonce: u64,
    pub reconcile_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyEnqueueResult {
    Inserted { command_id: String },
    Existing { command_id: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MultiVenueStoreError {
    #[error("multi-venue command input is invalid")]
    Invalid,
    #[error("multi-venue command conflicts with existing state")]
    Conflict,
    #[error("multi-venue command storage is unavailable")]
    Unavailable,
}

impl MultiVenueStore {
    pub async fn execution_context(
        &self,
        claim: &StrategyClaim,
    ) -> Result<venue_execution::DurableExecutionContext, MultiVenueStoreError> {
        let ExecutionCommand::Cancel(cancel) = &claim.command else {
            return Ok(Default::default());
        };
        let owner = &cancel.owner;
        let row = sqlx::query("SELECT strategy_command,native_order_id FROM venue_binance_commands WHERE client_order_id=$1 AND owner_user_id=$2 AND credential_id=$3 AND trading_account_id=$4 AND strategy_venue=$5 AND symbol=$6 AND strategy_command->'payload'->'owner'->>'strategy_instance_id'=$7 AND strategy_command->'payload'->'owner'->>'run_id'=$8")
            .bind(cancel.target_client_order_id.as_str()).bind(&claim.owner_user_id).bind(&claim.credential_id)
            .bind(&owner.account).bind(&owner.exchange).bind(owner.symbol.to_string()).bind(&owner.strategy_instance_id).bind(&owner.run_id)
            .fetch_one(&self.pool).await.map_err(|_| MultiVenueStoreError::Conflict)?;
        let context = venue_execution::DurableExecutionContext {
            target_command: Some(
                serde_json::from_value(
                    row.try_get("strategy_command")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                )
                .map_err(|_| MultiVenueStoreError::Conflict)?,
            ),
            target_native_order_id: row
                .try_get("native_order_id")
                .map_err(|_| MultiVenueStoreError::Conflict)?,
        };
        if !venue_execution::validate_durable_context(&claim.command, &context) {
            return Err(MultiVenueStoreError::Conflict);
        }
        Ok(context)
    }

    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns bounded account work for the shared executor. Pending rows are immediately due;
    /// accepted/reconcile rows are returned only at their durable deadline, while Sending rows
    /// remain recovery work after a crash.
    pub async fn pending_accounts(&self) -> Result<Vec<String>, MultiVenueStoreError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .filter(|value| *value > 0)
            .ok_or(MultiVenueStoreError::Unavailable)?;
        let rows = sqlx::query(
            "SELECT DISTINCT trading_account_id FROM venue_binance_commands \
             WHERE strategy_command IS NOT NULL AND \
             (command_state IN ('pending','sending') OR \
              (command_state IN ('accepted','reconcile_required') \
               AND (next_reconcile_ms IS NULL OR next_reconcile_ms <= $1))) \
             ORDER BY trading_account_id LIMIT 233",
        )
        .bind(ms(now_ms)?)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if rows.len() > 232 {
            return Err(MultiVenueStoreError::Conflict);
        }
        rows.into_iter()
            .map(|row| {
                row.try_get("trading_account_id")
                    .map_err(|_| MultiVenueStoreError::Unavailable)
            })
            .collect()
    }

    /// Inserts one strategy command after locking all credentials for the account.  The lock
    /// makes capacity and old-writer admission one account-level transaction, while the JSONB
    /// payload makes retries compare the original semantic command instead of only its ID.
    pub async fn enqueue(
        &self,
        owner_user_id: &str,
        credential_id: &str,
        command: ExecutionCommand,
        now_ms: u64,
    ) -> Result<StrategyEnqueueResult, MultiVenueStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let result = self
            .enqueue_in(&mut tx, owner_user_id, credential_id, command, now_ms)
            .await?;
        tx.commit()
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        Ok(result)
    }

    pub(crate) async fn enqueue_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        owner_user_id: &str,
        credential_id: &str,
        command: ExecutionCommand,
        now_ms: u64,
    ) -> Result<StrategyEnqueueResult, MultiVenueStoreError> {
        if owner_user_id.trim().is_empty() || credential_id.trim().is_empty() || now_ms == 0 {
            return Err(MultiVenueStoreError::Invalid);
        }
        command
            .validate_persisted_shape()
            .map_err(|_| MultiVenueStoreError::Invalid)?;
        let owner = command.mutation_owner();
        let venue = owner
            .exchange
            .parse::<VenueId>()
            .map_err(|_| MultiVenueStoreError::Invalid)?;
        if venue == VenueId::Binance {
            return Err(MultiVenueStoreError::Invalid);
        }
        let binding = GatewayBinding::new(
            venue,
            GatewayMode::Live,
            &owner.account,
            owner.symbol.clone(),
        )
        .map_err(|_| MultiVenueStoreError::Invalid)?;
        if !validate_durable_command(&binding, &command) {
            return Err(MultiVenueStoreError::Invalid);
        }
        let payload = serde_json::to_value(&command).map_err(|_| MultiVenueStoreError::Invalid)?;
        let command_id = command.command_id().as_str().to_owned();
        let client_order_id = command
            .native_client_id()
            .map(|id| id.as_str().to_owned())
            .unwrap_or_else(|| command_id.clone());
        let target_client_order_id = match &command {
            ExecutionCommand::Cancel(cancel) => Some(cancel.target_client_order_id.as_str()),
            _ => None,
        };
        let (phase, order_side, position_side, quantity, limit_price) = command_columns(&command)?;
        let now = ms(now_ms)?;

        let account_id: String = sqlx::query_scalar(
            "SELECT trading_account_id FROM venue_api_credentials \
             WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL",
        )
        .bind(credential_id)
        .bind(owner_user_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?
        .ok_or(MultiVenueStoreError::Conflict)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || $1,0))",
        )
        .bind(&account_id)
        .execute(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        sqlx::query(
            "SELECT trading_account_id FROM venue_user_trading_accounts \
             WHERE trading_account_id=$1 FOR UPDATE",
        )
        .bind(&account_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?
        .ok_or(MultiVenueStoreError::Conflict)?;
        let credential_rows = sqlx::query(
            "SELECT c.credential_id,c.user_id,c.trading_account_id,c.venue,c.verification_json \
             FROM venue_api_credentials c \
             WHERE c.trading_account_id=$1 AND c.deleted_ms IS NULL \
             ORDER BY c.credential_id FOR UPDATE",
        )
        .bind(&account_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let target = credential_rows.iter().find(|row| {
            row.try_get::<String, _>("credential_id")
                .ok()
                .is_some_and(|value| value == credential_id)
        });
        let target = target.ok_or(MultiVenueStoreError::Conflict)?;
        let target_account: String = target
            .try_get("trading_account_id")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let target_user: String = target
            .try_get("user_id")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let target_venue: String = target
            .try_get("venue")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let target_verified: Value = target
            .try_get("verification_json")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if target_account != account_id
            || target_user != owner_user_id
            || target_venue != venue.as_str()
            || target_verified.get("verification").and_then(Value::as_str) != Some("verified")
            || target_verified
                .get("strategy_execution")
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Err(MultiVenueStoreError::Conflict);
        }
        let account_venue: String = sqlx::query_scalar(
            "SELECT venue FROM venue_user_trading_accounts WHERE trading_account_id=$1 AND user_id=$2",
        )
        .bind(&account_id)
        .bind(owner_user_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?
        .ok_or(MultiVenueStoreError::Conflict)?;
        if account_venue != venue.as_str() {
            return Err(MultiVenueStoreError::Conflict);
        }
        if owner.account != account_id {
            return Err(MultiVenueStoreError::Conflict);
        }
        let old_writer: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes \
             WHERE venue=$1 AND mode='LIVE' AND trading_account_id=$2)",
        )
        .bind(venue.as_str())
        .bind(&account_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if old_writer {
            return Err(MultiVenueStoreError::Conflict);
        }
        let support_owner = sqlx::query(
            "SELECT instance_id,owner_user_id,credential_id FROM venue_support_martingale_instances WHERE trading_account_id=$1 AND lifecycle IN ('running','entry_paused','increase_paused','draining') LIMIT 2",
        )
        .bind(&account_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if support_owner.len() > 1 {
            return Err(MultiVenueStoreError::Conflict);
        }
        if let Some(support) = support_owner.first() {
            let support_instance: String = support
                .try_get("instance_id")
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
            let support_user: String = support
                .try_get("owner_user_id")
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
            let support_credential: String = support
                .try_get("credential_id")
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
            if support_instance != owner.strategy_instance_id
                || support_user != owner_user_id
                || support_credential != credential_id
            {
                return Err(MultiVenueStoreError::Conflict);
            }
        }
        let existing = sqlx::query(
            "SELECT command_id,owner_user_id,credential_id,trading_account_id,strategy_venue, \
                    strategy_command,client_order_id \
             FROM venue_binance_commands WHERE command_id=$1 OR client_order_id=$2 FOR UPDATE",
        )
        .bind(&command_id)
        .bind(&client_order_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if !existing.is_empty() {
            let same = existing.iter().any(|row| {
                row.try_get::<String, _>("command_id").ok().as_deref() == Some(command_id.as_str())
                    && row.try_get::<String, _>("owner_user_id").ok().as_deref()
                        == Some(owner_user_id)
                    && row.try_get::<String, _>("credential_id").ok().as_deref()
                        == Some(credential_id)
                    && row
                        .try_get::<String, _>("trading_account_id")
                        .ok()
                        .as_deref()
                        == Some(account_id.as_str())
                    && row.try_get::<String, _>("strategy_venue").ok().as_deref()
                        == Some(venue.as_str())
                    && row.try_get::<String, _>("client_order_id").ok().as_deref()
                        == Some(client_order_id.as_str())
                    && row.try_get::<Value, _>("strategy_command").ok().as_ref() == Some(&payload)
            });
            if same && existing.len() == 1 {
                return Ok(StrategyEnqueueResult::Existing { command_id });
            }
            return Err(MultiVenueStoreError::Conflict);
        }

        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN \
             ('pending','sending','accepted','reconcile_required')",
        )
        .bind(&account_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if active < 0
            || usize::try_from(active)
                .ok()
                .is_none_or(|count| count >= MAX_STRATEGY_QUEUE_DEPTH)
        {
            return Err(MultiVenueStoreError::Conflict);
        }

        if let ExecutionCommand::Cancel(cancel) = &command {
            let target_id = cancel.target_client_order_id.as_str();
            let target_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM venue_binance_commands \
                   WHERE client_order_id=$1 AND strategy_command IS NOT NULL \
                     AND owner_user_id=$2 AND credential_id=$3 AND trading_account_id=$4 \
                   AND strategy_venue=$5 \
                   AND strategy_command->'payload'->'owner'->>'strategy_instance_id'=$6 \
                   AND strategy_command->'payload'->'owner'->>'run_id'=$7 \
                   AND symbol=$8",
            )
            .bind(target_id)
            .bind(owner_user_id)
            .bind(credential_id)
            .bind(&account_id)
            .bind(venue.as_str())
            .bind(&owner.strategy_instance_id)
            .bind(&owner.run_id)
            .bind(owner.symbol.to_string())
            .fetch_one(&mut **tx)
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
            if target_count != 1 {
                return Err(MultiVenueStoreError::Conflict);
            }
        }

        let source_digest: Vec<u8> = Sha256::digest(
            serde_json::to_vec(&payload).map_err(|_| MultiVenueStoreError::Invalid)?,
        )
        .to_vec();
        let sequence:i64=sqlx::query_scalar("SELECT COALESCE(MAX(strategy_sequence),0)+1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_origin='strategy'")
            .bind(&account_id).fetch_one(&mut **tx).await.map_err(|_| MultiVenueStoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO venue_binance_commands \
             (command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol, \
              command_phase,order_kind,order_side,position_side,requested_quantity,limit_price, \
              target_client_order_id,rule_version,client_order_id,command_state,source_digest,strategy_command, \
              strategy_venue,strategy_nonce,created_ms,updated_ms,strategy_sequence) \
             VALUES ($1,'strategy',$2,$3,$4,$5,$6,'strategy',$7,$8,$9,$10,$11,'strategy-v1',$12, \
                     'pending',$13,$14,$15,NULL,$16,$16,$17)",
        )
        .bind(&command_id)
        .bind(owner_user_id)
        .bind(&account_id)
        .bind(credential_id)
        .bind(owner.symbol.to_string())
        .bind(phase)
        .bind(order_side)
        .bind(position_side)
        .bind(quantity)
        .bind(limit_price)
        .bind(target_client_order_id)
        .bind(&client_order_id)
        .bind(source_digest)
        .bind(&payload)
        .bind(venue.as_str())
        .bind(now)
        .bind(sequence)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            if error.as_database_error().is_some_and(|db| db.code().as_deref() == Some("23505")) {
                MultiVenueStoreError::Conflict
            } else {
                MultiVenueStoreError::Unavailable
            }
        })?;
        Ok(StrategyEnqueueResult::Inserted { command_id })
    }

    /// Claims one account-serialized strategy command. Existing unresolved work is returned with
    /// `reconcile_only=true`; only a Pending row is changed to Sending in this transaction.
    pub async fn claim(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<StrategyClaim>, MultiVenueStoreError> {
        if trading_account_id.trim().is_empty() || now_ms == 0 {
            return Err(MultiVenueStoreError::Invalid);
        }
        let now = ms(now_ms)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:' || $1,0))",
        )
        .bind(trading_account_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        sqlx::query(
            "SELECT trading_account_id FROM venue_user_trading_accounts \
             WHERE trading_account_id=$1 FOR UPDATE",
        )
        .bind(trading_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?
        .ok_or(MultiVenueStoreError::Conflict)?;
        let credential_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM venue_api_credentials WHERE trading_account_id=$1",
        )
        .bind(trading_account_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if credential_count == 0 {
            return Err(MultiVenueStoreError::Conflict);
        }
        sqlx::query(
            "SELECT credential_id FROM venue_api_credentials \
             WHERE trading_account_id=$1 ORDER BY credential_id FOR UPDATE",
        )
        .bind(trading_account_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT command_id,owner_user_id,credential_id,trading_account_id,strategy_venue, \
                    strategy_command,strategy_nonce,command_state,next_reconcile_ms \
             FROM venue_binance_commands WHERE trading_account_id=$1 \
               AND strategy_command IS NOT NULL AND command_state IN \
               ('sending','accepted','reconcile_required') \
             ORDER BY strategy_sequence,command_id LIMIT 1 FOR UPDATE",
        )
        .bind(trading_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let (row, reconcile_only) = match row {
            Some(row) => {
                let state: String = row
                    .try_get("command_state")
                    .map_err(|_| MultiVenueStoreError::Unavailable)?;
                let next: Option<i64> = row
                    .try_get("next_reconcile_ms")
                    .map_err(|_| MultiVenueStoreError::Unavailable)?;
                if state != "sending" && next.is_some_and(|deadline| deadline > now) {
                    tx.commit()
                        .await
                        .map_err(|_| MultiVenueStoreError::Unavailable)?;
                    return Ok(None);
                }
                (row, true)
            }
            None => {
                let row = sqlx::query(
                    "SELECT command_id,owner_user_id,credential_id,trading_account_id,strategy_venue, \
                    strategy_command,strategy_nonce,command_state,next_reconcile_ms \
             FROM venue_binance_commands WHERE trading_account_id=$1 \
               AND strategy_command IS NOT NULL AND command_state='pending' \
               AND EXISTS (SELECT 1 FROM venue_api_credentials c \
                   JOIN venue_user_trading_accounts a ON a.trading_account_id=c.trading_account_id \
                       AND a.user_id=c.user_id AND a.venue=c.venue \
                   WHERE c.credential_id=venue_binance_commands.credential_id \
                     AND c.user_id=venue_binance_commands.owner_user_id \
                     AND c.trading_account_id=venue_binance_commands.trading_account_id \
                     AND c.venue=venue_binance_commands.strategy_venue AND c.deleted_ms IS NULL \
                     AND c.verification_json->>'verification'='verified' \
                     AND c.verification_json->>'strategy_execution'='true') \
               AND NOT EXISTS (SELECT 1 FROM venue_control_strategy_scopes s \
                   WHERE s.venue=venue_binance_commands.strategy_venue AND s.mode='LIVE' \
                     AND s.trading_account_id=venue_binance_commands.trading_account_id) \
             ORDER BY strategy_sequence,command_id LIMIT 1 FOR UPDATE",
                )
                .bind(trading_account_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
                let Some(row) = row else {
                    tx.commit()
                        .await
                        .map_err(|_| MultiVenueStoreError::Unavailable)?;
                    return Ok(None);
                };
                (row, false)
            }
        };
        let command_id: String = row
            .try_get("command_id")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let owner_user_id: String = row
            .try_get("owner_user_id")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let credential_id: String = row
            .try_get("credential_id")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let venue = row
            .try_get::<String, _>("strategy_venue")
            .map_err(|_| MultiVenueStoreError::Unavailable)?
            .parse::<VenueId>()
            .map_err(|_| MultiVenueStoreError::Conflict)?;
        if venue == VenueId::Binance {
            return Err(MultiVenueStoreError::Conflict);
        }
        let payload: Value = row
            .try_get("strategy_command")
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let command: ExecutionCommand =
            serde_json::from_value(payload).map_err(|_| MultiVenueStoreError::Conflict)?;
        if command.command_id().as_str() != command_id
            || command.mutation_owner().account.as_str() != trading_account_id
            || command.mutation_owner().exchange.parse::<VenueId>().ok() != Some(venue)
            || command.validate_persisted_shape().is_err()
        {
            return Err(MultiVenueStoreError::Conflict);
        }
        let nonce = match row
            .try_get::<Option<i64>, _>("strategy_nonce")
            .map_err(|_| MultiVenueStoreError::Unavailable)?
        {
            Some(value) => u64::try_from(value).map_err(|_| MultiVenueStoreError::Conflict)?,
            None => {
                let current: i64 = sqlx::query_scalar(
                    "SELECT COALESCE(MAX(strategy_nonce),0)+1 FROM venue_binance_commands \
                     WHERE strategy_command IS NOT NULL AND strategy_venue=$1 \
                       AND trading_account_id=$2",
                )
                .bind(venue.as_str())
                .bind(trading_account_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
                let successor = current;
                let next = if venue == VenueId::Hyperliquid {
                    successor.max(now)
                } else {
                    successor
                };
                if next <= 0 {
                    return Err(MultiVenueStoreError::Conflict);
                }
                sqlx::query(
                    "UPDATE venue_binance_commands SET strategy_nonce=$1,updated_ms=updated_ms \
                     WHERE command_id=$2 AND command_origin='strategy' \
                       AND strategy_command IS NOT NULL AND strategy_nonce IS NULL",
                )
                .bind(next)
                .bind(&command_id)
                .execute(&mut *tx)
                .await
                .map_err(|_| MultiVenueStoreError::Unavailable)?;
                u64::try_from(next).map_err(|_| MultiVenueStoreError::Conflict)?
            }
        };
        if !reconcile_only {
            let changed = sqlx::query(
                "UPDATE venue_binance_commands SET command_state='sending',sending_ms=$1,updated_ms=$1 \
                 WHERE command_id=$2 AND command_origin='strategy' \
                   AND strategy_command IS NOT NULL AND command_state='pending'",
            )
            .bind(now)
            .bind(&command_id)
            .execute(&mut *tx)
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
            if changed.rows_affected() != 1 {
                return Err(MultiVenueStoreError::Conflict);
            }
        }
        tx.commit()
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        Ok(Some(StrategyClaim {
            command,
            owner_user_id,
            credential_id,
            venue,
            nonce,
            reconcile_only,
        }))
    }

    /// Finishes exactly the command represented by the claim.  The payload, owner, credential and
    /// nonce are part of the compare predicate so a stale worker cannot settle another command.
    pub async fn finish(
        &self,
        claim: &StrategyClaim,
        next: venue_control_protocol::kol::ExecutorCommandState,
        now_ms: u64,
        native_order_id: Option<&str>,
        sanitized_error_code: Option<&str>,
    ) -> Result<(), MultiVenueStoreError> {
        if now_ms == 0
            || matches!(
                next,
                venue_control_protocol::kol::ExecutorCommandState::Pending
                    | venue_control_protocol::kol::ExecutorCommandState::Sending
            )
        {
            return Err(MultiVenueStoreError::Invalid);
        }
        if native_order_id.is_some_and(|value| {
            value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace)
        }) {
            return Err(MultiVenueStoreError::Invalid);
        }
        let sanitized_error_code = match (next, sanitized_error_code) {
            (venue_control_protocol::kol::ExecutorCommandState::Rejected, Some(value))
                if valid_strategy_error_code(value) =>
            {
                Some(value)
            }
            (venue_control_protocol::kol::ExecutorCommandState::Rejected, _) => {
                return Err(MultiVenueStoreError::Invalid);
            }
            (_, None) => None,
            (_, Some(_)) => return Err(MultiVenueStoreError::Invalid),
        };
        let payload =
            serde_json::to_value(&claim.command).map_err(|_| MultiVenueStoreError::Invalid)?;
        let expected = if claim.reconcile_only {
            vec!["sending", "accepted", "reconcile_required"]
        } else {
            vec!["sending"]
        };
        let (state, terminal) = state_name(next)?;
        let now = ms(now_ms)?;
        let changed = sqlx::query(
            "UPDATE venue_binance_commands SET command_state=$1,sending_ms=CASE WHEN $1='cancelled' THEN NULL ELSE sending_ms END, \
             accepted_ms=CASE WHEN $2 THEN $3 ELSE accepted_ms END, \
             terminal_ms=CASE WHEN $4 THEN $3 ELSE terminal_ms END, native_order_id=COALESCE(native_order_id,$5), \
             sanitized_error_code=CASE WHEN $1='rejected' THEN $6 ELSE NULL END,updated_ms=$3 \
             WHERE command_id=$7 AND command_origin='strategy' AND owner_user_id=$8 AND credential_id=$9 \
               AND strategy_venue=$10 AND strategy_nonce=$11 AND strategy_command=$12 \
               AND command_state=ANY($13) \
               AND ($5::text IS NULL OR native_order_id IS NULL OR native_order_id=$5)",
        )
        .bind(state)
        .bind(next == venue_control_protocol::kol::ExecutorCommandState::Accepted)
        .bind(now)
        .bind(terminal)
        .bind(native_order_id)
        .bind(sanitized_error_code)
        .bind(claim.command.command_id().as_str())
        .bind(&claim.owner_user_id)
        .bind(&claim.credential_id)
        .bind(claim.venue.as_str())
        .bind(i64::try_from(claim.nonce).map_err(|_| MultiVenueStoreError::Conflict)?)
        .bind(payload)
        .bind(expected)
        .execute(&self.pool)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(MultiVenueStoreError::Conflict)
        }
    }

    /// Extends only the durable readback schedule of the original unresolved command.
    pub async fn backoff(
        &self,
        claim: &StrategyClaim,
        next_reconcile_ms: u64,
        now_ms: u64,
    ) -> Result<(), MultiVenueStoreError> {
        if !claim.reconcile_only || next_reconcile_ms <= now_ms || now_ms == 0 {
            return Err(MultiVenueStoreError::Invalid);
        }
        let payload =
            serde_json::to_value(&claim.command).map_err(|_| MultiVenueStoreError::Invalid)?;
        let changed = sqlx::query(
            "UPDATE venue_binance_commands SET reconcile_attempts=LEAST(reconcile_attempts+1,$1), \
             next_reconcile_ms=$2,updated_ms=$3 WHERE command_id=$4 AND owner_user_id=$5 \
             AND command_origin='strategy' AND credential_id=$6 AND strategy_venue=$7 AND strategy_nonce=$8 \
             AND strategy_command=$9 AND command_state IN ('sending','accepted','reconcile_required') \
             AND (next_reconcile_ms IS NULL OR next_reconcile_ms < $2)",
        )
        .bind(MAX_RECONCILE_ATTEMPTS)
        .bind(ms(next_reconcile_ms)?)
        .bind(ms(now_ms)?)
        .bind(claim.command.command_id().as_str())
        .bind(&claim.owner_user_id)
        .bind(&claim.credential_id)
        .bind(claim.venue.as_str())
        .bind(i64::try_from(claim.nonce).map_err(|_| MultiVenueStoreError::Conflict)?)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if changed.rows_affected() == 1 {
            Ok(())
        } else {
            Err(MultiVenueStoreError::Conflict)
        }
    }
}

fn valid_strategy_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

#[cfg(test)]
mod error_code_tests {
    use super::valid_strategy_error_code;

    #[test]
    fn strategy_error_codes_are_bounded_log_safe_identifiers() {
        assert!(valid_strategy_error_code("strategy_market_limits"));
        assert!(valid_strategy_error_code("strategy_okx_market_bbo"));
        assert!(valid_strategy_error_code("strategy_okx_market_metadata"));
        assert!(valid_strategy_error_code("strategy_okx_market_rules"));
        assert!(valid_strategy_error_code("okx_51008"));
        assert!(!valid_strategy_error_code(""));
        assert!(!valid_strategy_error_code("raw response"));
        assert!(!valid_strategy_error_code("exchange:secret"));
        assert!(!valid_strategy_error_code(&"x".repeat(129)));
    }
}

fn command_columns(
    command: &ExecutionCommand,
) -> Result<
    (
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        Option<String>,
        Option<String>,
    ),
    MultiVenueStoreError,
> {
    let owner = command.mutation_owner();
    let phase = match owner.purpose {
        OrderPurpose::Entry => "open",
        OrderPurpose::Protection
        | OrderPurpose::TakeProfit
        | OrderPurpose::Reduce
        | OrderPurpose::ExposureTakeProfit => "close",
    };
    match command {
        ExecutionCommand::Cancel(_cancel) => Ok(("cancel", None, None, None, None)),
        ExecutionCommand::PlaceLimit(order) => Ok((
            phase,
            Some(side_name(order.side)),
            Some(position_name(order.position_side)),
            Some(decimal_text(order.quantity)),
            Some(decimal_text(order.limit_price.value())),
        )),
        ExecutionCommand::PlaceMarket(order) => Ok((
            phase,
            Some(side_name(order.side)),
            Some(position_name(order.position_side)),
            Some(decimal_text(order.quantity)),
            None,
        )),
        ExecutionCommand::MarketReduce(order) => Ok((
            phase,
            Some(side_name(order.side)),
            Some(position_name(order.position_side)),
            Some(decimal_text(order.quantity)),
            None,
        )),
        ExecutionCommand::StopMarketCloseAll(order) => Ok((
            phase,
            Some(side_name(order.side)),
            Some(position_name(order.position_side)),
            None,
            Some(decimal_text(order.stop_price.value())),
        )),
        ExecutionCommand::StopMarketFullPosition(order) => Ok((
            phase,
            Some(side_name(order.side)),
            Some(position_name(order.position_side)),
            Some(decimal_text(order.quantity)),
            Some(decimal_text(order.trigger_price.value())),
        )),
    }
}

fn state_name(
    state: venue_control_protocol::kol::ExecutorCommandState,
) -> Result<(&'static str, bool), MultiVenueStoreError> {
    use venue_control_protocol::kol::ExecutorCommandState as State;
    match state {
        State::Accepted => Ok(("accepted", false)),
        State::Rejected => Ok(("rejected", true)),
        State::ReconcileRequired => Ok(("reconcile_required", false)),
        State::Reconciled => Ok(("reconciled", true)),
        State::Cancelled => Ok(("cancelled", true)),
        State::Pending | State::Sending => Err(MultiVenueStoreError::Invalid),
    }
}

const fn side_name(side: venue_domain::OrderSide) -> &'static str {
    match side {
        venue_domain::OrderSide::Buy => "buy",
        venue_domain::OrderSide::Sell => "sell",
    }
}

const fn position_name(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Net => "net",
        PositionSide::Long => "long",
        PositionSide::Short => "short",
    }
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn ms(value: u64) -> Result<i64, MultiVenueStoreError> {
    i64::try_from(value).map_err(|_| MultiVenueStoreError::Invalid)
}
