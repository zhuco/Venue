//! PostgreSQL facts owned by the singleton executor; no local journal is created.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::kol_executor::{
    BinanceCommandLedger, BinanceCommandLedgerError, ClaimedBinanceCommand, KolSourceFill,
    scaled_copy_quantity,
};
use venue_control_protocol::kol::ExecutorCommandState;

#[derive(Clone)]
pub struct PgExecutorStore {
    pool: PgPool,
}

/// A durable copy command created from an admitted source fill. The IDs are derived from the
/// relation target revision, so repeating the same authenticated exchange trade cannot create a
/// second physical request after a transaction retry or process restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedCopyCommand {
    pub command_id: String,
    pub client_order_id: String,
    pub relation_id: String,
    pub trading_account_id: String,
    pub target_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingActivation {
    pub relation_id: String,
    pub revision: u64,
    pub leader_user_id: String,
    pub leader_trading_account_id: String,
    pub leader_credential_id: String,
    pub follower_user_id: String,
    pub follower_trading_account_id: String,
    pub follower_credential_id: String,
}

impl PgExecutorStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Durable native trade identity makes repeated WS frames and restart replay idempotent.
    pub async fn record_source_fill(
        &self,
        kol_user_id: &str,
        fill: &KolSourceFill,
    ) -> Result<bool, BinanceCommandLedgerError> {
        let inserted = sqlx::query("INSERT INTO venue_kol_source_fills (kol_trading_account_id,kol_user_id,native_symbol,native_trade_id,symbol,order_side,position_side,quantity,price,occurred_ms,observed_ms,payload_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
            .bind(&fill.leader_trading_account_id).bind(kol_user_id).bind(&fill.native_symbol).bind(&fill.native_trade_id)
            .bind(&fill.symbol).bind(order_side(fill.order_side)).bind(position_side(fill.position_side))
            .bind(fill.quantity.to_string()).bind(fill.price.to_string()).bind(ms(fill.occurred_ms)?).bind(ms(fill.observed_ms)?).bind(fill.payload_digest.as_slice())
            .execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(inserted.rows_affected() == 1)
    }

    /// Persists a source fact, advances each active follower's desired hedge leg, and emits at
    /// most one command per relation/leg while no older physical command is unresolved. Newer
    /// fills only leave the target dirty; they never grow a second in-memory or SQL task queue.
    pub async fn record_source_fill_and_plan(
        &self,
        kol_user_id: &str,
        fill: &KolSourceFill,
        now_ms: u64,
    ) -> Result<Vec<PlannedCopyCommand>, BinanceCommandLedgerError> {
        let now = ms(now_ms)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let inserted = sqlx::query("INSERT INTO venue_kol_source_fills (kol_trading_account_id,kol_user_id,native_symbol,native_trade_id,symbol,order_side,position_side,quantity,price,occurred_ms,observed_ms,payload_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT DO NOTHING")
            .bind(&fill.leader_trading_account_id).bind(kol_user_id).bind(&fill.native_symbol).bind(&fill.native_trade_id)
            .bind(&fill.symbol).bind(order_side(fill.order_side)).bind(position_side(fill.position_side))
            .bind(fill.quantity.to_string()).bind(fill.price.to_string()).bind(ms(fill.occurred_ms)?).bind(ms(fill.observed_ms)?).bind(fill.payload_digest.as_slice())
            .execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if inserted.rows_affected() != 1 {
            tx.commit()
                .await
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            return Ok(Vec::new());
        }
        let relations = sqlx::query("SELECT r.relation_id,r.follower_user_id,r.follower_trading_account_id,r.credential_id,r.allocated_capital,r.multiplier,p.strategy_capital FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id WHERE r.kol_user_id=$1 AND r.leader_trading_account_id=$2 AND r.relation_state='active' AND r.allowed_symbols @> jsonb_build_array($3::text) ORDER BY r.relation_id FOR UPDATE OF r")
            .bind(kol_user_id).bind(&fill.leader_trading_account_id).bind(&fill.symbol)
            .fetch_all(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let mut planned = Vec::with_capacity(relations.len());
        for relation in relations {
            let relation_id: String = relation
                .try_get("relation_id")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let owner_user_id: String = relation
                .try_get("follower_user_id")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let trading_account_id: String = relation
                .try_get("follower_trading_account_id")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let credential_id: String = relation
                .try_get("credential_id")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let allocated = decimal(&relation, "allocated_capital")?;
            let multiplier = decimal(&relation, "multiplier")?;
            let strategy = decimal(&relation, "strategy_capital")?;
            let delta = scaled_copy_quantity(fill.quantity, allocated, strategy, multiplier)?;
            let existing = sqlx::query("SELECT target_quantity,observed_quantity,target_revision FROM venue_kol_copy_targets WHERE relation_id=$1 AND symbol=$2 AND position_side=$3 FOR UPDATE")
                .bind(&relation_id).bind(&fill.symbol).bind(position_side(fill.position_side)).fetch_optional(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let previous_target = existing
                .as_ref()
                .map(|row| decimal(row, "target_quantity"))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            let observed = existing
                .as_ref()
                .map(|row| decimal(row, "observed_quantity"))
                .transpose()?
                .unwrap_or(Decimal::ZERO);
            let previous_revision = existing
                .as_ref()
                .map(|row| {
                    row.try_get::<i64, _>("target_revision")
                        .map_err(|_| BinanceCommandLedgerError::Unavailable)
                })
                .transpose()?
                .unwrap_or(0);
            let increasing = matches!(
                (fill.position_side, fill.order_side),
                (
                    venue_domain::domain::PositionSide::Long,
                    venue_domain::domain::OrderSide::Buy
                ) | (
                    venue_domain::domain::PositionSide::Short,
                    venue_domain::domain::OrderSide::Sell
                )
            );
            let target = if increasing {
                previous_target
                    .checked_add(delta)
                    .ok_or(BinanceCommandLedgerError::Conflict)?
            } else {
                previous_target.checked_sub(delta).unwrap_or(Decimal::ZERO)
            };
            let revision = previous_revision
                .checked_add(1)
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            sqlx::query("INSERT INTO venue_kol_copy_targets (relation_id,symbol,position_side,copyable_quantity,target_quantity,observed_quantity,target_revision,last_native_symbol,last_native_trade_id,dirty,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,true,$10) ON CONFLICT (relation_id,symbol,position_side) DO UPDATE SET copyable_quantity=EXCLUDED.copyable_quantity,target_quantity=EXCLUDED.target_quantity,target_revision=EXCLUDED.target_revision,last_native_symbol=EXCLUDED.last_native_symbol,last_native_trade_id=EXCLUDED.last_native_trade_id,dirty=true,updated_ms=EXCLUDED.updated_ms")
                .bind(&relation_id).bind(&fill.symbol).bind(position_side(fill.position_side)).bind(target.to_string()).bind(target.to_string()).bind(observed.to_string()).bind(revision).bind(&fill.native_symbol).bind(&fill.native_trade_id).bind(now)
                .execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE relation_id=$1 AND symbol=$2 AND position_side=$3 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
                .bind(&relation_id).bind(&fill.symbol).bind(position_side(fill.position_side)).fetch_one(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            if blocked || target == observed {
                continue;
            }
            let opening = target > observed;
            let command_phase = if opening { "open" } else { "close" };
            let order_side = order_for(fill.position_side, opening);
            let command_id = deterministic_id(
                &relation_id,
                &fill.symbol,
                fill.position_side,
                revision,
                command_phase,
            );
            let requested = if opening {
                target - observed
            } else {
                observed - target
            };
            let inserted = sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,client_order_id,command_state,source_digest,created_ms,updated_ms) SELECT $1,'copy',$2,r.revision,$3,$4,$5,$6,$7,$8,$9,'market',$10,$11,$12,'binance-pm-um-v1',$13,'pending',$14,$15,$15 FROM venue_kol_follow_relations r WHERE r.relation_id=$2 AND r.relation_state='active' ON CONFLICT DO NOTHING")
                .bind(&command_id).bind(&relation_id).bind(revision).bind(&owner_user_id).bind(&trading_account_id).bind(&credential_id).bind(&fill.symbol).bind(position_side(fill.position_side)).bind(command_phase).bind(order_side).bind(requested.to_string()).bind(target.to_string()).bind(&command_id).bind(fill.payload_digest.as_slice()).bind(now)
                .execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            if inserted.rows_affected() == 1 {
                sqlx::query("UPDATE venue_kol_copy_targets SET dirty=false,updated_ms=$1 WHERE relation_id=$2 AND symbol=$3 AND position_side=$4 AND target_revision=$5")
                    .bind(now).bind(&relation_id).bind(&fill.symbol).bind(position_side(fill.position_side)).bind(revision).execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
                planned.push(PlannedCopyCommand {
                    command_id: command_id.clone(),
                    client_order_id: command_id,
                    relation_id,
                    trading_account_id,
                    target_revision: u64::try_from(revision)
                        .map_err(|_| BinanceCommandLedgerError::Conflict)?,
                });
            }
        }
        tx.commit()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(planned)
    }

    /// Restart recovery only returns identities to read back. It deliberately does not make a
    /// Sending or ReconcileRequired command eligible for another POST.
    pub async fn recover_nonterminal(
        &self,
    ) -> Result<Vec<ClaimedBinanceCommand>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT command_id,owner_user_id,trading_account_id,credential_id,client_order_id,command_state FROM venue_binance_commands WHERE command_state IN ('pending','sending','accepted','reconcile_required') ORDER BY created_ms,command_id")
            .fetch_all(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        rows.into_iter().map(recovery_row).collect()
    }

    /// Atomically changes one committed Pending command to Sending. The underlying PostgreSQL
    /// predicate fences a second claimant and later commands behind uncertain work.
    pub async fn claim_next_command(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<ClaimedBinanceCommand>, BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .claim_next(trading_account_id, now_ms)
            .await
    }

    /// Exposes only the command ledger's forward-only transition table. In particular, callers
    /// cannot turn Sending back into Pending after a timeout.
    pub async fn transition_command(
        &self,
        command_id: &str,
        next: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .settle(command_id, next, now_ms, sanitized_error_code)
            .await
    }

    /// A successful, independently signed baseline is the only path that promotes a requested
    /// relation. The singleton owns the slot allocation transaction.
    pub async fn complete_activation(
        &self,
        relation_id: &str,
        revision: u64,
        baseline_ms: u64,
    ) -> Result<(), BinanceCommandLedgerError> {
        let revision = i64::try_from(revision).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let now = ms(baseline_ms)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let slot: Option<i16> = sqlx::query_scalar("SELECT s::smallint FROM generate_series(1,200) s WHERE NOT EXISTS (SELECT 1 FROM venue_kol_follow_relations r WHERE r.active_slot=s) LIMIT 1")
            .fetch_optional(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let slot = slot.ok_or(BinanceCommandLedgerError::Conflict)?;
        let changed = sqlx::query("UPDATE venue_kol_follow_relations r SET relation_state='active',active_slot=$1,baseline_json=$2,attention_code=NULL,updated_ms=$3 FROM venue_kol_activation_requests a WHERE r.relation_id=$4 AND a.relation_id=r.relation_id AND a.request_state='pending' AND a.relation_revision=$5 AND r.relation_state='paused' AND r.revision=$5")
            .bind(slot).bind(json!({"baseline_ms": baseline_ms})).bind(now).bind(relation_id).bind(revision)
            .execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        sqlx::query("UPDATE venue_kol_activation_requests SET request_state='completed',updated_ms=$1 WHERE relation_id=$2 AND request_state='pending'")
            .bind(now).bind(relation_id).execute(&mut *tx).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        tx.commit()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)
    }

    /// Returns only a uniquely selected, still-verified leader credential. Ambiguous or stale
    /// credentials deliberately do not enter the executor's decryption boundary.
    pub async fn pending_activations(
        &self,
        now_ms: u64,
    ) -> Result<Vec<PendingActivation>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT a.relation_id,a.relation_revision,r.kol_user_id,r.leader_trading_account_id,r.follower_user_id,r.follower_trading_account_id,r.credential_id AS follower_credential_id,(SELECT c.credential_id FROM venue_api_credentials c WHERE c.user_id=r.kol_user_id AND c.trading_account_id=r.leader_trading_account_id AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' AND COALESCE((c.verification_json->>'expires_ms')::bigint,0)>$1 ORDER BY c.created_ms,c.credential_id LIMIT 1) AS leader_credential_id,(SELECT count(*) FROM venue_api_credentials c WHERE c.user_id=r.kol_user_id AND c.trading_account_id=r.leader_trading_account_id AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified' AND COALESCE((c.verification_json->>'expires_ms')::bigint,0)>$1) AS leader_credential_count FROM venue_kol_activation_requests a JOIN venue_kol_follow_relations r ON r.relation_id=a.relation_id WHERE a.request_state='pending' AND r.relation_state='paused' ORDER BY a.requested_ms,a.relation_id")
            .bind(ms(now_ms)?)
            .fetch_all(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let mut pending = Vec::with_capacity(rows.len());
        for row in rows {
            let count: i64 = row
                .try_get("leader_credential_count")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            if count != 1 {
                continue;
            }
            pending.push(PendingActivation {
                relation_id: row
                    .try_get("relation_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                revision: u64::try_from(
                    row.try_get::<i64, _>("relation_revision")
                        .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                )
                .map_err(|_| BinanceCommandLedgerError::Conflict)?,
                leader_user_id: row
                    .try_get("kol_user_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                leader_trading_account_id: row
                    .try_get("leader_trading_account_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                leader_credential_id: row
                    .try_get::<Option<String>, _>("leader_credential_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?
                    .ok_or(BinanceCommandLedgerError::Conflict)?,
                follower_user_id: row
                    .try_get("follower_user_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                follower_trading_account_id: row
                    .try_get("follower_trading_account_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                follower_credential_id: row
                    .try_get("follower_credential_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
            });
        }
        Ok(pending)
    }

    pub async fn reject_activation(
        &self,
        relation_id: &str,
        now_ms: u64,
        reason: &str,
    ) -> Result<(), BinanceCommandLedgerError> {
        if reason.is_empty() || reason.len() > 64 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let changed = sqlx::query("UPDATE venue_kol_activation_requests SET request_state='rejected',sanitized_reason=$1,updated_ms=$2 WHERE relation_id=$3 AND request_state='pending'")
            .bind(reason).bind(ms(now_ms)?).bind(relation_id).execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        (changed.rows_affected() == 1)
            .then_some(())
            .ok_or(BinanceCommandLedgerError::Conflict)
    }
}

fn recovery_row(
    row: sqlx::postgres::PgRow,
) -> Result<ClaimedBinanceCommand, BinanceCommandLedgerError> {
    let state = match row
        .try_get::<String, _>("command_state")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_str()
    {
        "pending" => venue_control_protocol::kol::ExecutorCommandState::Pending,
        "sending" => venue_control_protocol::kol::ExecutorCommandState::Sending,
        "accepted" => venue_control_protocol::kol::ExecutorCommandState::Accepted,
        "reconcile_required" => {
            venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired
        }
        _ => return Err(BinanceCommandLedgerError::Unavailable),
    };
    Ok(ClaimedBinanceCommand {
        command_id: row
            .try_get("command_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        owner_user_id: row
            .try_get("owner_user_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        trading_account_id: row
            .try_get("trading_account_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        credential_id: row
            .try_get("credential_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        client_order_id: row
            .try_get("client_order_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        state,
    })
}
fn ms(value: u64) -> Result<i64, BinanceCommandLedgerError> {
    i64::try_from(value).map_err(|_| BinanceCommandLedgerError::Conflict)
}
fn order_side(side: venue_domain::domain::OrderSide) -> &'static str {
    match side {
        venue_domain::domain::OrderSide::Buy => "buy",
        venue_domain::domain::OrderSide::Sell => "sell",
    }
}
fn position_side(side: venue_domain::domain::PositionSide) -> &'static str {
    match side {
        venue_domain::domain::PositionSide::Long => "long",
        venue_domain::domain::PositionSide::Short => "short",
        venue_domain::domain::PositionSide::Net => "net",
    }
}

fn decimal(row: &sqlx::postgres::PgRow, field: &str) -> Result<Decimal, BinanceCommandLedgerError> {
    Decimal::from_str(
        &row.try_get::<String, _>(field)
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
    )
    .map_err(|_| BinanceCommandLedgerError::Conflict)
}

fn order_for(side: venue_domain::domain::PositionSide, opening: bool) -> &'static str {
    match (side, opening) {
        (venue_domain::domain::PositionSide::Long, true)
        | (venue_domain::domain::PositionSide::Short, false) => "buy",
        (venue_domain::domain::PositionSide::Long, false)
        | (venue_domain::domain::PositionSide::Short, true) => "sell",
        (venue_domain::domain::PositionSide::Net, _) => "buy",
    }
}

fn deterministic_id(
    relation_id: &str,
    symbol: &str,
    side: venue_domain::domain::PositionSide,
    revision: i64,
    phase: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(relation_id.as_bytes());
    hasher.update([0]);
    hasher.update(symbol.as_bytes());
    hasher.update([0]);
    hasher.update(position_side(side).as_bytes());
    hasher.update([0]);
    hasher.update(revision.to_be_bytes());
    hasher.update([0]);
    hasher.update(phase.as_bytes());
    let digest = hasher.finalize();
    let encoded = digest
        .iter()
        .map(|value| format!("{value:02x}"))
        .collect::<String>();
    format!("k{}", &encoded[..35])
}
