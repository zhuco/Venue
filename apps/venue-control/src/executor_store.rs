//! PostgreSQL facts owned by the singleton executor; no local journal is created.

mod terminal_positions;
pub(crate) use terminal_positions::settle_reverse_child;

use std::{collections::BTreeSet, ops::Deref, str::FromStr};

use rust_decimal::Decimal;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Row};

use crate::{
    executor_exchange::ReconciledCloseReservation,
    kol_executor::{
        BinanceCommandLedger, BinanceCommandLedgerError, ClaimedBinanceBatch,
        ClaimedBinanceCommand, ClaimedBinanceOrder, KolSourceFill, MAX_ACCOUNT_QUEUE_DEPTH,
        scaled_copy_quantity,
    },
};
use venue_control_protocol::kol::ExecutorCommandState;

mod activation;
mod copy_drain;
mod copy_targets;
mod market;

pub const MIGRATION_0022: &str = include_str!("../migrations/0022_binance_reconcile_backoff.sql");

const RECONCILE_BASE_DELAY_MS: u64 = 500;
const RECONCILE_MAX_DELAY_MS: u64 = 8_000;
const RECONCILE_MAX_ATTEMPTS: u32 = 31;

/// Locks every non-deleted credential row for one trading account before a producer observes the
/// shared command depth. Holding this row lock through the caller's insert and commit makes
/// terminal, Copy and Grid admission one account-level critical section without adding a process
/// lease or a second writer authority.
pub(crate) async fn lock_account_command_queue(
    connection: &mut PgConnection,
    owner_user_id: &str,
    trading_account_id: &str,
    credential_id: &str,
) -> Result<usize, BinanceCommandLedgerError> {
    let credential_rows = sqlx::query(
        "SELECT credential_id,user_id FROM venue_api_credentials \
         WHERE trading_account_id=$1 AND deleted_ms IS NULL \
         ORDER BY credential_id FOR UPDATE",
    )
    .bind(trading_account_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let mut owns_active_credential = false;
    for row in credential_rows {
        let locked_credential_id: String = row
            .try_get("credential_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let locked_user_id: String = row
            .try_get("user_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        owns_active_credential |=
            locked_credential_id == credential_id && locked_user_id == owner_user_id;
    }
    if !owns_active_credential {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let raw_depth: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM venue_binance_commands \
         WHERE trading_account_id=$1 \
         AND command_state IN ('pending','sending','accepted','reconcile_required')",
    )
    .bind(trading_account_id)
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    usize::try_from(raw_depth).map_err(|_| BinanceCommandLedgerError::Conflict)
}

#[must_use]
pub(crate) const fn account_queue_has_capacity(current: usize, additional: usize) -> bool {
    current <= MAX_ACCOUNT_QUEUE_DEPTH
        && additional <= MAX_ACCOUNT_QUEUE_DEPTH.saturating_sub(current)
}

#[derive(Clone)]
pub struct PgExecutorStore {
    pool: PgPool,
}

impl PgExecutorStore {
    pub(crate) async fn terminal_open_credential_verified(
        &self,
        command: &ClaimedBinanceCommand,
    ) -> Result<bool, BinanceCommandLedgerError> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND trading_account_id=$3 AND deleted_ms IS NULL AND verification_json->>'verification'='verified')")
            .bind(&command.credential_id).bind(&command.owner_user_id).bind(&command.trading_account_id)
            .fetch_one(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)
    }
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
    pub request_id: String,
    pub revision: u64,
    pub leader_user_id: String,
    pub leader_trading_account_id: String,
    pub leader_credential_id: String,
    pub follower_user_id: String,
    pub follower_trading_account_id: String,
    pub follower_credential_id: String,
    pub symbols: BTreeSet<venue_domain::domain::Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveKolPrivateSource {
    pub kol_user_id: String,
    pub leader_trading_account_id: String,
    pub credential_id: String,
    pub symbols: Vec<venue_domain::domain::Symbol>,
}

/// A nonterminal command together with its durable signed-readback schedule. Pending and Sending
/// rows do not have a readback deadline; Accepted and ReconcileRequired rows always do after 0022.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoverableBinanceCommand {
    pub command: ClaimedBinanceCommand,
    pub grid_batch_id: Option<String>,
    pub dispatch_sequence: Option<u16>,
    pub reconcile_attempts: u32,
    pub next_reconcile_ms: Option<u64>,
}

impl RecoverableBinanceCommand {
    pub fn reconciliation_due(&self, now_ms: u64) -> Result<bool, BinanceCommandLedgerError> {
        match self.command.state {
            ExecutorCommandState::Sending => Ok(true),
            ExecutorCommandState::Accepted | ExecutorCommandState::ReconcileRequired => self
                .next_reconcile_ms
                .map(|deadline| deadline <= now_ms)
                .ok_or(BinanceCommandLedgerError::Conflict),
            ExecutorCommandState::Pending => Ok(false),
            ExecutorCommandState::Rejected
            | ExecutorCommandState::Reconciled
            | ExecutorCommandState::Cancelled => Err(BinanceCommandLedgerError::Conflict),
        }
    }
}

impl Deref for RecoverableBinanceCommand {
    type Target = ClaimedBinanceCommand;

    fn deref(&self) -> &Self::Target {
        &self.command
    }
}

impl PgExecutorStore {
    pub(crate) fn mirror_pool(&self) -> &PgPool {
        &self.pool
    }
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Selects at most the enabled KOL account streams. A leader credential must be unique and
    /// fresh; ambiguity is fail-closed before any decryption or listenKey request.
    pub async fn active_kol_private_sources(
        &self,
        _now_ms: u64,
    ) -> Result<Vec<ActiveKolPrivateSource>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT p.kol_user_id,p.leader_trading_account_id,ARRAY_AGG(DISTINCT symbols.value ORDER BY symbols.value) AS symbols,b.credential_id,(SELECT count(*) FROM venue_api_credentials c WHERE c.user_id=p.kol_user_id AND c.trading_account_id=p.leader_trading_account_id AND c.credential_id=b.credential_id AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified') AS credential_count FROM venue_kol_profiles p JOIN venue_leader_bots b ON b.owner_user_id=p.kol_user_id AND b.bot_state='running' JOIN venue_kol_follow_relations r ON r.kol_user_id=p.kol_user_id AND r.leader_trading_account_id=p.leader_trading_account_id AND r.relation_state='active' CROSS JOIN LATERAL jsonb_array_elements_text(r.allowed_symbols) AS symbols(value) WHERE p.profile_state='enabled' GROUP BY p.kol_user_id,p.leader_trading_account_id,b.credential_id ORDER BY p.kol_user_id")
            .fetch_all(&self.pool)
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if rows.len() > crate::kol_executor::MAX_ENABLED_KOLS {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let mut sources = Vec::with_capacity(rows.len());
        for row in rows {
            let count: i64 = row
                .try_get("credential_count")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            if count != 1 {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            let raw_symbols: Vec<String> = row
                .try_get("symbols")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let symbols = raw_symbols
                .into_iter()
                .map(|value| value.parse())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| BinanceCommandLedgerError::Conflict)?;
            if symbols.is_empty() {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            sources.push(ActiveKolPrivateSource {
                kol_user_id: row
                    .try_get("kol_user_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                leader_trading_account_id: row
                    .try_get("leader_trading_account_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                credential_id: row
                    .try_get::<Option<String>, _>("credential_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?
                    .ok_or(BinanceCommandLedgerError::Conflict)?,
                symbols,
            });
        }
        Ok(sources)
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
        copy_targets::record_source_fill_and_plan(self, kol_user_id, fill, now_ms).await
    }

    /// Restart recovery returns the immutable identity and durable readback schedule. It
    /// deliberately does not make a Sending or ReconcileRequired command eligible for another
    /// POST.
    pub async fn recover_nonterminal(
        &self,
    ) -> Result<Vec<RecoverableBinanceCommand>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol,order_side,position_side,requested_quantity,command_phase,order_kind,limit_price,selected_native_order_id,target_client_order_id,client_order_id,native_order_id,command_state,reconcile_attempts,next_reconcile_ms,grid_batch_id,dispatch_sequence,copy_risk FROM venue_binance_commands WHERE command_state IN ('pending','sending','accepted','reconcile_required') ORDER BY created_ms,COALESCE(grid_batch_id,command_id),COALESCE(dispatch_sequence,0),command_id")
            .fetch_all(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        rows.into_iter().map(recoverable_command).collect()
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

    /// Claims one non-Grid command or one durable Grid batch suffix. The ledger commits every
    /// returned child as `Sending` before the caller can enter the exchange mutation boundary.
    pub async fn claim_next_command_batch(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<ClaimedBinanceBatch>, BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .claim_next_batch(trading_account_id, now_ms)
            .await
    }

    /// Returns only the short projection-lag window left by earlier completed maker closes.
    /// Pending work is intentionally excluded: account serialization already fences in-flight
    /// mutation, and future commands must not reserve quantity merely because they are queued.
    pub async fn reconciled_close_reservations(
        &self,
        command: &ClaimedBinanceCommand,
    ) -> Result<Vec<ReconciledCloseReservation>, BinanceCommandLedgerError> {
        if command.state != ExecutorCommandState::Sending {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let (side, target_position_side, reducing) = match &command.order {
            ClaimedBinanceOrder::Market {
                side,
                position_side,
                reducing,
                ..
            }
            | ClaimedBinanceOrder::Limit {
                side,
                position_side,
                reducing,
                ..
            } => (*side, *position_side, *reducing),
            ClaimedBinanceOrder::CancelExact { .. } => return Ok(Vec::new()),
        };
        if !reducing {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            "SELECT prior.client_order_id,prior.requested_quantity,prior.updated_ms,\
             projection.observed_ms \
             FROM venue_binance_commands prior \
             JOIN venue_binance_account_projections projection \
               ON projection.credential_id=$2 AND projection.owner_user_id=$3 \
              AND projection.trading_account_id=$4 \
             WHERE prior.command_id<>$1 AND prior.credential_id=$2 \
               AND prior.owner_user_id=$3 AND prior.trading_account_id=$4 \
               AND prior.symbol=$5 AND prior.command_phase='close' \
               AND (prior.order_kind='limit_post_only' OR prior.order_kind='limit_gtc') AND prior.order_side=$6 \
               AND prior.position_side=$7 AND prior.command_state='reconciled' \
               AND prior.updated_ms>projection.observed_ms \
             ORDER BY prior.updated_ms,prior.command_id",
        )
        .bind(&command.command_id)
        .bind(&command.credential_id)
        .bind(&command.owner_user_id)
        .bind(&command.trading_account_id)
        .bind(command.symbol.to_string())
        .bind(order_side(side))
        .bind(position_side(target_position_side))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        rows.into_iter()
            .map(|row| {
                let quantity = decimal(&row, "requested_quantity")?;
                let reconciled_ms = unsigned_ms(&row, "updated_ms")?;
                let projection_observed_ms = unsigned_ms(&row, "observed_ms")?;
                if quantity <= Decimal::ZERO || reconciled_ms <= projection_observed_ms {
                    return Err(BinanceCommandLedgerError::Conflict);
                }
                Ok(ReconciledCloseReservation {
                    credential_id: command.credential_id.clone(),
                    trading_account_id: command.trading_account_id.clone(),
                    symbol: command.symbol.clone(),
                    client_order_id: row
                        .try_get("client_order_id")
                        .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                    side,
                    position_side: target_position_side,
                    quantity,
                    reconciled_ms,
                    projection_observed_ms,
                })
            })
            .collect()
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

    /// Atomically binds exact signed-readback identity to a forward-only ledger transition.
    pub async fn transition_command_with_readback(
        &self,
        command_id: &str,
        next: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
        native_order_id: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        BinanceCommandLedger::new(self.pool.clone())
            .settle_with_readback(
                command_id,
                next,
                now_ms,
                sanitized_error_code,
                native_order_id,
            )
            .await
    }

    /// Records one failed or still-pending signed readback without releasing the account fence.
    /// The predicate includes the durable attempt so concurrent workers cannot shorten or skip a
    /// deadline. This never returns a command to Pending and therefore cannot authorize a POST.
    pub async fn defer_reconciliation(
        &self,
        command: &RecoverableBinanceCommand,
        next_state: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
        native_order_id: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        let current_state = command.command.state;
        if !matches!(
            current_state,
            ExecutorCommandState::Accepted | ExecutorCommandState::ReconcileRequired
        ) || !matches!(
            next_state,
            ExecutorCommandState::Accepted | ExecutorCommandState::ReconcileRequired
        ) || (current_state == ExecutorCommandState::ReconcileRequired
            && next_state != ExecutorCommandState::ReconcileRequired)
            || sanitized_error_code.is_some_and(|value| value.is_empty() || value.len() > 64)
            || invalid_native_order_id(native_order_id)
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let next_attempt = command
            .reconcile_attempts
            .saturating_add(1)
            .min(RECONCILE_MAX_ATTEMPTS);
        let deadline = now_ms
            .checked_add(reconcile_delay_ms(next_attempt))
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        let changed = sqlx::query(
            "UPDATE venue_binance_commands SET command_state=$1,reconcile_attempts=$2,\
             next_reconcile_ms=$3,sanitized_error_code=$4,\
             native_order_id=COALESCE(native_order_id,$5),updated_ms=$6 \
             WHERE command_id=$7 AND command_state=$8 AND reconcile_attempts=$9 \
             AND ($5::text IS NULL OR native_order_id IS NULL OR native_order_id=$5)",
        )
        .bind(state_name(next_state))
        .bind(i32::try_from(next_attempt).map_err(|_| BinanceCommandLedgerError::Conflict)?)
        .bind(ms(deadline)?)
        .bind(sanitized_error_code)
        .bind(native_order_id)
        .bind(ms(now_ms)?)
        .bind(&command.command.command_id)
        .bind(state_name(current_state))
        .bind(
            i32::try_from(command.reconcile_attempts)
                .map_err(|_| BinanceCommandLedgerError::Conflict)?,
        )
        .execute(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        (changed.rows_affected() == 1)
            .then_some(())
            .ok_or(BinanceCommandLedgerError::Conflict)
    }

    /// Returns only a uniquely selected, still-verified leader credential. Ambiguous or stale
    /// credentials deliberately do not enter the executor's decryption boundary.
    pub async fn pending_activations(
        &self,
        _now_ms: u64,
    ) -> Result<Vec<PendingActivation>, BinanceCommandLedgerError> {
        let rows = sqlx::query("SELECT a.relation_id,a.request_id,a.relation_revision,r.kol_user_id,r.leader_trading_account_id,r.follower_user_id,r.follower_trading_account_id,r.credential_id AS follower_credential_id,r.allowed_symbols,b.credential_id AS leader_credential_id,(SELECT count(*) FROM venue_api_credentials c WHERE c.user_id=r.kol_user_id AND c.trading_account_id=r.leader_trading_account_id AND c.credential_id=b.credential_id AND c.deleted_ms IS NULL AND c.verification_json->>'verification'='verified') AS leader_credential_count FROM venue_kol_activation_requests a JOIN venue_kol_follow_relations r ON r.relation_id=a.relation_id JOIN venue_leader_bots b ON b.owner_user_id=r.kol_user_id AND b.bot_state='running' WHERE a.request_state='pending' AND r.relation_state='paused' ORDER BY a.requested_ms,a.relation_id")
            .fetch_all(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let mut pending = Vec::with_capacity(rows.len());
        for row in rows {
            let count: i64 = row
                .try_get("leader_credential_count")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            if count != 1 {
                continue;
            }
            let symbols = row
                .try_get::<serde_json::Value, _>("allowed_symbols")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?
                .as_array()
                .ok_or(BinanceCommandLedgerError::Conflict)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or(BinanceCommandLedgerError::Conflict)?
                        .parse()
                        .map_err(|_| BinanceCommandLedgerError::Conflict)
                })
                .collect::<Result<BTreeSet<_>, _>>()?;
            if symbols.is_empty() {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            pending.push(PendingActivation {
                relation_id: row
                    .try_get("relation_id")
                    .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
                request_id: row
                    .try_get("request_id")
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
                symbols,
            });
        }
        Ok(pending)
    }

    pub async fn reject_activation(
        &self,
        activation: &PendingActivation,
        now_ms: u64,
        reason: &str,
    ) -> Result<(), BinanceCommandLedgerError> {
        if reason.is_empty() || reason.len() > 64 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        sqlx::query("UPDATE venue_kol_activation_requests SET request_state='rejected',sanitized_reason=$1,updated_ms=$2 WHERE relation_id=$3 AND request_id=$4 AND relation_revision=$5 AND request_state='pending'")
            .bind(reason).bind(ms(now_ms)?).bind(&activation.relation_id)
            .bind(&activation.request_id).bind(ms(activation.revision)?)
            .execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(())
    }
}

fn recoverable_command(
    row: sqlx::postgres::PgRow,
) -> Result<RecoverableBinanceCommand, BinanceCommandLedgerError> {
    let grid_batch_id = row
        .try_get::<Option<String>, _>("grid_batch_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let dispatch_sequence = row
        .try_get::<Option<i64>, _>("dispatch_sequence")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .map(|value| u16::try_from(value).map_err(|_| BinanceCommandLedgerError::Conflict))
        .transpose()?;
    match (&grid_batch_id, dispatch_sequence) {
        (None, None) => {}
        (Some(batch_id), Some(sequence))
            if !batch_id.trim().is_empty()
                && batch_id.len() <= 64
                && (1..=16).contains(&sequence) => {}
        _ => return Err(BinanceCommandLedgerError::Conflict),
    }
    let raw_attempts: i32 = row
        .try_get("reconcile_attempts")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let reconcile_attempts = u32::try_from(raw_attempts)
        .ok()
        .filter(|value| *value <= RECONCILE_MAX_ATTEMPTS)
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let next_reconcile_ms = row
        .try_get::<Option<i64>, _>("next_reconcile_ms")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .map(|value| u64::try_from(value).map_err(|_| BinanceCommandLedgerError::Conflict))
        .transpose()?;
    let command = crate::kol_executor::claimed(row)?;
    let schedule_valid = match command.state {
        ExecutorCommandState::Accepted | ExecutorCommandState::ReconcileRequired => {
            next_reconcile_ms.is_some_and(|value| value > 0)
        }
        ExecutorCommandState::Pending | ExecutorCommandState::Sending => {
            reconcile_attempts == 0 && next_reconcile_ms.is_none()
        }
        ExecutorCommandState::Rejected
        | ExecutorCommandState::Reconciled
        | ExecutorCommandState::Cancelled => false,
    };
    if !schedule_valid {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    Ok(RecoverableBinanceCommand {
        command,
        grid_batch_id,
        dispatch_sequence,
        reconcile_attempts,
        next_reconcile_ms,
    })
}

const fn reconcile_delay_ms(attempt: u32) -> u64 {
    let shift = if attempt > 4 { 4 } else { attempt };
    let delay = RECONCILE_BASE_DELAY_MS << shift;
    if delay > RECONCILE_MAX_DELAY_MS {
        RECONCILE_MAX_DELAY_MS
    } else {
        delay
    }
}

fn invalid_native_order_id(native_order_id: Option<&str>) -> bool {
    native_order_id.is_some_and(|value| {
        value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace)
    })
}

const fn state_name(state: ExecutorCommandState) -> &'static str {
    match state {
        ExecutorCommandState::Pending => "pending",
        ExecutorCommandState::Sending => "sending",
        ExecutorCommandState::Accepted => "accepted",
        ExecutorCommandState::Rejected => "rejected",
        ExecutorCommandState::ReconcileRequired => "reconcile_required",
        ExecutorCommandState::Reconciled => "reconciled",
        ExecutorCommandState::Cancelled => "cancelled",
    }
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

fn unsigned_ms(row: &sqlx::postgres::PgRow, field: &str) -> Result<u64, BinanceCommandLedgerError> {
    u64::try_from(
        row.try_get::<i64, _>(field)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maker_close_reservation_is_bounded_by_the_signed_projection() {
        let source = include_str!("executor_store.rs");
        assert!(source.contains("prior.order_kind='limit_post_only'"));
        assert!(source.contains("prior.command_state='reconciled'"));
        assert!(source.contains("prior.updated_ms>projection.observed_ms"));
        assert!(source.contains("projection.credential_id=$2"));
        assert!(!source.contains(concat!(
            "prior.command_state IN ('pending'",
            ",'sending','accepted','reconcile_required','reconciled')"
        )));
    }

    #[test]
    fn reconciliation_backoff_is_deterministic_and_capped() {
        assert_eq!(reconcile_delay_ms(0), 500);
        assert_eq!(reconcile_delay_ms(1), 1_000);
        assert_eq!(reconcile_delay_ms(2), 2_000);
        assert_eq!(reconcile_delay_ms(3), 4_000);
        assert_eq!(reconcile_delay_ms(4), 8_000);
        assert_eq!(reconcile_delay_ms(31), 8_000);
    }

    #[test]
    fn account_queue_capacity_includes_every_new_command_in_the_same_transaction() {
        assert!(account_queue_has_capacity(0, MAX_ACCOUNT_QUEUE_DEPTH));
        assert!(account_queue_has_capacity(MAX_ACCOUNT_QUEUE_DEPTH - 1, 1));
        assert!(account_queue_has_capacity(MAX_ACCOUNT_QUEUE_DEPTH, 0));
        assert!(!account_queue_has_capacity(MAX_ACCOUNT_QUEUE_DEPTH, 1));
        assert!(!account_queue_has_capacity(MAX_ACCOUNT_QUEUE_DEPTH + 1, 0));
        assert!(!account_queue_has_capacity(15, 2));
    }

    #[test]
    fn unresolved_command_is_not_due_before_its_durable_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = RecoverableBinanceCommand {
            command: ClaimedBinanceCommand {
                origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
                copy_risk: None,
                command_id: "command".into(),
                owner_user_id: "owner".into(),
                trading_account_id: "account".into(),
                credential_id: "credential".into(),
                symbol: "BTC/USDT".parse()?,
                order: ClaimedBinanceOrder::Market {
                    side: venue_domain::domain::OrderSide::Buy,
                    position_side: venue_domain::domain::PositionSide::Long,
                    quantity: Decimal::new(1, 3),
                    reducing: false,
                },
                client_order_id: "client".into(),
                native_order_id: None,
                state: ExecutorCommandState::ReconcileRequired,
            },
            grid_batch_id: None,
            dispatch_sequence: None,
            reconcile_attempts: 2,
            next_reconcile_ms: Some(2_000),
        };
        assert!(!command.reconciliation_due(1_999)?);
        assert!(command.reconciliation_due(2_000)?);
        Ok(())
    }

    #[test]
    fn migration_backfills_and_constrains_the_durable_schedule() {
        for required in [
            "reconcile_attempts",
            "next_reconcile_ms",
            "venue_binance_command_reconcile_schedule_trigger",
            "BETWEEN 0 AND 31",
            "clock_timestamp()",
        ] {
            assert!(MIGRATION_0022.contains(required), "missing {required}");
        }
    }
}
