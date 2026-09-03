//! PostgreSQL command-ledger transitions for the one Binance executor. This module deliberately
//! has no exchange transport: callers can only acquire a committed Pending command once, then
//! settle it through the narrow state machine after their signed exchange readback.

use std::collections::{BTreeMap, VecDeque};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use venue_control_protocol::kol::ExecutorCommandState;
use venue_domain::domain::{FieldState, Fill, OrderSide, PositionSide, Symbol};
use venue_gateway_binance::BinancePrivateFillEvent;

/// Fixed KOL MVP scheduling bounds. One central executor owns these queues; they are data
/// structures, not per-account tasks or durable journals.
pub const MAX_ENABLED_KOLS: usize = 5;
pub const MAX_ENABLED_FOLLOWERS: usize = 200;
pub const MAX_ACCOUNT_QUEUE_DEPTH: usize = 16;
pub const MAX_GLOBAL_IN_FLIGHT: usize = 32;

#[derive(Clone)]
pub struct BinanceCommandLedger {
    pool: PgPool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedBinanceCommand {
    pub command_id: String,
    pub owner_user_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub order: ClaimedBinanceOrder,
    pub client_order_id: String,
    pub native_order_id: Option<String>,
    pub state: ExecutorCommandState,
}

/// One account-serialized claim. Grid commands from the same durable mutation batch are claimed
/// together so their shared exchange preflight can be reused; non-Grid work remains a one-command
/// batch. PostgreSQL has already moved every returned row to `Sending` before this value exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedBinanceBatch {
    pub grid_batch_id: Option<String>,
    /// Present for private-event hot plans. Non-Grid work and cold signed-convergence batches
    /// have no planner generations, but the latter still require a valid durable batch digest.
    pub grid_context: Option<GridBatchDispatchContext>,
    pub commands: Vec<ClaimedBinanceCommand>,
}

/// Durable planner facts bound to a Grid mutation batch. The executor reads this receipt while
/// claiming the command rows; it never reconstructs dispatch authority from an in-memory cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridBatchDispatchContext {
    pub batch_digest: [u8; 32],
    pub private_generation: u64,
    pub private_observed_ms: u64,
    pub instrument_generation: u64,
    pub source_event_received_ms: Option<u64>,
    /// True only when the claim transaction locked the credential's current projection and found
    /// the exact private generation and observation timestamp stored in this batch receipt.
    pub private_projection_current: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimedBinanceOrder {
    Market {
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        reducing: bool,
    },
    LimitPostOnly {
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        price: Decimal,
        reducing: bool,
    },
    CancelExact {
        native_order_id: Option<String>,
        target_client_order_id: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceCommandLedgerError {
    #[error("Binance command ledger is unavailable")]
    Unavailable,
    #[error("Binance command is absent or cannot take this state transition")]
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KolSourceFill {
    pub leader_trading_account_id: String,
    pub native_symbol: String,
    pub native_trade_id: String,
    pub symbol: String,
    pub order_side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub occurred_ms: u64,
    pub observed_ms: u64,
    pub payload_digest: [u8; 32],
}

/// Converts only an adapter-admitted authenticated fill. A missing Hedge Mode leg or invalid
/// time is rejected before it can become a durable copy target.
pub fn source_fill_from_private(
    leader_trading_account_id: &str,
    event: &BinancePrivateFillEvent,
) -> Result<KolSourceFill, BinanceCommandLedgerError> {
    source_fill_from_signed(leader_trading_account_id, &event.fill, event.received_at_ms)
}

/// Converts a fill from the signed REST suffix used after startup, reconnect, or a failed source
/// persistence turn. The same native identity and digest as the live TRADE frame make overlap
/// replay idempotent before the signed cursor is advanced.
pub fn source_fill_from_signed(
    leader_trading_account_id: &str,
    fill: &Fill,
    observed_ms: u64,
) -> Result<KolSourceFill, BinanceCommandLedgerError> {
    let position_side = match fill.position_side {
        FieldState::Known(side @ (PositionSide::Long | PositionSide::Short)) => side,
        FieldState::Known(PositionSide::Net)
        | FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => {
            return Err(BinanceCommandLedgerError::Conflict);
        }
    };
    let occurred_ms = fill
        .exchange_time_ms
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    if leader_trading_account_id.is_empty()
        || fill.fill_id.is_empty()
        || fill.quantity <= Decimal::ZERO
        || observed_ms < occurred_ms
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let mut digest = Sha256::new();
    digest.update(leader_trading_account_id.as_bytes());
    digest.update(fill.symbol.to_string().as_bytes());
    digest.update(fill.fill_id.as_bytes());
    digest.update(fill.order_id.as_bytes());
    digest.update(fill.quantity.to_string().as_bytes());
    digest.update(fill.price.value().to_string().as_bytes());
    let payload_digest = digest.finalize().into();
    Ok(KolSourceFill {
        leader_trading_account_id: leader_trading_account_id.to_owned(),
        native_symbol: fill.symbol.to_string().replace('/', ""),
        native_trade_id: fill.fill_id.clone(),
        symbol: fill.symbol.to_string(),
        order_side: fill.side,
        position_side,
        quantity: fill.quantity,
        price: fill.price.value(),
        occurred_ms,
        observed_ms,
        payload_digest,
    })
}

/// Ratio-only target calculation used after a source fill is persisted. Rule rounding, current
/// price checks and follower position clipping stay at the Binance dispatch boundary.
pub fn scaled_copy_quantity(
    leader_fill_quantity: Decimal,
    follower_allocated_capital: Decimal,
    leader_strategy_capital: Decimal,
    multiplier: Decimal,
) -> Result<Decimal, BinanceCommandLedgerError> {
    if leader_fill_quantity <= Decimal::ZERO
        || follower_allocated_capital <= Decimal::ZERO
        || leader_strategy_capital <= Decimal::ZERO
        || multiplier <= Decimal::ZERO
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    leader_fill_quantity
        .checked_mul(follower_allocated_capital)
        .and_then(|value| value.checked_mul(multiplier))
        .and_then(|value| value.checked_div(leader_strategy_capital))
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(BinanceCommandLedgerError::Conflict)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedCommand<T> {
    pub trading_account_id: String,
    pub command: T,
}

#[derive(Debug)]
struct AccountQueue<T> {
    queued: VecDeque<T>,
    in_flight: bool,
}

/// Central, deterministic round-robin scheduler. It has bounded memory and never runs more
/// than one physical command for an account, while unrelated accounts retain concurrency.
#[derive(Debug)]
pub struct AccountSerialScheduler<T> {
    accounts: BTreeMap<String, AccountQueue<T>>,
    ready: VecDeque<String>,
    max_global_in_flight: usize,
    global_in_flight: usize,
}

impl<T> AccountSerialScheduler<T> {
    #[must_use]
    pub fn new(max_global_in_flight: usize) -> Self {
        Self {
            accounts: BTreeMap::new(),
            ready: VecDeque::new(),
            max_global_in_flight,
            global_in_flight: 0,
        }
    }

    pub fn enqueue(
        &mut self,
        trading_account_id: String,
        command: T,
    ) -> Result<(), BinanceCommandLedgerError> {
        if trading_account_id.is_empty()
            || self.max_global_in_flight == 0
            || (!self.accounts.contains_key(&trading_account_id)
                && self.accounts.len() >= MAX_ENABLED_FOLLOWERS)
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let queue = self
            .accounts
            .entry(trading_account_id.clone())
            .or_insert(AccountQueue {
                queued: VecDeque::new(),
                in_flight: false,
            });
        if queue.queued.len() >= MAX_ACCOUNT_QUEUE_DEPTH {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        queue.queued.push_back(command);
        if !queue.in_flight && !self.ready.iter().any(|value| value == &trading_account_id) {
            self.ready.push_back(trading_account_id);
        }
        Ok(())
    }

    pub fn claim_next(&mut self) -> Option<QueuedCommand<T>> {
        if self.global_in_flight >= self.max_global_in_flight {
            return None;
        }
        while let Some(account_id) = self.ready.pop_front() {
            let queue = self.accounts.get_mut(&account_id)?;
            if queue.in_flight {
                continue;
            }
            let command = queue.queued.pop_front()?;
            queue.in_flight = true;
            self.global_in_flight = self.global_in_flight.saturating_add(1);
            return Some(QueuedCommand {
                trading_account_id: account_id,
                command,
            });
        }
        None
    }

    pub fn settle(&mut self, trading_account_id: &str) -> Result<(), BinanceCommandLedgerError> {
        let queue = self
            .accounts
            .get_mut(trading_account_id)
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        if !queue.in_flight || self.global_in_flight == 0 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        queue.in_flight = false;
        self.global_in_flight -= 1;
        if !queue.queued.is_empty() {
            self.ready.push_back(trading_account_id.to_owned());
        }
        Ok(())
    }

    #[must_use]
    pub const fn in_flight(&self) -> usize {
        self.global_in_flight
    }
}

impl BinanceCommandLedger {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Claims the oldest eligible command for exactly one account. A Sending or
    /// ReconcileRequired command fences all later commands for that account.
    pub async fn claim_next(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<ClaimedBinanceCommand>, BinanceCommandLedgerError> {
        let now = i64::try_from(now_ms).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let row = sqlx::query(
            "WITH candidate AS ( \
             SELECT c.command_id FROM venue_binance_commands c \
             WHERE c.trading_account_id=$1 AND c.command_state='pending' \
             AND (c.command_origin<>'grid' OR EXISTS (SELECT 1 \
                  FROM venue_binance_grid_mutation_batches current_batch \
                  WHERE current_batch.batch_id=c.grid_batch_id \
                    AND (current_batch.predecessor_batch_id IS NULL OR NOT EXISTS (\
                      SELECT 1 FROM venue_binance_commands predecessor \
                      WHERE predecessor.grid_batch_id=current_batch.predecessor_batch_id \
                        AND predecessor.command_state<>'reconciled')))) \
             AND NOT EXISTS (SELECT 1 FROM venue_control_strategy_scopes legacy \
                 WHERE legacy.venue='binance' AND legacy.mode='LIVE' \
                 AND legacy.trading_account_id=c.trading_account_id) \
             AND NOT EXISTS (SELECT 1 FROM venue_binance_commands blocked \
                  WHERE blocked.trading_account_id=c.trading_account_id \
                  AND blocked.command_state IN ('sending','accepted','reconcile_required')) \
             AND (c.command_origin<>'grid' OR (c.command_phase='cancel' AND EXISTS (\
                   SELECT 1 FROM venue_binance_grid_instances lifecycle \
                   WHERE lifecycle.instance_id=c.grid_instance_id AND lifecycle.instance_state IN (\
                     'paused','stop_pending','reset_required','needs_attention'))) OR NOT EXISTS (\
                   SELECT 1 FROM venue_binance_commands dependency \
                   WHERE dependency.grid_batch_id=c.grid_batch_id \
                     AND dependency.order_kind='limit_post_only' \
                     AND dependency.dispatch_sequence<c.dispatch_sequence \
                     AND dependency.command_state<>'reconciled')) \
             ORDER BY c.created_ms,COALESCE(c.grid_batch_id,c.command_id),\
                      COALESCE(c.dispatch_sequence,0),c.command_id \
             LIMIT 1 FOR UPDATE SKIP LOCKED) \
             UPDATE venue_binance_commands c SET command_state='sending',sending_ms=$2,updated_ms=$2 \
             FROM candidate WHERE c.command_id=candidate.command_id \
             RETURNING c.command_id,c.owner_user_id,c.trading_account_id,c.credential_id,c.symbol,c.order_side,c.position_side,c.requested_quantity,c.command_phase,c.order_kind,c.limit_price,c.selected_native_order_id,c.target_client_order_id,c.client_order_id,c.native_order_id,c.command_state",
        )
        .bind(trading_account_id)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        row.map(claimed).transpose()
    }

    /// Atomically claims the oldest eligible command, or the still-pending suffix of its Grid
    /// batch. Marking the complete suffix `Sending` in one commit is the no-replay boundary: a
    /// crash may leave commands requiring signed reconciliation, but can never return an
    /// unattempted child to `Pending` and accidentally POST it after restart.
    pub async fn claim_next_batch(
        &self,
        trading_account_id: &str,
        now_ms: u64,
    ) -> Result<Option<ClaimedBinanceBatch>, BinanceCommandLedgerError> {
        if trading_account_id.is_empty() {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let now = i64::try_from(now_ms).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let credentials = sqlx::query(
            "SELECT credential_id FROM venue_api_credentials \
             WHERE trading_account_id=$1 AND deleted_ms IS NULL \
             ORDER BY credential_id FOR UPDATE",
        )
        .bind(trading_account_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if credentials.is_empty() {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let candidate = sqlx::query(
            "SELECT c.command_id,c.command_origin,c.grid_batch_id,c.dispatch_sequence \
             FROM venue_binance_commands c \
             WHERE c.trading_account_id=$1 AND c.command_state='pending' \
             AND (c.command_origin<>'grid' OR EXISTS (SELECT 1 \
                 FROM venue_binance_grid_mutation_batches current_batch \
                 WHERE current_batch.batch_id=c.grid_batch_id \
                   AND (current_batch.predecessor_batch_id IS NULL OR NOT EXISTS (\
                     SELECT 1 FROM venue_binance_commands predecessor \
                     WHERE predecessor.grid_batch_id=current_batch.predecessor_batch_id \
                       AND predecessor.command_state<>'reconciled')))) \
             AND NOT EXISTS (SELECT 1 FROM venue_control_strategy_scopes legacy \
                 WHERE legacy.venue='binance' AND legacy.mode='LIVE' \
                 AND legacy.trading_account_id=c.trading_account_id) \
             AND NOT EXISTS (SELECT 1 FROM venue_binance_commands blocked \
                 WHERE blocked.trading_account_id=c.trading_account_id \
                 AND blocked.command_state IN ('sending','accepted','reconcile_required')) \
             AND (c.command_origin<>'grid' OR (c.command_phase='cancel' AND EXISTS (\
                 SELECT 1 FROM venue_binance_grid_instances lifecycle \
                 WHERE lifecycle.instance_id=c.grid_instance_id AND lifecycle.instance_state IN (\
                   'paused','stop_pending','reset_required','needs_attention'))) OR NOT EXISTS (\
                 SELECT 1 FROM venue_binance_commands dependency \
                 WHERE dependency.grid_batch_id=c.grid_batch_id \
                   AND dependency.order_kind='limit_post_only' \
                   AND dependency.dispatch_sequence<c.dispatch_sequence \
                   AND dependency.command_state<>'reconciled')) \
             ORDER BY c.created_ms,COALESCE(c.grid_batch_id,c.command_id),\
                      COALESCE(c.dispatch_sequence,0),c.command_id \
             LIMIT 1 FOR UPDATE",
        )
        .bind(trading_account_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let Some(candidate) = candidate else {
            tx.commit()
                .await
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            return Ok(None);
        };
        let command_id: String = candidate
            .try_get("command_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let origin: String = candidate
            .try_get("command_origin")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let grid_batch_id: Option<String> = candidate
            .try_get("grid_batch_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let dispatch_sequence: Option<i64> = candidate
            .try_get("dispatch_sequence")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if (origin == "grid") != grid_batch_id.is_some()
            || (origin == "grid") != dispatch_sequence.is_some()
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let grid_context = match grid_batch_id.as_deref() {
            Some(batch_id) => locked_grid_batch_context(&mut tx, batch_id, &command_id).await?,
            None => None,
        };
        let rows = sqlx::query(
            "WITH selected AS ( \
               SELECT command_id FROM venue_binance_commands \
               WHERE trading_account_id=$1 AND command_state='pending' \
                 AND (($2::text IS NULL AND command_id=$3) OR \
                      ($2::text IS NOT NULL AND grid_batch_id=$2 AND dispatch_sequence>=$4)) \
               ORDER BY COALESCE(dispatch_sequence,0),command_id FOR UPDATE) \
             UPDATE venue_binance_commands c \
             SET command_state='sending',sending_ms=$5,updated_ms=$5 \
             FROM selected WHERE c.command_id=selected.command_id \
             RETURNING c.command_id,c.owner_user_id,c.trading_account_id,c.credential_id,c.symbol,\
               c.order_side,c.position_side,c.requested_quantity,c.command_phase,c.order_kind,\
               c.limit_price,c.selected_native_order_id,c.target_client_order_id,c.client_order_id,\
               c.native_order_id,c.command_state,c.grid_batch_id,c.dispatch_sequence",
        )
        .bind(trading_account_id)
        .bind(grid_batch_id.as_deref())
        .bind(&command_id)
        .bind(dispatch_sequence.unwrap_or(0))
        .bind(now)
        .fetch_all(&mut *tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let batch = claimed_batch(grid_batch_id, grid_context, dispatch_sequence, rows)?;
        tx.commit()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(Some(batch))
    }

    /// A transport timeout or malformed ACK must become ReconcileRequired, never Pending.
    /// Only the allowed durable transitions are accepted, so this method cannot be a retry API.
    pub async fn settle(
        &self,
        command_id: &str,
        next: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        self.settle_with_readback(command_id, next, now_ms, sanitized_error_code, None)
            .await
    }

    /// Persists an exact signed-readback identity together with the state transition. Once an
    /// exchange order identity is observed, a conflicting identity can never replace it.
    pub async fn settle_with_readback(
        &self,
        command_id: &str,
        next: ExecutorCommandState,
        now_ms: u64,
        sanitized_error_code: Option<&str>,
        native_order_id: Option<&str>,
    ) -> Result<(), BinanceCommandLedgerError> {
        if native_order_id.is_some_and(|value| {
            value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace)
        }) {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let now = i64::try_from(now_ms).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let (from, terminal_ms, accepted_ms) = match next {
            ExecutorCommandState::Accepted => ("sending", None, Some(now)),
            ExecutorCommandState::Rejected => ("sending,reconcile_required", Some(now), None),
            ExecutorCommandState::ReconcileRequired => ("sending,accepted", None, None),
            ExecutorCommandState::Reconciled => ("accepted,reconcile_required", Some(now), None),
            ExecutorCommandState::Cancelled => ("pending", Some(now), None),
            ExecutorCommandState::Pending | ExecutorCommandState::Sending => {
                return Err(BinanceCommandLedgerError::Conflict);
            }
        };
        let states = from.split(',').collect::<Vec<_>>();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let changed = sqlx::query(
            "UPDATE venue_binance_commands SET command_state=$1,accepted_ms=COALESCE($2,accepted_ms), \
             terminal_ms=COALESCE($3,terminal_ms),sanitized_error_code=$4, \
             native_order_id=COALESCE(native_order_id,$5),updated_ms=$6 \
             WHERE command_id=$7 AND command_state = ANY($8) \
             AND ($5::text IS NULL OR native_order_id IS NULL OR native_order_id=$5) \
             RETURNING command_id,command_origin,command_phase,order_kind,trading_account_id,grid_instance_id,\
                       symbol,client_order_id,target_client_order_id,selected_native_order_id,\
                       native_order_id",
        )
        .bind(state_name(next))
        .bind(accepted_ms)
        .bind(terminal_ms)
        .bind(sanitized_error_code)
        .bind(native_order_id)
        .bind(now)
        .bind(command_id)
        .bind(states)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let changed = changed.ok_or(BinanceCommandLedgerError::Conflict)?;
        if next == ExecutorCommandState::Reconciled {
            synchronize_grid_owner(&mut tx, &changed, now).await?;
        } else if matches!(
            next,
            ExecutorCommandState::Rejected | ExecutorCommandState::Cancelled
        ) {
            terminalize_unsubmitted_grid_owner(&mut tx, &changed, now).await?;
        }
        tx.commit()
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)
    }
}

async fn terminalize_unsubmitted_grid_owner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    command: &sqlx::postgres::PgRow,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    let origin: String = command
        .try_get("command_origin")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let phase: String = command
        .try_get("command_phase")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let native_order_id: Option<String> = command
        .try_get("native_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    if origin != "grid" || phase == "cancel" || native_order_id.is_some() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE venue_binance_grid_order_owners SET order_state='terminal',\
         last_seen_ms=GREATEST(last_seen_ms,$1) WHERE place_command_id=$2 \
         AND native_order_id IS NULL AND order_state='working'",
    )
    .bind(now)
    .bind(command_id_from_row(command)?)
    .execute(&mut **tx)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GridOwnerSettlement {
    None,
    BindPlace {
        client_order_id: String,
        native_order_id: String,
    },
    TerminalCancel {
        target_client_order_id: String,
        selected_native_order_id: String,
    },
}

async fn synchronize_grid_owner(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    command: &sqlx::postgres::PgRow,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    let origin: String = command
        .try_get("command_origin")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    if origin != "grid" {
        return Ok(());
    }
    let phase: String = command
        .try_get("command_phase")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let kind: String = command
        .try_get("order_kind")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let client_order_id: String = command
        .try_get("client_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let target_client_order_id: Option<String> = command
        .try_get("target_client_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let selected_native_order_id: Option<String> = command
        .try_get("selected_native_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let native_order_id: Option<String> = command
        .try_get("native_order_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let settlement = grid_owner_settlement(
        &phase,
        &kind,
        &client_order_id,
        target_client_order_id.as_deref(),
        selected_native_order_id.as_deref(),
        native_order_id.as_deref(),
    )?;
    let instance_id: String = command
        .try_get::<Option<String>, _>("grid_instance_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let trading_account_id: String = command
        .try_get("trading_account_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let symbol: String = command
        .try_get("symbol")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let affected = match settlement {
        GridOwnerSettlement::None => return Ok(()),
        GridOwnerSettlement::BindPlace {
            client_order_id,
            native_order_id,
        } => sqlx::query(
            "UPDATE venue_binance_grid_order_owners owner \
             SET native_order_id=COALESCE(owner.native_order_id,$1),\
                 last_seen_ms=GREATEST(owner.last_seen_ms,$2) \
             WHERE owner.place_command_id=$3 AND owner.instance_id=$4 \
               AND owner.trading_account_id=$5 AND owner.symbol=$6 \
               AND owner.client_order_id=$7 \
               AND (owner.native_order_id IS NULL OR owner.native_order_id=$1) \
               AND NOT EXISTS (SELECT 1 FROM venue_binance_grid_order_owners conflicting \
                   WHERE conflicting.trading_account_id=owner.trading_account_id \
                     AND conflicting.symbol=owner.symbol AND conflicting.native_order_id=$1 \
                     AND conflicting.client_order_id<>owner.client_order_id)",
        )
        .bind(native_order_id)
        .bind(now)
        .bind(command_id_from_row(command)?)
        .bind(instance_id)
        .bind(trading_account_id)
        .bind(symbol)
        .bind(client_order_id)
        .execute(&mut **tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .rows_affected(),
        GridOwnerSettlement::TerminalCancel {
            target_client_order_id,
            selected_native_order_id,
        } => sqlx::query(
            "UPDATE venue_binance_grid_order_owners owner \
             SET order_state='terminal',last_seen_ms=GREATEST(owner.last_seen_ms,$1) \
             WHERE owner.instance_id=$2 AND owner.trading_account_id=$3 AND owner.symbol=$4 \
               AND owner.client_order_id=$5 AND owner.native_order_id=$6",
        )
        .bind(now)
        .bind(instance_id)
        .bind(trading_account_id)
        .bind(symbol)
        .bind(target_client_order_id)
        .bind(selected_native_order_id)
        .execute(&mut **tx)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .rows_affected(),
    };
    (affected == 1)
        .then_some(())
        .ok_or(BinanceCommandLedgerError::Conflict)
}

fn command_id_from_row(
    command: &sqlx::postgres::PgRow,
) -> Result<String, BinanceCommandLedgerError> {
    command
        .try_get("command_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)
}

fn grid_owner_settlement(
    phase: &str,
    kind: &str,
    client_order_id: &str,
    target_client_order_id: Option<&str>,
    selected_native_order_id: Option<&str>,
    native_order_id: Option<&str>,
) -> Result<GridOwnerSettlement, BinanceCommandLedgerError> {
    match (phase, kind) {
        ("open" | "close", "market") => Ok(GridOwnerSettlement::None),
        ("open" | "close", "limit_post_only") => {
            let native_order_id = native_order_id
                .filter(|value| valid_order_identity(value, 128))
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            valid_order_identity(client_order_id, 36)
                .then_some(GridOwnerSettlement::BindPlace {
                    client_order_id: client_order_id.to_owned(),
                    native_order_id: native_order_id.to_owned(),
                })
                .ok_or(BinanceCommandLedgerError::Conflict)
        }
        ("cancel", "cancel_exact") => {
            let target = target_client_order_id
                .filter(|value| valid_order_identity(value, 36))
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            let selected = selected_native_order_id
                .filter(|value| valid_order_identity(value, 128))
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            if native_order_id.is_some_and(|value| value != selected) {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            Ok(GridOwnerSettlement::TerminalCancel {
                target_client_order_id: target.to_owned(),
                selected_native_order_id: selected.to_owned(),
            })
        }
        _ => Err(BinanceCommandLedgerError::Conflict),
    }
}

fn valid_order_identity(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value.trim() == value
        && !value.chars().any(char::is_whitespace)
}

pub(crate) fn claimed(
    row: sqlx::postgres::PgRow,
) -> Result<ClaimedBinanceCommand, BinanceCommandLedgerError> {
    let state = parse_state(
        &row.try_get::<String, _>("command_state")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
    )?;
    let phase = row
        .try_get::<String, _>("command_phase")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let order = claimed_order(&row, &phase)?;
    let native_order_id = optional_native_id(&row, "native_order_id")?;
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
        symbol: row
            .try_get::<String, _>("symbol")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?
            .parse()
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        order,
        client_order_id: row
            .try_get("client_order_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        native_order_id,
        state,
    })
}

fn claimed_batch(
    grid_batch_id: Option<String>,
    grid_context: Option<GridBatchDispatchContext>,
    first_dispatch_sequence: Option<i64>,
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<ClaimedBinanceBatch, BinanceCommandLedgerError> {
    if rows.is_empty() || rows.len() > MAX_ACCOUNT_QUEUE_DEPTH {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let mut decoded = rows
        .into_iter()
        .map(|row| {
            let row_batch_id = row
                .try_get::<Option<String>, _>("grid_batch_id")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let sequence = row
                .try_get::<Option<i64>, _>("dispatch_sequence")
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
            let command = claimed(row)?;
            Ok((row_batch_id, sequence, command))
        })
        .collect::<Result<Vec<_>, BinanceCommandLedgerError>>()?;
    decoded
        .sort_by_key(|(_, sequence, command)| (sequence.unwrap_or(0), command.command_id.clone()));
    match (&grid_batch_id, &grid_context, first_dispatch_sequence) {
        (None, None, None) => {
            if decoded.len() != 1 || decoded[0].0.is_some() || decoded[0].1.is_some() {
                return Err(BinanceCommandLedgerError::Conflict);
            }
        }
        (Some(batch_id), _, Some(first)) if !batch_id.is_empty() && (1..=16).contains(&first) => {
            let first_command = decoded
                .first()
                .map(|(_, _, command)| command)
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            let mut cancellation_seen = false;
            for (index, (row_batch_id, sequence, command)) in decoded.iter().enumerate() {
                let expected = first
                    .checked_add(
                        i64::try_from(index).map_err(|_| BinanceCommandLedgerError::Conflict)?,
                    )
                    .ok_or(BinanceCommandLedgerError::Conflict)?;
                if row_batch_id.as_deref() != Some(batch_id)
                    || *sequence != Some(expected)
                    || command.owner_user_id != first_command.owner_user_id
                    || command.trading_account_id != first_command.trading_account_id
                    || command.credential_id != first_command.credential_id
                    || command.symbol != first_command.symbol
                {
                    return Err(BinanceCommandLedgerError::Conflict);
                }
                match &command.order {
                    ClaimedBinanceOrder::CancelExact { .. } => cancellation_seen = true,
                    ClaimedBinanceOrder::Market { .. }
                    | ClaimedBinanceOrder::LimitPostOnly { .. }
                        if cancellation_seen =>
                    {
                        return Err(BinanceCommandLedgerError::Conflict);
                    }
                    ClaimedBinanceOrder::Market { .. }
                    | ClaimedBinanceOrder::LimitPostOnly { .. } => {}
                }
            }
        }
        _ => return Err(BinanceCommandLedgerError::Conflict),
    }
    Ok(ClaimedBinanceBatch {
        grid_batch_id,
        grid_context,
        commands: decoded.into_iter().map(|(_, _, command)| command).collect(),
    })
}

async fn locked_grid_batch_context(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    grid_batch_id: &str,
    command_id: &str,
) -> Result<Option<GridBatchDispatchContext>, BinanceCommandLedgerError> {
    let row = sqlx::query(
        "SELECT batch.batch_digest,batch.private_generation,batch.private_observed_ms,\
                batch.instrument_generation,batch.source_event_received_ms,\
                command.credential_id,command.owner_user_id,command.trading_account_id \
         FROM venue_binance_grid_mutation_batches batch \
         JOIN venue_binance_commands command ON command.grid_batch_id=batch.batch_id \
         WHERE batch.batch_id=$1 AND command.command_id=$2 \
         FOR UPDATE OF batch",
    )
    .bind(grid_batch_id)
    .bind(command_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?
    .ok_or(BinanceCommandLedgerError::Conflict)?;
    let context = validated_grid_batch_context(
        row.try_get("batch_digest")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        row.try_get("private_generation")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        row.try_get("private_observed_ms")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        row.try_get("instrument_generation")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
        row.try_get("source_event_received_ms")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
    )?;
    let Some(mut context) = context else {
        return Ok(None);
    };
    let credential_id: String = row
        .try_get("credential_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let owner_user_id: String = row
        .try_get("owner_user_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let trading_account_id: String = row
        .try_get("trading_account_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    context.private_projection_current = locked_private_projection_matches(
        tx,
        &credential_id,
        &owner_user_id,
        &trading_account_id,
        &context,
    )
    .await?;
    Ok(Some(context))
}

async fn locked_private_projection_matches(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    credential_id: &str,
    owner_user_id: &str,
    trading_account_id: &str,
    context: &GridBatchDispatchContext,
) -> Result<bool, BinanceCommandLedgerError> {
    let projection = sqlx::query(
        "SELECT owner_user_id,trading_account_id,observed_ms,private_generation \
         FROM venue_binance_account_projections WHERE credential_id=$1 FOR UPDATE",
    )
    .bind(credential_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let Some(projection) = projection else {
        return Ok(false);
    };
    let current_owner_user_id: String = projection
        .try_get("owner_user_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let current_trading_account_id: String = projection
        .try_get("trading_account_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let current_observed_ms = projection
        .try_get::<i64, _>("observed_ms")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let current_private_generation = projection
        .try_get::<i64, _>("private_generation")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    Ok(current_owner_user_id == owner_user_id
        && current_trading_account_id == trading_account_id
        && u64::try_from(current_observed_ms).ok() == Some(context.private_observed_ms)
        && u64::try_from(current_private_generation).ok() == Some(context.private_generation))
}

fn validated_grid_batch_context(
    batch_digest: Vec<u8>,
    private_generation: Option<i64>,
    private_observed_ms: Option<i64>,
    instrument_generation: Option<i64>,
    source_event_received_ms: Option<i64>,
) -> Result<Option<GridBatchDispatchContext>, BinanceCommandLedgerError> {
    let batch_digest = batch_digest
        .try_into()
        .map_err(|_| BinanceCommandLedgerError::Conflict)?;
    let (private_generation, private_observed_ms, instrument_generation) = match (
        private_generation,
        private_observed_ms,
        instrument_generation,
    ) {
        (None, None, None) if source_event_received_ms.is_none() => return Ok(None),
        (Some(private), Some(observed), Some(instrument)) => (private, observed, instrument),
        _ => return Err(BinanceCommandLedgerError::Conflict),
    };
    let private_generation = u64::try_from(private_generation)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let private_observed_ms = u64::try_from(private_observed_ms)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let instrument_generation = u64::try_from(instrument_generation)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let source_event_received_ms = source_event_received_ms
        .map(u64::try_from)
        .transpose()
        .map_err(|_| BinanceCommandLedgerError::Conflict)?;
    if source_event_received_ms.is_some_and(|value| value < private_observed_ms) {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    Ok(Some(GridBatchDispatchContext {
        batch_digest,
        private_generation,
        private_observed_ms,
        instrument_generation,
        source_event_received_ms,
        private_projection_current: false,
    }))
}

fn claimed_order(
    row: &sqlx::postgres::PgRow,
    phase: &str,
) -> Result<ClaimedBinanceOrder, BinanceCommandLedgerError> {
    match row
        .try_get::<String, _>("order_kind")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_str()
    {
        "market" => Ok(ClaimedBinanceOrder::Market {
            side: required_order_side(row)?,
            position_side: required_position_side(row)?,
            quantity: required_quantity(row)?,
            reducing: phase == "close",
        }),
        "limit_post_only" => Ok(ClaimedBinanceOrder::LimitPostOnly {
            side: required_order_side(row)?,
            position_side: required_position_side(row)?,
            quantity: required_quantity(row)?,
            price: optional_decimal(row, "limit_price")?
                .ok_or(BinanceCommandLedgerError::Unavailable)?,
            reducing: phase == "close",
        }),
        "cancel_exact" if phase == "cancel" => {
            let native_order_id = optional_native_id(row, "selected_native_order_id")?;
            let target_client_order_id = optional_native_id(row, "target_client_order_id")?;
            if native_order_id.is_none() && target_client_order_id.is_none() {
                return Err(BinanceCommandLedgerError::Unavailable);
            }
            Ok(ClaimedBinanceOrder::CancelExact {
                native_order_id,
                target_client_order_id,
            })
        }
        _ => Err(BinanceCommandLedgerError::Unavailable),
    }
}

fn required_order_side(
    row: &sqlx::postgres::PgRow,
) -> Result<OrderSide, BinanceCommandLedgerError> {
    match row
        .try_get::<Option<String>, _>("order_side")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_deref()
    {
        Some("buy") => Ok(OrderSide::Buy),
        Some("sell") => Ok(OrderSide::Sell),
        _ => Err(BinanceCommandLedgerError::Unavailable),
    }
}

fn required_position_side(
    row: &sqlx::postgres::PgRow,
) -> Result<PositionSide, BinanceCommandLedgerError> {
    match row
        .try_get::<Option<String>, _>("position_side")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_deref()
    {
        Some("long") => Ok(PositionSide::Long),
        Some("short") => Ok(PositionSide::Short),
        _ => Err(BinanceCommandLedgerError::Unavailable),
    }
}

fn required_quantity(row: &sqlx::postgres::PgRow) -> Result<Decimal, BinanceCommandLedgerError> {
    row.try_get::<Option<String>, _>("requested_quantity")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .ok_or(BinanceCommandLedgerError::Unavailable)?
        .parse()
        .map_err(|_| BinanceCommandLedgerError::Unavailable)
}

fn optional_native_id(
    row: &sqlx::postgres::PgRow,
    field: &str,
) -> Result<Option<String>, BinanceCommandLedgerError> {
    let value = row
        .try_get::<Option<String>, _>(field)
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    if value.as_deref().is_some_and(|value| {
        value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace)
    }) {
        return Err(BinanceCommandLedgerError::Unavailable);
    }
    Ok(value)
}

fn optional_decimal(
    row: &sqlx::postgres::PgRow,
    field: &str,
) -> Result<Option<Decimal>, BinanceCommandLedgerError> {
    row.try_get::<Option<String>, _>(field)
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .map(|value| {
            value
                .parse()
                .map_err(|_| BinanceCommandLedgerError::Unavailable)
        })
        .transpose()
}

fn state_name(state: ExecutorCommandState) -> &'static str {
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

fn parse_state(value: &str) -> Result<ExecutorCommandState, BinanceCommandLedgerError> {
    match value {
        "pending" => Ok(ExecutorCommandState::Pending),
        "sending" => Ok(ExecutorCommandState::Sending),
        "accepted" => Ok(ExecutorCommandState::Accepted),
        "rejected" => Ok(ExecutorCommandState::Rejected),
        "reconcile_required" => Ok(ExecutorCommandState::ReconcileRequired),
        "reconciled" => Ok(ExecutorCommandState::Reconciled),
        "cancelled" => Ok(ExecutorCommandState::Cancelled),
        _ => Err(BinanceCommandLedgerError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::{Fill, Price};

    #[test]
    fn scheduler_serializes_each_account_but_allows_other_accounts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut scheduler = AccountSerialScheduler::new(2);
        scheduler.enqueue("account-a".into(), 1_u8)?;
        scheduler.enqueue("account-a".into(), 2_u8)?;
        scheduler.enqueue("account-b".into(), 3_u8)?;
        let first = scheduler.claim_next().ok_or("first command absent")?;
        let second = scheduler.claim_next().ok_or("second command absent")?;
        assert_ne!(first.trading_account_id, second.trading_account_id);
        assert!(scheduler.claim_next().is_none());
        scheduler.settle(&first.trading_account_id)?;
        let third = scheduler.claim_next().ok_or("third command absent")?;
        assert_eq!(third.trading_account_id, first.trading_account_id);
        assert_eq!(third.command, 2);
        Ok(())
    }

    #[test]
    fn scheduler_rejects_unbounded_account_backlog() -> Result<(), Box<dyn std::error::Error>> {
        let mut scheduler = AccountSerialScheduler::new(1);
        for value in 0..MAX_ACCOUNT_QUEUE_DEPTH {
            scheduler.enqueue("account-a".into(), value)?;
        }
        assert_eq!(
            scheduler.enqueue("account-a".into(), 99).err(),
            Some(BinanceCommandLedgerError::Conflict)
        );
        Ok(())
    }

    #[test]
    fn copy_ratio_is_positive_and_overflow_safe() {
        assert_eq!(
            scaled_copy_quantity(
                Decimal::new(5, 3),
                Decimal::new(100, 0),
                Decimal::new(1_000, 0),
                Decimal::new(2, 0),
            ),
            Ok(Decimal::new(1, 3))
        );
        assert!(
            scaled_copy_quantity(Decimal::ZERO, Decimal::ONE, Decimal::ONE, Decimal::ONE).is_err()
        );
    }

    #[test]
    fn authenticated_hedge_fill_is_the_only_source_fill_admitted()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = BinancePrivateFillEvent {
            stream_private_generation: 3,
            private_generation: 3,
            received_at_ms: 200,
            fill: Fill {
                fill_id: "trade-7".into(),
                execution_sequence: FieldState::Known(7),
                order_id: "order-7".into(),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::new(1, 3),
                price: Price::new(Decimal::new(100_000, 0))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Missing,
                exchange_time_ms: Some(199),
            },
            client_order_id: FieldState::Missing,
            original_quantity: FieldState::Missing,
            cumulative_filled_quantity: FieldState::Missing,
            order_state: FieldState::Missing,
        };
        let source = source_fill_from_private("leader-a", &event)?;
        assert_eq!(source.native_symbol, "BTCUSDT");
        assert_eq!(source.position_side, PositionSide::Long);
        assert_eq!(source.observed_ms, 200);
        let mut invalid = event;
        invalid.fill.position_side = FieldState::Known(PositionSide::Net);
        assert_eq!(
            source_fill_from_private("leader-a", &invalid).err(),
            Some(BinanceCommandLedgerError::Conflict)
        );
        Ok(())
    }

    #[test]
    fn grid_batch_claim_sql_requires_prior_places_and_keeps_lifecycle_cancel_override() {
        let source = include_str!("kol_executor.rs");
        assert!(source.contains("dependency.dispatch_sequence<c.dispatch_sequence"));
        assert!(source.contains("dependency.command_state<>'reconciled'"));
        assert!(source.contains("dependency.order_kind='limit_post_only'"));
        assert!(source.contains("'paused','stop_pending','reset_required','needs_attention'"));
        assert!(source.contains("COALESCE(c.dispatch_sequence,0)"));
        assert!(
            source.contains(
                "FROM venue_binance_account_projections WHERE credential_id=$1 FOR UPDATE"
            )
        );
    }

    #[test]
    fn grid_batch_dispatch_context_requires_complete_valid_durable_facts() {
        let context =
            validated_grid_batch_context(vec![7_u8; 32], Some(11), Some(100), Some(13), Some(101));
        assert_eq!(
            context,
            Ok(Some(GridBatchDispatchContext {
                batch_digest: [7_u8; 32],
                private_generation: 11,
                private_observed_ms: 100,
                instrument_generation: 13,
                source_event_received_ms: Some(101),
                private_projection_current: false,
            }))
        );
        assert_eq!(
            validated_grid_batch_context(vec![7_u8; 32], None, None, None, None),
            Ok(None)
        );
        assert_eq!(
            validated_grid_batch_context(vec![7_u8; 31], None, None, None, None),
            Err(BinanceCommandLedgerError::Conflict)
        );
        assert_eq!(
            validated_grid_batch_context(vec![7_u8; 32], None, Some(1), Some(1), None),
            Err(BinanceCommandLedgerError::Conflict)
        );
        assert_eq!(
            validated_grid_batch_context(vec![7_u8; 32], Some(1), Some(2), Some(1), Some(1)),
            Err(BinanceCommandLedgerError::Conflict)
        );
    }

    #[test]
    fn grid_owner_policy_binds_places_and_targets_only_exact_cancels() {
        assert_eq!(
            grid_owner_settlement(
                "open",
                "limit_post_only",
                "grid-place-1",
                None,
                None,
                Some("native-1"),
            ),
            Ok(GridOwnerSettlement::BindPlace {
                client_order_id: "grid-place-1".into(),
                native_order_id: "native-1".into(),
            })
        );
        assert_eq!(
            grid_owner_settlement(
                "cancel",
                "cancel_exact",
                "grid-cancel-1",
                Some("grid-place-1"),
                Some("native-1"),
                Some("native-1"),
            ),
            Ok(GridOwnerSettlement::TerminalCancel {
                target_client_order_id: "grid-place-1".into(),
                selected_native_order_id: "native-1".into(),
            })
        );
        assert_eq!(
            grid_owner_settlement(
                "cancel",
                "cancel_exact",
                "grid-cancel-1",
                Some("grid-place-1"),
                Some("native-1"),
                Some("native-other"),
            ),
            Err(BinanceCommandLedgerError::Conflict)
        );
        assert_eq!(
            grid_owner_settlement("open", "limit_post_only", "grid-place-1", None, None, None),
            Err(BinanceCommandLedgerError::Conflict)
        );
        assert_eq!(
            grid_owner_settlement("open", "market", "grid-market-1", None, None, None),
            Ok(GridOwnerSettlement::None)
        );
    }

    #[test]
    fn reconciled_owner_sync_is_transactional_and_does_not_apply_to_other_states() {
        let source = include_str!("kol_executor.rs");
        let settle_start = source
            .find("pub async fn settle_with_readback")
            .expect("settlement entry");
        let settle_end = source[settle_start..]
            .find("enum GridOwnerSettlement")
            .map(|offset| settle_start + offset)
            .expect("settlement boundary");
        let settle = &source[settle_start..settle_end];
        let begin = settle
            .find(".pool\n            .begin()")
            .expect("transaction begin");
        let command_update = settle
            .find("UPDATE venue_binance_commands SET command_state")
            .expect("command update");
        let owner_sync = settle
            .find("synchronize_grid_owner(&mut tx")
            .expect("owner sync");
        let commit = settle.find("tx.commit()").expect("transaction commit");
        assert!(begin < command_update && command_update < owner_sync && owner_sync < commit);
        assert!(source.contains("if next == ExecutorCommandState::Reconciled"));
        assert!(source.contains("owner.place_command_id=$3"));
        assert!(source.contains("owner.client_order_id=$5 AND owner.native_order_id=$6"));
        assert!(source.contains("SET native_order_id=COALESCE(owner.native_order_id,$1)"));
        assert!(source.contains("SET order_state='terminal'"));
    }

    #[test]
    fn grid_claims_are_gated_by_the_durable_predecessor_chain() {
        let source = include_str!("kol_executor.rs");
        let claim = source
            .find("pub async fn claim_next_batch")
            .map(|start| &source[start..])
            .expect("batch claim");
        assert!(claim.contains("current_batch.predecessor_batch_id IS NULL"));
        assert!(claim.contains("predecessor.command_state<>'reconciled'"));
        assert!(claim.contains("FOR UPDATE OF batch"));
    }
}
