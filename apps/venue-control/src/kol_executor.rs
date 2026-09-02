//! PostgreSQL command-ledger transitions for the one Binance executor. This module deliberately
//! has no exchange transport: callers can only acquire a committed Pending command once, then
//! settle it through the narrow state machine after their signed exchange readback.

use std::collections::{BTreeMap, VecDeque};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use venue_control_protocol::kol::ExecutorCommandState;
use venue_domain::domain::{FieldState, OrderSide, PositionSide};
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
    pub client_order_id: String,
    pub state: ExecutorCommandState,
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
    let position_side = match event.fill.position_side {
        FieldState::Known(side @ (PositionSide::Long | PositionSide::Short)) => side,
        FieldState::Known(PositionSide::Net)
        | FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => {
            return Err(BinanceCommandLedgerError::Conflict);
        }
    };
    let occurred_ms = event
        .fill
        .exchange_time_ms
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    if leader_trading_account_id.is_empty()
        || event.fill.fill_id.is_empty()
        || event.fill.quantity <= Decimal::ZERO
        || event.received_at_ms < occurred_ms
    {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let mut digest = Sha256::new();
    digest.update(leader_trading_account_id.as_bytes());
    digest.update(event.fill.symbol.to_string().as_bytes());
    digest.update(event.fill.fill_id.as_bytes());
    digest.update(event.fill.order_id.as_bytes());
    digest.update(event.fill.quantity.to_string().as_bytes());
    digest.update(event.fill.price.value().to_string().as_bytes());
    let payload_digest = digest.finalize().into();
    Ok(KolSourceFill {
        leader_trading_account_id: leader_trading_account_id.to_owned(),
        native_symbol: event.fill.symbol.to_string().replace('/', ""),
        native_trade_id: event.fill.fill_id.clone(),
        symbol: event.fill.symbol.to_string(),
        order_side: event.fill.side,
        position_side,
        quantity: event.fill.quantity,
        price: event.fill.price.value(),
        occurred_ms,
        observed_ms: event.received_at_ms,
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
             AND NOT EXISTS (SELECT 1 FROM venue_binance_commands blocked \
                 WHERE blocked.trading_account_id=c.trading_account_id \
                 AND blocked.command_state IN ('sending','reconcile_required')) \
             ORDER BY c.created_ms,c.command_id LIMIT 1 FOR UPDATE SKIP LOCKED) \
             UPDATE venue_binance_commands c SET command_state='sending',sending_ms=$2,updated_ms=$2 \
             FROM candidate WHERE c.command_id=candidate.command_id \
             RETURNING c.command_id,c.owner_user_id,c.trading_account_id,c.credential_id,c.client_order_id,c.command_state",
        )
        .bind(trading_account_id)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        row.map(claimed).transpose()
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
        let now = i64::try_from(now_ms).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let (from, terminal_ms, accepted_ms) = match next {
            ExecutorCommandState::Accepted => ("sending", None, Some(now)),
            ExecutorCommandState::Rejected => ("sending", Some(now), None),
            ExecutorCommandState::ReconcileRequired => ("sending,accepted", None, None),
            ExecutorCommandState::Reconciled => ("accepted,reconcile_required", Some(now), None),
            ExecutorCommandState::Cancelled => ("pending", Some(now), None),
            ExecutorCommandState::Pending | ExecutorCommandState::Sending => {
                return Err(BinanceCommandLedgerError::Conflict);
            }
        };
        let states = from.split(',').collect::<Vec<_>>();
        let changed = sqlx::query(
            "UPDATE venue_binance_commands SET command_state=$1,accepted_ms=COALESCE($2,accepted_ms), \
             terminal_ms=COALESCE($3,terminal_ms),sanitized_error_code=$4,updated_ms=$5 \
             WHERE command_id=$6 AND command_state = ANY($7)",
        )
        .bind(state_name(next))
        .bind(accepted_ms)
        .bind(terminal_ms)
        .bind(sanitized_error_code)
        .bind(now)
        .bind(command_id)
        .bind(states)
        .execute(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        (changed.rows_affected() == 1)
            .then_some(())
            .ok_or(BinanceCommandLedgerError::Conflict)
    }
}

fn claimed(row: sqlx::postgres::PgRow) -> Result<ClaimedBinanceCommand, BinanceCommandLedgerError> {
    let state = parse_state(
        &row.try_get::<String, _>("command_state")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?,
    )?;
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
}
