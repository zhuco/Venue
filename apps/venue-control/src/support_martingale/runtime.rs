//! Support-martingale orchestration inside the shared multi-venue executor.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;
use venue_control_protocol::support_martingale::{
    SupportMartingaleHealth, SupportMartingaleInstance, SupportMartingaleLifecycle,
};
use venue_domain::{
    CancelCommand, CommandId, ExecutionCommand, LimitTimeInForce, MarketOrderCommand, OrderCommand,
    OrderOwner, OrderPurpose, OrderSide, OrderState, PositionSide, Price, Symbol,
};
use venue_execution::SignedAccountSnapshot;

use crate::multi_venue_credentials::StrategyCredentialStore;
use crate::multi_venue_store::MultiVenueStoreError;

use super::{
    BinanceReferenceClient, Plan, PlannerInput, ReferenceSnapshot, SupportMartingaleCommandKind,
    SupportMartingaleRuntimeSymbolState, SupportMartingaleStore, TakeProfitOrder, plan,
    plan_take_profit_only,
};

const REFERENCE_CACHE_MS: u64 = 10_000;
const SIGNAL_SEND_AGE_MS: u64 = 30_000;

#[derive(Clone)]
pub struct SupportMartingaleRuntime {
    store: SupportMartingaleStore,
    credentials: StrategyCredentialStore,
    reference: BinanceReferenceClient,
    cache: Arc<Mutex<BTreeMap<String, CachedReference>>>,
}

#[derive(Clone)]
struct CachedReference {
    symbols: Vec<Symbol>,
    snapshot: ReferenceSnapshot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SupportSendKind {
    Entry,
    Add,
    TakeProfit,
    CancelTakeProfit,
    StopLoss,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SupportSendFence {
    kind: SupportSendKind,
    price_ceiling: Option<Decimal>,
    stop_floor: Option<Decimal>,
}

impl SupportSendFence {
    pub(crate) fn market_allows(&self, price: Decimal) -> bool {
        self.price_ceiling.is_none_or(|ceiling| price <= ceiling)
            && self.stop_floor.is_none_or(|floor| price > floor)
    }

    pub(crate) fn validates(
        &self,
        command: &ExecutionCommand,
        snapshot: &SignedAccountSnapshot,
    ) -> bool {
        let symbol = &command.mutation_owner().symbol;
        let long = snapshot
            .positions()
            .iter()
            .filter(|position| {
                position.symbol == *symbol && position.position_side == PositionSide::Long
            })
            .collect::<Vec<_>>();
        let long_quantity = match long.as_slice() {
            [] => Decimal::ZERO,
            [position] => position.quantity,
            _ => return false,
        };
        let has_short = snapshot.positions().iter().any(|position| {
            position.symbol == *symbol
                && position.position_side == PositionSide::Short
                && !position.quantity.is_zero()
        });
        if has_short {
            return false;
        }
        let has_reduce = snapshot
            .open_orders()
            .iter()
            .any(|order| order.symbol == *symbol && order.reduce_only);
        match self.kind {
            SupportSendKind::Entry => long_quantity.is_zero() && !has_reduce,
            SupportSendKind::Add => long_quantity > Decimal::ZERO && !has_reduce,
            SupportSendKind::TakeProfit | SupportSendKind::StopLoss => {
                long_quantity > Decimal::ZERO && !has_reduce
            }
            SupportSendKind::CancelTakeProfit => true,
        }
    }
}

struct PendingCommand {
    command_id: String,
    kind: String,
    state: String,
    command: ExecutionCommand,
}

struct RestingTakeProfit {
    command_id: String,
    command: ExecutionCommand,
    observed_fill: Decimal,
}

impl SupportMartingaleRuntime {
    pub fn new(
        pool: PgPool,
        credentials: StrategyCredentialStore,
    ) -> Result<Self, MultiVenueStoreError> {
        let reference = BinanceReferenceClient::new(Duration::from_secs(8), 5_000)
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        Ok(Self {
            store: SupportMartingaleStore::new(pool),
            credentials,
            reference,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn account_turn(&self, account: &str) -> Result<(), MultiVenueStoreError> {
        for instance_id in self
            .store
            .active_instances(Some(account))
            .await
            .map_err(map_store)?
        {
            let owner: String = sqlx::query_scalar(
                "SELECT owner_user_id FROM venue_support_martingale_instances WHERE instance_id=$1 AND trading_account_id=$2",
            )
            .bind(&instance_id)
            .bind(account)
            .fetch_one(&self.store.pool)
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
            self.instance_turn(&owner, &instance_id).await?;
        }
        Ok(())
    }

    async fn instance_turn(
        &self,
        owner: &str,
        instance_id: &str,
    ) -> Result<(), MultiVenueStoreError> {
        let mut instance = self
            .store
            .get(owner, instance_id)
            .await
            .map_err(map_store)?;
        let mut runtime = self
            .store
            .load_runtime_state(owner, instance_id)
            .await
            .map_err(map_store)?;
        for symbol in instance.config.symbols.clone() {
            let Some(runtime_symbol) = runtime
                .symbols
                .iter()
                .find(|state| state.symbol == symbol)
                .cloned()
            else {
                self.store
                    .mark_health(
                        instance_id,
                        SupportMartingaleHealth::NeedsAttention,
                        Some("symbol_state_missing"),
                        now_ms()?,
                    )
                    .await
                    .map_err(map_store)?;
                continue;
            };
            if runtime_symbol.pending_command_id.is_some() {
                self.settle_pending(owner, instance_id, &symbol).await?;
                instance = self
                    .store
                    .get(owner, instance_id)
                    .await
                    .map_err(map_store)?;
                runtime = self
                    .store
                    .load_runtime_state(owner, instance_id)
                    .await
                    .map_err(map_store)?;
                if runtime
                    .symbols
                    .iter()
                    .find(|state| state.symbol == symbol)
                    .is_some_and(|state| state.pending_command_id.is_some())
                {
                    return Ok(());
                }
            }
            self.symbol_turn(owner, &instance, &runtime, &symbol)
                .await?;
            if self
                .account_has_unresolved(&instance.trading_account_id)
                .await?
            {
                return Ok(());
            }
            instance = self
                .store
                .get(owner, instance_id)
                .await
                .map_err(map_store)?;
            runtime = self
                .store
                .load_runtime_state(owner, instance_id)
                .await
                .map_err(map_store)?;
        }
        Ok(())
    }

    async fn symbol_turn(
        &self,
        owner: &str,
        instance: &SupportMartingaleInstance,
        _runtime: &super::SupportMartingaleRuntimeState,
        symbol: &Symbol,
    ) -> Result<(), MultiVenueStoreError> {
        let state = instance
            .symbols
            .iter()
            .find(|state| state.symbol == *symbol)
            .ok_or(MultiVenueStoreError::Conflict)?;
        let resting = self
            .resting_take_profit(&instance.instance_id, symbol)
            .await?;
        let queries = resting
            .as_ref()
            .map(|order| vec![order.command.clone()])
            .unwrap_or_default();
        let (snapshot, market, observations) = self
            .credentials
            .grid_facts(owner, &instance.credential_id, symbol.clone(), queries)
            .await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let now = now_ms()?;
        let observed_take_profit_terminal = observations
            .first()
            .is_some_and(|observation| terminal(observation.state));
        if let (Some(resting), Some(observation)) = (resting.as_ref(), observations.first()) {
            self.store
                .observe_take_profit(
                    owner,
                    &resting.command_id,
                    observation.filled_quantity,
                    observed_take_profit_terminal,
                    now,
                )
                .await
                .map_err(map_store)?;
        }
        if snapshot
            .open_orders()
            .iter()
            .any(|order| order.symbol == *symbol && order.external)
        {
            self.store
                .mark_health(
                    &instance.instance_id,
                    SupportMartingaleHealth::NeedsAttention,
                    Some("external_open_order"),
                    now,
                )
                .await
                .map_err(map_store)?;
            return Ok(());
        }
        let positions = snapshot
            .positions()
            .iter()
            .filter(|position| position.symbol == *symbol && !position.quantity.is_zero())
            .collect::<Vec<_>>();
        if positions
            .iter()
            .any(|position| position.position_side != PositionSide::Long)
            || positions.len() > 1
        {
            self.store
                .mark_health(
                    &instance.instance_id,
                    SupportMartingaleHealth::NeedsAttention,
                    Some("unexpected_position"),
                    now,
                )
                .await
                .map_err(map_store)?;
            return Ok(());
        }
        let position = positions.first().copied();
        if state.layer == 0 && state.quantity.is_zero() && position.is_some() {
            self.store
                .mark_health(
                    &instance.instance_id,
                    SupportMartingaleHealth::NeedsAttention,
                    Some("external_position"),
                    now,
                )
                .await
                .map_err(map_store)?;
            return Ok(());
        }
        let current_tp = resting
            .as_ref()
            .filter(|_| !observed_take_profit_terminal)
            .and_then(|resting| {
                let ExecutionCommand::PlaceLimit(order) = &resting.command else {
                    return None;
                };
                Some(TakeProfitOrder {
                    client_order_id: order.client_order_id.as_str().to_owned(),
                    price: order.limit_price.value(),
                    quantity: order.quantity,
                    filled_quantity: observations
                        .first()
                        .map(|value| value.filled_quantity)
                        .unwrap_or(resting.observed_fill),
                })
            });
        let (quantity, average, invested, pnl) = match position {
            Some(position) => {
                let average = position.entry_price.ok_or(MultiVenueStoreError::Conflict)?;
                let invested = average
                    .checked_mul(position.quantity)
                    .ok_or(MultiVenueStoreError::Conflict)?;
                let pnl = position
                    .mark_price
                    .and_then(|mark| mark.checked_sub(average))
                    .and_then(|delta| delta.checked_mul(position.quantity));
                (position.quantity, Some(average), invested, pnl)
            }
            None => (Decimal::ZERO, None, Decimal::ZERO, Some(Decimal::ZERO)),
        };
        self.store
            .sync_symbol_facts(
                owner,
                &instance.instance_id,
                symbol,
                quantity,
                average,
                invested,
                current_tp.as_ref().map(|value| value.price),
                pnl,
                now,
            )
            .await
            .map_err(map_store)?;
        let instance = self
            .store
            .get(owner, &instance.instance_id)
            .await
            .map_err(map_store)?;
        let runtime = self
            .store
            .load_runtime_state(owner, &instance.instance_id)
            .await
            .map_err(map_store)?;
        let state = instance
            .symbols
            .iter()
            .find(|state| state.symbol == *symbol)
            .ok_or(MultiVenueStoreError::Conflict)?;
        let runtime_state = runtime
            .symbols
            .iter()
            .find(|state| state.symbol == *symbol)
            .ok_or(MultiVenueStoreError::Conflict)?;
        if let Some(stop) =
            super::stop_loss::stop_loss_plan(&instance, symbol, &snapshot, current_tp.as_ref(), now)
        {
            return self
                .apply_plan(
                    owner,
                    &instance,
                    runtime_state,
                    resting.as_ref().filter(|_| !observed_take_profit_terminal),
                    stop,
                    current_tp.as_ref(),
                    now,
                )
                .await;
        }
        if state.status == "sl_failed" {
            return Ok(());
        }
        if quantity > Decimal::ZERO
            && current_tp.is_none()
            && runtime_state.health_reason.as_deref() != Some("external_position")
        {
            if runtime_state
                .cooldown_until_ms
                .is_some_and(|until| now < until)
            {
                return Ok(());
            }
            if matches!(
                instance.lifecycle,
                SupportMartingaleLifecycle::IncreasePaused | SupportMartingaleLifecycle::Draining
            ) {
                let plan = plan_take_profit_only(&instance, symbol, &snapshot, &market);
                return self
                    .apply_plan(owner, &instance, runtime_state, None, plan, None, now)
                    .await;
            }
        }
        let permits_risk = match instance.lifecycle {
            SupportMartingaleLifecycle::Running => true,
            SupportMartingaleLifecycle::EntryPaused => quantity > Decimal::ZERO,
            SupportMartingaleLifecycle::IncreasePaused
            | SupportMartingaleLifecycle::Draining
            | SupportMartingaleLifecycle::Stopped => false,
        };
        if !permits_risk {
            if quantity > Decimal::ZERO && current_tp.is_none() {
                let tp = plan_take_profit_only(&instance, symbol, &snapshot, &market);
                return self
                    .apply_plan(owner, &instance, runtime_state, None, tp, None, now)
                    .await;
            }
            return Ok(());
        }
        if runtime_state
            .cooldown_until_ms
            .is_some_and(|until| now < until)
        {
            return Ok(());
        }
        let reference_result = if instance.config.entry_mode
            == venue_control_protocol::support_martingale::MartingaleEntryMode::FixedPrice
        {
            Ok(ReferenceSnapshot {
                fetched_at_ms: now,
                btc_environment: Vec::new(),
                symbols: BTreeMap::new(),
            })
        } else {
            self.reference_snapshot(&instance, now).await
        };
        let reference = match reference_result {
            Ok(value) => value,
            Err(error) => {
                if quantity > Decimal::ZERO && current_tp.is_none() {
                    let tp = plan_take_profit_only(&instance, symbol, &snapshot, &market);
                    return self
                        .apply_plan(owner, &instance, runtime_state, None, tp, None, now)
                        .await;
                }
                self.store
                    .mark_health(
                        &instance.instance_id,
                        SupportMartingaleHealth::Unavailable,
                        Some(error),
                        now,
                    )
                    .await
                    .map_err(map_store)?;
                return Ok(());
            }
        };
        if instance.health != SupportMartingaleHealth::Healthy
            || runtime_state.health_reason.is_some()
        {
            self.store
                .mark_health(
                    &instance.instance_id,
                    SupportMartingaleHealth::Healthy,
                    None,
                    now,
                )
                .await
                .map_err(map_store)?;
        }
        let consumed = self
            .store
            .consumed_supports(
                &instance.instance_id,
                symbol,
                runtime_state.cycle_id.as_deref(),
            )
            .await
            .map_err(map_store)?;
        let input = PlannerInput {
            instance: &instance,
            symbol,
            reference: &reference,
            account: &snapshot,
            execution_market: &market,
            consumed_supports: &consumed,
            last_support_lower: runtime_state.last_support_lower,
            current_take_profit: current_tp.as_ref(),
            prefer_add_after_cancel: state.status == "add_ready",
            now_ms: now,
        };
        let mut planned = plan(&input);
        if state.status == "add_ready" && !matches!(planned, Plan::MarketEntry { .. }) {
            planned = plan_take_profit_only(&instance, symbol, &snapshot, &market);
        }
        self.apply_plan(
            owner,
            &instance,
            runtime_state,
            resting.as_ref().filter(|_| !observed_take_profit_terminal),
            planned,
            current_tp.as_ref(),
            now,
        )
        .await
    }

    async fn apply_plan(
        &self,
        owner: &str,
        instance: &SupportMartingaleInstance,
        state: &SupportMartingaleRuntimeSymbolState,
        resting: Option<&RestingTakeProfit>,
        plan: Plan,
        _take_profit: Option<&TakeProfitOrder>,
        now: u64,
    ) -> Result<(), MultiVenueStoreError> {
        match plan {
            Plan::Noop(_) => Ok(()),
            Plan::MarketEntry {
                symbol,
                support_id,
                support_lower,
                support_upper,
                layer,
                notional,
                quantity,
                ..
            } => {
                let cycle = state.cycle_id.clone().unwrap_or_else(|| {
                    format!(
                        "c{}{}",
                        instance.revision,
                        state.decision_sequence.saturating_add(1)
                    )
                });
                let kind = if layer == 0 {
                    SupportMartingaleCommandKind::Entry
                } else {
                    SupportMartingaleCommandKind::Add
                };
                let command = market_command(
                    instance,
                    &symbol,
                    &cycle,
                    state.decision_sequence.saturating_add(1),
                    quantity,
                )?;
                let request = command.command_id().as_str().to_owned();
                self.store
                    .enqueue_command(
                        owner,
                        &instance.instance_id,
                        &symbol,
                        kind,
                        Some(&cycle),
                        Some(&support_id),
                        Some((support_lower, support_upper)),
                        &request,
                        notional,
                        command,
                        now,
                    )
                    .await
                    .map_err(map_store)?;
                Ok(())
            }
            Plan::CancelTakeProfit {
                symbol,
                client_order_id,
                for_stop_loss,
            } => {
                let resting = resting
                    .filter(|value| {
                        value
                            .command
                            .native_client_id()
                            .is_some_and(|id| id.as_str() == client_order_id)
                    })
                    .ok_or(MultiVenueStoreError::Conflict)?;
                let command = cancel_command(
                    instance,
                    &resting.command,
                    state.decision_sequence.saturating_add(1),
                )?;
                let request = command.command_id().as_str().to_owned();
                self.store
                    .enqueue_command(
                        owner,
                        &instance.instance_id,
                        &symbol,
                        if for_stop_loss {
                            SupportMartingaleCommandKind::CancelForStopLoss
                        } else {
                            SupportMartingaleCommandKind::CancelTakeProfit
                        },
                        state.cycle_id.as_deref(),
                        None,
                        None,
                        &request,
                        Decimal::ZERO,
                        command,
                        now,
                    )
                    .await
                    .map_err(map_store)?;
                Ok(())
            }
            Plan::MarketStopLoss {
                symbol,
                quantity,
                position_generation,
            } => {
                let cycle = state
                    .cycle_id
                    .as_deref()
                    .ok_or(MultiVenueStoreError::Conflict)?;
                let id = identity(
                    instance,
                    &symbol,
                    "stop",
                    state.decision_sequence.saturating_add(1),
                )?;
                let request = id.as_str().to_owned();
                let command = ExecutionCommand::MarketReduce(venue_domain::MarketReduceCommand {
                    command_id: id.clone(),
                    client_order_id: id.clone(),
                    risk_episode_id: id,
                    owner: self::owner(instance, &symbol, cycle, OrderPurpose::Protection),
                    position_side: PositionSide::Long,
                    side: OrderSide::Sell,
                    quantity,
                    position_generation,
                });
                self.store
                    .enqueue_command(
                        owner,
                        &instance.instance_id,
                        &symbol,
                        SupportMartingaleCommandKind::StopLoss,
                        Some(cycle),
                        None,
                        None,
                        &request,
                        Decimal::ZERO,
                        command,
                        now,
                    )
                    .await
                    .map_err(map_store)?;
                Ok(())
            }
            Plan::LimitTakeProfit {
                symbol,
                price,
                quantity,
                ..
            } => {
                let cycle = state
                    .cycle_id
                    .as_deref()
                    .ok_or(MultiVenueStoreError::Conflict)?;
                let command = take_profit_command(
                    instance,
                    &symbol,
                    cycle,
                    state.decision_sequence.saturating_add(1),
                    quantity,
                    price,
                )?;
                let request = command.command_id().as_str().to_owned();
                self.store
                    .enqueue_command(
                        owner,
                        &instance.instance_id,
                        &symbol,
                        SupportMartingaleCommandKind::TakeProfit,
                        Some(cycle),
                        None,
                        None,
                        &request,
                        Decimal::ZERO,
                        command,
                        now,
                    )
                    .await
                    .map_err(map_store)?;
                Ok(())
            }
        }
    }

    async fn settle_pending(
        &self,
        owner: &str,
        instance_id: &str,
        symbol: &Symbol,
    ) -> Result<(), MultiVenueStoreError> {
        let Some(pending) = self.pending_command(instance_id, symbol).await? else {
            return Ok(());
        };
        match pending.state.as_str() {
            "pending" | "sending" | "accepted" | "reconcile_required" => return Ok(()),
            "rejected" | "cancelled" => {
                self.store
                    .settle_command(owner, &pending.command_id, Decimal::ZERO, true, now_ms()?)
                    .await
                    .map_err(map_store)?;
            }
            "reconciled" if matches!(pending.kind.as_str(), "cancel_tp" | "cancel_sl_tp") => {
                self.store
                    .settle_command(owner, &pending.command_id, Decimal::ZERO, true, now_ms()?)
                    .await
                    .map_err(map_store)?;
            }
            "reconciled" => {
                let instance = self
                    .store
                    .get(owner, instance_id)
                    .await
                    .map_err(map_store)?;
                let (_, _, observations) = self
                    .credentials
                    .grid_facts(
                        owner,
                        &instance.credential_id,
                        symbol.clone(),
                        vec![pending.command.clone()],
                    )
                    .await
                    .map_err(|_| MultiVenueStoreError::Unavailable)?;
                let observation = observations.first().ok_or(MultiVenueStoreError::Conflict)?;
                let order_terminal = terminal(observation.state);
                if matches!(pending.kind.as_str(), "entry" | "add" | "sl") && !order_terminal {
                    return Ok(());
                }
                self.store
                    .settle_command(
                        owner,
                        &pending.command_id,
                        observation.filled_quantity,
                        order_terminal,
                        now_ms()?,
                    )
                    .await
                    .map_err(map_store)?;
            }
            _ => return Err(MultiVenueStoreError::Conflict),
        }
        Ok(())
    }

    async fn pending_command(
        &self,
        instance_id: &str,
        symbol: &Symbol,
    ) -> Result<Option<PendingCommand>, MultiVenueStoreError> {
        let row = sqlx::query("SELECT c.command_id,c.kind,b.command_state,b.strategy_command FROM venue_support_martingale_commands c JOIN venue_binance_commands b USING(command_id) WHERE c.instance_id=$1 AND c.symbol=$2 AND NOT c.ledger_settled ORDER BY c.created_ms LIMIT 2")
            .bind(instance_id).bind(symbol.to_string()).fetch_all(&self.store.pool).await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if row.len() > 1 {
            return Err(MultiVenueStoreError::Conflict);
        }
        row.into_iter()
            .next()
            .map(|row| {
                Ok(PendingCommand {
                    command_id: row
                        .try_get("command_id")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                    kind: row
                        .try_get("kind")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                    state: row
                        .try_get("command_state")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                    command: serde_json::from_value(
                        row.try_get("strategy_command")
                            .map_err(|_| MultiVenueStoreError::Conflict)?,
                    )
                    .map_err(|_| MultiVenueStoreError::Conflict)?,
                })
            })
            .transpose()
    }

    async fn resting_take_profit(
        &self,
        instance_id: &str,
        symbol: &Symbol,
    ) -> Result<Option<RestingTakeProfit>, MultiVenueStoreError> {
        let rows = sqlx::query("SELECT c.command_id,c.observed_fill::text AS observed_fill,b.strategy_command FROM venue_support_martingale_commands c JOIN venue_binance_commands b USING(command_id) WHERE c.instance_id=$1 AND c.symbol=$2 AND c.kind='tp' AND c.ledger_settled AND NOT c.terminal ORDER BY c.created_ms DESC LIMIT 2")
            .bind(instance_id).bind(symbol.to_string()).fetch_all(&self.store.pool).await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        if rows.len() > 1 {
            return Err(MultiVenueStoreError::Conflict);
        }
        rows.into_iter()
            .next()
            .map(|row| {
                Ok(RestingTakeProfit {
                    command_id: row
                        .try_get("command_id")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                    observed_fill: decimal_text(
                        row.try_get("observed_fill")
                            .map_err(|_| MultiVenueStoreError::Conflict)?,
                    )?,
                    command: serde_json::from_value(
                        row.try_get("strategy_command")
                            .map_err(|_| MultiVenueStoreError::Conflict)?,
                    )
                    .map_err(|_| MultiVenueStoreError::Conflict)?,
                })
            })
            .transpose()
    }

    async fn reference_snapshot(
        &self,
        instance: &SupportMartingaleInstance,
        now: u64,
    ) -> Result<ReferenceSnapshot, &'static str> {
        {
            let cache = self.cache.lock().await;
            if let Some(cached) = cache.get(&instance.instance_id) {
                if cached.symbols == instance.config.symbols
                    && now.saturating_sub(cached.snapshot.fetched_at_ms) <= REFERENCE_CACHE_MS
                {
                    return Ok(cached.snapshot.clone());
                }
            }
        }
        let generation = now
            .checked_div(REFERENCE_CACHE_MS)
            .and_then(|value| value.checked_add(1))
            .ok_or("reference_generation")?;
        let snapshot = self
            .reference
            .fetch_snapshot(&instance.config.symbols, now, generation)
            .await
            .map_err(|_| "binance_reference_unavailable")?;
        let mut cache = self.cache.lock().await;
        cache.insert(
            instance.instance_id.clone(),
            CachedReference {
                symbols: instance.config.symbols.clone(),
                snapshot: snapshot.clone(),
            },
        );
        Ok(snapshot)
    }

    async fn account_has_unresolved(&self, account: &str) -> Result<bool, MultiVenueStoreError> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(account).fetch_one(&self.store.pool).await.map_err(|_| MultiVenueStoreError::Unavailable)
    }

    pub(crate) async fn send_fence(
        &self,
        command: &ExecutionCommand,
        now: u64,
    ) -> Result<Option<SupportSendFence>, MultiVenueStoreError> {
        let row = sqlx::query("SELECT c.kind,c.created_ms,i.lifecycle,i.health,i.config,s.last_support_upper::text AS last_support_upper,s.average_price::text AS average_price FROM venue_support_martingale_commands c JOIN venue_support_martingale_instances i USING(instance_id) JOIN venue_support_martingale_symbol_states s ON s.instance_id=c.instance_id AND s.symbol=c.symbol WHERE c.command_id=$1 AND c.instance_id=$2 AND c.symbol=$3")
            .bind(command.command_id().as_str()).bind(&command.mutation_owner().strategy_instance_id)
            .bind(command.mutation_owner().symbol.to_string()).fetch_optional(&self.store.pool).await
            .map_err(|_| MultiVenueStoreError::Unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let kind: String = row
            .try_get("kind")
            .map_err(|_| MultiVenueStoreError::Conflict)?;
        let created: i64 = row
            .try_get("created_ms")
            .map_err(|_| MultiVenueStoreError::Conflict)?;
        let created = u64::try_from(created).map_err(|_| MultiVenueStoreError::Conflict)?;
        let lifecycle: String = row
            .try_get("lifecycle")
            .map_err(|_| MultiVenueStoreError::Conflict)?;
        let health: String = row
            .try_get("health")
            .map_err(|_| MultiVenueStoreError::Conflict)?;
        let kind = match kind.as_str() {
            "entry" if lifecycle == "running" && health == "healthy" => SupportSendKind::Entry,
            "add"
                if matches!(lifecycle.as_str(), "running" | "entry_paused")
                    && health == "healthy" =>
            {
                SupportSendKind::Add
            }
            "tp" if lifecycle != "stopped" => SupportSendKind::TakeProfit,
            "cancel_tp" | "cancel_sl_tp" if lifecycle != "stopped" => {
                SupportSendKind::CancelTakeProfit
            }
            "sl" if lifecycle != "stopped" => SupportSendKind::StopLoss,
            _ => return Err(MultiVenueStoreError::Conflict),
        };
        if matches!(kind, SupportSendKind::Entry | SupportSendKind::Add)
            && (now < created || now.saturating_sub(created) > SIGNAL_SEND_AGE_MS)
        {
            return Err(MultiVenueStoreError::Conflict);
        }
        let mut price_ceiling = None;
        let mut stop_floor = None;
        if matches!(kind, SupportSendKind::Entry | SupportSendKind::Add) {
            let config: venue_control_protocol::support_martingale::SupportMartingaleConfig =
                serde_json::from_value(
                    row.try_get("config")
                        .map_err(|_| MultiVenueStoreError::Conflict)?,
                )
                .map_err(|_| MultiVenueStoreError::Conflict)?;
            if config.entry_mode
                == venue_control_protocol::support_martingale::MartingaleEntryMode::FixedPrice
            {
                let value: String = row
                    .try_get("last_support_upper")
                    .map_err(|_| MultiVenueStoreError::Conflict)?;
                price_ceiling = Some(value.parse().map_err(|_| MultiVenueStoreError::Conflict)?);
            }
            if let Some(parameters) = config
                .symbol_parameters
                .iter()
                .find(|p| p.symbol == command.mutation_owner().symbol)
            {
                let average: Option<String> = row
                    .try_get("average_price")
                    .map_err(|_| MultiVenueStoreError::Conflict)?;
                let average = average
                    .map(|s| s.parse::<Decimal>())
                    .transpose()
                    .map_err(|_| MultiVenueStoreError::Conflict)?;
                stop_floor = parameters.stop_loss.and_then(|sl| {
                    sl.trigger_price(average.or(parameters.entry_price).unwrap_or_default())
                });
            }
        }
        Ok(Some(SupportSendFence {
            kind,
            price_ceiling,
            stop_floor,
        }))
    }
}

fn terminal(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    )
}

fn market_command(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    cycle: &str,
    sequence: u64,
    quantity: Decimal,
) -> Result<ExecutionCommand, MultiVenueStoreError> {
    let id = identity(instance, symbol, "market", sequence)?;
    Ok(ExecutionCommand::PlaceMarket(MarketOrderCommand {
        command_id: id.clone(),
        client_order_id: id,
        owner: owner(instance, symbol, cycle, OrderPurpose::Entry),
        position_side: PositionSide::Long,
        side: OrderSide::Buy,
        quantity,
        reduce_only: false,
    }))
}

fn take_profit_command(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    cycle: &str,
    sequence: u64,
    quantity: Decimal,
    price: Price,
) -> Result<ExecutionCommand, MultiVenueStoreError> {
    let id = identity(instance, symbol, "tp", sequence)?;
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: id.clone(),
        client_order_id: id,
        owner: owner(instance, symbol, cycle, OrderPurpose::TakeProfit),
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity,
        limit_price: price,
        time_in_force: LimitTimeInForce::Gtc,
        reduce_only: true,
    }))
}

fn cancel_command(
    instance: &SupportMartingaleInstance,
    target: &ExecutionCommand,
    sequence: u64,
) -> Result<ExecutionCommand, MultiVenueStoreError> {
    let id = identity(
        instance,
        &target.mutation_owner().symbol,
        "cancel",
        sequence,
    )?;
    Ok(ExecutionCommand::Cancel(CancelCommand {
        command_id: id,
        owner: target.mutation_owner().clone(),
        target_client_order_id: target
            .native_client_id()
            .ok_or(MultiVenueStoreError::Conflict)?
            .clone(),
    }))
}

fn owner(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    cycle: &str,
    purpose: OrderPurpose,
) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: instance.instance_id.clone(),
        run_id: cycle.to_owned(),
        exchange: instance.execution_venue.as_str().to_owned(),
        account: instance.trading_account_id.clone(),
        symbol: symbol.clone(),
        purpose,
    }
}

fn identity(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    purpose: &str,
    sequence: u64,
) -> Result<CommandId, MultiVenueStoreError> {
    let raw = format!(
        "{}:{}:{}:{purpose}:{sequence}",
        instance.instance_id, instance.revision, symbol
    );
    let digest = Sha256::digest(raw.as_bytes());
    let value: String = digest[..14]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    CommandId::new(format!("sm{value}")).map_err(|_| MultiVenueStoreError::Invalid)
}

fn decimal_text(value: String) -> Result<Decimal, MultiVenueStoreError> {
    value.parse().map_err(|_| MultiVenueStoreError::Conflict)
}

fn now_ms() -> Result<u64, MultiVenueStoreError> {
    crate::multi_venue_runtime::now_ms().map_err(|_| MultiVenueStoreError::Unavailable)
}

fn map_store(error: super::SupportMartingaleStoreError) -> MultiVenueStoreError {
    match error {
        super::SupportMartingaleStoreError::Invalid => MultiVenueStoreError::Invalid,
        super::SupportMartingaleStoreError::Conflict => MultiVenueStoreError::Conflict,
        super::SupportMartingaleStoreError::Unavailable => MultiVenueStoreError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_execution::SignedAccountPositionMode;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    #[test]
    fn fixed_entry_rechecks_ceiling_and_stop_floor_before_sending() {
        let fence = SupportSendFence {
            kind: SupportSendKind::Entry,
            price_ceiling: Some(Decimal::from(100)),
            stop_floor: Some(Decimal::from(90)),
        };
        assert!(fence.market_allows(Decimal::from(100)));
        assert!(fence.market_allows(Decimal::from(95)));
        assert!(!fence.market_allows(Decimal::from(101)));
        assert!(!fence.market_allows(Decimal::from(90)));
    }

    #[test]
    fn add_send_fence_rejects_a_filled_take_profit_race() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol = Symbol::new("SOL", "USDT")?;
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            symbol.clone(),
        )?;
        let snapshot = SignedAccountSnapshot::complete(
            binding,
            1,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            vec![],
            vec![],
            "cursor".into(),
            vec![],
        )?;
        let command = ExecutionCommand::PlaceMarket(MarketOrderCommand {
            command_id: CommandId::new("command1")?,
            client_order_id: CommandId::new("client1")?,
            owner: OrderOwner {
                strategy_instance_id: "instance1".into(),
                run_id: "cycle1".into(),
                exchange: "bybit".into(),
                account: "00000000-0000-4000-8000-000000000001".into(),
                symbol,
                purpose: OrderPurpose::Entry,
            },
            position_side: PositionSide::Long,
            side: OrderSide::Buy,
            quantity: Decimal::ONE,
            reduce_only: false,
        });
        assert!(
            !SupportSendFence {
                kind: SupportSendKind::Add,
                price_ceiling: None,
                stop_floor: None
            }
            .validates(&command, &snapshot)
        );
        Ok(())
    }
}
