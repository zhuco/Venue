//! Stateless Binance Grid convergence driver for the singleton executor. PostgreSQL and signed
//! exchange projections are the only durable facts. This module deliberately
//! owns no Actor, checkpoint, local WAL, writer lease, or order-takeover path.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    grid::{GridAnchor, GridInstanceState, GridOrderRole as ProtocolOrderRole},
    kol::{
        ExecutorCommandState, ExecutorOrderKind, TerminalAccountProjection, TerminalOpenOrder,
        TerminalPositionMode,
    },
};
use venue_domain::domain::{
    AccountRiskSnapshot, Amount, Asset, LegRiskSnapshot, PositionSide, Price, RiskSourceStatus,
};
use venue_gateway_binance::{
    BinanceGridBootstrapMarketFacts, BinanceGridMarketReader, BinanceTransportLimits,
    GatewayBinding, GatewayMode, VenueId,
};
use venue_strategies::hedged_grid::{
    GridBestBook, GridCloseReservations, GridConvergenceFacts, GridExposureReduction,
    GridInstrumentLimits, GridInventoryAdjustment, GridMakerFill, GridOrderIntent, GridOrderKey,
    GridPlanDirective, GridPlanner, GridPlannerConfig, GridPlannerControl, GridPlannerInput,
    GridPosition, GridProfitReductionPolicy, GridReplenishmentPolicy, GridResetPolicy,
    GridRiskConversion, GridRiskFacts, GridRollingAnchor,
};

use crate::{
    BinanceGridStore, GridCommandIntent, GridConvergenceUpdate, GridDesiredOrder,
    GridDesiredSurface, GridFillAllocation, GridLedgerCommand, GridOrderOwnership,
    GridOwnedOrderState, GridRuntimeRecord, GridStoreError,
    executor_runtime::CommandWake,
    grid_store::GridPlanMutationBatch,
    private_projection::{BinancePrivateProjectionStore, PrivateProjectionError},
};

const GRID_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const MAX_FILL_BATCH: u16 = 1_000;
const RULE_VERSION_PREFIX: &str = "binance-pm-um-grid";

#[path = "grid_runtime/support.rs"]
mod support;
use support::*;
#[path = "grid_runtime/batch.rs"]
mod batch;
use batch::*;
#[path = "grid_runtime/driver.rs"]
mod driver;
#[path = "grid_runtime/fast_path.rs"]
mod fast_path;
#[path = "grid_runtime/fills.rs"]
mod fills;
#[path = "grid_runtime/stream_overlay.rs"]
mod stream_overlay;
use fast_path::GridHotPathState;
pub use fast_path::{GRID_PRIVATE_STREAM_CHANNEL_CAPACITY, GridPrivateStreamSignal};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceGridRuntimeError {
    #[error("grid storage is unavailable")]
    Store,
    #[error("signed private projection is unavailable")]
    PrivateProjection,
    #[error("Binance public market evidence is unavailable")]
    Market,
    #[error("grid facts are invalid or incomplete")]
    Facts,
    #[error("grid planning failed")]
    Planner,
    #[error("signed order surface conflicts with durable Grid ownership")]
    SurfaceConflict,
    #[error("system clock is unavailable")]
    Clock,
}

impl From<GridStoreError> for BinanceGridRuntimeError {
    fn from(error: GridStoreError) -> Self {
        eprintln!("Binance Grid storage operation failed: {error}");
        Self::Store
    }
}

impl From<PrivateProjectionError> for BinanceGridRuntimeError {
    fn from(_: PrivateProjectionError) -> Self {
        Self::PrivateProjection
    }
}

/// The single-process coordinator. Physical mutations are only enqueued into the existing
/// PostgreSQL command ledger; the account-serial executor remains the sole network writer.
pub struct BinanceGridRuntime {
    store: BinanceGridStore,
    projections: BinancePrivateProjectionStore,
    transport_limits: BinanceTransportLimits,
    markets: BTreeMap<String, BinanceGridMarketReader>,
    hot_path: GridHotPathState,
}

impl BinanceGridRuntime {
    #[must_use]
    pub fn new(
        store: BinanceGridStore,
        projections: BinancePrivateProjectionStore,
        transport_limits: BinanceTransportLimits,
    ) -> Self {
        Self::with_hot_path(
            store,
            projections,
            transport_limits,
            None,
            None,
            CommandWake::new(),
            crate::GridHotDispatchCache::new(),
        )
    }

    #[must_use]
    pub fn with_private_stream(
        store: BinanceGridStore,
        projections: BinancePrivateProjectionStore,
        transport_limits: BinanceTransportLimits,
        private_stream: tokio::sync::mpsc::Receiver<GridPrivateStreamSignal>,
        recovery_requests: tokio::sync::mpsc::Sender<String>,
        command_wake: CommandWake,
        hot_dispatch: crate::GridHotDispatchCache,
    ) -> Self {
        Self::with_hot_path(
            store,
            projections,
            transport_limits,
            Some(private_stream),
            Some(recovery_requests),
            command_wake,
            hot_dispatch,
        )
    }

    fn with_hot_path(
        store: BinanceGridStore,
        projections: BinancePrivateProjectionStore,
        transport_limits: BinanceTransportLimits,
        private_stream: Option<tokio::sync::mpsc::Receiver<GridPrivateStreamSignal>>,
        recovery_requests: Option<tokio::sync::mpsc::Sender<String>>,
        command_wake: CommandWake,
        hot_dispatch: crate::GridHotDispatchCache,
    ) -> Self {
        Self {
            store,
            projections,
            transport_limits,
            markets: BTreeMap::new(),
            hot_path: GridHotPathState::new(
                private_stream,
                recovery_requests,
                command_wake,
                hot_dispatch,
            ),
        }
    }

    pub async fn run_once(&mut self) -> Result<usize, BinanceGridRuntimeError> {
        let records = self.store.list_runtime_instances().await?;
        self.hot_path.replace_records(&records);
        let mut progressed = 0_usize;
        for record in records {
            match self.process(record.clone()).await {
                Ok(changed) => progressed = progressed.saturating_add(usize::from(changed)),
                Err(error) => {
                    eprintln!(
                        "Binance Grid instance {} turn failed: {error}",
                        record.instance.instance_id
                    );
                    let code = match error {
                        BinanceGridRuntimeError::Market => "market_unavailable",
                        BinanceGridRuntimeError::PrivateProjection => "private_unavailable",
                        BinanceGridRuntimeError::Facts => "facts_invalid",
                        BinanceGridRuntimeError::Planner => "planner_invalid",
                        BinanceGridRuntimeError::SurfaceConflict => {
                            if matches!(
                                record.instance.state,
                                GridInstanceState::StartPending
                                    | GridInstanceState::Running
                                    | GridInstanceState::Blocked
                            ) {
                                let _ = self
                                    .store
                                    .settle_runtime_state(
                                        &record.instance.instance_id,
                                        record.instance.state,
                                        GridInstanceState::ResetRequired,
                                        Some("surface_conflict"),
                                        now_ms()?,
                                    )
                                    .await;
                            } else if matches!(
                                record.instance.state,
                                GridInstanceState::Paused
                                    | GridInstanceState::StopPending
                                    | GridInstanceState::ResetRequired
                            ) {
                                let _ = self
                                    .store
                                    .settle_runtime_state(
                                        &record.instance.instance_id,
                                        record.instance.state,
                                        GridInstanceState::NeedsAttention,
                                        Some("surface_conflict"),
                                        now_ms()?,
                                    )
                                    .await;
                            }
                            continue;
                        }
                        BinanceGridRuntimeError::Store | BinanceGridRuntimeError::Clock => continue,
                    };
                    let _ = self.block_if_running(&record, code, now_ms()?).await;
                }
            }
        }
        Ok(progressed)
    }

    async fn process(
        &mut self,
        mut record: GridRuntimeRecord,
    ) -> Result<bool, BinanceGridRuntimeError> {
        match record.instance.state {
            GridInstanceState::Paused if !record.instance.dirty => return Ok(false),
            GridInstanceState::NeedsAttention => return Ok(false),
            GridInstanceState::Draft | GridInstanceState::Stopped => return Ok(false),
            _ => {}
        }
        let now = now_ms()?;
        let Some(projection) = self
            .projections
            .load_owned(&record.owner_user_id, &record.instance.credential_id)
            .await?
        else {
            if self.settle_lifecycle_timeout(&record, now).await? {
                return Ok(true);
            }
            self.block_if_running(&record, "private_missing", now)
                .await?;
            return Ok(false);
        };
        if projection.trading_account_id != record.instance.trading_account_id
            || projection.credential_id != record.instance.credential_id
            || projection.position_mode != TerminalPositionMode::Hedge
            || projection.observed_ms > now
            || now.saturating_sub(projection.observed_ms)
                > record.instance.config.reset_policy.stale_private_ms
        {
            if self.settle_lifecycle_timeout(&record, now).await? {
                return Ok(true);
            }
            self.block_if_running(&record, "private_stale", now).await?;
            return Ok(false);
        }
        let ownership = self
            .store
            .load_owned_orders(&record.instance.instance_id)
            .await?;
        let mut actual = self
            .synchronize_actual_surface(&record, &projection, ownership, now)
            .await?;

        match record.instance.state {
            GridInstanceState::Paused => {
                return self.finish_pause(&record, &projection, &actual, now).await;
            }
            GridInstanceState::StopPending => {
                return self.finish_stop(&record, &projection, &actual, now).await;
            }
            GridInstanceState::ResetRequired => {
                return self.finish_reset(&record, &projection, &actual, now).await;
            }
            _ => {}
        }
        self.add_command_reservations(&record, &projection, &mut actual)
            .await?;
        let private = private_facts(&record, &projection, &actual)?;
        if record.instance.dirty {
            let desired = self
                .store
                .load_desired_orders(&record.instance.instance_id)
                .await?;
            if let Some(desired) = desired.as_ref()
                && !desired.orders.is_empty()
            {
                let market = self.refresh_market(&record, now).await?;
                if !desired_valid_for_market(&record, &desired, &market) {
                    self.store
                        .settle_runtime_state(
                            &record.instance.instance_id,
                            record.instance.state,
                            GridInstanceState::ResetRequired,
                            Some("desired_facts_changed"),
                            now,
                        )
                        .await?;
                    return Ok(true);
                }
                let running = self.ensure_running(&record, now).await?;
                let result = self
                    .reconcile_desired(&running, &projection, &actual, &desired, now)
                    .await?;
                if matches!(
                    &result,
                    ReconcileResult::Converged | ReconcileResult::FactsChanged
                ) {
                    self.allocate_included_fills(&running, desired.plan_revision)
                        .await?;
                }
                return self
                    .finish_reconcile(&running, &projection, &desired, result, now)
                    .await;
            }
            let (current, desired) = self
                .prepare_empty_surface(&record, &projection, desired, now)
                .await?;
            if !actual.orders.is_empty() {
                let result = self
                    .reconcile_desired(&current, &projection, &actual, &desired, now)
                    .await?;
                return self
                    .finish_reconcile(&current, &projection, &desired, result, now)
                    .await;
            }
            let Some(updated) = self
                .market_plan_ready(&current, &projection, &desired, now)
                .await?
            else {
                return Ok(false);
            };
            record = updated;
        }

        let market = self.refresh_market(&record, now).await?;
        let risk = self.risk_facts(&record, &projection, &private, now).await?;
        // Market and quote-to-USD evidence are timestamped after their asynchronous HTTP
        // responses arrive. Re-sample the planner clock so fresh evidence cannot appear to come
        // from the future merely because this turn began before those requests completed.
        let now = now_ms()?;
        let fills = self
            .load_fill_batch(&record, &actual, projection.observed_ms)
            .await?;
        let planner_fills = fills
            .iter()
            .filter(|fill| fill.config_revision == record.instance.config_revision)
            .cloned()
            .collect::<Vec<_>>();
        let fill_totals = if planner_fills.is_empty() {
            BTreeMap::new()
        } else {
            self.store
                .load_grid_fill_totals(&record.instance.instance_id)
                .await?
        };
        let plan = GridPlanner::plan(&GridPlannerInput {
            config: planner_config(&record)?,
            instrument: market
                .rules
                .metadata()
                .map_err(|_| BinanceGridRuntimeError::Facts)?,
            instrument_limits: GridInstrumentLimits {
                minimum_quantity: market.rules.minimum_quantity,
                maximum_quantity: market.rules.maximum_quantity,
                minimum_price: Price::new(market.rules.minimum_price)
                    .map_err(|_| BinanceGridRuntimeError::Facts)?,
                maximum_price: Price::new(market.rules.maximum_price)
                    .map_err(|_| BinanceGridRuntimeError::Facts)?,
            },
            book: GridBestBook {
                bid: market.bid,
                ask: market.ask,
                observed_at_ms: market.observed_at_ms,
            },
            inventory: private.inventory,
            owned_orders: actual.intents.clone(),
            maker_fills: maker_fill_hints(&planner_fills, &actual, &fill_totals)?,
            pending_place_keys: BTreeSet::new(),
            other_close_reservations: actual.other_close_reservations.clone(),
            rolling_anchor: record
                .instance
                .anchor
                .as_ref()
                .map(|anchor| planner_anchor(anchor, record.instance.config_revision))
                .transpose()?,
            convergence: GridConvergenceFacts {
                pending_since_ms: record.instance.convergence_started_ms,
                consecutive_failures: u32::from(record.instance.consecutive_failures),
            },
            risk,
            control: GridPlannerControl::Run,
            now_ms: now,
        })
        .map_err(|_| BinanceGridRuntimeError::Planner)?;

        if requires_private_surface_retry(&plan.directive) {
            self.block_if_running(&record, "private_surface_unsettled", now)
                .await?;
            return Ok(false);
        }
        match plan.directive {
            GridPlanDirective::Blocked { reason } => {
                self.block_if_running(&record, blocked_code(reason), now)
                    .await?;
                Ok(false)
            }
            GridPlanDirective::ResetRequired { trigger, .. } => {
                let updated = self
                    .store
                    .settle_runtime_state(
                        &record.instance.instance_id,
                        record.instance.state,
                        GridInstanceState::ResetRequired,
                        Some(reset_code(trigger)),
                        now,
                    )
                    .await?;
                let reset_record = GridRuntimeRecord {
                    owner_user_id: record.owner_user_id.clone(),
                    instance: updated,
                    tail_batch_id: record.tail_batch_id.clone(),
                };
                let desired = empty_surface(
                    &reset_record,
                    empty_digest(),
                    reset_record.instance.plan_revision,
                );
                let _ = self
                    .reconcile_desired(&reset_record, &projection, &actual, &desired, now)
                    .await?;
                self.persist_fill_allocations(&fills, projection.observed_ms)
                    .await?;
                Ok(true)
            }
            GridPlanDirective::Converge {
                rolling_anchor,
                desired_orders,
            } => {
                let running = self.ensure_running(&record, now).await?;
                self.apply_converge(
                    &running,
                    &projection,
                    &actual,
                    rolling_anchor,
                    desired_orders,
                    fills,
                    now,
                )
                .await
            }
            GridPlanDirective::Replenish { adjustments, .. } => {
                let running = self.ensure_running(&record, now).await?;
                self.apply_market_action(
                    &running,
                    &projection,
                    &actual,
                    &market,
                    MarketAction::Replenish(adjustments),
                    fills,
                    now,
                )
                .await
            }
            GridPlanDirective::ReduceExposure { reductions, .. } => {
                let running = self.ensure_running(&record, now).await?;
                self.apply_market_action(
                    &running,
                    &projection,
                    &actual,
                    &market,
                    MarketAction::Reduce(reductions),
                    fills,
                    now,
                )
                .await
            }
            GridPlanDirective::Stop { .. } => Err(BinanceGridRuntimeError::Planner),
        }
    }

    async fn refresh_market(
        &mut self,
        record: &GridRuntimeRecord,
        now: u64,
    ) -> Result<BinanceGridBootstrapMarketFacts, BinanceGridRuntimeError> {
        let id = record.instance.instance_id.clone();
        if !self.markets.contains_key(&id) {
            let binding = GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                record.instance.trading_account_id.clone(),
                record.instance.symbol.clone(),
            )
            .map_err(|_| BinanceGridRuntimeError::Facts)?;
            let reader = BinanceGridMarketReader::new(binding, self.transport_limits)
                .map_err(|_| BinanceGridRuntimeError::Market)?;
            self.markets.insert(id.clone(), reader);
        }
        let state = self
            .markets
            .get_mut(&id)
            .ok_or(BinanceGridRuntimeError::Market)?;
        let facts = state
            .refresh(now)
            .await
            .map_err(|_| BinanceGridRuntimeError::Market)?;
        self.hot_path.cache_market(id, facts.clone());
        Ok(facts)
    }

    async fn risk_facts(
        &mut self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        private: &PrivateFacts,
        _now: u64,
    ) -> Result<Option<GridRiskFacts>, BinanceGridRuntimeError> {
        if !record.instance.config.profit_reduction.enabled {
            return Ok(None);
        }
        let mut usd_assets = projection
            .assets
            .iter()
            .filter(|asset| asset.asset == "USD");
        let equity = usd_assets
            .next()
            .filter(|asset| asset.equity > Decimal::ZERO)
            .ok_or(BinanceGridRuntimeError::Facts)?
            .equity;
        if usd_assets.next().is_some() {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let state = self
            .markets
            .get(&record.instance.instance_id)
            .ok_or(BinanceGridRuntimeError::Market)?;
        let conversion = state
            .quote_usd_evidence(
                projection.private_generation,
                record.instance.config.reset_policy.stale_market_ms,
            )
            .await
            .map_err(|_| BinanceGridRuntimeError::Market)?;
        if conversion.private_generation != projection.private_generation
            || conversion.asset.as_str() != record.instance.symbol.quote()
            || conversion.usd_per_asset <= Decimal::ZERO
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let usd = Asset::new("USD").map_err(|_| BinanceGridRuntimeError::Facts)?;
        let account = AccountRiskSnapshot {
            exchange: "binance".to_owned(),
            account: record.instance.trading_account_id.clone(),
            risk_currency: usd.clone(),
            account_equity: equity,
            private_generation: projection.private_generation,
            observed_at_ms: projection.observed_ms,
            source_status: RiskSourceStatus::Complete,
        };
        let mut legs = Vec::new();
        for position in &private.positions {
            if position.quantity.is_zero() {
                continue;
            }
            let entry = position.entry_price.ok_or(BinanceGridRuntimeError::Facts)?;
            let mark = position.mark_price.ok_or(BinanceGridRuntimeError::Facts)?;
            if entry <= Decimal::ZERO || mark <= Decimal::ZERO {
                return Err(BinanceGridRuntimeError::Facts);
            }
            let quote_notional = position
                .quantity
                .checked_mul(mark)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let notional = quote_notional
                .checked_mul(conversion.usd_per_asset)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let price_delta = match position.position_side {
                PositionSide::Long => mark.checked_sub(entry),
                PositionSide::Short => entry.checked_sub(mark),
                PositionSide::Net => None,
            }
            .ok_or(BinanceGridRuntimeError::Facts)?;
            let pnl = price_delta
                .checked_mul(position.quantity)
                .and_then(|value| value.checked_mul(conversion.usd_per_asset))
                .ok_or(BinanceGridRuntimeError::Facts)?;
            legs.push(LegRiskSnapshot {
                symbol: position.symbol.clone(),
                position_side: position.position_side,
                quantity: position.quantity,
                mark_price: Price::new(mark).map_err(|_| BinanceGridRuntimeError::Facts)?,
                contract_multiplier: conversion.usd_per_asset,
                notional,
                unrealized_pnl: pnl,
                risk_currency: usd.clone(),
                private_generation: projection.private_generation,
                observed_at_ms: projection.observed_ms,
            });
        }
        let quote_per_risk_unit = Decimal::ONE
            .checked_div(conversion.usd_per_asset)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        Ok(Some(GridRiskFacts {
            account,
            legs,
            conversion: GridRiskConversion {
                risk_currency: usd,
                quote_currency: conversion.asset,
                quote_per_risk_unit,
                private_generation: projection.private_generation,
                observed_at_ms: conversion.observed_at_ms,
            },
        }))
    }

    async fn apply_converge(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        anchor: GridRollingAnchor,
        orders: Vec<GridOrderIntent>,
        fills: Vec<GridFillAllocation>,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let loaded = self
            .store
            .load_desired_orders(&record.instance.instance_id)
            .await?;
        let next = record
            .instance
            .plan_revision
            .checked_add(1)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        let desired_orders = desired_orders(
            &record.instance.instance_id,
            record.instance.config_revision,
            &orders,
            loaded.as_ref(),
            next,
        )?;
        let digest = desired_digest(&anchor, &desired_orders);
        let unchanged = loaded.as_ref().is_some_and(|surface| {
            surface.config_revision == record.instance.config_revision
                && surface.desired_digest == digest
                && surface.orders == desired_orders
        });
        let current_fills = fills
            .iter()
            .filter(|fill| fill.config_revision == record.instance.config_revision)
            .cloned()
            .collect::<Vec<_>>();
        if unchanged && current_fills.is_empty() {
            let desired = GridDesiredSurface {
                instance_id: record.instance.instance_id.clone(),
                symbol: record.instance.symbol.clone(),
                config_revision: record.instance.config_revision,
                plan_revision: record.instance.plan_revision,
                desired_digest: digest,
                orders: desired_orders,
            };
            let result = self
                .reconcile_desired(record, projection, actual, &desired, now)
                .await?;
            if matches!(
                &result,
                ReconcileResult::Converged | ReconcileResult::FactsChanged
            ) {
                self.persist_fill_allocations(&fills, projection.observed_ms)
                    .await?;
            }
            return self
                .finish_reconcile(record, projection, &desired, result, now)
                .await;
        }
        let plan_revision = if unchanged {
            record.instance.plan_revision
        } else {
            next
        };
        let desired = GridDesiredSurface {
            instance_id: record.instance.instance_id.clone(),
            symbol: record.instance.symbol.clone(),
            config_revision: record.instance.config_revision,
            plan_revision,
            desired_digest: digest,
            orders: desired_orders,
        };
        let durable_anchor = if unchanged {
            record
                .instance
                .anchor
                .clone()
                .ok_or(BinanceGridRuntimeError::Facts)?
        } else {
            GridAnchor {
                revision: plan_revision,
                instrument_generation: anchor.instrument_generation,
                price: anchor.anchor_price.value(),
                price_step: anchor.step.value(),
                grid_quantity: anchor.grid_quantity,
                source_native_trade_id: current_fills
                    .last()
                    .map(|fill| fill.native_trade_id.clone()),
                observed_ms: projection.observed_ms,
            }
        };
        let (placements, cancellations) = desired_diff(&desired, actual);
        let mut mutation = prepare_mutation_batch(
            record,
            &desired,
            placements,
            cancellations,
            0,
            anchor.instrument_generation,
            now,
        )?;
        let native_trade_ids = current_fills
            .iter()
            .map(|fill| fill.native_trade_id.clone())
            .collect::<Vec<_>>();
        bind_plan_batch_identity(&mut mutation, record, &desired, &native_trade_ids);
        let committed = self
            .store
            .commit_plan_mutation_batch(
                &GridPlanMutationBatch {
                    mutation,
                    expected_plan_revision: record.instance.plan_revision,
                    expected_desired_digest: loaded.as_ref().map(|surface| surface.desired_digest),
                    predecessor_batch_id: record.tail_batch_id.clone(),
                    expected_private_generation: projection.private_generation,
                    expected_private_observed_ms: projection.observed_ms,
                    source_event_received_ms: None,
                    require_empty_account_queue: false,
                    anchor: durable_anchor,
                    desired_orders: desired.orders,
                    fill_allocations: current_fills,
                    last_facts_ms: projection.observed_ms,
                },
                now,
            )
            .await?;
        if committed.receipt.command_count != 0 {
            self.hot_path.wake_commands();
        }
        let prior_fills = fills
            .iter()
            .filter(|fill| fill.config_revision != record.instance.config_revision)
            .cloned()
            .collect::<Vec<_>>();
        self.persist_fill_allocations(&prior_fills, projection.observed_ms)
            .await?;
        Ok(true)
    }

    async fn apply_market_action(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        market: &BinanceGridBootstrapMarketFacts,
        action: MarketAction,
        fills: Vec<GridFillAllocation>,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let digest = action_digest(record, projection, market, &action)?;
        let loaded = self
            .store
            .load_desired_orders(&record.instance.instance_id)
            .await?;
        let unchanged = loaded
            .as_ref()
            .is_some_and(|surface| surface.orders.is_empty() && surface.desired_digest == digest);
        let (current, plan_revision) = if unchanged {
            (record.clone(), record.instance.plan_revision)
        } else {
            let next = record
                .instance
                .plan_revision
                .checked_add(1)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let next_anchor = record.instance.anchor.as_ref().map(|anchor| {
                let mut anchor = anchor.clone();
                anchor.revision = next;
                anchor
            });
            let summary = self
                .store
                .commit_plan_surface(
                    &record.instance.instance_id,
                    record.instance.revision,
                    record.instance.config_revision,
                    record.instance.plan_revision,
                    next,
                    next_anchor.as_ref(),
                    digest,
                    &[],
                    projection.observed_ms,
                    now,
                )
                .await?;
            (
                GridRuntimeRecord {
                    owner_user_id: record.owner_user_id.clone(),
                    instance: summary,
                    tail_batch_id: record.tail_batch_id.clone(),
                },
                next,
            )
        };
        let empty = empty_surface(&current, digest, plan_revision);
        let reconcile = self
            .reconcile_desired(&current, projection, actual, &empty, now)
            .await?;
        if reconcile != ReconcileResult::Converged {
            return self
                .finish_reconcile(&current, projection, &empty, reconcile, now)
                .await;
        }
        let rule_version = rule_version(market.rules.instrument.generation);
        match action {
            MarketAction::Replenish(adjustments) => {
                for adjustment in adjustments {
                    self.enqueue_market(
                        &current,
                        plan_revision,
                        digest,
                        &rule_version,
                        adjustment.position,
                        ProtocolOrderRole::Open,
                        adjustment.quantity,
                        "replenish",
                        now,
                    )
                    .await?;
                }
            }
            MarketAction::Reduce(reductions) => {
                for reduction in reductions {
                    self.enqueue_market(
                        &current,
                        plan_revision,
                        digest,
                        &rule_version,
                        reduction.position,
                        ProtocolOrderRole::Close,
                        reduction.quantity,
                        "reduce",
                        now,
                    )
                    .await?;
                }
            }
        }
        self.persist_fill_allocations(&fills, projection.observed_ms)
            .await?;
        Ok(true)
    }

    async fn enqueue_market(
        &self,
        record: &GridRuntimeRecord,
        plan_revision: u64,
        digest: [u8; 32],
        rule_version: &str,
        position: GridPosition,
        role: ProtocolOrderRole,
        quantity: Decimal,
        action: &str,
        now: u64,
    ) -> Result<(), BinanceGridRuntimeError> {
        let side = protocol_position(position);
        let semantic = format!("{action}:{}", side_name(side));
        let command_id = durable_id(
            "gm",
            &record.instance.instance_id,
            record.instance.config_revision,
            plan_revision,
            &semantic,
            58,
        );
        let client_order_id = durable_id(
            "vgm",
            &record.instance.instance_id,
            record.instance.config_revision,
            plan_revision,
            &semantic,
            36,
        );
        self.store
            .enqueue_command(
                &GridLedgerCommand {
                    command_id,
                    client_order_id,
                    instance_id: record.instance.instance_id.clone(),
                    config_revision: record.instance.config_revision,
                    plan_revision,
                    semantic_key: semantic,
                    rule_version: rule_version.to_owned(),
                    source_digest: digest,
                    intent: GridCommandIntent::Market {
                        position_side: side,
                        role,
                        quantity,
                    },
                },
                now,
            )
            .await?;
        self.hot_path.wake_commands();
        Ok(())
    }

    async fn reconcile_desired(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        desired: &GridDesiredSurface,
        now: u64,
    ) -> Result<ReconcileResult, BinanceGridRuntimeError> {
        if desired.instance_id != record.instance.instance_id
            || desired.symbol != record.instance.symbol
            || desired.config_revision != record.instance.config_revision
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let desired_by_id = desired
            .orders
            .iter()
            .map(|order| (order.client_order_id.as_str(), order))
            .collect::<BTreeMap<_, _>>();
        let statuses = self
            .store
            .load_grid_commands(
                &record.instance.instance_id,
                record.instance.config_revision,
                desired.plan_revision,
            )
            .await?;
        let mut place_statuses = statuses
            .iter()
            .filter(|status| status.order_kind == ExecutorOrderKind::LimitPostOnly)
            .map(|status| (status.command_id.clone(), (status.state, status.updated_ms)))
            .collect::<BTreeMap<_, _>>();
        let prior_plans = prior_command_surfaces(
            desired,
            &actual.ownership,
            record.instance.config_revision,
            desired.plan_revision,
        );
        for (config_revision, plan_revision) in prior_plans {
            for status in self
                .store
                .load_grid_commands(&record.instance.instance_id, config_revision, plan_revision)
                .await?
            {
                if status.order_kind == ExecutorOrderKind::LimitPostOnly {
                    place_statuses
                        .entry(status.command_id)
                        .or_insert((status.state, status.updated_ms));
                }
            }
        }
        let mut facts_changed = false;
        for (client_order_id, order) in &actual.orders {
            if let Some(wanted) = desired_by_id.get(client_order_id.as_str()) {
                match actual_matches_desired(order, wanted)? {
                    DesiredOrderMatch::Exact => {}
                    DesiredOrderMatch::Partial => facts_changed = true,
                    DesiredOrderMatch::Conflict => {
                        return Ok(ReconcileResult::ResetRequired);
                    }
                }
            }
        }
        let mut missing = desired
            .orders
            .iter()
            .filter(|wanted| !actual.orders.contains_key(&wanted.client_order_id))
            .collect::<Vec<_>>();
        missing.sort_by_key(|order| order_priority(order));
        let mut placements = Vec::new();
        let mut completed_clients = BTreeSet::new();
        let mut unresolved_existing = 0_usize;
        for wanted in &missing {
            if let Some(owner) = actual.ownership.get(&wanted.client_order_id) {
                let status = place_statuses.get(&owner.place_command_id).copied();
                match missing_place_result(
                    status,
                    projection.observed_ms,
                    record.instance.updated_ms,
                ) {
                    MissingPlaceResult::Pending => {
                        unresolved_existing = unresolved_existing.saturating_add(1);
                    }
                    MissingPlaceResult::Failed(count) => {
                        return Ok(ReconcileResult::Failed {
                            client: wanted.client_order_id.clone(),
                            count,
                        });
                    }
                    MissingPlaceResult::FactsChanged => {
                        facts_changed = true;
                        completed_clients.insert(wanted.client_order_id.clone());
                    }
                    MissingPlaceResult::ResetRequired => {
                        return Ok(ReconcileResult::ResetRequired);
                    }
                }
            } else {
                placements.push(*wanted);
            }
        }
        if !desired.orders.is_empty()
            && !desired_closes_fit(
                desired,
                &private_facts(record, projection, actual)?.inventory,
                &actual.other_close_reservations,
                &actual.orders,
                &actual.ownership,
                &completed_clients,
            )?
        {
            return Ok(ReconcileResult::ResetRequired);
        }
        let mut cancellations = Vec::new();
        if !facts_changed {
            for client_order_id in actual.orders.keys() {
                if !desired_by_id.contains_key(client_order_id.as_str()) {
                    let prior = statuses.iter().find(|status| {
                        status.order_kind == ExecutorOrderKind::CancelExact
                            && status.target_client_order_id.as_deref()
                                == Some(client_order_id.as_str())
                    });
                    match prior.map(|status| status.state) {
                        Some(state) if is_nonterminal(state) => {}
                        Some(ExecutorCommandState::Reconciled)
                            if prior.is_some_and(|status| {
                                projection.observed_ms <= status.updated_ms
                            }) => {}
                        Some(_) => return Ok(ReconcileResult::ResetRequired),
                        None => cancellations.push(client_order_id.as_str()),
                    }
                }
            }
        }
        let in_flight = statuses
            .iter()
            .filter(|status| is_nonterminal(status.state))
            .count();
        let new_placement_count = placements.len();
        if unresolved_existing == 0 && (!placements.is_empty() || !cancellations.is_empty()) {
            let generation = record
                .instance
                .anchor
                .as_ref()
                .map_or(1, |anchor| anchor.instrument_generation);
            let batch = prepare_mutation_batch(
                record,
                desired,
                placements,
                cancellations,
                in_flight,
                generation,
                now,
            )?;
            if !batch.placements.is_empty() || !batch.cancellations.is_empty() {
                let receipt = self.store.enqueue_mutation_batch(&batch, now).await?;
                if receipt.command_count != 0 {
                    self.hot_path.wake_commands();
                }
                return Ok(ReconcileResult::Pending);
            }
        }
        if unresolved_existing != 0 || new_placement_count != 0 {
            return Ok(ReconcileResult::Pending);
        }
        if facts_changed {
            return Ok(ReconcileResult::FactsChanged);
        }
        if !actual
            .orders
            .keys()
            .all(|client| desired_by_id.contains_key(client.as_str()))
        {
            return Ok(ReconcileResult::Pending);
        }
        if self
            .store
            .has_nonterminal_grid_mutations(&record.instance.instance_id, None)
            .await?
        {
            Ok(ReconcileResult::Pending)
        } else {
            Ok(ReconcileResult::Converged)
        }
    }

    async fn finish_reconcile(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        desired: &GridDesiredSurface,
        result: ReconcileResult,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        match result {
            ReconcileResult::Pending => {
                self.store
                    .update_convergence(
                        &GridConvergenceUpdate {
                            instance_id: record.instance.instance_id.clone(),
                            expected_instance_revision: record.instance.revision,
                            expected_state: record.instance.state,
                            expected_plan_revision: desired.plan_revision,
                            next_plan_revision: desired.plan_revision,
                            desired_digest: desired.desired_digest,
                            dirty: true,
                            consecutive_failures: record.instance.consecutive_failures,
                            last_facts_ms: projection.observed_ms,
                        },
                        now,
                    )
                    .await?;
                Ok(true)
            }
            ReconcileResult::Failed { client, count } => {
                let summary = self
                    .store
                    .update_convergence(
                        &GridConvergenceUpdate {
                            instance_id: record.instance.instance_id.clone(),
                            expected_instance_revision: record.instance.revision,
                            expected_state: record.instance.state,
                            expected_plan_revision: desired.plan_revision,
                            next_plan_revision: desired.plan_revision,
                            desired_digest: desired.desired_digest,
                            dirty: true,
                            consecutive_failures: record
                                .instance
                                .consecutive_failures
                                .saturating_add(u16::from(count)),
                            last_facts_ms: projection.observed_ms,
                        },
                        now,
                    )
                    .await?;
                if summary.state == GridInstanceState::ResetRequired {
                    return Ok(true);
                }
                let next = summary
                    .plan_revision
                    .checked_add(1)
                    .ok_or(BinanceGridRuntimeError::Facts)?;
                let mut orders = desired.orders.clone();
                let failed = orders
                    .iter_mut()
                    .find(|order| order.client_order_id == client)
                    .ok_or(BinanceGridRuntimeError::Facts)?;
                failed.client_order_id = durable_id(
                    "vgp",
                    &summary.instance_id,
                    summary.config_revision,
                    next,
                    &failed.key.encoded(),
                    36,
                );
                let anchor = summary
                    .anchor
                    .as_ref()
                    .ok_or(BinanceGridRuntimeError::Facts)?;
                let digest =
                    desired_digest(&planner_anchor(anchor, summary.config_revision)?, &orders);
                let mut next_anchor = anchor.clone();
                next_anchor.revision = next;
                self.store
                    .commit_plan_surface(
                        &summary.instance_id,
                        summary.revision,
                        summary.config_revision,
                        summary.plan_revision,
                        next,
                        Some(&next_anchor),
                        digest,
                        &orders,
                        projection.observed_ms,
                        now,
                    )
                    .await?;
                Ok(true)
            }
            ReconcileResult::Converged | ReconcileResult::FactsChanged => {
                if record.instance.dirty {
                    self.store
                        .update_convergence(
                            &GridConvergenceUpdate {
                                instance_id: record.instance.instance_id.clone(),
                                expected_instance_revision: record.instance.revision,
                                expected_state: record.instance.state,
                                expected_plan_revision: desired.plan_revision,
                                next_plan_revision: desired.plan_revision,
                                desired_digest: desired.desired_digest,
                                dirty: false,
                                consecutive_failures: 0,
                                last_facts_ms: projection.observed_ms,
                            },
                            now,
                        )
                        .await?;
                }
                Ok(true)
            }
            ReconcileResult::ResetRequired => {
                self.store
                    .settle_runtime_state(
                        &record.instance.instance_id,
                        record.instance.state,
                        GridInstanceState::ResetRequired,
                        Some("surface_conflict"),
                        now,
                    )
                    .await?;
                Ok(true)
            }
        }
    }

    async fn finish_stop(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            self.store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    GridInstanceState::StopPending,
                    GridInstanceState::Stopped,
                    None,
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    async fn finish_pause(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            self.store
                .update_convergence(
                    &GridConvergenceUpdate {
                        instance_id: record.instance.instance_id.clone(),
                        expected_instance_revision: record.instance.revision,
                        expected_state: GridInstanceState::Paused,
                        expected_plan_revision: record.instance.plan_revision,
                        next_plan_revision: record.instance.plan_revision,
                        desired_digest: desired.desired_digest,
                        dirty: false,
                        consecutive_failures: 0,
                        last_facts_ms: projection.observed_ms,
                    },
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    async fn finish_reset(
        &mut self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            if self.settle_lifecycle_timeout(record, now).await? {
                return Ok(true);
            }
            let _ = self.refresh_market(record, now).await?;
            self.store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    GridInstanceState::ResetRequired,
                    GridInstanceState::Running,
                    None,
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    async fn lifecycle_commands_observed(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
    ) -> Result<bool, BinanceGridRuntimeError> {
        if self
            .store
            .has_nonterminal_grid_mutations(&record.instance.instance_id, None)
            .await?
        {
            return Ok(false);
        }
        let latest_current_plan = self
            .store
            .load_grid_commands(
                &record.instance.instance_id,
                record.instance.config_revision,
                record.instance.plan_revision,
            )
            .await?
            .into_iter()
            .map(|command| command.updated_ms)
            .max()
            .unwrap_or(record.instance.updated_ms);
        let latest = self
            .store
            .latest_grid_command_updated_ms(&record.instance.instance_id)
            .await?
            .unwrap_or(record.instance.updated_ms)
            .max(latest_current_plan)
            .max(record.instance.updated_ms);
        Ok(projection.observed_ms > latest)
    }

    async fn settle_lifecycle_timeout(
        &self,
        record: &GridRuntimeRecord,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let Some(code) = lifecycle_timeout_code(
            record.instance.state,
            record.instance.convergence_started_ms,
            record.instance.config.reset_policy.convergence_timeout_ms,
            now,
        ) else {
            return Ok(false);
        };
        self.store
            .settle_runtime_state(
                &record.instance.instance_id,
                record.instance.state,
                GridInstanceState::NeedsAttention,
                Some(code),
                now,
            )
            .await?;
        Ok(true)
    }

    async fn synchronize_actual_surface(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        owners: Vec<GridOrderOwnership>,
        now: u64,
    ) -> Result<ActualSurface, BinanceGridRuntimeError> {
        let mut ownership = owners
            .into_iter()
            .map(|owner| (owner.client_order_id.clone(), owner))
            .collect::<BTreeMap<_, _>>();
        let mut orders = BTreeMap::new();
        let mut intents = Vec::new();
        let mut reservations = GridCloseReservations::default();
        let mut seen_clients = BTreeSet::new();
        for order in projection
            .open_orders
            .iter()
            .filter(|order| order.symbol == record.instance.symbol)
        {
            if !seen_clients.insert(order.client_order_id.clone()) {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            let remaining = remaining_quantity(order)?;
            if let Some(owner) = ownership.get_mut(&order.client_order_id) {
                validate_owned_order(record, owner, order)?;
                if owner.state == GridOwnedOrderState::Terminal {
                    if projection.observed_ms <= owner.last_seen_ms {
                        continue;
                    }
                    return Err(BinanceGridRuntimeError::SurfaceConflict);
                }
                owner.native_order_id = order.native_order_id.clone();
                owner.filled_quantity = order
                    .filled_quantity
                    .ok_or(BinanceGridRuntimeError::Facts)?;
                owner.last_seen_ms = owner
                    .last_seen_ms
                    .max(projection.observed_ms)
                    .max(owner.first_seen_ms);
                owner.state = GridOwnedOrderState::Working;
                self.store.record_order_ownership(owner).await?;
                if orders
                    .insert(order.client_order_id.clone(), order.clone())
                    .is_some()
                {
                    return Err(BinanceGridRuntimeError::SurfaceConflict);
                }
                if remaining > Decimal::ZERO {
                    intents.push(GridOrderIntent {
                        key: GridOrderKey {
                            epoch: owner.config_revision,
                            position: strategy_position(owner.key.position_side)?,
                            role: strategy_role(owner.key.role),
                            level: owner.key.sequence,
                        },
                        side: order.order_side,
                        price: Price::new(order.limit_price.ok_or(BinanceGridRuntimeError::Facts)?)
                            .map_err(|_| BinanceGridRuntimeError::Facts)?,
                        quantity: remaining,
                        reduce_only: owner.key.role == ProtocolOrderRole::Close,
                    });
                }
            } else if is_close_order(order) {
                match order.position_side {
                    PositionSide::Long => {
                        reservations.long_quantity = reservations
                            .long_quantity
                            .checked_add(remaining)
                            .ok_or(BinanceGridRuntimeError::Facts)?
                    }
                    PositionSide::Short => {
                        reservations.short_quantity = reservations
                            .short_quantity
                            .checked_add(remaining)
                            .ok_or(BinanceGridRuntimeError::Facts)?
                    }
                    PositionSide::Net => return Err(BinanceGridRuntimeError::Facts),
                }
            }
        }
        for owner in ownership.values_mut() {
            // An older central snapshot cannot disprove a newer signed placement readback.
            if !orders.contains_key(&owner.client_order_id)
                && owner.state == GridOwnedOrderState::Working
                && owner.native_order_id.is_some()
                && projection.observed_ms > owner.last_seen_ms
            {
                owner.state = GridOwnedOrderState::Terminal;
                owner.last_seen_ms = owner
                    .last_seen_ms
                    .max(projection.observed_ms)
                    .max(now.min(projection.observed_ms));
                self.store.record_order_ownership(owner).await?;
            }
        }
        intents.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(ActualSurface {
            ownership,
            orders,
            intents,
            other_close_reservations: reservations,
        })
    }

    async fn add_command_reservations(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &mut ActualSurface,
    ) -> Result<(), BinanceGridRuntimeError> {
        for reservation in self
            .store
            .load_reduce_reservations(&record.instance.trading_account_id, &record.instance.symbol)
            .await?
        {
            let visible = projection.open_orders.iter().any(|order| {
                order.symbol == record.instance.symbol
                    && order.client_order_id == reservation.client_order_id
            });
            if reservation.grid_instance_id.as_deref() == Some(&record.instance.instance_id)
                || visible
                || (reservation.state == ExecutorCommandState::Reconciled
                    && reservation.updated_ms <= projection.observed_ms)
            {
                continue;
            }
            let total = match reservation.position_side {
                PositionSide::Long => &mut actual.other_close_reservations.long_quantity,
                PositionSide::Short => &mut actual.other_close_reservations.short_quantity,
                PositionSide::Net => return Err(BinanceGridRuntimeError::Facts),
            };
            *total = total
                .checked_add(reservation.quantity)
                .ok_or(BinanceGridRuntimeError::Facts)?;
        }
        Ok(())
    }

    async fn prepare_empty_surface(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        desired: Option<GridDesiredSurface>,
        now: u64,
    ) -> Result<(GridRuntimeRecord, GridDesiredSurface), BinanceGridRuntimeError> {
        if let Some(desired) = desired {
            if !desired.orders.is_empty() {
                return Err(BinanceGridRuntimeError::SurfaceConflict);
            }
            return Ok((self.ensure_running(record, now).await?, desired));
        }
        let running = self.ensure_running(record, now).await?;
        let digest = empty_digest();
        let summary = self
            .store
            .commit_plan_surface(
                &running.instance.instance_id,
                running.instance.revision,
                running.instance.config_revision,
                running.instance.plan_revision,
                running.instance.plan_revision,
                None,
                digest,
                &[],
                projection.observed_ms,
                now,
            )
            .await?;
        let current = GridRuntimeRecord {
            owner_user_id: running.owner_user_id,
            instance: summary,
            tail_batch_id: running.tail_batch_id,
        };
        let desired = empty_surface(&current, digest, current.instance.plan_revision);
        Ok((current, desired))
    }

    async fn market_plan_ready(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        desired: &GridDesiredSurface,
        now: u64,
    ) -> Result<Option<GridRuntimeRecord>, BinanceGridRuntimeError> {
        let unresolved = self
            .store
            .has_nonterminal_grid_mutations(&record.instance.instance_id, None)
            .await?;
        let statuses = self
            .store
            .load_grid_commands(
                &record.instance.instance_id,
                record.instance.config_revision,
                record.instance.plan_revision,
            )
            .await?;
        let market = statuses
            .iter()
            .filter(|status| status.order_kind == ExecutorOrderKind::Market)
            .collect::<Vec<_>>();
        let latest_command_ms = statuses.iter().map(|status| status.updated_ms).max();
        let teardown_ready =
            signed_teardown_ready(true, unresolved, latest_command_ms, projection.observed_ms);
        if market.is_empty() && teardown_ready {
            return Ok(Some(record.clone()));
        }
        let (market_ready, failed, latest_failure) = market_status(
            market
                .iter()
                .map(|status| (status.state, status.updated_ms)),
            projection.observed_ms,
        );
        let ready = market_ready && teardown_ready;
        let newly_failed = failed && latest_failure > record.instance.updated_ms;
        let summary = self
            .store
            .update_convergence(
                &GridConvergenceUpdate {
                    instance_id: record.instance.instance_id.clone(),
                    expected_instance_revision: record.instance.revision,
                    expected_state: record.instance.state,
                    expected_plan_revision: desired.plan_revision,
                    next_plan_revision: desired.plan_revision,
                    desired_digest: desired.desired_digest,
                    dirty: !ready || failed,
                    consecutive_failures: record
                        .instance
                        .consecutive_failures
                        .saturating_add(if newly_failed { 1 } else { 0 }),
                    last_facts_ms: projection.observed_ms,
                },
                now,
            )
            .await?;
        if !ready || summary.state == GridInstanceState::ResetRequired {
            return Ok(None);
        }
        if failed {
            let running = self
                .ensure_running(
                    &GridRuntimeRecord {
                        owner_user_id: record.owner_user_id.clone(),
                        instance: summary,
                        tail_batch_id: record.tail_batch_id.clone(),
                    },
                    now,
                )
                .await?;
            let next = running
                .instance
                .plan_revision
                .checked_add(1)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let next_anchor = running.instance.anchor.as_ref().map(|anchor| {
                let mut anchor = anchor.clone();
                anchor.revision = next;
                anchor
            });
            let summary = self
                .store
                .commit_plan_surface(
                    &running.instance.instance_id,
                    running.instance.revision,
                    running.instance.config_revision,
                    running.instance.plan_revision,
                    next,
                    next_anchor.as_ref(),
                    desired.desired_digest,
                    &[],
                    projection.observed_ms,
                    now,
                )
                .await?;
            return Ok(Some(GridRuntimeRecord {
                owner_user_id: running.owner_user_id,
                instance: summary,
                tail_batch_id: running.tail_batch_id,
            }));
        }
        Ok(Some(GridRuntimeRecord {
            owner_user_id: record.owner_user_id.clone(),
            instance: summary,
            tail_batch_id: record.tail_batch_id.clone(),
        }))
    }

    async fn allocate_included_fills(
        &self,
        record: &GridRuntimeRecord,
        _plan_revision: u64,
    ) -> Result<(), BinanceGridRuntimeError> {
        let Some(last_facts) = record.instance.last_facts_ms else {
            return Ok(());
        };
        let fills = self
            .store
            .load_unallocated_fills(&record.instance.instance_id, 0, MAX_FILL_BATCH)
            .await?;
        self.persist_fill_allocations(&fills, last_facts).await
    }

    async fn persist_fill_allocations(
        &self,
        fills: &[GridFillAllocation],
        included_through_ms: u64,
    ) -> Result<(), BinanceGridRuntimeError> {
        for fill in fills
            .iter()
            .filter(|fill| fill.observed_ms <= included_through_ms)
        {
            self.store.record_fill_allocation(fill).await?;
        }
        Ok(())
    }

    async fn ensure_running(
        &self,
        record: &GridRuntimeRecord,
        now: u64,
    ) -> Result<GridRuntimeRecord, BinanceGridRuntimeError> {
        if matches!(
            record.instance.state,
            GridInstanceState::StartPending | GridInstanceState::Blocked
        ) {
            let instance = self
                .store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    record.instance.state,
                    GridInstanceState::Running,
                    None,
                    now,
                )
                .await?;
            return Ok(GridRuntimeRecord {
                owner_user_id: record.owner_user_id.clone(),
                instance,
                tail_batch_id: record.tail_batch_id.clone(),
            });
        }
        Ok(record.clone())
    }

    async fn block_if_running(
        &self,
        record: &GridRuntimeRecord,
        code: &str,
        now: u64,
    ) -> Result<(), BinanceGridRuntimeError> {
        if matches!(
            record.instance.state,
            GridInstanceState::StartPending | GridInstanceState::Running
        ) {
            self.store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    record.instance.state,
                    GridInstanceState::Blocked,
                    Some(code),
                    now,
                )
                .await?;
        }
        Ok(())
    }
}

struct ActualSurface {
    ownership: BTreeMap<String, GridOrderOwnership>,
    orders: BTreeMap<String, TerminalOpenOrder>,
    intents: Vec<GridOrderIntent>,
    other_close_reservations: GridCloseReservations,
}

#[derive(Clone, Debug, Serialize)]
enum MarketAction {
    Replenish(Vec<GridInventoryAdjustment>),
    Reduce(Vec<GridExposureReduction>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReconcileResult {
    Pending,
    Converged,
    FactsChanged,
    Failed { client: String, count: bool },
    ResetRequired,
}

fn planner_config(
    record: &GridRuntimeRecord,
) -> Result<GridPlannerConfig, BinanceGridRuntimeError> {
    let config = &record.instance.config;
    let quote =
        Asset::new(record.instance.symbol.quote()).map_err(|_| BinanceGridRuntimeError::Facts)?;
    Ok(GridPlannerConfig {
        instance_id: record.instance.instance_id.clone(),
        revision: record.instance.config_revision,
        symbol: record.instance.symbol.clone(),
        order_notional: Amount::new(quote.clone(), config.order_notional),
        maximum_grid_notional: Amount::new(quote.clone(), config.max_total_notional),
        spacing_rate: config.spacing_rate,
        grid_count: u8::try_from(config.grid_levels).map_err(|_| BinanceGridRuntimeError::Facts)?,
        replenishment: config
            .inventory_replenishment
            .enabled
            .then(|| GridReplenishmentPolicy {
                minimum_leg_notional: Amount::new(
                    quote.clone(),
                    config.inventory_replenishment.minimum_inventory_notional,
                ),
                target_leg_notional: Amount::new(
                    quote.clone(),
                    config.inventory_replenishment.target_inventory_notional,
                ),
                max_single_notional: Amount::new(
                    quote.clone(),
                    config
                        .inventory_replenishment
                        .max_single_replenishment_notional,
                ),
            }),
        profit_reduction: config
            .profit_reduction
            .enabled
            .then(|| GridProfitReductionPolicy {
                inventory_equity_multiple: config.profit_reduction.inventory_equity_multiple,
                minimum_profit_rate: config.profit_reduction.minimum_unrealized_profit_rate,
                reduction_fraction: config.profit_reduction.reduction_fraction,
                max_single_notional: Amount::new(
                    quote,
                    config.profit_reduction.max_single_reduce_notional,
                ),
            }),
        reset_policy: GridResetPolicy {
            max_market_age_ms: config.reset_policy.stale_market_ms,
            max_private_age_ms: config.reset_policy.stale_private_ms,
            convergence_timeout_ms: config.reset_policy.convergence_timeout_ms,
            failure_threshold: u32::from(config.reset_policy.max_consecutive_failures),
        },
    })
}

fn planner_anchor(
    anchor: &GridAnchor,
    config_revision: u64,
) -> Result<GridRollingAnchor, BinanceGridRuntimeError> {
    Ok(GridRollingAnchor {
        revision: config_revision,
        instrument_generation: anchor.instrument_generation,
        anchor_price: Price::new(anchor.price).map_err(|_| BinanceGridRuntimeError::Facts)?,
        step: Price::new(anchor.price_step).map_err(|_| BinanceGridRuntimeError::Facts)?,
        grid_quantity: anchor.grid_quantity,
    })
}

fn maker_fill_hints(
    fills: &[GridFillAllocation],
    actual: &ActualSurface,
    signed_totals: &BTreeMap<String, Decimal>,
) -> Result<Vec<GridMakerFill>, BinanceGridRuntimeError> {
    let mut last_for_complete = BTreeMap::<String, String>::new();
    for fill in fills {
        if fill.maker != Some(true) {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let owner = actual
            .ownership
            .get(&fill.client_order_id)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        let total = signed_totals
            .get(&fill.client_order_id)
            .copied()
            .ok_or(BinanceGridRuntimeError::Facts)?;
        if fill_complete(
            total,
            owner.quantity,
            actual.orders.contains_key(&fill.client_order_id),
        )? {
            let last = last_for_complete
                .entry(fill.client_order_id.clone())
                .or_default();
            if fill.native_trade_id > *last {
                *last = fill.native_trade_id.clone();
            }
        }
    }
    fills
        .iter()
        .map(|fill| {
            let owner = actual
                .ownership
                .get(&fill.client_order_id)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            Ok(GridMakerFill {
                fill_id: fill.native_trade_id.clone(),
                source_order: GridOrderIntent {
                    key: GridOrderKey {
                        epoch: owner.config_revision,
                        position: strategy_position(owner.key.position_side)?,
                        role: strategy_role(owner.key.role),
                        level: owner.key.sequence,
                    },
                    side: owner.key.order_side(),
                    price: Price::new(owner.limit_price)
                        .map_err(|_| BinanceGridRuntimeError::Facts)?,
                    quantity: owner.quantity,
                    reduce_only: owner.key.role == ProtocolOrderRole::Close,
                },
                complete: last_for_complete.get(&fill.client_order_id)
                    == Some(&fill.native_trade_id),
                maker: true,
            })
        })
        .collect()
}

fn validate_owned_order(
    record: &GridRuntimeRecord,
    owner: &GridOrderOwnership,
    order: &TerminalOpenOrder,
) -> Result<(), BinanceGridRuntimeError> {
    let filled = order
        .filled_quantity
        .ok_or(BinanceGridRuntimeError::Facts)?;
    if owner.instance_id != record.instance.instance_id
        || owner.trading_account_id != record.instance.trading_account_id
        || owner.symbol != record.instance.symbol
        || order.native_order_id.is_none()
        || order.position_side != owner.key.position_side
        || order.order_side != owner.key.order_side()
        || order.quantity != owner.quantity
        || filled < Decimal::ZERO
        || filled > order.quantity
        || order.limit_price != Some(owner.limit_price)
        || !order.post_only
    {
        return Err(BinanceGridRuntimeError::SurfaceConflict);
    }
    Ok(())
}

fn fill_matches_owner(fill: &GridFillAllocation, owner: &GridOrderOwnership) -> bool {
    fill.instance_id == owner.instance_id
        && fill.trading_account_id == owner.trading_account_id
        && fill.config_revision == owner.config_revision
        && fill.client_order_id == owner.client_order_id
        && fill.symbol == owner.symbol
        && fill.position_side == owner.key.position_side
        && fill.role == owner.key.role
}

fn action_digest(
    record: &GridRuntimeRecord,
    projection: &TerminalAccountProjection,
    market: &BinanceGridBootstrapMarketFacts,
    action: &MarketAction,
) -> Result<[u8; 32], BinanceGridRuntimeError> {
    let value = serde_json::to_vec(&(
        record.instance.config_revision,
        projection.private_generation,
        projection.observed_ms,
        market.rules.instrument.generation,
        action,
    ))
    .map_err(|_| BinanceGridRuntimeError::Facts)?;
    Ok(Sha256::digest(value).into())
}

#[cfg(test)]
#[path = "grid_runtime/tests.rs"]
mod tests;
