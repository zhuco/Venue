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
    BinanceGridMarketReader, BinanceGridReferenceFacts, BinanceTransportLimits, GatewayBinding,
    GatewayMode, VenueId,
};
use venue_strategies::hedged_grid::{
    GridCloseReservations, GridConvergenceFacts, GridExposureReduction, GridInstrumentLimits,
    GridInventoryAdjustment, GridMakerFill, GridOrderIntent, GridOrderKey, GridPlanDirective,
    GridPlanner, GridPlannerConfig, GridPlannerControl, GridPlannerInput, GridPosition,
    GridProfitReductionPolicy, GridReferencePrice, GridReplenishmentPolicy, GridResetPolicy,
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
mod reconcile;
mod risk;
#[path = "grid_runtime/stream_overlay.rs"]
mod stream_overlay;
use fast_path::GridHotPathState;
pub use fast_path::{GRID_PRIVATE_STREAM_CHANNEL_CAPACITY, GridPrivateStreamSignal};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceGridRuntimeError {
    #[error("Grid cold convergence was superseded by a newer durable revision")]
    Superseded,
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
        if error == GridStoreError::Conflict {
            // A hot authenticated-fill batch uses the same revision CAS as the cold supervisor.
            // Its committed revision is the authoritative next input, so a cold reader that began
            // earlier must reload rather than report an unavailable store or change lifecycle.
            return Self::Superseded;
        }
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
    risk_credentials: Option<crate::executor_secret::ExecutorSecretProvider>,
    started_ms: u64,
}

struct GridApplyContext<'a> {
    record: &'a GridRuntimeRecord,
    projection: &'a TerminalAccountProjection,
    actual: &'a ActualSurface,
    now: u64,
}

struct GridMarketCommand<'a> {
    record: &'a GridRuntimeRecord,
    plan_revision: u64,
    digest: [u8; 32],
    rule_version: &'a str,
    position: GridPosition,
    role: ProtocolOrderRole,
    quantity: Decimal,
    action: &'a str,
    now: u64,
}

impl BinanceGridRuntime {
    pub fn with_risk_credentials(
        mut self,
        credentials: crate::executor_secret::ExecutorSecretProvider,
    ) -> Self {
        self.risk_credentials = Some(credentials);
        self
    }
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
            risk_credentials: None,
            started_ms: now_ms().unwrap_or(u64::MAX),
        }
    }

    pub async fn run_once(&mut self) -> Result<usize, BinanceGridRuntimeError> {
        let records = self.store.list_runtime_instances().await?;
        self.hot_path.replace_records(&records);
        let mut progressed = 0_usize;
        for record in records {
            match self.process(record.clone()).await {
                Ok(changed) => progressed = progressed.saturating_add(usize::from(changed)),
                Err(BinanceGridRuntimeError::Superseded) => {
                    tracing::debug!(
                        target: "venue_control::grid_runtime",
                        instance_id = %record.instance.instance_id,
                        "Grid cold turn was superseded by an authenticated mutation batch"
                    );
                    continue;
                }
                Err(error) => {
                    eprintln!(
                        "Binance Grid instance {} turn failed: {error}",
                        record.instance.instance_id
                    );
                    if error == BinanceGridRuntimeError::Market
                        && record.instance.state == GridInstanceState::Running
                    {
                        // Public mark/rules supervision is not the user execution stream.
                        // Keep fill-driven rolling active; do not produce a cold mutation.
                        continue;
                    }
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
                        BinanceGridRuntimeError::Superseded
                        | BinanceGridRuntimeError::Store
                        | BinanceGridRuntimeError::Clock => continue,
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
        let first_rejected = if matches!(
            record.instance.state,
            GridInstanceState::StartPending
                | GridInstanceState::Running
                | GridInstanceState::Blocked
        ) {
            self.store
                .exchange_rejection_started_ms(
                    &record.instance.instance_id,
                    record.instance.config_revision,
                )
                .await?
        } else {
            None
        };
        if crate::grid_store::rejection::rejection_reset_due(first_rejected, now) {
            self.store
                .settle_runtime_state_checked(
                    &record.instance.instance_id,
                    Some(record.instance.revision),
                    record.instance.state,
                    GridInstanceState::ResetRequired,
                    Some("exchange_rejection_delay_elapsed"),
                    now,
                )
                .await?;
            return Ok(true);
        }
        let Some(mut projection) = self
            .projections
            .load_healthy_owned(&record.owner_user_id, &record.instance.credential_id)
            .await?
        else {
            if self.settle_lifecycle_timeout(&record, now).await? {
                return Ok(true);
            }
            self.block_if_running(&record, "private_missing", now)
                .await?;
            return Ok(false);
        };
        if now.saturating_sub(projection.observed_ms)
            > record.instance.config.reset_policy.stale_private_ms
            && self.hot_path.recent_stream(
                &record.instance.instance_id,
                now,
                record.instance.config.reset_policy.stale_private_ms,
            )
        {
            return Ok(false);
        }
        if projection.trading_account_id != record.instance.trading_account_id
            || projection.observed_ms < self.started_ms
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

        // Exact lifecycle cancellations rely on signed private facts and must still progress
        // while the public market endpoint is unavailable.
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
        let reference = self.refresh_market(&record, &projection, now).await?;
        for position in projection
            .positions
            .iter_mut()
            .filter(|position| position.symbol == record.instance.symbol)
        {
            position.mark_price = Some(reference.price.value());
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
                let market = self.refresh_market(&record, &projection, now).await?;
                if !desired_valid_for_market(&record, desired, &market) {
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
                    .reconcile_desired(&running, &projection, &actual, desired, now)
                    .await?;
                if matches!(
                    &result,
                    ReconcileResult::Converged | ReconcileResult::FactsChanged
                ) {
                    self.allocate_included_fills(&running, desired.plan_revision)
                        .await?;
                }
                return self
                    .finish_reconcile(&running, &projection, desired, result, now)
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

        let market = self.refresh_market(&record, &projection, now).await?;
        let risk = match self.risk_facts(&record, &projection, &private, now).await {
            Ok(risk) => risk,
            Err(error) => {
                tracing::warn!(target: "venue_control::grid_hot_path", %error, "Profit reduction verification unavailable; ordinary Grid rolling remains active");
                None
            }
        };
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
        let mut config = planner_config(&record)?;
        if risk.is_none() {
            config.profit_reduction = None;
        }
        let plan = GridPlanner::plan(&GridPlannerInput {
            config,
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
            book: None,
            reference_price: Some(GridReferencePrice {
                price: market.price,
                observed_at_ms: market.observed_at_ms,
            }),
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
                pending_since_ms: first_rejected
                    .is_none()
                    .then_some(record.instance.convergence_started_ms)
                    .flatten(),
                consecutive_failures: if first_rejected.is_some() {
                    0
                } else {
                    u32::from(record.instance.consecutive_failures)
                },
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
                    GridApplyContext {
                        record: &running,
                        projection: &projection,
                        actual: &actual,
                        now,
                    },
                    rolling_anchor,
                    desired_orders,
                    fills,
                )
                .await
            }
            GridPlanDirective::Replenish { adjustments, .. } => {
                let running = self.ensure_running(&record, now).await?;
                self.apply_market_action(
                    GridApplyContext {
                        record: &running,
                        projection: &projection,
                        actual: &actual,
                        now,
                    },
                    &market,
                    MarketAction::Replenish(adjustments),
                    fills,
                )
                .await
            }
            GridPlanDirective::ReduceExposure { reductions, .. } => {
                let running = self.ensure_running(&record, now).await?;
                self.apply_market_action(
                    GridApplyContext {
                        record: &running,
                        projection: &projection,
                        actual: &actual,
                        now,
                    },
                    &market,
                    MarketAction::Reduce(reductions),
                    fills,
                )
                .await
            }
            GridPlanDirective::Stop { .. } => Err(BinanceGridRuntimeError::Planner),
        }
    }

    async fn refresh_market(
        &mut self,
        record: &GridRuntimeRecord,
        _projection: &TerminalAccountProjection,
        now: u64,
    ) -> Result<BinanceGridReferenceFacts, BinanceGridRuntimeError> {
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
            .refresh_reference(None, now)
            .await
            .map_err(|error| {
                tracing::warn!(target: "venue_control::grid_hot_path", %error, "Grid reference price or instrument refresh failed");
                BinanceGridRuntimeError::Market
            })?;
        self.hot_path.cache_market(id, facts.clone());
        Ok(facts)
    }

    async fn apply_converge(
        &self,
        context: GridApplyContext<'_>,
        anchor: GridRollingAnchor,
        orders: Vec<GridOrderIntent>,
        fills: Vec<GridFillAllocation>,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let GridApplyContext {
            record,
            projection,
            actual,
            now,
        } = context;
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
        context: GridApplyContext<'_>,
        market: &BinanceGridReferenceFacts,
        action: MarketAction,
        fills: Vec<GridFillAllocation>,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let GridApplyContext {
            record,
            projection,
            actual,
            now,
        } = context;
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
                    self.enqueue_market(GridMarketCommand {
                        record: &current,
                        plan_revision,
                        digest,
                        rule_version: &rule_version,
                        position: adjustment.position,
                        role: ProtocolOrderRole::Open,
                        quantity: adjustment.quantity,
                        action: "replenish",
                        now,
                    })
                    .await?;
                }
            }
            MarketAction::Reduce(reductions) => {
                for reduction in reductions {
                    self.enqueue_market(GridMarketCommand {
                        record: &current,
                        plan_revision,
                        digest,
                        rule_version: &rule_version,
                        position: reduction.position,
                        role: ProtocolOrderRole::Close,
                        quantity: reduction.quantity,
                        action: "reduce",
                        now,
                    })
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
        command: GridMarketCommand<'_>,
    ) -> Result<(), BinanceGridRuntimeError> {
        let GridMarketCommand {
            record,
            plan_revision,
            digest,
            rule_version,
            position,
            role,
            quantity,
            action,
            now,
        } = command;
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

#[cfg(test)]
mod runtime_error_tests {
    use super::*;

    #[test]
    fn durable_cas_conflict_only_supersedes_a_cold_turn() {
        assert_eq!(
            BinanceGridRuntimeError::from(GridStoreError::Conflict),
            BinanceGridRuntimeError::Superseded
        );
        assert_eq!(
            BinanceGridRuntimeError::from(GridStoreError::Unavailable),
            BinanceGridRuntimeError::Store
        );
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
    Failed { clients: Vec<String>, count: bool },
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
    market: &BinanceGridReferenceFacts,
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
