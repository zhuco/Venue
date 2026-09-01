use std::{
    io,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use venue_domain::domain::{Asset, FieldState, Fill, NativeOrderFamily, OrderState, PositionSide};
use venue_gateway_api::{GatewayBinding, VenueId};
use venue_runtime::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountPhysicalGateway, AccountRecoveryOutcome, AccountRecoveryReport, AccountRecoveryRequest,
    AccountRiskEvidence, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};
use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey, StrategyKind};
use venue_strategies::hedged_grid::{GridDecision, GridInventory, GridPhase};
use venue_strategies::hedged_grid::{HedgedGridBinding, HedgedGridParams, HedgedGridState};

use super::*;
use crate::NodeGridRecoveryPolicy;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

struct State {
    generation: u64,
    dispatches: usize,
    accept_dispatch: bool,
    risk_evidence_fails: bool,
    long_quantity: Decimal,
    short_quantity: Decimal,
    open_orders: Vec<SignedAccountOrderFact>,
    fills: Vec<Fill>,
}

struct Gateway {
    binding: GatewayBinding,
    state: Arc<Mutex<State>>,
}

impl AccountPhysicalGateway for Gateway {
    type Error = io::Error;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        AccountRecoveryReport::new(
            self.binding.clone(),
            now().map_err(io::Error::other)?,
            request
                .unresolved()
                .iter()
                .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
                .collect(),
        )
        .map_err(io::Error::other)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let state = self
            .state
            .lock()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        if state.risk_evidence_fails {
            return Err(AccountHostValidationError::RiskEvidenceStage(
                "test_grid_risk",
            ));
        }
        let generation = state.generation.max(1);
        AccountRiskEvidence::complete(
            self.binding.clone(),
            now().map_err(|_| AccountHostValidationError::RiskEvidence)?,
            generation,
            Vec::new(),
            Vec::new(),
        )
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        let now = now().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        state.generation = state.generation.saturating_add(1);
        let generation = state.generation;
        let fills = std::mem::take(&mut state.fills);
        SignedAccountSnapshot::complete_with_fills(
            self.binding.clone(),
            now,
            10_000,
            generation,
            1,
            SignedAccountPositionMode::Hedge,
            state.open_orders.clone(),
            vec![
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Long,
                    quantity: state.long_quantity,
                    entry_price: (state.long_quantity != Decimal::ZERO)
                        .then_some(Decimal::new(100, 0)),
                    mark_price: Some(Decimal::new(100, 0)),
                },
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Short,
                    quantity: state.short_quantity,
                    entry_price: (state.short_quantity != Decimal::ZERO)
                        .then_some(Decimal::new(100, 0)),
                    mark_price: Some(Decimal::new(100, 0)),
                },
            ],
            fills,
            format!("fills:{generation}"),
            request
                .unresolved()
                .iter()
                .map(|command| SignedUnknownFact {
                    command_id: command.command_id().clone(),
                    result: SignedUnknownResult::Unknown,
                })
                .collect(),
        )
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        let Ok(mut state) = self.state.lock() else {
            return AccountGatewayResult::Unknown;
        };
        state.dispatches = state.dispatches.saturating_add(1);
        if !state.accept_dispatch {
            return AccountGatewayResult::Unknown;
        }
        let venue_order_id = format!("grid-native-{}", state.dispatches);
        match permit.command() {
            ExecutionCommand::PlaceLimit(order) => {
                state.open_orders.push(SignedAccountOrderFact {
                    created_at_ms: Some(1),
                    time_in_force: Some(order.time_in_force),
                    client_order_id: order.client_order_id.as_str().to_owned(),
                    venue_order_id: Some(venue_order_id.clone()),
                    symbol: order.owner.symbol.clone(),
                    family: NativeOrderFamily::UmOrder,
                    side: order.side,
                    position_side: order.position_side,
                    quantity: order.quantity,
                    limit_price: Some(order.limit_price.value()),
                    reduce_only: order.reduce_only,
                    owner: Some(order.owner.clone()),
                    external: false,
                    state: Some(OrderState::New),
                    filled_quantity: Some(Decimal::ZERO),
                });
            }
            ExecutionCommand::Cancel(cancel) => {
                state.open_orders.retain(|order| {
                    order.client_order_id != cancel.target_client_order_id.as_str()
                });
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return AccountGatewayResult::Unknown;
            }
        }
        AccountGatewayResult::Accepted { venue_order_id }
    }
}

fn now() -> Result<u64, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "clock")?
        .as_millis()
        .try_into()
        .map_err(|_| "clock")
}

fn launch(root: &std::path::Path) -> Result<NodeLaunch, Box<dyn std::error::Error>> {
    Ok(NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            root.to_str().ok_or("non-utf8 test root")?,
        ],
    )?)
}

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    let account = AccountKey::new(VenueId::Bybit, ACCOUNT.to_owned())?;
    let key = StrategyInstanceKey::new(
        account,
        StrategyKind::HedgedGrid,
        "grid-bootstrap".to_owned(),
        "DOGE/USDT".parse()?,
    )?;
    Ok(StrategyBinding::new(
        key,
        "run-bootstrap",
        "grid-bootstrap-config",
    )?)
}

fn initial(grid_count: u8) -> Result<HedgedGridState, Box<dyn std::error::Error>> {
    Ok(HedgedGridState::new_with_params(
        HedgedGridBinding {
            strategy_instance_id: "grid-bootstrap".to_owned(),
            run_id: "run-bootstrap".to_owned(),
            exchange: "bybit".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            config_version: "grid-bootstrap-config".to_owned(),
            owner_scope: "grid-bootstrap".to_owned(),
        },
        HedgedGridParams::fixed_release(Asset::new("USDT")?, grid_count)?,
    )?)
}

fn market() -> Result<GridBootstrapMarket, Box<dyn std::error::Error>> {
    Ok(GridBootstrapMarket {
        bid: Price::new(Decimal::new(998, 1))?,
        ask: Price::new(Decimal::new(1002, 1))?,
        price_tick: Price::new(Decimal::new(1, 1))?,
        quantity_step: Decimal::new(1, 2),
        minimum_quantity: Decimal::new(1, 2),
        maximum_quantity: Decimal::new(1000, 0),
        minimum_notional: Decimal::new(5, 0),
        observed_at_ms: now()?,
    })
}

#[allow(clippy::type_complexity)]
fn resident(
    root: &std::path::Path,
    grid_count: u8,
) -> Result<
    (
        ProductionResident<Gateway>,
        Arc<Mutex<State>>,
        StrategyBinding,
    ),
    Box<dyn std::error::Error>,
> {
    let launch = launch(root)?;
    let state = Arc::new(Mutex::new(State {
        generation: 0,
        dispatches: 0,
        accept_dispatch: false,
        risk_evidence_fails: false,
        long_quantity: Decimal::ZERO,
        short_quantity: Decimal::ZERO,
        open_orders: Vec::new(),
        fills: Vec::new(),
    }));
    let gateway = Gateway {
        binding: launch.binding().clone(),
        state: state.clone(),
    };
    let mut resident = ProductionResident::open(&launch, gateway)?;
    let binding = binding()?;
    resident.register_grid_actor(
        binding.clone(),
        initial(grid_count)?,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        true,
    )?;
    Ok((resident, state, binding))
}

#[test]
fn bootstrap_batch_risk_failure_dispatches_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 2)?;
    state.lock().map_err(|_| "state")?.risk_evidence_fails = true;
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    assert!(
        resident
            .bootstrap_grid_from_signed_market(&binding, snapshot, market()?)
            .is_err()
    );
    assert_eq!(state.lock().map_err(|_| "state")?.dispatches, 0);
    assert_eq!(
        resident.strategy_lifecycle(&binding),
        Some(venue_runtime::account::InstanceLifecycle::Paused)
    );
    Ok(())
}

#[test]
fn bootstrap_rechecks_each_opening_after_the_closing_wave_and_never_sends_a_crossing_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 2)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    let initial_market = market()?;
    let mut moved_market = initial_market.clone();
    moved_market.bid = Price::new(Decimal::new(997, 1))?;
    moved_market.ask = Price::new(Decimal::new(998, 1))?;
    moved_market.observed_at_ms = now()?;

    assert!(
        resident
            .bootstrap_grid_from_signed_market_with_refresh(
                &binding,
                snapshot,
                initial_market,
                move |_| Ok(Some(moved_market.clone())),
            )
            .is_err()
    );
    let state_guard = state.lock().map_err(|_| "state")?;
    assert!(state_guard.dispatches > 0);
    assert!(!state_guard.open_orders.is_empty());
    assert!(
        state_guard
            .open_orders
            .iter()
            .all(|order| order.reduce_only)
    );
    assert_eq!(
        resident.strategy_lifecycle(&binding),
        Some(venue_runtime::account::InstanceLifecycle::Paused)
    );
    drop(state_guard);
    drop(resident);

    state.lock().map_err(|_| "state")?.generation = 0;
    let second_launch = launch(directory.path())?;
    let gateway = Gateway {
        binding: second_launch.binding().clone(),
        state: state.clone(),
    };
    let mut reopened = ProductionResident::open(&second_launch, gateway)?;
    reopened.register_grid_actor(
        binding.clone(),
        initial(2)?,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        true,
    )?;
    assert!(state.lock().map_err(|_| "state")?.open_orders.is_empty());
    assert!(reopened.take_grid_bootstrap_request(&binding)?);
    let snapshot = reopened.refresh_signed_snapshot()?;
    let stable_market = market()?;
    let opening_market = stable_market.clone();
    reopened.bootstrap_grid_from_signed_market_with_refresh(
        &binding,
        snapshot,
        stable_market,
        move |_| {
            let mut opening_market = opening_market.clone();
            opening_market.observed_at_ms = now().map_err(|_| NodeError::ResidentRuntime)?;
            Ok(Some(opening_market))
        },
    )?;
    assert!(
        reopened
            .grid_bridges
            .get(&binding.key)
            .ok_or("rebuilt grid")?
            .signed_desired_matches(&state.lock().map_err(|_| "state")?.open_orders)
    );
    Ok(())
}

#[test]
fn existing_grid_restart_signs_cancels_empty_and_rebuilds_higher_epoch()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 2)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident
        .bootstrap_grid_from_signed_market(&binding, snapshot, market()?)
        .map_err(|error| format!("first bootstrap: {error}"))?;
    let first_order_count = resident
        .grid_bridges
        .get(&binding.key)
        .ok_or("first grid")?
        .expected_signed_surface()?
        .len();
    assert!(first_order_count > 0);
    assert_eq!(
        resident
            .grid_bridges
            .get(&binding.key)
            .and_then(|bridge| bridge.grid.epoch.as_ref())
            .map(|epoch| epoch.epoch),
        Some(1)
    );
    assert_eq!(
        state.lock().map_err(|_| "state")?.open_orders.len(),
        first_order_count
    );
    drop(resident);

    // Two old children are absent while no writer/private consumer is resident. The signed facts
    // do not prove whether they filled or were cancelled, and deliberately contain no replayable
    // fill rows. Startup reset must not invent that cause: it retires only the signed-absent
    // children, cancels the survivors and rebuilds from fresh signed inventory.
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.open_orders.drain(..2);
        state.long_quantity = Decimal::new(18, 2);
        state.short_quantity = Decimal::new(6, 2);
    }

    // A real adapter process restarts its private generation at one while the Host keeps the
    // prior durable floor. Preserve exchange orders, but reset only this process-local counter.
    state.lock().map_err(|_| "state")?.generation = 0;

    let second_launch = launch(directory.path())?;
    let gateway = Gateway {
        binding: second_launch.binding().clone(),
        state: state.clone(),
    };
    let mut reopened = ProductionResident::open(&second_launch, gateway)?;
    reopened
        .register_grid_actor(
            binding.clone(),
            initial(2)?,
            NodeGridRecoveryPolicy::BootstrapWhenAbsent,
            true,
        )
        .map_err(|error| format!("restart registration: {error}"))?;
    assert!(state.lock().map_err(|_| "state")?.open_orders.is_empty());
    assert_eq!(
        reopened.strategy_lifecycle(&binding),
        Some(venue_runtime::account::InstanceLifecycle::Running)
    );
    assert!(
        reopened
            .grid_bridges
            .get(&binding.key)
            .ok_or("drained grid")?
            .needs_reconciliation_rebuild()
    );
    assert!(reopened.take_grid_bootstrap_request(&binding)?);
    let snapshot = reopened.refresh_signed_snapshot()?;
    assert!(snapshot.open_orders().is_empty());
    assert_eq!(
        reopened
            .grid_bridges
            .get(&binding.key)
            .ok_or("queued rebuild")?
            .next_install_epoch()?,
        2
    );
    let mut preview = reopened
        .grid_bridges
        .get(&binding.key)
        .ok_or("rebuild preview")?
        .clone();
    assert_eq!(preview.grid.phase, GridPhase::ResettingGrid);
    assert!(!preview.grid.suppress_replenishment_until_inventory_recovers);
    assert_eq!(
        preview.grid.observe_inventory(GridInventory {
            private_generation: snapshot.private_generation(),
            private_observed_at_ms: snapshot.observed_at_ms(),
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::new(12, 2),
            short_quantity: Decimal::new(12, 2),
        })?,
        GridDecision::Noop
    );
    reopened
        .bootstrap_grid_from_signed_market(&binding, snapshot, market()?)
        .map_err(|error| format!("second bootstrap: {error}"))?;
    let rebuilt = reopened
        .grid_bridges
        .get(&binding.key)
        .ok_or("rebuilt grid")?;
    assert_eq!(
        rebuilt.grid.epoch.as_ref().map(|epoch| epoch.epoch),
        Some(2)
    );
    assert!(!rebuilt.has_startup_reconciliation());
    assert_eq!(
        state.lock().map_err(|_| "state")?.open_orders.len(),
        rebuilt.expected_signed_surface()?.len()
    );

    let (raw_private_generation, filled_order, dispatches_before) = {
        let mut state = state.lock().map_err(|_| "state")?;
        let filled_order = state
            .open_orders
            .iter()
            .find(|order| !order.reduce_only)
            .cloned()
            .ok_or("rebuilt entry order")?;
        let filled_native = filled_order
            .venue_order_id
            .as_deref()
            .ok_or("rebuilt native order")?
            .to_owned();
        state
            .open_orders
            .retain(|order| order.venue_order_id.as_deref() != Some(filled_native.as_str()));
        (state.generation, filled_order, state.dispatches)
    };
    let normalized_private_generation = reopened.runtime().active_private_generation();
    assert!(normalized_private_generation > raw_private_generation);
    assert_eq!(
        reopened
            .host
            .normalize_current_gateway_private_generation(raw_private_generation)?,
        normalized_private_generation
    );
    let facts_path = reopened.artifacts_root.join("facts.jsonl");
    let facts_before = std::fs::read_to_string(&facts_path)
        .unwrap_or_default()
        .lines()
        .count();
    let fill = Fill {
        fill_id: "restart-ratchet-first-fill".to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: filled_order
            .venue_order_id
            .clone()
            .ok_or("rebuilt native order")?,
        symbol: filled_order.symbol,
        side: filled_order.side,
        position_side: FieldState::Known(filled_order.position_side),
        quantity: filled_order.quantity,
        price: Price::new(filled_order.limit_price.ok_or("rebuilt limit price")?)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(true),
        exchange_time_ms: Some(now()?),
    };
    let mut stale_fill = fill.clone();
    stale_fill.fill_id = "restart-ratchet-stale-fill".to_owned();
    let consumed = reopened.consume_private_fill(
        "bybit",
        PrivateFillFact {
            source_private_generation: raw_private_generation,
            received_at_ms: now()?,
            fill: fill.clone(),
        },
    );
    let facts_after = std::fs::read_to_string(&facts_path)
        .unwrap_or_default()
        .lines()
        .count();
    let consumed = consumed.map_err(|error| {
        let (dispatches_after, raw_after, open_after) = state
            .lock()
            .map(|state| {
                (
                    state.dispatches,
                    state.generation,
                    state.open_orders.len(),
                )
            })
            .unwrap_or_default();
        let fill_recorded = reopened.grid_bridges.get(&binding.key).is_some_and(|bridge| {
            bridge
                .grid
                .owned_fill_records
                .contains_key("restart-ratchet-first-fill")
        });
        let lifecycle = reopened.strategy_lifecycle(&binding);
        let health = reopened.runtime.health();
        let fault = reopened.runtime.fault_reason();
        io::Error::other(format!(
            "restart-ratcheted private fill failed with facts {facts_before}->{facts_after}, dispatches {dispatches_before}->{dispatches_after}, raw {raw_private_generation}->{raw_after}, open {open_after}, fill_recorded {fill_recorded}, lifecycle {lifecycle:?}, health {health:?}, fault {fault:?}: {error}"
        ))
    })?;
    assert!(consumed);
    let observed = reopened
        .grid_bridges
        .get(&binding.key)
        .and_then(|bridge| {
            bridge
                .grid
                .owned_fill_records
                .get("restart-ratchet-first-fill")
        })
        .ok_or("restart fill record")?;
    assert_eq!(observed.private_generation, normalized_private_generation);
    assert_eq!(
        state.lock().map_err(|_| "state")?.dispatches,
        dispatches_before + 3
    );
    let facts_after_first_fill = std::fs::read_to_string(&facts_path)
        .unwrap_or_default()
        .lines()
        .count();
    let replay_private_generation = state.lock().map_err(|_| "state")?.generation;
    assert_eq!(
        reopened
            .host
            .normalize_current_gateway_private_generation(replay_private_generation)?,
        reopened.runtime().active_private_generation()
    );
    assert!(reopened.consume_private_fill(
        "bybit",
        PrivateFillFact {
            source_private_generation: replay_private_generation,
            received_at_ms: now()?,
            fill,
        },
    )?);
    assert_eq!(
        std::fs::read_to_string(&facts_path)
            .unwrap_or_default()
            .lines()
            .count(),
        facts_after_first_fill
    );
    assert_eq!(
        state.lock().map_err(|_| "state")?.dispatches,
        dispatches_before + 3
    );
    assert!(
        reopened
            .consume_private_fill(
                "bybit",
                PrivateFillFact {
                    source_private_generation: raw_private_generation,
                    received_at_ms: now()?,
                    fill: stale_fill,
                },
            )
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(&facts_path)
            .unwrap_or_default()
            .lines()
            .count(),
        facts_after
    );
    Ok(())
}

#[test]
fn concurrent_signed_fills_are_staged_before_pending_batches_restore_the_full_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;

    let (raw_generation, dispatches_before, expected_count, first_fill, second_fill) = {
        let mut state = state.lock().map_err(|_| "state")?;
        let selected = state
            .open_orders
            .iter()
            .filter(|order| !order.reduce_only)
            .take(2)
            .cloned()
            .collect::<Vec<_>>();
        let [first, second] = selected.as_slice() else {
            return Err("two entry orders".into());
        };
        let make_fill = |id: &str,
                         sequence: u64,
                         order: &SignedAccountOrderFact|
         -> Result<Fill, Box<dyn std::error::Error>> {
            Ok(Fill {
                fill_id: id.to_owned(),
                execution_sequence: FieldState::Known(sequence),
                order_id: order.venue_order_id.clone().ok_or("signed native order")?,
                symbol: order.symbol.clone(),
                side: order.side,
                position_side: FieldState::Known(order.position_side),
                quantity: order.quantity,
                price: Price::new(order.limit_price.ok_or("signed limit price")?)?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(now()?),
            })
        };
        let first_fill = make_fill("burst-fill-1", 1, first)?;
        let second_fill = make_fill("burst-fill-2", 2, second)?;
        let filled_native = [first_fill.order_id.as_str(), second_fill.order_id.as_str()];
        let expected_count = state.open_orders.len();
        state.open_orders.retain(|order| {
            order
                .venue_order_id
                .as_deref()
                .is_none_or(|native| !filled_native.contains(&native))
        });
        state.fills = vec![first_fill.clone(), second_fill.clone()];
        (
            state.generation,
            state.dispatches,
            expected_count,
            first_fill,
            second_fill,
        )
    };

    assert!(resident.consume_private_fill(
        "bybit",
        PrivateFillFact {
            source_private_generation: raw_generation,
            received_at_ms: now()?,
            fill: first_fill,
        },
    )?);
    let state = state.lock().map_err(|_| "state")?;
    assert_eq!(state.dispatches, dispatches_before + 6);
    assert_eq!(state.open_orders.len(), expected_count);
    drop(state);
    let bridge = resident
        .grid_bridges
        .get(&binding.key)
        .ok_or("grid bridge")?;
    assert!(bridge.pending_dispatch_plans()?.is_empty());
    assert!(
        bridge.signed_desired_matches(
            resident
                .host
                .latest_signed_snapshot()
                .ok_or("latest signed snapshot")?
                .open_orders()
        )
    );
    assert!(bridge.grid.owned_fill_records.contains_key("burst-fill-1"));
    assert!(
        bridge
            .grid
            .owned_fill_records
            .contains_key(&second_fill.fill_id)
    );
    assert!(
        resident
            .host
            .latest_signed_snapshot()
            .ok_or("latest signed snapshot")?
            .fills()
            .is_empty()
    );
    Ok(())
}

#[test]
fn periodic_signed_supervision_drains_unexplained_gap_before_one_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    let initial_count = state.lock().map_err(|_| "state")?.open_orders.len();
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.open_orders.pop().ok_or("installed order")?;
        assert_eq!(state.open_orders.len(), initial_count - 1);
    }

    assert!(resident.supervise_grid_signed_surface_once(&binding)?);
    assert!(state.lock().map_err(|_| "state")?.open_orders.is_empty());
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    assert_eq!(
        state.lock().map_err(|_| "state")?.open_orders.len(),
        initial_count
    );
    assert!(!resident.supervise_grid_signed_surface_once(&binding)?);
    Ok(())
}

#[test]
fn periodic_signed_supervision_never_cancels_when_surface_has_an_external_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    let (dispatches, order_count) = {
        let mut state = state.lock().map_err(|_| "state")?;
        let mut external = state.open_orders.first().cloned().ok_or("grid order")?;
        external.client_order_id = "external-order".to_owned();
        external.venue_order_id = Some("external-native".to_owned());
        external.owner = None;
        external.external = true;
        state.open_orders.push(external);
        (state.dispatches, state.open_orders.len())
    };

    assert!(
        resident
            .supervise_grid_signed_surface_once(&binding)
            .is_err()
    );
    let state = state.lock().map_err(|_| "state")?;
    assert_eq!(state.dispatches, dispatches);
    assert_eq!(state.open_orders.len(), order_count);
    Ok(())
}

#[test]
fn periodic_signed_supervision_never_resets_a_pending_unknown_rolling_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    let dispatches = {
        let mut state = state.lock().map_err(|_| "state")?;
        let order = state
            .open_orders
            .iter()
            .find(|order| !order.reduce_only)
            .cloned()
            .ok_or("entry order")?;
        let fill = Fill {
            fill_id: "unknown-rolling-fill".to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: order.venue_order_id.clone().ok_or("native order")?,
            symbol: order.symbol.clone(),
            side: order.side,
            position_side: FieldState::Known(order.position_side),
            quantity: order.quantity,
            price: Price::new(order.limit_price.ok_or("limit price")?)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(now()?),
        };
        state
            .open_orders
            .retain(|candidate| candidate.venue_order_id != order.venue_order_id);
        state.fills = vec![fill];
        state.accept_dispatch = false;
        state.dispatches
    };

    assert!(
        resident
            .supervise_grid_signed_surface_once(&binding)
            .is_err()
    );
    let after_unknown = state.lock().map_err(|_| "state")?.dispatches;
    assert_eq!(after_unknown, dispatches + 1);
    assert!(
        resident
            .supervise_grid_signed_surface_once(&binding)
            .is_err()
    );
    assert_eq!(state.lock().map_err(|_| "state")?.dispatches, after_unknown);
    Ok(())
}

#[test]
fn signed_supervision_recovers_missed_fill_before_considering_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    let (initial_count, dispatches, fill) = {
        let mut state = state.lock().map_err(|_| "state")?;
        let order = state
            .open_orders
            .iter()
            .find(|order| !order.reduce_only)
            .cloned()
            .ok_or("entry order")?;
        let fill = Fill {
            fill_id: "missed-private-fill".to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: order.venue_order_id.clone().ok_or("native order")?,
            symbol: order.symbol.clone(),
            side: order.side,
            position_side: FieldState::Known(order.position_side),
            quantity: order.quantity,
            price: Price::new(order.limit_price.ok_or("limit price")?)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(now()?),
        };
        let initial_count = state.open_orders.len();
        let dispatches = state.dispatches;
        state
            .open_orders
            .retain(|candidate| candidate.venue_order_id != order.venue_order_id);
        state.fills = vec![fill.clone()];
        (initial_count, dispatches, fill)
    };

    assert!(!resident.supervise_grid_signed_surface_once(&binding)?);
    let state = state.lock().map_err(|_| "state")?;
    assert_eq!(state.dispatches, dispatches + 3);
    assert_eq!(state.open_orders.len(), initial_count);
    assert!(
        resident
            .grid_bridges
            .get(&binding.key)
            .ok_or("bridge")?
            .grid
            .owned_fill_records
            .contains_key(&fill.fill_id)
    );
    Ok(())
}

#[test]
fn bootstrap_admission_error_retains_risk_stage() {
    let stage = AccountHostValidationError::RiskEvidenceStage("quote_rates");
    let message = grid_bootstrap_admission_error(VenueId::Binance, &stage).to_string();
    assert!(message.contains("failed closed at quote_rates"));
}

#[test]
fn recovered_restart_keeps_the_initial_replenishment_latch_cleared()
-> Result<(), Box<dyn std::error::Error>> {
    let mut bridge = grid::GridBridgeState::bootstrap(initial(1)?)?;
    apply_grid_restart_replenishment_policy(&mut bridge, true, false)?;
    assert!(bridge.grid.suppress_replenishment_until_inventory_recovers);

    bridge.grid.phase = venue_strategies::hedged_grid::GridPhase::Running;
    bridge.grid.suppress_replenishment_until_inventory_recovers = false;
    apply_grid_restart_replenishment_policy(&mut bridge, true, false)?;
    assert!(!bridge.grid.suppress_replenishment_until_inventory_recovers);

    bridge.grid.suppress_replenishment_until_inventory_recovers = true;
    assert!(apply_grid_restart_replenishment_policy(&mut bridge, false, false).is_err());
    Ok(())
}

#[test]
fn custody_only_actor_checkpoint_rearms_first_bootstrap_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 2)?;
    let replay = resident
        .grid_bridges
        .get(&binding.key)
        .ok_or("grid bridge")?
        .checkpoint_bytes()?;
    let applied = resident
        .runtime
        .persist_resident_semantic_turn(&binding, replay)?;
    persist_anchor(&resident.artifacts_root, &binding, &applied)?;
    drop(resident);

    let second_launch = launch(directory.path())?;
    let gateway = Gateway {
        binding: second_launch.binding().clone(),
        state: state.clone(),
    };
    let mut reopened = ProductionResident::open(&second_launch, gateway)?;
    reopened.register_grid_actor(
        binding.clone(),
        initial(2)?,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        true,
    )?;
    assert!(reopened.take_grid_bootstrap_request(&binding)?);
    assert!(!reopened.take_grid_bootstrap_request(&binding)?);
    drop(reopened);

    let third_launch = launch(directory.path())?;
    let gateway = Gateway {
        binding: third_launch.binding().clone(),
        state,
    };
    let mut fenced = ProductionResident::open(&third_launch, gateway)?;
    fenced.register_grid_actor(
        binding.clone(),
        initial(2)?,
        NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        true,
    )?;
    assert!(!fenced.take_grid_bootstrap_request(&binding)?);
    assert_eq!(
        fenced.strategy_lifecycle(&binding),
        Some(venue_runtime::account::InstanceLifecycle::Paused)
    );
    Ok(())
}

#[test]
fn shared_grid_private_fill_uses_signed_private_generation_not_connection_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (mut resident, state, binding) = resident(directory.path(), 3)?;
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state.accept_dispatch = true;
        state.long_quantity = Decimal::new(12, 2);
        state.short_quantity = Decimal::new(12, 2);
    }
    assert!(resident.take_grid_bootstrap_request(&binding)?);
    let snapshot = resident.refresh_signed_snapshot()?;
    resident.bootstrap_grid_from_signed_market(&binding, snapshot, market()?)?;
    let (source_private_generation, signed_order) = {
        let state = state.lock().map_err(|_| "state")?;
        (
            state.generation,
            state
                .open_orders
                .iter()
                .find(|order| !order.reduce_only)
                .cloned()
                .ok_or("signed Grid order")?,
        )
    };
    assert!(resident.runtime().connection_generation() > source_private_generation);
    let fill = Fill {
        fill_id: "grid-private-generation".to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: signed_order
            .venue_order_id
            .clone()
            .ok_or("signed native order")?,
        symbol: signed_order.symbol,
        side: signed_order.side,
        position_side: FieldState::Known(signed_order.position_side),
        quantity: signed_order.quantity,
        price: Price::new(signed_order.limit_price.ok_or("signed limit price")?)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(true),
        exchange_time_ms: Some(now()?),
    };
    {
        let mut state = state.lock().map_err(|_| "state")?;
        state
            .open_orders
            .retain(|order| order.venue_order_id.as_deref() != Some(fill.order_id.as_str()));
        state.fills.push(fill.clone());
    }
    assert!(
        resident
            .consume_private_fill(
                "bybit",
                PrivateFillFact {
                    source_private_generation,
                    received_at_ms: now()?,
                    fill,
                },
            )
            .map_err(|error| io::Error::other(format!("private fill: {error}")))?
    );
    let observed = resident
        .grid_bridges
        .get(&binding.key)
        .and_then(|bridge| {
            bridge
                .grid
                .owned_fill_records
                .get("grid-private-generation")
        })
        .ok_or("grid fill record")?;
    assert_eq!(observed.private_generation, source_private_generation);
    assert_ne!(
        observed.private_generation,
        resident.runtime().connection_generation()
    );
    drop(resident);

    let second_launch = launch(directory.path())?;
    let gateway = Gateway {
        binding: second_launch.binding().clone(),
        state,
    };
    let mut reopened = ProductionResident::open(&second_launch, gateway)?;
    reopened.register_actor(binding.clone())?;
    let recovered_checkpoint = reopened
        .runtime
        .resident_actor_checkpoint(&binding)?
        .ok_or("recovered grid checkpoint")?;
    reopened
        .runtime
        .persist_resident_semantic_turn(&binding, recovered_checkpoint)?;
    Ok(())
}
