use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use serde::Serialize;
use venue_control_protocol::ControlAction;
use venue_domain::domain::{ExecutionCommand, Price};
use venue_runtime::{
    AccountPhysicalGateway, AccountSymbolSet, AppliedStrategyTurnReceipt, StrategyBinding,
    account::{AccountKey, AccountRuntime, AccountRuntimeHost, ResidentActorAppliedArtifacts},
};
use venue_storage::ActorAppliedAnchor;

use crate::{NodeError, NodeLaunch};

mod copy;
#[cfg_attr(not(feature = "binance"), allow(dead_code))]
pub(crate) mod grid;
pub(crate) mod manual;
pub(crate) mod scalping;
pub use copy::{ResidentCopyReconciliation, ResidentCopyResult};
pub(crate) mod control;

const ACTOR_APPLIED_JOURNAL: &str = "actor-applied.jsonl";
const ACTOR_APPLIED_CHECKPOINT: &str = "actor-applied.json";
const ACTOR_APPLIED_ANCHOR: &str = "actor-applied.anchor.json";

/// The one production composition used by a Node: Runtime owns turns/lane and this wrapper owns
/// the one Host.  It deliberately provides no gateway or prepared-command escape hatch.
pub struct ProductionResident<G> {
    runtime: AccountRuntime,
    host: AccountRuntimeHost<G>,
    artifacts_root: PathBuf,
    grid_bridges: BTreeMap<venue_runtime::StrategyInstanceKey, grid::GridBridgeState>,
    grid_bindings: BTreeMap<venue_runtime::StrategyInstanceKey, StrategyBinding>,
    grid_bootstrap_pending: BTreeSet<venue_runtime::StrategyInstanceKey>,
    scalping_bridges: BTreeMap<venue_runtime::StrategyInstanceKey, scalping::ScalpingBridgeState>,
    scalping_bindings: BTreeMap<venue_runtime::StrategyInstanceKey, StrategyBinding>,
    scalping_books: BTreeMap<venue_runtime::StrategyInstanceKey, venue_indicators::OrderBook>,
    scalping_features:
        BTreeMap<venue_runtime::StrategyInstanceKey, venue_indicators::ScalpingPublicMarketSource>,
    #[cfg(feature = "bitget")]
    scalping_bitget_books:
        BTreeMap<venue_runtime::StrategyInstanceKey, scalping::BitgetScalpingBookBridge>,
    #[cfg(feature = "gate")]
    scalping_gate_books:
        BTreeMap<venue_runtime::StrategyInstanceKey, scalping::GateScalpingBookBridge>,
    #[cfg_attr(
        not(any(feature = "binance", feature = "bitget", feature = "gate")),
        allow(dead_code)
    )]
    scalping_capture_sequence: BTreeMap<venue_runtime::StrategyInstanceKey, u64>,
}

/// The strictly bounded market input needed by the one Grid bootstrap calculation.  The Binance
/// gateway is the only production producer today; keeping this value free of native protocol
/// types lets the bootstrap path be exercised against the real Host/WAL composition without
/// granting a fake gateway any mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(feature = "binance"), allow(dead_code))]
pub(crate) struct GridBootstrapMarket {
    pub(crate) bid: Price,
    pub(crate) ask: Price,
    pub(crate) price_tick: Price,
    pub(crate) quantity_step: rust_decimal::Decimal,
    pub(crate) minimum_quantity: rust_decimal::Decimal,
    pub(crate) maximum_quantity: rust_decimal::Decimal,
    pub(crate) minimum_notional: rust_decimal::Decimal,
    pub(crate) observed_at_ms: u64,
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    pub fn open(launch: &NodeLaunch, gateway: G) -> Result<Self, NodeError> {
        Self::open_with_symbols(launch, AccountSymbolSet::single(launch.binding()), gateway)
    }

    /// Multiple strategy symbols deliberately still enter one runtime Host.  Only the adapter's
    /// rules catalog changes per symbol; account recovery, WAL and execution lane do not.
    pub fn open_with_symbols(
        launch: &NodeLaunch,
        configured_symbols: AccountSymbolSet,
        gateway: G,
    ) -> Result<Self, NodeError> {
        let venue = launch.binding().venue;
        let mut host = match launch.legacy_v1_predecessor() {
            Some(predecessor) => AccountRuntimeHost::open_with_symbols_and_legacy_v1_predecessor(
                launch.artifacts_root(),
                launch.binding().clone(),
                configured_symbols,
                gateway,
                predecessor.clone(),
            ),
            None => AccountRuntimeHost::open_with_symbols(
                launch.artifacts_root(),
                launch.binding().clone(),
                configured_symbols,
                gateway,
            ),
        }
        .map_err(|error| NodeError::LiveHost {
            venue,
            message: error.to_string(),
        })?;
        let account = AccountKey::new(
            launch.binding().venue,
            launch.binding().trading_account_id.clone(),
        )
        .map_err(|error| NodeError::LiveHost {
            venue,
            message: error.to_string(),
        })?;
        let mut runtime = AccountRuntime::new(account);
        host.bootstrap_runtime(&mut runtime)
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: error.to_string(),
            })?;
        runtime
            .attach_private_ingress(launch.artifacts_root().join("facts.jsonl"))
            .map_err(resident_error)?;
        Ok(Self {
            runtime,
            host,
            artifacts_root: launch.artifacts_root(),
            grid_bridges: BTreeMap::new(),
            grid_bindings: BTreeMap::new(),
            grid_bootstrap_pending: BTreeSet::new(),
            scalping_bridges: BTreeMap::new(),
            scalping_bindings: BTreeMap::new(),
            scalping_books: BTreeMap::new(),
            scalping_features: BTreeMap::new(),
            #[cfg(feature = "bitget")]
            scalping_bitget_books: BTreeMap::new(),
            #[cfg(feature = "gate")]
            scalping_gate_books: BTreeMap::new(),
            scalping_capture_sequence: BTreeMap::new(),
        })
    }

    #[must_use]
    pub const fn runtime(&self) -> &AccountRuntime {
        &self.runtime
    }

    #[must_use]
    pub fn has_unresolved(&self) -> bool {
        self.host.has_unresolved()
    }

    /// Closed orders disappear from open-order snapshots. Resolve their fills from the same
    /// account WAL, never by treating every trade on the actor's symbol as strategy-owned.
    pub(crate) fn owner_for_signed_fill(
        &self,
        fill: &venue_domain::Fill,
    ) -> Option<venue_domain::OrderOwner> {
        use venue_domain::{FieldState, NativeOrderFamily, PositionSide};
        let command = self
            .host
            .command_snapshot_by_venue_order_id(NativeOrderFamily::UmOrder, &fill.order_id)?;
        let (owner, side, position_side) = match command {
            ExecutionCommand::PlaceLimit(row) => (row.owner, row.side, row.position_side),
            ExecutionCommand::PlaceMarket(row) => (row.owner, row.side, row.position_side),
            ExecutionCommand::MarketReduce(row) => (row.owner, row.side, row.position_side),
            _ => return None,
        };
        if fill.symbol != owner.symbol
            || fill.side != side
            || matches!(fill.position_side, FieldState::Known(value)
                if value != PositionSide::Net && value != position_side)
        {
            return None;
        }
        Some(owner)
    }

    /// The Host persists complete signed facts before updating Runtime's generation and risk
    /// fence. An operator Pause remains latched; only exact signed outcomes settle Unknown.
    pub fn refresh_signed_snapshot(
        &mut self,
    ) -> Result<venue_runtime::SignedAccountSnapshot, NodeError> {
        self.host
            .refresh_runtime_signed_snapshot(&mut self.runtime)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })
    }

    /// Reads the current adapter-validated rules identity through the sole account Host. This
    /// is deliberately separate from configuration: a planning fact cannot invent settlement or
    /// market type from a symbol.
    pub fn current_instrument(
        &mut self,
    ) -> Result<venue_runtime::AccountInstrumentIdentity, NodeError> {
        self.host
            .current_instrument()
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })
    }

    pub fn current_instrument_for(
        &mut self,
        symbol: &venue_domain::domain::Symbol,
    ) -> Result<venue_runtime::AccountInstrumentIdentity, NodeError> {
        self.host
            .current_instrument_for(symbol)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })
    }

    #[must_use]
    pub fn strategy_lifecycle(
        &self,
        binding: &StrategyBinding,
    ) -> Option<venue_runtime::account::InstanceLifecycle> {
        self.runtime.strategy_lifecycle(binding)
    }

    pub fn register_actor(&mut self, binding: StrategyBinding) -> Result<(), NodeError> {
        self.register_actor_with_anchor(binding, None)
    }

    /// Registers a Grid actor and restores its state only from the matching Runtime-owned Actor
    /// Applied checkpoint. The bridge is deliberately absent for a generic actor: a Grid route
    /// cannot be reconstructed from BBO, side, or an exchange order id alone.
    pub fn register_grid_actor(
        &mut self,
        binding: StrategyBinding,
        initial: venue_strategies::hedged_grid::HedgedGridState,
        recovery: crate::NodeGridRecoveryPolicy,
        skip_inventory_replenishment_until_recovered: bool,
    ) -> Result<(), NodeError> {
        if binding.key.strategy_kind != venue_runtime::StrategyKind::HedgedGrid
            || self.grid_bridges.contains_key(&binding.key)
        {
            return Err(NodeError::ResidentRuntime);
        }
        self.register_actor(binding.clone())?;
        let checkpoint = self
            .runtime
            .resident_actor_checkpoint(&binding)
            .map_err(resident_error)?;
        let first_bootstrap =
            checkpoint.is_none() && recovery == crate::NodeGridRecoveryPolicy::BootstrapWhenAbsent;
        let mut bridge =
            grid::GridBridgeState::restore_or_bootstrap(checkpoint, initial, recovery)?;
        if bridge.grid.suppress_replenishment_until_inventory_recovers
            != skip_inventory_replenishment_until_recovered
        {
            if skip_inventory_replenishment_until_recovered
                && bridge.grid.phase == venue_strategies::hedged_grid::GridPhase::Recovering
            {
                bridge
                    .grid
                    .request_restart_without_replenishment()
                    .map_err(|_| NodeError::ResidentRuntime)?;
            } else {
                // The durable checkpoint is the mode authority after bootstrap. A changed config
                // digest must not silently turn this market-order avoidance latch off or on.
                return Err(NodeError::ResidentRuntime);
            }
        }
        let key = binding.key.clone();
        self.grid_bridges.insert(key.clone(), bridge);
        self.grid_bindings.insert(key.clone(), binding);
        if first_bootstrap {
            self.grid_bootstrap_pending.insert(key);
        }
        Ok(())
    }

    /// Consumes the one permitted first-install attempt. Checkpoint recovery never sets this
    /// flag, so a restart cannot manufacture another epoch from fresh BBO after an earlier
    /// accepted, rejected, or Unknown installation attempt.
    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    pub(crate) fn take_grid_bootstrap_request(&mut self, binding: &StrategyBinding) -> bool {
        self.grid_bindings
            .get(&binding.key)
            .is_some_and(|registered| registered == binding)
            && self.grid_bootstrap_pending.remove(&binding.key)
    }

    pub fn register_actor_with_anchor(
        &mut self,
        binding: StrategyBinding,
        _expected_anchor: Option<ActorAppliedAnchor>,
    ) -> Result<(), NodeError> {
        let actor_root = self
            .artifacts_root
            .join("strategies")
            .join(&binding.key.instance_id);
        fs::create_dir_all(&actor_root).map_err(|_| NodeError::ResidentArtifacts)?;
        self.runtime
            .register_strategy(binding.clone())
            .map_err(resident_error)?;
        let artifacts = actor_artifacts(&actor_root)?;
        self.host
            .install_resident_actor_applied_artifacts(&mut self.runtime, &binding, artifacts)
            .map_err(|_| NodeError::ResidentRuntime)?;
        self.runtime
            .activate_resident_strategy(&binding)
            .map_err(resident_error)
    }

    /// Control actions are first represented by the durable resident Actor checkpoint, then
    /// applied to the in-memory runtime state.  Receipt publication is deliberately left to the
    /// delivery driver so an uncertain HTTP result cannot cause a second Actor application.
    pub fn apply_control_action(
        &mut self,
        binding: &StrategyBinding,
        action: ControlAction,
    ) -> Result<AppliedStrategyTurnReceipt, NodeError> {
        // A manual order needs its complete TradeIntent, not just a lifecycle action. Until
        // that adapter-neutral path exists it must not persist an Applied or resume an Actor.
        if action == ControlAction::Trade {
            return Err(NodeError::ResidentRuntime);
        }
        let replay = serde_json::to_vec(&ResidentControlReplay { action })
            .map_err(|_| NodeError::ResidentRuntime)?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        let result = match action {
            ControlAction::Pause => self.runtime.request_pause(&binding.key).map(|_| ()),
            ControlAction::Resume => self.runtime.request_resume(&binding.key).map(|_| ()),
            ControlAction::Stop => self.runtime.request_stop(&binding.key).map(|_| ()),
            ControlAction::Flatten => self.runtime.request_flatten(&binding.key).map(|_| ()),
            ControlAction::Trade => return Err(NodeError::ResidentRuntime),
        };
        result.map_err(resident_error)?;
        Ok(applied)
    }

    /// Persists the semantic input before Host prepares the same physical command, admits it to
    /// the Runtime lane, and dispatches via the one account writer.  Repeating a command id is
    /// delegated to the Host WAL; unknown outcomes are never re-submitted here.
    pub fn submit_operator_command(
        &mut self,
        binding: &StrategyBinding,
        command: ExecutionCommand,
    ) -> Result<(), NodeError> {
        let command_id = command.command_id().clone();
        // The canary's fill fallback needs a signed leg baseline from immediately before its
        // sole dispatch.  Take it before persisting the Actor turn: a fresh private generation
        // invalidates prior Applied receipts, so it cannot sit between persistence and admission.
        let signed_before = self.refresh_signed_snapshot()?;
        let position_before = match &command {
            ExecutionCommand::PlaceLimit(order) => Some(manual::signed_position_quantity(
                &signed_before,
                &order.owner.symbol,
                order.position_side,
            )),
            _ => None,
        };
        let replay = serde_json::to_vec(&ResidentReplay { command: &command })
            .map_err(|_| NodeError::ResidentRuntime)?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        self.host
            .prepare_and_admit_operator(
                &mut self.runtime,
                binding,
                &applied,
                venue_runtime::account::AccountLanePriority::Normal,
                command.clone(),
            )
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let _follow_up = self
            .runtime
            .dispatch_next_with_host(&mut self.host)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        match self
            .host
            .command_status(&command_id)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?
            .map(|status| status.state().clone())
        {
            Some(venue_runtime::CommandState::Accepted { venue_order_id }) => {
                let signed_after = self.refresh_signed_snapshot()?;
                if manual::signed_operator_canary_matches(
                    &command,
                    &venue_order_id,
                    position_before,
                    &signed_after,
                ) {
                    Ok(())
                } else {
                    Err(NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: "operator canary is Accepted in the WAL but its fresh complete signed readback does not yet match".to_owned(),
                    })
                }
            }
            Some(venue_runtime::CommandState::Rejected { reason }) => Err(NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: format!("operator canary rejected: {reason}"),
            }),
            Some(venue_runtime::CommandState::Prepared)
            | Some(venue_runtime::CommandState::Submitted)
            | Some(venue_runtime::CommandState::Unknown { .. })
            | None => Err(NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: "operator canary outcome is unresolved; signed reconciliation is required"
                    .to_owned(),
            }),
        }
    }

    /// The shared part of the Binance initial-install path. The concrete adapter owns the only
    /// production market read; tests inject the already-normalized bounded facts and still pass
    /// through the account Host, WAL and execution lane below.
    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    pub(crate) fn bootstrap_grid_from_signed_market(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        market: GridBootstrapMarket,
    ) -> Result<(), NodeError> {
        let result = self.bootstrap_grid_from_signed_market_inner(binding, snapshot, market);
        if result.is_err() {
            // Registration starts the generic actor so it can own the sole account route, but a
            // Grid without an exactly signed desired surface is never allowed to keep accepting
            // risk after a failed first install.
            let _paused = self.runtime.request_pause(&binding.key);
        }
        result
    }

    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    fn bootstrap_grid_from_signed_market_inner(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        market: GridBootstrapMarket,
    ) -> Result<(), NodeError> {
        use rust_decimal::Decimal;
        use venue_domain::domain::PositionSide;
        use venue_strategies::hedged_grid::{GridEpoch, GridInventory};

        if market.bid >= market.ask
            || market.price_tick.value() <= Decimal::ZERO
            || market.quantity_step <= Decimal::ZERO
            || market.minimum_quantity <= Decimal::ZERO
            || market.maximum_quantity < market.minimum_quantity
            || market.minimum_notional <= Decimal::ZERO
            || market.observed_at_ms == 0
        {
            return Err(NodeError::ResidentRuntime);
        }
        if market.observed_at_ms < snapshot.observed_at_ms()
            || market
                .observed_at_ms
                .saturating_sub(snapshot.observed_at_ms())
                > 3_000
        {
            return Err(NodeError::ResidentRuntime);
        }
        let leg = |side| {
            snapshot.positions().iter().find(|position| {
                position.symbol == binding.key.symbol && position.position_side == side
            })
        };
        let long = leg(PositionSide::Long).ok_or(NodeError::ResidentRuntime)?;
        let short = leg(PositionSide::Short).ok_or(NodeError::ResidentRuntime)?;
        let mark = match (long.mark_price, short.mark_price) {
            (Some(left), Some(right)) if left == right => {
                Price::new(left).map_err(|_| NodeError::ResidentRuntime)?
            }
            _ => return Err(NodeError::ResidentRuntime),
        };
        let inventory = GridInventory {
            private_generation: snapshot.private_generation(),
            private_observed_at_ms: snapshot.observed_at_ms(),
            mark_price: mark,
            long_quantity: long.quantity,
            short_quantity: short.quantity,
        };
        let (plan, replay) = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            let anchor = (market.bid.value() + market.ask.value()) / Decimal::from(2_u8);
            let tick = market.price_tick.value();
            let anchor = anchor - anchor % tick;
            let step_raw = anchor * bridge.grid.params.spacing_rate;
            let step = step_raw - step_raw % tick;
            let max_open = anchor.checked_add(step).ok_or(NodeError::ResidentRuntime)?;
            let quantity_raw = bridge
                .grid
                .params
                .order_notional
                .value
                .checked_div(max_open)
                .ok_or(NodeError::ResidentRuntime)?;
            let quantity = (quantity_raw / market.quantity_step).ceil() * market.quantity_step;
            if quantity < market.minimum_quantity || quantity > market.maximum_quantity {
                return Err(NodeError::ResidentRuntime);
            }
            let epoch = GridEpoch {
                epoch: 1,
                anchor_price: Price::new(anchor).map_err(|_| NodeError::ResidentRuntime)?,
                step: Price::new(step).map_err(|_| NodeError::ResidentRuntime)?,
                grid_quantity: quantity,
                passive_book_fallback: None,
            };
            let plan = bridge
                .install_initial_epoch(inventory, epoch)
                .map_err(|_| NodeError::ResidentRuntime)?;
            if plan.commands.iter().any(|command| {
                matches!(command, ExecutionCommand::PlaceLimit(order)
                    if !order.reduce_only
                        && order.quantity * order.limit_price.value() < market.minimum_notional)
            }) {
                return Err(NodeError::ResidentRuntime);
            }
            let replay = bridge.checkpoint_bytes()?;
            (plan, replay)
        };
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;

        // All admission occurs before the first dispatch. The Host's durable reservations make
        // the later candidates see the entire batch, including the opening wave.
        for command in &plan.commands {
            if self
                .host
                .prepare_and_admit_operator(
                    &mut self.runtime,
                    binding,
                    &applied,
                    venue_runtime::account::AccountLanePriority::Normal,
                    command.clone(),
                )
                .is_err()
            {
                self.host
                    .reject_prepared_batch(&mut self.runtime, "grid_bootstrap_batch_rejected")
                    .map_err(|_| NodeError::ResidentRuntime)?;
                self.pause_grid_after_bootstrap_failure(binding)?;
                return Err(NodeError::ResidentRuntime);
            }
        }
        for _ in &plan.commands {
            if self
                .runtime
                .dispatch_next_with_host(&mut self.host)
                .is_err()
            {
                self.pause_grid_after_bootstrap_failure(binding)?;
                return Err(NodeError::ResidentRuntime);
            }
        }
        let mut accepted = Vec::new();
        for command in &plan.commands {
            let status = self
                .host
                .command_status(command.command_id())
                .map_err(|_| NodeError::ResidentRuntime)?
                .ok_or(NodeError::ResidentRuntime)?;
            if let venue_runtime::CommandState::Accepted { venue_order_id } = status.state() {
                accepted.push((command.command_id().clone(), venue_order_id.clone()));
            } else {
                self.pause_grid_after_bootstrap_failure(binding)?;
                return Err(NodeError::ResidentRuntime);
            }
        }
        let replay = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            bridge
                .bind_accepted_plan(&plan, &accepted)
                .map_err(|_| NodeError::ResidentRuntime)?;
            bridge.checkpoint_bytes()?
        };
        let accepted_turn = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &accepted_turn)?;
        let confirmed = self.refresh_signed_snapshot()?;
        let bridge = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let exact = bridge.signed_desired_matches(confirmed.open_orders());
        if confirmed.private_generation() <= snapshot.private_generation() || !exact {
            self.pause_grid_after_bootstrap_failure(binding)?;
            return Err(NodeError::ResidentRuntime);
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    fn pause_grid_after_bootstrap_failure(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        self.runtime
            .request_pause(&binding.key)
            .map_err(resident_error)
    }
}

#[derive(Serialize)]
struct ResidentReplay<'a> {
    command: &'a ExecutionCommand,
}

#[derive(Serialize)]
struct ResidentControlReplay {
    action: ControlAction,
}

fn actor_artifacts(root: &Path) -> Result<ResidentActorAppliedArtifacts, NodeError> {
    let journal = root.join(ACTOR_APPLIED_JOURNAL);
    let checkpoint = root.join(ACTOR_APPLIED_CHECKPOINT);
    let anchor = root.join(ACTOR_APPLIED_ANCHOR);
    let existing = [journal.exists(), checkpoint.exists(), anchor.exists()];
    match existing {
        [false, false, false] => Ok(ResidentActorAppliedArtifacts::create_new(
            journal, checkpoint,
        )),
        [true, true, true] => {
            let encoded = fs::read(anchor).map_err(|_| NodeError::ResidentArtifacts)?;
            let anchor = serde_json::from_slice::<ActorAppliedAnchor>(&encoded)
                .map_err(|_| NodeError::ResidentArtifacts)?;
            Ok(ResidentActorAppliedArtifacts::open_existing(
                journal, checkpoint, anchor,
            ))
        }
        _ => Err(NodeError::ResidentArtifacts),
    }
}

fn persist_anchor(
    artifacts_root: &Path,
    binding: &StrategyBinding,
    applied: &AppliedStrategyTurnReceipt,
) -> Result<(), NodeError> {
    let anchor = applied
        .actor_applied_anchor()
        .ok_or(NodeError::ResidentRuntime)?;
    persist_actor_anchor(artifacts_root, binding, &anchor)
}

fn persist_actor_anchor(
    artifacts_root: &Path,
    binding: &StrategyBinding,
    anchor: &ActorAppliedAnchor,
) -> Result<(), NodeError> {
    let path = artifacts_root
        .join("strategies")
        .join(&binding.key.instance_id)
        .join(ACTOR_APPLIED_ANCHOR);
    let encoded = serde_json::to_vec(anchor).map_err(|_| NodeError::ResidentArtifacts)?;
    let temporary = path.with_extension("tmp");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|_| NodeError::ResidentArtifacts)?;
    file.write_all(&encoded)
        .map_err(|_| NodeError::ResidentArtifacts)?;
    file.sync_all().map_err(|_| NodeError::ResidentArtifacts)?;
    drop(file);
    fs::rename(temporary, &path).map_err(|_| NodeError::ResidentArtifacts)?;
    #[cfg(unix)]
    fs::File::open(path.parent().ok_or(NodeError::ResidentArtifacts)?)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| NodeError::ResidentArtifacts)?;
    Ok(())
}

fn resident_error(error: venue_runtime::account::AccountRuntimeError) -> NodeError {
    let _ = error;
    NodeError::ResidentRuntime
}

#[cfg(feature = "binance")]
impl ProductionResident<venue_gateway_binance::BinanceAccountGateway> {
    /// First-install Grid path. The gateway reads only current BBO/rules; the Host obtains the
    /// complete signed snapshot. Missing hedge legs, low inventory, stale BBO or any batch
    /// admission failure stop before dispatch.
    pub fn bootstrap_binance_grid_once(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        let snapshot = self.refresh_signed_snapshot()?;
        let market = self
            .host
            .with_gateway_read(|gateway| gateway.fresh_grid_bootstrap_market())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        self.bootstrap_grid_from_signed_market(
            binding,
            snapshot,
            GridBootstrapMarket {
                bid: market.bid,
                ask: market.ask,
                price_tick: market.rules.instrument.price_tick,
                quantity_step: market.rules.instrument.quantity_step,
                minimum_quantity: market.rules.minimum_quantity,
                maximum_quantity: market.rules.maximum_quantity,
                minimum_notional: market.rules.instrument.minimum_notional.value,
                observed_at_ms: market.observed_at_ms,
            },
        )
    }
    /// Reads at most one bounded Binance private frame, fsyncs its normalized Fill through the
    /// shared account facts journal, then applies the exact routed Grid reducer turn. No raw
    /// payload is retained and this path contains no BBO/REST request.
    pub fn poll_binance_grid_private_once(&mut self) -> Result<bool, NodeError> {
        use venue_domain::domain::{DomainEvent, EventId, NativeOrderFamily};
        use venue_runtime::{account::AccountPrivateFactInput, strategy::StrategyInput};

        let event = self
            .host
            .with_gateway_read(|gateway| gateway.poll_private_fill())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let Some(event) = event else {
            return Ok(false);
        };
        let event_id = EventId::new(format!("binance-fill-{}", event.fill.fill_id))
            .map_err(|_| NodeError::ResidentRuntime)?;
        let manual_fill = event.fill.clone();
        let report = self
            .runtime
            .ingest_private(
                AccountPrivateFactInput::new(
                    event_id,
                    event.private_generation,
                    event.received_at_ms,
                    Some(NativeOrderFamily::UmOrder),
                    DomainEvent::Fill(event.fill),
                )
                .map_err(|_| NodeError::ResidentRuntime)?,
            )
            .map_err(resident_error)?;
        if report.reconcile.is_some() || report.duplicate || report.pending_batch {
            return Err(NodeError::ResidentRuntime);
        }
        for delivery in report.deliveries {
            if self.grid_bridges.contains_key(&delivery.target) {
                let binding = self
                    .grid_bindings
                    .get(&delivery.target)
                    .cloned()
                    .ok_or(NodeError::ResidentRuntime)?;
                if self.manual_owns_fill(&binding, &manual_fill)? {
                    let turn = self
                        .runtime
                        .begin_private_strategy_turn(&binding)
                        .map_err(resident_error)?
                        .ok_or(NodeError::ResidentRuntime)?;
                    if !matches!(turn.input(), StrategyInput::Private(_)) {
                        return Err(NodeError::ResidentRuntime);
                    }
                    let replay = self.manual_checkpoint_bytes(&binding)?;
                    let applied = self
                        .runtime
                        .persist_manual_private_strategy_turn(&binding, replay)
                        .map_err(resident_error)?;
                    persist_anchor(&self.artifacts_root, &binding, &applied)?;
                    continue;
                }
                let turn = self
                    .runtime
                    .begin_private_strategy_turn(&binding)
                    .map_err(resident_error)?
                    .ok_or(NodeError::ResidentRuntime)?;
                let StrategyInput::Private(fact) = turn.input() else {
                    return Err(NodeError::ResidentRuntime);
                };
                let DomainEvent::Fill(fill) = fact.record().event.clone() else {
                    return Err(NodeError::ResidentRuntime);
                };
                let private_generation = fact.record().header.generation;
                let bridge = self
                    .grid_bridges
                    .get_mut(&delivery.target)
                    .ok_or(NodeError::ResidentRuntime)?;
                let decision = bridge
                    .observe_persisted_fill(&fill, private_generation)
                    .map_err(|_| NodeError::ResidentRuntime)?;
                let plans = match &decision {
                    venue_strategies::hedged_grid::GridDecision::Noop => Vec::new(),
                    venue_strategies::hedged_grid::GridDecision::Actions(actions) => actions
                        .iter()
                        .map(|action| bridge.plan_dispatch(action))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|_| NodeError::ResidentRuntime)?,
                    venue_strategies::hedged_grid::GridDecision::Blocked => {
                        return Err(NodeError::ResidentRuntime);
                    }
                };
                let replay = bridge.checkpoint_bytes()?;
                let applied = self
                    .runtime
                    .persist_private_strategy_turn(&binding, replay)
                    .map_err(resident_error)?;
                persist_anchor(&self.artifacts_root, &binding, &applied)?;
                for plan in &plans {
                    for command in &plan.commands {
                        if self
                            .host
                            .prepare_and_admit_operator(
                                &mut self.runtime,
                                &binding,
                                &applied,
                                venue_runtime::account::AccountLanePriority::Normal,
                                command.clone(),
                            )
                            .is_err()
                        {
                            self.host
                                .reject_prepared_batch(
                                    &mut self.runtime,
                                    "grid_rolling_batch_rejected",
                                )
                                .map_err(|_| NodeError::ResidentRuntime)?;
                            self.pause_grid_after_bootstrap_failure(&binding)?;
                            return Err(NodeError::ResidentRuntime);
                        }
                    }
                }
                let command_count = plans.iter().map(|plan| plan.commands.len()).sum::<usize>();
                for _ in 0..command_count {
                    self.runtime
                        .dispatch_next_with_host(&mut self.host)
                        .map_err(|error| NodeError::LiveHost {
                            venue: self.host.binding().venue,
                            message: error.to_string(),
                        })?;
                }
                let mut accepted = Vec::new();
                for plan in &plans {
                    for command in &plan.commands {
                        let status = self
                            .host
                            .command_status(command.command_id())
                            .map_err(|error| NodeError::LiveHost {
                                venue: self.host.binding().venue,
                                message: error.to_string(),
                            })?
                            .ok_or(NodeError::ResidentRuntime)?;
                        if let venue_runtime::CommandState::Accepted { venue_order_id } =
                            status.state()
                        {
                            accepted.push((command.command_id().clone(), venue_order_id.clone()));
                        } else {
                            self.pause_grid_after_bootstrap_failure(&binding)?;
                            return Err(NodeError::ResidentRuntime);
                        }
                    }
                }
                for plan in &plans {
                    bridge
                        .bind_accepted_plan(plan, &accepted)
                        .map_err(|_| NodeError::ResidentRuntime)?;
                }
                if !plans.is_empty() {
                    let replay = bridge.checkpoint_bytes()?;
                    let accepted_turn = self
                        .runtime
                        .persist_resident_semantic_turn(&binding, replay)
                        .map_err(resident_error)?;
                    persist_anchor(&self.artifacts_root, &binding, &accepted_turn)?;
                    let confirmed = self.refresh_signed_snapshot()?;
                    let bridge = self
                        .grid_bridges
                        .get(&delivery.target)
                        .ok_or(NodeError::ResidentRuntime)?;
                    if confirmed.private_generation() <= private_generation
                        || !bridge.signed_desired_matches(confirmed.open_orders())
                    {
                        self.pause_grid_after_bootstrap_failure(&binding)?;
                        return Err(NodeError::ResidentRuntime);
                    }
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use std::{
        io,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use rust_decimal::Decimal;
    use venue_domain::domain::{Asset, PositionSide};
    use venue_gateway_api::{GatewayBinding, VenueId};
    use venue_runtime::{
        AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
        AccountPhysicalGateway, AccountRecoveryOutcome, AccountRecoveryReport,
        AccountRecoveryRequest, AccountRiskEvidence, SignedAccountPositionFact,
        SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
    };
    use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey, StrategyKind};
    use venue_strategies::hedged_grid::{HedgedGridBinding, HedgedGridParams, HedgedGridState};

    use super::*;
    use crate::NodeGridRecoveryPolicy;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    struct State {
        generation: u64,
        dispatches: usize,
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
                    .map(|command| {
                        AccountRecoveryOutcome::still_unknown(command.command_id().clone())
                    })
                    .collect(),
            )
            .map_err(io::Error::other)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            let generation = self
                .state
                .lock()
                .map_err(|_| AccountHostValidationError::RiskEvidence)?
                .generation
                .max(1);
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
            SignedAccountSnapshot::complete(
                self.binding.clone(),
                now,
                1,
                generation,
                1,
                SignedAccountPositionMode::Hedge,
                Vec::new(),
                vec![
                    SignedAccountPositionFact {
                        symbol: self.binding.symbol.clone(),
                        position_side: PositionSide::Long,
                        quantity: Decimal::ZERO,
                        entry_price: None,
                        mark_price: Some(Decimal::new(100, 0)),
                    },
                    SignedAccountPositionFact {
                        symbol: self.binding.symbol.clone(),
                        position_side: PositionSide::Short,
                        quantity: Decimal::ZERO,
                        entry_price: None,
                        mark_price: Some(Decimal::new(100, 0)),
                    },
                ],
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

        fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
            let Ok(mut state) = self.state.lock() else {
                return AccountGatewayResult::Unknown;
            };
            state.dispatches = state.dispatches.saturating_add(1);
            AccountGatewayResult::Unknown
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
}
