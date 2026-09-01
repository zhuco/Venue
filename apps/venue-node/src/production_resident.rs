use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_control_protocol::{ControlAction, ControlCommandRequest};
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
mod grid_recovery;
use grid_recovery::SignedGridRecoveryOutcome;
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
    manual_bindings: BTreeMap<venue_runtime::StrategyInstanceKey, StrategyBinding>,
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
#[cfg_attr(
    not(any(feature = "binance", feature = "bitget", feature = "gate")),
    allow(dead_code)
)]
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

/// Private stream adapters expose only the normalized fact needed by the shared Grid path. The
/// exchange frame and credentials stay inside its gateway, while the account Runtime remains the
/// sole durable generation and delivery authority.
#[cfg_attr(
    not(any(feature = "binance", feature = "bitget", feature = "gate")),
    allow(dead_code)
)]
pub(crate) struct PrivateFillFact {
    pub(crate) source_private_generation: u64,
    pub(crate) received_at_ms: u64,
    pub(crate) fill: venue_domain::Fill,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingGridWalDisposition {
    AllAbsent,
    TerminalRejectedOrAbsent,
    RequiresSignedReconciliation,
}

fn classify_pending_grid_wal<E>(
    pending_transactions: &[(String, Vec<venue_domain::CommandId>)],
    mut command_state: impl FnMut(
        &venue_domain::CommandId,
    ) -> Result<Option<venue_runtime::CommandState>, E>,
) -> Result<PendingGridWalDisposition, E> {
    let mut rejected = false;
    for command_id in pending_transactions
        .iter()
        .flat_map(|(_, command_ids)| command_ids)
    {
        match command_state(command_id)? {
            None => {}
            Some(venue_runtime::CommandState::Rejected { .. }) => rejected = true,
            Some(
                venue_runtime::CommandState::Prepared
                | venue_runtime::CommandState::Submitted
                | venue_runtime::CommandState::Accepted { .. }
                | venue_runtime::CommandState::Unknown { .. },
            ) => return Ok(PendingGridWalDisposition::RequiresSignedReconciliation),
        }
    }
    Ok(if rejected {
        PendingGridWalDisposition::TerminalRejectedOrAbsent
    } else {
        PendingGridWalDisposition::AllAbsent
    })
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
            manual_bindings: BTreeMap::new(),
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

    /// Periodic private supervision compares semantics, not a target count. A missing child may
    /// be retired only by a complete signed snapshot; surviving children still drain through the
    /// same WAL lane before the venue-specific caller installs one replacement epoch.
    #[cfg_attr(not(any(feature = "binance", test)), allow(dead_code))]
    pub(crate) fn supervise_grid_signed_surface_once(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<bool, NodeError> {
        let snapshot = self.refresh_signed_snapshot()?;
        let exact = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .signed_desired_matches(snapshot.open_orders());
        if exact {
            return match self.recover_grid_from_signed_fills(binding, snapshot)? {
                SignedGridRecoveryOutcome::Converged => Ok(false),
                SignedGridRecoveryOutcome::UnexplainedSurface => Err(self.grid_recovery_error(
                    "signed Grid surface was exact before recovery but did not converge",
                )),
            };
        }
        // A complete stream outage can leave a valid signed fill waiting in the next snapshot.
        // Preserve the ordinary two-place/one-cancel reducer path before declaring unexplained
        // drift. If that convergence cannot prove the whole surface, startup reset re-reads the
        // latest signed facts and drains only its exact owned subset.
        match self.recover_grid_from_signed_fills(binding, snapshot)? {
            SignedGridRecoveryOutcome::Converged => return Ok(false),
            SignedGridRecoveryOutcome::UnexplainedSurface => {}
        }
        self.require_grid_reset_wal_safe(binding)?;
        self.reset_grid_on_startup(binding)?;
        Ok(true)
    }

    fn require_grid_reset_wal_safe(&self, binding: &StrategyBinding) -> Result<(), NodeError> {
        let bridge = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let pending_transactions = bridge
            .pending_transaction_command_ids()
            .map_err(|_| NodeError::ResidentRuntime)?;
        let expected = bridge
            .pending_dispatch_plans()
            .map_err(|_| NodeError::ResidentRuntime)?
            .into_iter()
            .flat_map(|plan| plan.commands)
            .collect::<Vec<_>>();
        if expected.iter().any(|command| {
            self.host
                .command_snapshot(command.command_id())
                .is_some_and(|actual| actual != *command)
        }) {
            return Err(self.grid_recovery_error(
                "periodic Grid reset found conflicting pending WAL command bytes",
            ));
        }
        let disposition = classify_pending_grid_wal(&pending_transactions, |command_id| {
            self.host
                .command_status(command_id)
                .map(|status| status.map(|status| status.state().clone()))
        })
        .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
        if disposition == PendingGridWalDisposition::RequiresSignedReconciliation {
            return Err(self.grid_recovery_error(
                "periodic Grid reset is fenced by Prepared, Submitted, Accepted, or Unknown WAL",
            ));
        }
        Ok(())
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
        let manual = binding.key.strategy_kind == venue_runtime::StrategyKind::Manual;
        self.register_actor_with_anchor(binding.clone(), None)?;
        if manual {
            self.manual_bindings.insert(binding.key.clone(), binding);
        }
        Ok(())
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
        self.register_grid_actor_with_reset_rebuild_confirmation(
            binding,
            initial,
            recovery,
            skip_inventory_replenishment_until_recovered,
            false,
        )
    }

    pub fn register_grid_actor_with_reset_rebuild_confirmation(
        &mut self,
        binding: StrategyBinding,
        initial: venue_strategies::hedged_grid::HedgedGridState,
        recovery: crate::NodeGridRecoveryPolicy,
        skip_inventory_replenishment_until_recovered: bool,
        confirm_reset_rebuild: bool,
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
        let mut bridge =
            grid::GridBridgeState::restore_or_bootstrap(checkpoint, initial, recovery)?;
        let pending_transactions = bridge
            .pending_transaction_command_ids()
            .map_err(|_| NodeError::ResidentRuntime)?;
        let pending_expected_commands = bridge
            .pending_dispatch_plans()
            .map_err(|_| NodeError::ResidentRuntime)?
            .into_iter()
            .flat_map(|plan| plan.commands)
            .collect::<Vec<_>>();
        for expected in &pending_expected_commands {
            let status = self
                .host
                .command_status(expected.command_id())
                .map_err(|error| NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: format!("pending Grid command WAL lookup failed: {error}"),
                })?;
            if status.is_some()
                && self.host.command_snapshot(expected.command_id()).as_ref() != Some(expected)
            {
                return Err(NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: "pending Grid command WAL bytes differ from checkpoint".to_owned(),
                });
            }
        }
        // Host open has already fenced crash-window Prepared records to terminal Rejected and
        // Submitted records to Unknown. Exact Absent/Rejected families may therefore abandon the
        // old projection into signed-surface reset; Accepted/Unknown stay paused and no old id is
        // ever dispatched here.
        let mut pending_wal = if pending_transactions.is_empty() {
            PendingGridWalDisposition::AllAbsent
        } else {
            classify_pending_grid_wal(&pending_transactions, |id| {
                self.host
                    .command_status(id)
                    .map(|status| status.map(|status| status.state().clone()))
                    .map_err(|error| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: format!("pending Grid command WAL classification failed: {error}"),
                    })
            })?
        };
        if pending_expected_commands.iter().any(|expected| {
            self.host
                .command_snapshot(expected.command_id())
                .is_some_and(|actual| actual != *expected)
        }) {
            pending_wal = PendingGridWalDisposition::RequiresSignedReconciliation;
        }
        let restart_from_signed_surface = matches!(
            pending_wal,
            PendingGridWalDisposition::AllAbsent
                | PendingGridWalDisposition::TerminalRejectedOrAbsent
        );
        let first_bootstrap = bridge.needs_initial_bootstrap();
        let reset_rebuild = confirm_reset_rebuild && bridge.needs_reset_rebuild();
        let reconciliation_rebuild = bridge.needs_reconciliation_rebuild();
        let unconfirmed_install = bridge.has_unconfirmed_install_surface();
        let has_installed_epoch = bridge.grid.epoch.is_some();
        let bootstrap_requires_reconciliation = bridge.bootstrap_requires_reconciliation();
        if first_bootstrap && recovery == crate::NodeGridRecoveryPolicy::RequireExisting {
            return Err(NodeError::ResidentArtifacts);
        }
        apply_grid_restart_replenishment_policy(
            &mut bridge,
            skip_inventory_replenishment_until_recovered,
            bootstrap_requires_reconciliation,
        )?;
        let key = binding.key.clone();
        self.grid_bridges.insert(key.clone(), bridge);
        self.grid_bindings.insert(key.clone(), binding);
        if unconfirmed_install {
            let binding = self
                .grid_bindings
                .get(&key)
                .cloned()
                .ok_or(NodeError::ResidentRuntime)?;
            self.recover_unconfirmed_grid_install_on_startup(&binding)?;
        } else if !first_bootstrap
            && !reset_rebuild
            && !reconciliation_rebuild
            && has_installed_epoch
            && restart_from_signed_surface
        {
            let binding = self
                .grid_bindings
                .get(&key)
                .cloned()
                .ok_or(NodeError::ResidentRuntime)?;
            self.reset_grid_on_startup(&binding)?;
        } else if first_bootstrap || reset_rebuild || reconciliation_rebuild {
            self.grid_bootstrap_pending.insert(key.clone());
        } else if bootstrap_requires_reconciliation {
            self.runtime.request_pause(&key).map_err(resident_error)?;
        } else {
            let binding = self
                .grid_bindings
                .get(&key)
                .cloned()
                .ok_or(NodeError::ResidentRuntime)?;
            self.recover_grid_from_latest_signed_fills(&binding)?;
        }
        Ok(())
    }

    /// Consumes the one permitted first-install attempt. A checkpoint containing an epoch never
    /// sets this flag, so a restart cannot manufacture another epoch after an earlier accepted,
    /// rejected, or Unknown installation attempt. An exact uninstalled checkpoint produced only
    /// by predecessor cancellation remains eligible for the successor's first epoch.
    #[cfg_attr(
        not(any(feature = "binance", feature = "bitget", feature = "gate")),
        allow(dead_code)
    )]
    pub(crate) fn take_grid_bootstrap_request(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<bool, NodeError> {
        if self.grid_bindings.get(&binding.key) != Some(binding)
            || !self.grid_bootstrap_pending.contains(&binding.key)
        {
            return Ok(false);
        }
        let replay = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            if bridge.needs_initial_bootstrap() {
                bridge
                    .mark_bootstrap_attempted()
                    .map_err(|_| NodeError::ResidentRuntime)?;
            } else if bridge.needs_reset_rebuild() {
                bridge
                    .mark_reset_rebuild_attempted()
                    .map_err(|_| NodeError::ResidentRuntime)?;
            } else if !bridge.needs_reconciliation_rebuild() {
                return Err(NodeError::ResidentRuntime);
            }
            bridge.checkpoint_bytes()?
        };
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        if !self.grid_bootstrap_pending.remove(&binding.key) {
            return Err(NodeError::ResidentRuntime);
        }
        Ok(true)
    }

    pub fn register_actor_with_anchor(
        &mut self,
        binding: StrategyBinding,
        _expected_anchor: Option<ActorAppliedAnchor>,
    ) -> Result<(), NodeError> {
        let venue = self.host.binding().venue;
        let actor_root = self
            .artifacts_root
            .join("strategies")
            .join(&binding.key.instance_id);
        fs::create_dir_all(&actor_root).map_err(|_| NodeError::ResidentArtifacts)?;
        self.runtime
            .register_strategy(binding.clone())
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: format!("resident strategy registration failed: {error}"),
            })?;
        let artifacts = actor_artifacts(&actor_root)?;
        self.host
            .install_resident_actor_applied_artifacts(&mut self.runtime, &binding, artifacts)
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: format!("resident Actor-applied recovery failed: {error}"),
            })?;
        self.runtime
            .activate_resident_strategy(&binding)
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: format!("resident strategy activation failed: {error}"),
            })
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
        let replay = serde_json::to_vec(&ResidentControlReplay::legacy(action))
            .map_err(|_| NodeError::ResidentRuntime)?;
        self.apply_persisted_control_action(binding, action, replay)
    }

    /// Persists the exact Control delivery identity with the semantic lifecycle turn. A later
    /// ReconcileOnly lease may use this checkpoint as evidence, but an older action-only
    /// checkpoint can never be mistaken for the current request.
    pub fn apply_control_delivery(
        &mut self,
        binding: &StrategyBinding,
        delivery_id: &str,
        command: &ControlCommandRequest,
    ) -> Result<AppliedStrategyTurnReceipt, NodeError> {
        if command.action == ControlAction::Trade || delivery_id.trim().is_empty() {
            return Err(NodeError::ResidentRuntime);
        }
        let replay = ResidentControlReplay::for_delivery(delivery_id, command)?;
        let encoded = serde_json::to_vec(&replay).map_err(|_| NodeError::ResidentRuntime)?;
        self.apply_persisted_control_action(binding, command.action, encoded)
    }

    /// Returns a read-only commitment only when the latest Runtime-verified Actor checkpoint is
    /// bound to this exact request and its in-memory lifecycle already reflects the requested
    /// Pause/Resume transition. It never replays an action or grants mutation authority.
    pub fn reconcile_control_delivery(
        &self,
        binding: &StrategyBinding,
        delivery_id: &str,
        command: &ControlCommandRequest,
    ) -> Result<Option<[u8; 32]>, NodeError> {
        if command.action == ControlAction::Trade || delivery_id.trim().is_empty() {
            return Ok(None);
        }
        let Some(encoded) = self
            .runtime
            .resident_actor_checkpoint(binding)
            .map_err(resident_error)?
        else {
            return Ok(None);
        };
        let recovered = serde_json::from_slice::<ResidentControlReplay>(&encoded)
            .map_err(|_| NodeError::ResidentRuntime)?;
        let expected = ResidentControlReplay::for_delivery(delivery_id, command)?;
        if recovered != expected {
            return Ok(None);
        }
        let lifecycle = self.strategy_lifecycle(binding);
        let converged = match command.action {
            ControlAction::Pause => {
                lifecycle == Some(venue_runtime::account::InstanceLifecycle::Paused)
            }
            ControlAction::Resume => matches!(
                lifecycle,
                Some(
                    venue_runtime::account::InstanceLifecycle::Recovering
                        | venue_runtime::account::InstanceLifecycle::Running
                )
            ),
            ControlAction::Stop | ControlAction::Flatten => {
                lifecycle == Some(venue_runtime::account::InstanceLifecycle::Stopping)
            }
            ControlAction::Trade => false,
        };
        if !converged {
            return Ok(None);
        }
        let mut digest = Sha256::new();
        digest.update(b"venue.node.control-reconciliation.v1");
        digest.update(binding.key.account.exchange.as_str().as_bytes());
        digest.update(binding.key.account.account.as_bytes());
        digest.update(binding.key.instance_id.as_bytes());
        digest.update(binding.key.symbol.to_string().as_bytes());
        digest.update(binding.config_digest.as_bytes());
        digest.update(encoded);
        Ok(Some(digest.finalize().into()))
    }

    fn apply_persisted_control_action(
        &mut self,
        binding: &StrategyBinding,
        action: ControlAction,
        replay: Vec<u8>,
    ) -> Result<AppliedStrategyTurnReceipt, NodeError> {
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        if matches!(
            action,
            ControlAction::Pause
                | ControlAction::Resume
                | ControlAction::Stop
                | ControlAction::Flatten
        ) {
            self.host
                .reject_prepared_managed_grid_batch(
                    &mut self.runtime,
                    &binding.key,
                    "grid_control_transition",
                )
                .map_err(|_| NodeError::ResidentRuntime)?;
        }
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

    /// A consumed initial-install request may not be retried, but it must leave the registered
    /// actor unable to accept risk when its required signed/public evidence was unavailable.
    #[cfg_attr(not(any(feature = "bitget", feature = "gate")), allow(dead_code))]
    pub(crate) fn fail_grid_bootstrap(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        if self.grid_bindings.get(&binding.key) != Some(binding) {
            return Err(NodeError::ResidentRuntime);
        }
        if self.strategy_lifecycle(binding)
            == Some(venue_runtime::account::InstanceLifecycle::Paused)
        {
            return Ok(());
        }
        self.runtime
            .request_pause(&binding.key)
            .map_err(resident_error)
    }

    /// Cancels at most one currently-open Stage-7 order through the same Host/WAL/lane before a
    /// replacement Grid may bootstrap.  The old Owner is never registered as a strategy: the
    /// Host re-derives this route from the exact Runtime generation and rejects every other kind
    /// of mutation.  Call again only after this method has signedly converged the first route.
    pub fn cancel_legacy_v1_grid_custody_once(&mut self) -> Result<bool, NodeError> {
        // Startup bootstrap and the prior successful cancellation both leave a fresh, persisted
        // Host snapshot. Reuse that exact generation for the next route instead of doubling the
        // exchange-wide signed reads in the finite predecessor-drain loop.
        let routes = self
            .host
            .legacy_v1_custody_routes_from_current_snapshot()
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let Some(route) = routes.first().cloned() else {
            return Ok(false);
        };
        let matching = self
            .grid_bindings
            .values()
            .filter(|binding| binding.key.symbol == route.owner.symbol)
            .cloned()
            .collect::<Vec<_>>();
        let [binding] = matching.as_slice() else {
            return Err(NodeError::ResidentRuntime);
        };
        let replay = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .checkpoint_bytes()?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        let command_id = self
            .host
            .prepare_and_admit_legacy_v1_custody_cancel(
                &mut self.runtime,
                binding,
                &applied,
                &route,
            )
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        self.runtime
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
            Some(venue_runtime::CommandState::Accepted { .. }) => {}
            Some(venue_runtime::CommandState::Rejected { reason }) => {
                return Err(NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: format!("legacy custody cancel rejected: {reason}"),
                });
            }
            Some(venue_runtime::CommandState::Prepared)
            | Some(venue_runtime::CommandState::Submitted)
            | Some(venue_runtime::CommandState::Unknown { .. })
            | None => {
                return Err(NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: "legacy custody cancel unresolved; signed reconciliation is required"
                        .to_owned(),
                });
            }
        }
        self.refresh_signed_snapshot()?;
        if self
            .host
            .legacy_v1_custody_routes_from_current_snapshot()
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?
            .iter()
            .any(|candidate| candidate == &route)
        {
            return Err(NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: "legacy custody cancel is Accepted but the order remains open in fresh signed facts"
                    .to_owned(),
            });
        }
        Ok(true)
    }

    /// The shared part of the Binance initial-install path. The concrete adapter owns the only
    /// production market read; tests inject the already-normalized bounded facts and still pass
    /// through the account Host, WAL and execution lane below.
    #[cfg_attr(not(any(feature = "bitget", feature = "gate", test)), allow(dead_code))]
    pub(crate) fn bootstrap_grid_from_signed_market(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        market: GridBootstrapMarket,
    ) -> Result<(), NodeError> {
        self.bootstrap_grid_from_signed_market_with_refresh(binding, snapshot, market, |_| Ok(None))
    }

    fn bootstrap_grid_from_signed_market_with_refresh(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        market: GridBootstrapMarket,
        refresh_opening_market: impl FnMut(
            &mut AccountRuntimeHost<G>,
        ) -> Result<Option<GridBootstrapMarket>, NodeError>,
    ) -> Result<(), NodeError> {
        let result = self.bootstrap_grid_from_signed_market_inner(
            binding,
            snapshot,
            market,
            refresh_opening_market,
        );
        if result.is_err() {
            // Registration starts the generic actor so it can own the sole account route, but a
            // Grid without an exactly signed desired surface is never allowed to keep accepting
            // risk after a failed first install.
            let _paused = self.runtime.request_pause(&binding.key);
        }
        result
    }

    #[cfg_attr(
        not(any(feature = "binance", feature = "bitget", feature = "gate")),
        allow(dead_code)
    )]
    fn bootstrap_grid_from_signed_market_inner(
        &mut self,
        binding: &StrategyBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        market: GridBootstrapMarket,
        mut refresh_opening_market: impl FnMut(
            &mut AccountRuntimeHost<G>,
        )
            -> Result<Option<GridBootstrapMarket>, NodeError>,
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
        let now_ms = unix_now_ms()?;
        if market.observed_at_ms < snapshot.observed_at_ms()
            || market
                .observed_at_ms
                .saturating_sub(snapshot.observed_at_ms())
                > 3_000
            || market.observed_at_ms > now_ms
            || now_ms.saturating_sub(market.observed_at_ms) > 3_000
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
            // Binance reports the Hedge short leg as a signed negative quantity; Grid inventory
            // is per-leg capacity and is therefore always stored as an absolute quantity.
            short_quantity: short.quantity.abs(),
        };
        let signed_surface = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .expected_signed_surface()?;
        self.host
            .confirm_managed_grid_surface(&mut self.runtime, binding, signed_surface.clone())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: format!("Grid signed-empty-surface admission rejected: {error}"),
            })?;
        let venue = self.host.binding().venue;
        let (plan, replay) = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            let epoch_number =
                bridge
                    .next_install_epoch()
                    .map_err(|error| NodeError::LiveHost {
                        venue,
                        message: format!("Grid next install epoch is invalid: {error}"),
                    })?;
            let anchor = (market.bid.value() + market.ask.value()) / Decimal::from(2_u8);
            let tick = market.price_tick.value();
            let anchor = anchor - anchor % tick;
            let step_raw = anchor * bridge.grid.params.spacing_rate;
            let step = step_raw - step_raw % tick;
            let quantity = grid::minimum_grid_quantity(
                bridge.grid.params.order_notional.value,
                anchor,
                step,
                bridge.grid.params.grid_count,
                market.quantity_step,
            )?;
            if quantity < market.minimum_quantity || quantity > market.maximum_quantity {
                return Err(NodeError::ResidentRuntime);
            }
            let epoch = GridEpoch {
                epoch: epoch_number,
                anchor_price: Price::new(anchor).map_err(|_| NodeError::ResidentRuntime)?,
                step: Price::new(step).map_err(|_| NodeError::ResidentRuntime)?,
                grid_quantity: quantity,
                passive_book_fallback: None,
            };
            let plan = if epoch_number == 1 {
                bridge
                    .install_initial_epoch(inventory, epoch)
                    .map_err(|error| NodeError::LiveHost {
                        venue,
                        message: format!("initial Grid epoch planning failed: {error}"),
                    })?
            } else {
                bridge
                    .install_rebuilt_epoch(inventory, epoch)
                    .map_err(|error| NodeError::LiveHost {
                        venue,
                        message: format!("reconciled Grid epoch planning failed: {error}"),
                    })?
            };
            if plan.commands.iter().any(|command| {
                matches!(command, ExecutionCommand::PlaceLimit(order)
                    if !order.reduce_only
                        && order.quantity * order.limit_price.value() < market.minimum_notional)
            }) {
                return Err(NodeError::ResidentRuntime);
            }
            if !grid_install_commands_are_ordered_and_passive(&plan.commands, &market) {
                return Err(NodeError::LiveHost {
                    venue,
                    message: "Grid bootstrap contains a crossing post-only order or an invalid wave order"
                        .to_owned(),
                });
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
        if let Err(error) = self.host.prepare_and_admit_managed_grid_batch(
            &mut self.runtime,
            binding,
            &applied,
            venue_runtime::account::AccountLanePriority::Normal,
            signed_surface,
            &plan.commands,
        ) {
            // Admission can reject before it creates any Prepared record. Cleanup is still
            // attempted, but an empty-batch cleanup result must never erase the actionable
            // signed-risk or surface error that stopped dispatch.
            let _cleanup = self
                .host
                .reject_prepared_batch(&mut self.runtime, "grid_bootstrap_batch_rejected");
            let _pause = self.pause_grid_after_bootstrap_failure(binding);
            return Err(grid_bootstrap_admission_error(
                self.host.binding().venue,
                &error,
            ));
        }
        for command in &plan.commands {
            if matches!(command, ExecutionCommand::PlaceLimit(order) if !order.reduce_only) {
                match refresh_opening_market(&mut self.host) {
                    Ok(Some(opening_market))
                        if grid_opening_market_matches(
                            &market,
                            &opening_market,
                            unix_now_ms()?,
                        ) && grid_command_is_passive(command, &opening_market) => {}
                    Ok(Some(_)) => {
                        self.host
                            .reject_prepared_batch(
                                &mut self.runtime,
                                "grid_bootstrap_opening_book_crossed",
                            )
                            .map_err(|_| NodeError::ResidentRuntime)?;
                        self.pause_grid_after_bootstrap_failure(binding)?;
                        return Err(NodeError::LiveHost {
                            venue,
                            message: "Grid opening wave became crossing or stale after closing-wave dispatch"
                                .to_owned(),
                        });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        self.host
                            .reject_prepared_batch(
                                &mut self.runtime,
                                "grid_bootstrap_opening_book_unavailable",
                            )
                            .map_err(|_| NodeError::ResidentRuntime)?;
                        self.pause_grid_after_bootstrap_failure(binding)?;
                        return Err(error);
                    }
                }
            }
            if self
                .runtime
                .dispatch_next_with_host(&mut self.host)
                .is_err()
            {
                self.host
                    .reject_prepared_batch(&mut self.runtime, "grid_bootstrap_dispatch_stopped")
                    .map_err(|_| NodeError::ResidentRuntime)?;
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
                self.host
                    .reject_prepared_batch(&mut self.runtime, "grid_bootstrap_nonaccepted")
                    .map_err(|_| NodeError::ResidentRuntime)?;
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
                .map_err(|error| NodeError::LiveHost {
                    venue,
                    message: format!("Grid Accepted routes could not bind: {error}"),
                })?;
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
            return Err(NodeError::LiveHost {
                venue,
                message: "Grid install did not reach an exact newer signed surface".to_owned(),
            });
        }
        let replay = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            bridge
                .confirm_installed_surface()
                .map_err(|error| NodeError::LiveHost {
                    venue,
                    message: format!("Grid installed surface state is invalid: {error}"),
                })?;
            bridge.checkpoint_bytes()?
        };
        let confirmed_turn = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &confirmed_turn)?;
        let expected = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .expected_signed_surface()?;
        self.host
            .confirm_managed_grid_surface(&mut self.runtime, binding, expected)
            .map_err(|error| NodeError::LiveHost {
                venue,
                message: format!("Grid final signed surface confirmation failed: {error}"),
            })?;
        Ok(())
    }

    /// Applies one adapter-normalized fill through the sole Runtime/Host composition. This shared
    /// path deliberately knows no native private protocol: adapters must establish the stream,
    /// generation, symbol and client identity before this bounded fact can enter the journal.
    #[cfg_attr(
        not(any(feature = "binance", feature = "bitget", feature = "gate")),
        allow(dead_code)
    )]
    pub(crate) fn consume_private_fill(
        &mut self,
        venue: &str,
        event: PrivateFillFact,
    ) -> Result<bool, NodeError> {
        use venue_domain::domain::{DomainEvent, EventId, NativeOrderFamily};
        use venue_runtime::{account::AccountPrivateFactInput, strategy::StrategyInput};

        // The adapter separately proves the immutable socket generation and binds the dequeued
        // frame to its latest complete signed snapshot generation. Host alone maps that snapshot
        // generation into its durable restart-ratcheted domain. Runtime's facts journal remains
        // separately keyed by the connection generation used by its private router.
        let active_private_generation = self.runtime.active_private_generation();
        let connection_generation = self.runtime.connection_generation();
        let normalized_private_generation = self
            .host
            .normalize_current_gateway_private_generation(event.source_private_generation)
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: format!(
                    "private stream generation is outside the current signed snapshot: {error}"
                ),
            })?;
        if normalized_private_generation != active_private_generation || connection_generation == 0
        {
            return Err(NodeError::ResidentRuntime);
        }
        if let Some(owner) = self.owner_for_signed_fill(&event.fill) {
            if let Some((key, _)) = self
                .grid_bindings
                .iter()
                .find(|(_, binding)| binding.matches_owner(&owner))
            {
                let application = self
                    .grid_bridges
                    .get(key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .signed_fill_application(&event.fill)
                    .map_err(|error| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: format!(
                            "private Grid fill conflicts with its durable record: {error}"
                        ),
                    })?;
                if application == grid::SignedGridFillApplication::ExactDuplicate {
                    return Ok(true);
                }
            }
        }
        let event_id = EventId::new(format!("{venue}-fill-{}", event.fill.fill_id))
            .map_err(|_| NodeError::ResidentRuntime)?;
        let routed_fill = event.fill.clone();
        let report = self
            .runtime
            .ingest_private(
                AccountPrivateFactInput::new(
                    event_id,
                    connection_generation,
                    event.received_at_ms,
                    Some(NativeOrderFamily::UmOrder),
                    DomainEvent::Fill(event.fill),
                )
                .map_err(|_| NodeError::ResidentRuntime)?,
            )
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: format!("private fact journal or route commit failed: {error}"),
            })?;
        if report.duplicate
            && report.reconcile.is_none()
            && !report.pending_batch
            && report.deliveries.is_empty()
        {
            return Ok(true);
        }
        if report.reconcile.is_some() || report.duplicate || report.pending_batch {
            return Err(NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: format!(
                    "private fact did not produce one complete new route: reconcile={:?}, duplicate={}, pending_batch={}",
                    report.reconcile, report.duplicate, report.pending_batch
                ),
            });
        }
        for delivery in report.deliveries {
            if let Some(binding) = self.manual_bindings.get(&delivery.target).cloned() {
                if !self.manual_owns_fill(&binding, &routed_fill)? {
                    return Err(NodeError::ResidentRuntime);
                }
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
            if self.grid_bridges.contains_key(&delivery.target) {
                let binding = self
                    .grid_bindings
                    .get(&delivery.target)
                    .cloned()
                    .ok_or(NodeError::ResidentRuntime)?;
                let turn = self
                    .runtime
                    .begin_private_strategy_turn(&binding)
                    .map_err(|error| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: format!("Grid private turn could not begin: {error}"),
                    })?
                    .ok_or_else(|| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: "Grid private route produced no actor turn".to_owned(),
                    })?;
                let StrategyInput::Private(fact) = turn.input() else {
                    return Err(NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: "Grid private route was preceded by a non-private actor turn"
                            .to_owned(),
                    });
                };
                let DomainEvent::Fill(fill) = fact.record().event.clone() else {
                    return Err(NodeError::ResidentRuntime);
                };
                // The persisted fact header carries the router connection generation. Grid's
                // reducer cursor is the separately signed private generation validated above.
                let private_generation = normalized_private_generation;
                let bridge = self
                    .grid_bridges
                    .get_mut(&delivery.target)
                    .ok_or(NodeError::ResidentRuntime)?;
                let decision = bridge
                    .observe_persisted_fill(&fill, private_generation)
                    .map_err(|error| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: format!(
                            "Grid reducer rejected private fill {}: {error}",
                            fill.fill_id
                        ),
                    })?;
                let _plans = match &decision {
                    venue_strategies::hedged_grid::GridDecision::Noop => Vec::new(),
                    venue_strategies::hedged_grid::GridDecision::Actions(actions) => actions
                        .iter()
                        .map(|action| bridge.plan_dispatch(action))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| NodeError::LiveHost {
                            venue: self.host.binding().venue,
                            message: format!("Grid private fill dispatch plan is invalid: {error}"),
                        })?,
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
                // A private frame proves the fill, but it does not prove the complete account
                // surface at the dispatch boundary. Refresh first, fold every concurrently signed
                // fill into the durable Actor, then drain its pending batches one at a time from
                // the exact pre-dispatch surface. This keeps a burst of fills from authorizing a
                // rolling batch against the stale pre-fill order set.
                let confirmed = self.refresh_signed_snapshot()?;
                if confirmed.private_generation() <= private_generation {
                    self.pause_grid_after_bootstrap_failure(&binding)?;
                    return Err(NodeError::ResidentRuntime);
                }
                if self.recover_grid_from_signed_fills(&binding, confirmed)?
                    != SignedGridRecoveryOutcome::Converged
                {
                    self.pause_grid_after_bootstrap_failure(&binding)?;
                    return Err(self.grid_recovery_error(
                        "private Grid fill recovery did not converge from signed facts",
                    ));
                }
            }
        }
        Ok(true)
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

fn apply_grid_restart_replenishment_policy(
    bridge: &mut grid::GridBridgeState,
    skip_until_recovered: bool,
    bootstrap_requires_reconciliation: bool,
) -> Result<(), NodeError> {
    match (
        bridge.grid.suppress_replenishment_until_inventory_recovers,
        skip_until_recovered,
    ) {
        (false, true)
            if bridge.grid.phase == venue_strategies::hedged_grid::GridPhase::Recovering
                && !bootstrap_requires_reconciliation =>
        {
            bridge
                .grid
                .request_restart_without_replenishment()
                .map_err(|_| NodeError::ResidentRuntime)
        }
        // The configured option is an initial-recovery latch, not a permanent mode. Once a
        // signed inventory has cleared it, a normal Running restart must not re-arm it or reject
        // the durable checkpoint.
        (false, true) | (false, false) | (true, true) => Ok(()),
        // Turning an already-durable suppression latch off still requires a new config identity;
        // otherwise a restart could silently enable market replenishment.
        (true, false) => Err(NodeError::ResidentRuntime),
    }
}

#[derive(Serialize)]
struct ResidentReplay<'a> {
    command: &'a ExecutionCommand,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResidentControlReplay {
    action: ControlAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_sha256: Option<[u8; 32]>,
}

impl ResidentControlReplay {
    fn legacy(action: ControlAction) -> Self {
        Self {
            action,
            request_id: None,
            delivery_id: None,
            request_sha256: None,
        }
    }

    fn for_delivery(delivery_id: &str, command: &ControlCommandRequest) -> Result<Self, NodeError> {
        let encoded = serde_json::to_vec(command).map_err(|_| NodeError::ResidentRuntime)?;
        Ok(Self {
            action: command.action,
            request_id: Some(command.request_id.clone()),
            delivery_id: Some(delivery_id.to_owned()),
            request_sha256: Some(Sha256::digest(encoded).into()),
        })
    }
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

fn grid_bootstrap_admission_error(
    venue: venue_gateway_api::VenueId,
    error: &impl std::fmt::Display,
) -> NodeError {
    NodeError::LiveHost {
        venue,
        message: format!("Grid bootstrap batch admission rejected before dispatch: {error}"),
    }
}

fn grid_install_commands_are_ordered_and_passive(
    commands: &[ExecutionCommand],
    market: &GridBootstrapMarket,
) -> bool {
    let mut opening_started = false;
    commands.iter().all(|command| {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return false;
        };
        if !order.reduce_only {
            opening_started = true;
        } else if opening_started {
            return false;
        }
        grid_command_is_passive(command, market)
    })
}

fn grid_command_is_passive(command: &ExecutionCommand, market: &GridBootstrapMarket) -> bool {
    let ExecutionCommand::PlaceLimit(order) = command else {
        return false;
    };
    match order.side {
        venue_domain::domain::OrderSide::Buy => order.limit_price.value() < market.ask.value(),
        venue_domain::domain::OrderSide::Sell => order.limit_price.value() > market.bid.value(),
    }
}

fn grid_opening_market_matches(
    planned: &GridBootstrapMarket,
    refreshed: &GridBootstrapMarket,
    now_ms: u64,
) -> bool {
    refreshed.bid < refreshed.ask
        && refreshed.price_tick == planned.price_tick
        && refreshed.quantity_step == planned.quantity_step
        && refreshed.minimum_quantity == planned.minimum_quantity
        && refreshed.maximum_quantity == planned.maximum_quantity
        && refreshed.minimum_notional == planned.minimum_notional
        && refreshed.observed_at_ms >= planned.observed_at_ms
        && refreshed.observed_at_ms <= now_ms
        && now_ms.saturating_sub(refreshed.observed_at_ms) <= 3_000
}

fn unix_now_ms() -> Result<u64, NodeError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| NodeError::ResidentRuntime)?
            .as_millis(),
    )
    .map_err(|_| NodeError::ResidentRuntime)
}

#[cfg(feature = "binance")]
impl ProductionResident<venue_gateway_binance::BinanceAccountGateway> {
    /// Opens the authenticated account stream before any startup Grid mutation. Signed REST
    /// refreshes keep this socket alive, so fills during drain/rebuild remain observable.
    pub fn prime_binance_private_stream_once(&mut self) -> Result<(), NodeError> {
        self.host
            .with_gateway_read(|gateway| gateway.prime_private_stream())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })
    }

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
        let venue = self.host.binding().venue;
        self.bootstrap_grid_from_signed_market_with_refresh(
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
            move |host| {
                let market = host
                    .with_gateway_read(|gateway| gateway.fresh_grid_bootstrap_market())
                    .map_err(|error| NodeError::LiveHost {
                        venue,
                        message: error.to_string(),
                    })?;
                Ok(Some(GridBootstrapMarket {
                    bid: market.bid,
                    ask: market.ask,
                    price_tick: market.rules.instrument.price_tick,
                    quantity_step: market.rules.instrument.quantity_step,
                    minimum_quantity: market.rules.minimum_quantity,
                    maximum_quantity: market.rules.maximum_quantity,
                    minimum_notional: market.rules.instrument.minimum_notional.value,
                    observed_at_ms: market.observed_at_ms,
                }))
            },
        )
    }
    /// Reads at most one bounded Binance private frame, fsyncs its normalized Fill through the
    /// shared account facts journal, then applies the exact routed Grid reducer turn. No raw
    /// payload is retained and this path contains no BBO/REST request.
    pub fn poll_binance_grid_private_once(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<bool, NodeError> {
        let event = match self
            .host
            .with_gateway_read(|gateway| gateway.poll_private_fill())
        {
            Ok(event) => event,
            Err(venue_runtime::account::AccountRuntimeHostError::Gateway(
                venue_gateway_binance::BinanceAccountGatewayError::PrivateStream
                | venue_gateway_binance::BinanceAccountGatewayError::Transport(
                    venue_gateway_binance::BinanceTransportError::Disconnected
                    | venue_gateway_binance::BinanceTransportError::EndOfStream
                    | venue_gateway_binance::BinanceTransportError::Timeout
                    | venue_gateway_binance::BinanceTransportError::Protocol,
                ),
            )) => return self.supervise_binance_grid_once(binding).map(|_| true),
            Err(error) => {
                return Err(NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: error.to_string(),
                });
            }
        };
        let Some(event) = event else {
            return Ok(false);
        };
        match event {
            venue_gateway_binance::BinancePrivateAccountEvent::Fill(event) => self
                .consume_private_fill(
                    "binance",
                    PrivateFillFact {
                        source_private_generation: event.private_generation,
                        received_at_ms: event.received_at_ms,
                        fill: event.fill,
                    },
                ),
            venue_gateway_binance::BinancePrivateAccountEvent::ReconcileRequired { .. } => {
                self.supervise_binance_grid_once(binding).map(|_| true)
            }
        }
    }

    /// Compares the complete signed Binance order surface with the reducer's exact desired
    /// routes. Missed authenticated fills are recovered first; an otherwise unexplained gap is
    /// drained through WAL-owned cancels and rebuilt once from a fresh BBO and signed inventory.
    pub fn supervise_binance_grid_once(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<bool, NodeError> {
        let reset = self.supervise_grid_signed_surface_once(binding)?;
        if reset && self.take_grid_bootstrap_request(binding)? {
            self.bootstrap_binance_grid_once(binding)?;
        }
        Ok(reset)
    }
}

#[cfg(feature = "bitget")]
impl ProductionResident<venue_gateway_bitget::BitgetAccountGateway> {
    /// Installs the first Grid epoch from the one signed account snapshot and the BBO reconstructed
    /// by the caller's same-socket Bitget `books` bridge. No REST/ticker market read is allowed
    /// between those facts and the first durable semantic turn.
    pub(crate) fn bootstrap_bitget_grid_from_sequenced_bbo(
        &mut self,
        binding: &StrategyBinding,
        gateway_binding: &venue_gateway_api::GatewayBinding,
        snapshot: venue_runtime::SignedAccountSnapshot,
        bbo: scalping::BitgetGridBootstrapBbo,
    ) -> Result<(), NodeError> {
        if snapshot.binding() != gateway_binding
            || gateway_binding.venue != self.host.binding().venue
            || gateway_binding.trading_account_id != self.host.binding().trading_account_id
            || gateway_binding.symbol != binding.key.symbol
        {
            self.fail_grid_bootstrap(binding)?;
            return Err(NodeError::ResidentRuntime);
        }
        let rules = match self.host.with_gateway_read(|gateway| {
            gateway.grid_bootstrap_rules(gateway_binding, snapshot.rules_generation())
        }) {
            Ok(rules) => rules,
            Err(error) => {
                self.fail_grid_bootstrap(binding)?;
                return Err(NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: error.to_string(),
                });
            }
        };
        let maximum_quantity = match rules
            .maximum_order_quantity
            .filter(|maximum| *maximum >= rules.snapshot.metadata.quantity.minimum)
        {
            Some(maximum) => maximum,
            None => {
                self.fail_grid_bootstrap(binding)?;
                return Err(NodeError::ResidentRuntime);
            }
        };
        self.bootstrap_grid_from_signed_market(
            binding,
            snapshot,
            GridBootstrapMarket {
                bid: bbo.bid,
                ask: bbo.ask,
                price_tick: rules.snapshot.metadata.instrument.price_tick,
                quantity_step: rules.snapshot.metadata.instrument.quantity_step,
                minimum_quantity: rules.snapshot.metadata.quantity.minimum,
                maximum_quantity,
                minimum_notional: rules.snapshot.metadata.instrument.minimum_notional.value,
                observed_at_ms: bbo.observed_at_ms,
            },
        )
    }

    /// The authenticated UTA `fill` source is converted to the Runtime-owned facts journal
    /// before any Grid reducer runs; stream authorization is checked by the adapter.
    pub(crate) fn poll_bitget_grid_private_once(&mut self) -> Result<bool, NodeError> {
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
        self.consume_private_fill(
            "bitget",
            PrivateFillFact {
                source_private_generation: event.source_private_generation,
                received_at_ms: event.received_at_ms,
                fill: event.fill,
            },
        )
    }
}

#[cfg(feature = "gate")]
impl ProductionResident<venue_gateway_gate::GateAccountGateway> {
    /// Gate follows the same signed-inventory and bounded-BBO first-install transaction as
    /// Binance. Its gateway validates Gate-native contract semantics before this adapter-neutral
    /// Grid surface is constructed.
    pub fn bootstrap_gate_grid_once(&mut self, binding: &StrategyBinding) -> Result<(), NodeError> {
        let snapshot = self.refresh_signed_snapshot()?;
        let market = self
            .host
            .with_gateway_read(|gateway| gateway.fresh_grid_bootstrap_market())
            .map_err(|error| NodeError::LiveHost {
                venue: self.host.binding().venue,
                message: error.to_string(),
            })?;
        let maximum_quantity = market
            .rules
            .maximum_contracts
            .and_then(|contracts| contracts.checked_mul(market.rules.quanto_multiplier))
            .filter(|quantity| *quantity >= market.rules.minimum_quantity())
            .ok_or(NodeError::ResidentRuntime)?;
        self.bootstrap_grid_from_signed_market(
            binding,
            snapshot,
            GridBootstrapMarket {
                bid: market.bid,
                ask: market.ask,
                price_tick: market.rules.instrument.price_tick,
                quantity_step: market.rules.instrument.quantity_step,
                minimum_quantity: market.rules.minimum_quantity(),
                maximum_quantity,
                minimum_notional: market.rules.instrument.minimum_notional.value,
                observed_at_ms: market.observed_at_ms,
            },
        )
    }

    /// The Gate adapter has already authenticated, bounded and normalized the native
    /// `futures.usertrades` update. Runtime still owns durable fact ingress and every resulting
    /// Grid mutation through the one account lane.
    pub fn poll_gate_grid_private_once(&mut self) -> Result<bool, NodeError> {
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
        self.consume_private_fill(
            "gate",
            PrivateFillFact {
                source_private_generation: event.source_private_generation,
                received_at_ms: event.received_at_ms,
                fill: event.fill,
            },
        )
    }
}

#[cfg(test)]
mod bootstrap_tests;
