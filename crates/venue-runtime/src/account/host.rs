use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use rust_decimal::Decimal;
use venue_domain::domain::{CommandId, ExecutionCommand, OrderOwner, OrderPurpose};
use venue_execution::{
    AccountCommandStatus, AccountDispatchOutcome, AccountHostError,
    AccountLimitNormalizationIntent, AccountMutationHost, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountSymbolSet, HostPreparedCommand, LegacyV1CustodyRoute,
    ManagedGridSurfaceReceipt, execution_command_sha256,
};
use venue_gateway_api::GatewayBinding;

use super::{
    AccountKey, AccountLanePriority, AccountModelError, AccountRuntime, AccountRuntimeError,
    CopyActorAppliedReceipt, ResidentActorAppliedArtifacts, StrategyBinding, StrategyInstanceKey,
};
use crate::{AppliedStrategyTurnReceipt, execution::DurableCommandIdentityAllocation};

/// The narrow resident-side composition of the runtime execution lane and the account mutation
/// host. It deliberately has one synchronous submission method: the contained host owns the
/// only account writer lock, WAL, UNKNOWN fence, and physical gateway permit.
#[derive(Debug)]
pub struct AccountRuntimeHost<G> {
    account: AccountKey,
    host: AccountMutationHost<G>,
    prepared: BTreeMap<venue_domain::domain::CommandId, PreparedLaneAdmission>,
    managed_grid_prepared: BTreeSet<venue_domain::domain::CommandId>,
    managed_grid_batch_target: Option<crate::StrategyInstanceKey>,
}

#[derive(Debug, Eq, PartialEq)]
struct PreparedAdmissionCommitment {
    target: crate::StrategyInstanceKey,
    connection_generation: u64,
    private_generation: u64,
    config_digest: String,
    config_epoch: u64,
    turn_sequence: u64,
    priority: AccountLanePriority,
    command_sha256: [u8; 32],
    allocation_sequence: u64,
    allocation_record_sha256: [u8; 32],
}

#[derive(Debug)]
struct PreparedLaneAdmission {
    commitment: PreparedAdmissionCommitment,
    proof: HostPreparedCommand,
}

fn managed_grid_scope_owner(binding: &StrategyBinding) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.key.instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.key.account.exchange.as_str().to_owned(),
        account: binding.key.account.account.clone(),
        symbol: binding.key.symbol.clone(),
        purpose: OrderPurpose::Entry,
    }
}

fn managed_surface_receipt_matches(
    receipt: &ManagedGridSurfaceReceipt,
    binding: &StrategyBinding,
    owner: &OrderOwner,
) -> bool {
    receipt.binding().venue.as_str() == binding.key.account.exchange.as_str()
        && receipt.binding().trading_account_id == binding.key.account.account
        && receipt.binding().symbol == binding.key.symbol
        && receipt.owner() == owner
        && receipt.private_generation() > 0
        && !receipt.surface_sha256().iter().all(|byte| *byte == 0)
}

fn managed_grid_batch_shape(
    signed_surface: &BTreeMap<CommandId, OrderOwner>,
    commands: &[ExecutionCommand],
) -> bool {
    let post_only_limit = |command: &ExecutionCommand| {
        matches!(command, ExecutionCommand::PlaceLimit(order)
            if order.time_in_force == venue_domain::LimitTimeInForce::PostOnly)
    };
    if signed_surface.is_empty() {
        return commands.len() <= 200 && commands.iter().all(post_only_limit);
    }
    matches!(
        commands,
        [first, second, ExecutionCommand::Cancel(cancel)]
            if post_only_limit(first)
                && post_only_limit(second)
                && signed_surface.get(&cancel.target_client_order_id) == Some(&cancel.owner)
    )
}

impl<G: AccountPhysicalGateway> AccountRuntimeHost<G> {
    /// Runs one adapter read through the already-owned account gateway. The callback receives no
    /// Host permit and therefore cannot create a physical mutation; production feed pumps use it
    /// solely for bounded, normalized private/public evidence reads.
    pub fn with_gateway_read<T>(
        &mut self,
        operation: impl FnOnce(&mut G) -> Result<T, G::Error>,
    ) -> Result<T, AccountRuntimeHostError<G::Error>> {
        self.host
            .with_gateway_read(operation)
            .map_err(|error| AccountRuntimeHostError::Host(AccountHostError::Gateway(error)))
    }
    pub fn open(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        gateway: G,
    ) -> Result<Self, AccountRuntimeHostError<G::Error>> {
        Self::open_with_symbols(
            artifacts_root,
            binding.clone(),
            AccountSymbolSet::single(&binding),
            gateway,
        )
    }

    /// Opens the one account writer for an explicit, symbol-unique strategy set.  The symbols
    /// share the same host/WAL/lane; this merely gives signed-readback coverage a finite scope.
    pub fn open_with_symbols(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        gateway: G,
    ) -> Result<Self, AccountRuntimeHostError<G::Error>> {
        let account = AccountKey::new(binding.venue, binding.trading_account_id.clone())
            .map_err(AccountRuntimeHostError::Model)?;
        let host = AccountMutationHost::open_with_symbols(
            artifacts_root,
            binding,
            configured_symbols,
            Decimal::TEN,
            gateway,
        )
        .map_err(AccountRuntimeHostError::Host)?;
        Ok(Self {
            account,
            host,
            prepared: BTreeMap::new(),
            managed_grid_prepared: BTreeSet::new(),
            managed_grid_batch_target: None,
        })
    }

    /// Explicit takeover path for frozen Stage-7 product scopes. There is deliberately no
    /// automatic legacy discovery: the deployment supplies one persisted predecessor record.
    pub fn open_with_legacy_v1_predecessor(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        gateway: G,
        predecessor: venue_execution::LegacyV1WriterPredecessor,
    ) -> Result<Self, AccountRuntimeHostError<G::Error>> {
        Self::open_with_symbols_and_legacy_v1_predecessor(
            artifacts_root,
            binding.clone(),
            AccountSymbolSet::single(&binding),
            gateway,
            predecessor,
        )
    }

    pub fn open_with_symbols_and_legacy_v1_predecessor(
        artifacts_root: impl Into<PathBuf>,
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        gateway: G,
        predecessor: venue_execution::LegacyV1WriterPredecessor,
    ) -> Result<Self, AccountRuntimeHostError<G::Error>> {
        let account = AccountKey::new(binding.venue, binding.trading_account_id.clone())
            .map_err(AccountRuntimeHostError::Model)?;
        let host = AccountMutationHost::open_with_symbols_and_legacy_v1_predecessor(
            artifacts_root,
            binding,
            configured_symbols,
            Decimal::TEN,
            gateway,
            predecessor,
        )
        .map_err(AccountRuntimeHostError::Host)?;
        Ok(Self {
            account,
            host,
            prepared: BTreeMap::new(),
            managed_grid_prepared: BTreeSet::new(),
            managed_grid_batch_target: None,
        })
    }

    #[must_use]
    pub const fn account(&self) -> &AccountKey {
        &self.account
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        self.host.binding()
    }

    #[must_use]
    pub fn has_unresolved(&self) -> bool {
        self.host.has_unresolved()
    }

    pub fn command_status(
        &self,
        command_id: &venue_domain::domain::CommandId,
    ) -> Result<Option<AccountCommandStatus>, AccountRuntimeHostError<G::Error>> {
        self.host
            .command_status(command_id)
            .map_err(AccountRuntimeHostError::Host)
    }

    /// Returns only the detached command persisted in the Host WAL. This cannot prepare or
    /// dispatch a mutation; Copy uses it to reconcile an already-admitted command identity.
    #[must_use]
    pub fn command_snapshot(
        &self,
        command_id: &venue_domain::domain::CommandId,
    ) -> Option<ExecutionCommand> {
        self.host.command_snapshot(command_id)
    }

    /// Read-only durable owner recovery for fills whose exchange payload omits client order id.
    /// A native id remains unowned unless its accepted WAL family resolves exactly.
    #[must_use]
    pub fn command_snapshot_by_venue_order_id(
        &self,
        family: venue_domain::domain::NativeOrderFamily,
        venue_order_id: &str,
    ) -> Option<ExecutionCommand> {
        self.host
            .command_snapshot_by_venue_order_id(family, venue_order_id)
    }

    pub(crate) fn accepted_order_routes_for_owner(
        &self,
        owner: &venue_domain::domain::OrderOwner,
    ) -> Vec<venue_execution::NativeOrderRoute> {
        self.host.accepted_order_routes_for_owner(owner)
    }

    /// Production actor recovery must flow through the sole Host so a checkpoint may bind to a
    /// verified older WAL prefix but never to a caller-supplied or unrelated WAL head.
    pub fn install_resident_actor_applied_artifacts(
        &self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        artifacts: ResidentActorAppliedArtifacts,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account || binding.key.account != self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        let store = artifacts
            .open_store(binding.clone())
            .map_err(AccountRuntimeHostError::Runtime)?;
        if let Some(recovered) = store
            .recover()
            .map_err(AccountRuntimeError::ActorApplied)
            .map_err(AccountRuntimeHostError::Runtime)?
            && !self
                .host
                .validates_historical_wal_head(recovered.receipt().wal())
        {
            return Err(AccountRuntimeHostError::Runtime(
                AccountRuntimeError::ActorAppliedStore,
            ));
        }
        let routes = self
            .host
            .accepted_order_routes()
            .into_iter()
            .filter(|route| binding.matches_owner(&route.owner))
            .collect();
        runtime
            .hydrate_host_wal_routes(routes)
            .map_err(AccountRuntimeHostError::Runtime)?;
        runtime
            .install_host_verified_actor_applied_store(store)
            .map_err(AccountRuntimeHostError::Runtime)
    }

    /// Reconciles one durable command and immediately mirrors the resulting Host WAL head and
    /// every exact registered Accepted route into Runtime. This path is read-only at the venue:
    /// an Unknown may converge, but the command is never dispatched again.
    pub fn reconcile_runtime_command_status(
        &mut self,
        runtime: &mut AccountRuntime,
        command_id: &venue_domain::domain::CommandId,
    ) -> Result<Option<AccountCommandStatus>, AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        let current = self
            .host
            .command_status(command_id)
            .map_err(AccountRuntimeHostError::Host)?;
        if current.as_ref().is_some_and(|status| {
            matches!(
                status.state(),
                venue_execution::CommandState::Submitted
                    | venue_execution::CommandState::Unknown { .. }
            )
        }) {
            // The Runtime-aware durable refresh asks for every unresolved identity, settles only
            // signed outcomes, installs the new private generation/risk fence and hydrates routes.
            // It never dispatches, so an Unknown cannot become a retry.
            self.refresh_runtime_signed_snapshot(runtime)?;
        } else {
            self.sync_runtime_wal_and_registered_routes(runtime)?;
        }
        self.host
            .command_status(command_id)
            .map_err(AccountRuntimeHostError::Host)
    }

    fn sync_runtime_wal_and_registered_routes(
        &self,
        runtime: &mut AccountRuntime,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        runtime
            .advance_resident_wal_head(
                self.host
                    .runtime_wal_head()
                    .map_err(AccountRuntimeHostError::Host)?,
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        let routes = self
            .host
            .accepted_order_routes()
            .into_iter()
            .filter(|route| {
                runtime
                    .registry
                    .active_bindings()
                    .any(|binding| binding.matches_owner(&route.owner))
            })
            .collect();
        runtime
            .hydrate_host_wal_routes(routes)
            .map_err(AccountRuntimeHostError::Runtime)
    }

    /// Collects and fsyncs a complete signed adapter snapshot through the sole Host, then makes
    /// the matching AccountRuntime Ready.  No caller can supply a bootstrap receipt directly.
    pub fn bootstrap_runtime(
        &mut self,
        runtime: &mut AccountRuntime,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        let receipt = self
            .host
            .durable_runtime_bootstrap()
            .map_err(AccountRuntimeHostError::Host)?;
        runtime
            .install_production_signed_bootstrap(&receipt)
            .map_err(AccountRuntimeHostError::Runtime)
    }

    /// Read-only signed facts for resident projections. This exposes no Host, gateway, permit,
    /// prepare proof, or dispatch capability.
    pub fn refresh_signed_snapshot(
        &mut self,
    ) -> Result<venue_execution::SignedAccountSnapshot, AccountRuntimeHostError<G::Error>> {
        self.host
            .refresh_signed_snapshot()
            .map_err(AccountRuntimeHostError::Host)
    }

    /// Read-only adapter instrument fact for planning. It carries no host permit, write access,
    /// or configuration-derived market inference.
    pub fn current_instrument(
        &mut self,
    ) -> Result<venue_execution::AccountInstrumentIdentity, AccountRuntimeHostError<G::Error>> {
        self.host
            .current_instrument()
            .map_err(AccountRuntimeHostError::Host)
    }

    pub fn current_instrument_for(
        &mut self,
        symbol: &venue_domain::domain::Symbol,
    ) -> Result<venue_execution::AccountInstrumentIdentity, AccountRuntimeHostError<G::Error>> {
        self.host
            .current_instrument_for(symbol)
            .map_err(AccountRuntimeHostError::Host)
    }

    /// Refreshes Runtime's production risk fence only from a Host-persisted, complete signed
    /// snapshot. The caller never supplies facts or an UNKNOWN resolution, so neither a paused
    /// lifecycle nor an unresolved command can be cleared by Node input.
    pub fn refresh_runtime_signed_snapshot(
        &mut self,
        runtime: &mut AccountRuntime,
    ) -> Result<venue_execution::SignedAccountSnapshot, AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        let receipt = self
            .host
            .durable_runtime_refresh()
            .map_err(AccountRuntimeHostError::Host)?;
        let advances_private_generation =
            receipt.snapshot().private_generation() > runtime.last_reconciliation_generation;
        let snapshot = runtime
            .refresh_production_signed_snapshot(&receipt)
            .map_err(AccountRuntimeHostError::Runtime)?;
        if advances_private_generation {
            self.reject_queued_prepared("private_generation_advanced_before_dispatch")?;
            // The rejection records are part of the same Host WAL but were appended after the
            // signed snapshot receipt.  Advance through this Host-only path so later actor
            // receipts cannot bind an obsolete pre-rejection head.
        }
        // durable_runtime_refresh may have settled Unknown -> Accepted. Rebuild the exact
        // registered route before any subsequent private fill or Actor receipt can be handled.
        // This also advances through any Prepared rejection appended just above.
        self.sync_runtime_wal_and_registered_routes(runtime)?;
        Ok(snapshot)
    }

    /// Returns the exact legacy routes derived from the most recently installed Runtime
    /// generation. This method performs no read, so an Actor turn persisted immediately after it
    /// is bound to the same signed facts that custody preparation rechecks.
    pub fn legacy_v1_custody_routes_from_current_snapshot(
        &self,
    ) -> Result<Vec<LegacyV1CustodyRoute>, AccountRuntimeHostError<G::Error>> {
        self.host
            .legacy_v1_custody_routes_from_latest_signed_snapshot()
            .map_err(AccountRuntimeHostError::Host)
    }

    /// Admits an operator-generated semantic command only after the sole Host has fsynced the
    /// corresponding WAL record. The prepared proof never leaves this resident wrapper.
    pub fn prepare_and_admit_operator(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        command: ExecutionCommand,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account
            || binding.key.account != self.account
            || command.mutation_owner().exchange != self.account.exchange.as_str()
            || command.mutation_owner().account != self.account.account
            || !binding.matches_owner(command.mutation_owner())
        {
            return Err(AccountRuntimeHostError::Scope);
        }
        let prepared = self
            .host
            .prepare_for_lane(command)
            .map_err(AccountRuntimeHostError::Host)?;
        self.admit_prepared_operator(runtime, binding, applied, priority, prepared, false)
    }

    /// Installs only an in-memory, target-specific Grid authority after Host proves the caller's
    /// expected set is exactly the fresh signed WAL-owned surface. A later refresh revokes it.
    pub fn confirm_managed_grid_surface(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        expected_open_orders: BTreeMap<CommandId, OrderOwner>,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if binding.key.strategy_kind != crate::StrategyKind::HedgedGrid
            || runtime.account() != &self.account
            || binding.key.account != self.account
        {
            return Err(AccountRuntimeHostError::Scope);
        }
        let owner = managed_grid_scope_owner(binding);
        let receipt = self
            .host
            .confirm_managed_grid_surface(&owner, expected_open_orders)
            .map_err(AccountRuntimeHostError::Host)?;
        if !managed_surface_receipt_matches(&receipt, binding, &owner) {
            return Err(AccountRuntimeHostError::Scope);
        }
        runtime
            .install_managed_grid_surface(
                binding,
                receipt.private_generation(),
                receipt.surface_sha256(),
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        self.managed_grid_batch_target = None;
        Ok(())
    }

    /// Consumes one exact Grid surface into one bounded batch. Initial installation is Place-only
    /// and capped at 200 children; a rolling transaction is exactly two PlaceLimit plus one
    /// Cancel of the previously signed surface. There is no second batch before signed refresh.
    pub fn prepare_and_admit_managed_grid_batch(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        expected_open_orders: BTreeMap<CommandId, OrderOwner>,
        commands: &[ExecutionCommand],
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if binding.key.strategy_kind != crate::StrategyKind::HedgedGrid
            || priority != AccountLanePriority::Normal
            || commands.is_empty()
            || commands.len() > 200
            || runtime.account() != &self.account
            || binding.key.account != self.account
            || commands.iter().any(|command| {
                command.mutation_owner().exchange != self.account.exchange.as_str()
                    || command.mutation_owner().account != self.account.account
                    || !binding.matches_owner(command.mutation_owner())
                    || !matches!(
                        command,
                        ExecutionCommand::PlaceLimit(_) | ExecutionCommand::Cancel(_)
                    )
            })
            || commands
                .iter()
                .map(ExecutionCommand::command_id)
                .collect::<BTreeSet<_>>()
                .len()
                != commands.len()
            || !managed_grid_batch_shape(&expected_open_orders, commands)
        {
            return Err(AccountRuntimeHostError::Scope);
        }
        let owner = managed_grid_scope_owner(binding);
        let surface = self
            .host
            .confirm_managed_grid_surface(&owner, expected_open_orders)
            .map_err(AccountRuntimeHostError::Host)?;
        if !managed_surface_receipt_matches(&surface, binding, &owner) {
            return Err(AccountRuntimeHostError::Scope);
        }
        let private_generation = surface.private_generation();
        let surface_sha256 = surface.surface_sha256();
        let command_ids = commands
            .iter()
            .map(|command| command.command_id().clone())
            .collect::<BTreeSet<_>>();
        runtime
            .begin_managed_grid_batch(
                binding,
                applied,
                private_generation,
                surface_sha256,
                command_ids,
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        self.managed_grid_batch_target = Some(binding.key.clone());
        let prepared = match self
            .host
            .prepare_managed_grid_batch_for_lane(surface, commands)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                runtime
                    .abort_managed_grid_batch(&binding.key)
                    .map_err(AccountRuntimeHostError::Runtime)?;
                self.managed_grid_batch_target = None;
                return Err(AccountRuntimeHostError::Host(error));
            }
        };
        let mut prepared = prepared.into_iter();
        while let Some(proof) = prepared.next() {
            if let Err(error) =
                self.admit_prepared_operator(runtime, binding, applied, priority, proof, true)
            {
                for untouched in prepared {
                    self.host
                        .reject_prepared_without_dispatch(
                            &untouched,
                            "managed_grid_batch_admission_failed",
                        )
                        .map_err(AccountRuntimeHostError::Host)?;
                }
                runtime
                    .abort_managed_grid_batch(&binding.key)
                    .map_err(AccountRuntimeHostError::Runtime)?;
                self.managed_grid_batch_target = None;
                self.reject_prepared_batch(runtime, "managed_grid_batch_admission_failed")?;
                return Err(error);
            }
        }
        Ok(())
    }

    fn admit_prepared_operator(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        prepared: HostPreparedCommand,
        managed_grid: bool,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        let command_id = prepared.command_id().clone();
        let commitment =
            match PreparedAdmissionCommitment::new(binding, applied, priority, &prepared) {
                Ok(commitment) => commitment,
                Err(error) => {
                    self.host
                        .reject_prepared_without_dispatch(&prepared, "runtime_admission_failed")
                        .map_err(AccountRuntimeHostError::Host)?;
                    return Err(AccountRuntimeHostError::Runtime(error));
                }
            };
        if let Some(existing) = self.prepared.get(&command_id) {
            if existing.commitment == commitment && runtime.has_active_execution(&command_id) {
                return Ok(());
            }
            return Err(AccountRuntimeHostError::PreparedAdmissionConflict);
        }
        let allocation = match DurableCommandIdentityAllocation::from_host_prepared(
            prepared.receipt_sequence(),
            prepared.receipt_digest(),
            prepared.cancel_target_family(),
        ) {
            Ok(allocation) => allocation,
            Err(error) => {
                self.host
                    .reject_prepared_without_dispatch(&prepared, "runtime_admission_failed")
                    .map_err(AccountRuntimeHostError::Host)?;
                return Err(AccountRuntimeHostError::Runtime(
                    AccountRuntimeError::ExecutionLane(error),
                ));
            }
        };
        let admission = if managed_grid {
            runtime.admit_host_prepared_managed_grid_execution(
                binding,
                applied,
                priority,
                prepared.command().clone(),
                allocation,
            )
        } else {
            runtime.admit_host_prepared_execution(
                binding,
                applied,
                priority,
                prepared.command().clone(),
                allocation,
            )
        };
        if let Err(error) = admission {
            self.host
                .reject_prepared_without_dispatch(&prepared, "runtime_admission_failed")
                .map_err(AccountRuntimeHostError::Host)?;
            return Err(AccountRuntimeHostError::Runtime(error));
        }
        self.prepared.insert(
            command_id.clone(),
            PreparedLaneAdmission {
                commitment,
                proof: prepared,
            },
        );
        if managed_grid {
            self.managed_grid_prepared.insert(command_id);
        }
        runtime
            .advance_resident_wal_head(
                self.host
                    .runtime_wal_head()
                    .map_err(AccountRuntimeHostError::Host)?,
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        Ok(())
    }

    /// The only transition that permits an old Owner through the unified lane. The Host first
    /// rechecks the supplied route against its current signed snapshot and creates only Cancel;
    /// Runtime then accepts it under the current actor's durable turn with a sealed exception.
    pub fn prepare_and_admit_legacy_v1_custody_cancel(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        route: &LegacyV1CustodyRoute,
    ) -> Result<venue_domain::domain::CommandId, AccountRuntimeHostError<G::Error>> {
        if runtime.account() != &self.account
            || binding.key.account != self.account
            || binding.key.symbol != route.owner.symbol
        {
            return Err(AccountRuntimeHostError::Scope);
        }
        let prepared = self
            .host
            .prepare_legacy_v1_custody_cancel_for_lane(route)
            .map_err(AccountRuntimeHostError::Host)?;
        let command_id = prepared.command_id().clone();
        let commitment = PreparedAdmissionCommitment::new(
            binding,
            applied,
            AccountLanePriority::Critical,
            &prepared,
        )
        .map_err(AccountRuntimeHostError::Runtime)?;
        if let Some(existing) = self.prepared.get(&command_id) {
            if existing.commitment == commitment && runtime.has_active_execution(&command_id) {
                return Ok(command_id);
            }
            return Err(AccountRuntimeHostError::PreparedAdmissionConflict);
        }
        let allocation = DurableCommandIdentityAllocation::from_host_prepared(
            prepared.receipt_sequence(),
            prepared.receipt_digest(),
            prepared.cancel_target_family(),
        )
        .map_err(|error| {
            AccountRuntimeHostError::Runtime(AccountRuntimeError::ExecutionLane(error))
        })?;
        runtime
            .admit_host_prepared_legacy_v1_custody_cancel(
                binding,
                applied,
                prepared.command().clone(),
                allocation,
                route,
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        runtime
            .advance_resident_wal_head(
                self.host
                    .runtime_wal_head()
                    .map_err(AccountRuntimeHostError::Host)?,
            )
            .map_err(AccountRuntimeHostError::Runtime)?;
        self.prepared.insert(
            command_id.clone(),
            PreparedLaneAdmission {
                commitment,
                proof: prepared,
            },
        );
        Ok(command_id)
    }

    /// Copy has a public semantic Actor receipt rather than a generic Runtime turn receipt.
    /// Resolve it through Runtime's current durable receipt first, then use the identical Host
    /// preparation and lane-admission path as Grid and Scalping.
    pub fn prepare_and_admit_copy_actor(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &CopyActorAppliedReceipt,
        priority: AccountLanePriority,
        command: ExecutionCommand,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        let turn = runtime
            .current_copy_actor_turn(binding, applied)
            .map_err(AccountRuntimeHostError::Runtime)?;
        self.prepare_and_admit_operator(runtime, binding, &turn, priority, command)
    }

    /// A resident supplies only semantic quote exposure. Host derives price/quantity from fresh
    /// adapter facts before the normal Prepared/WAL/lane path begins, binding the result to the
    /// exact already-durable Actor Applied turn.
    pub fn normalize_and_prepare_limit(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        let command = self
            .host
            .normalize_limit_intent(intent)
            .map_err(AccountRuntimeHostError::Host)?;
        self.prepare_and_admit_operator(runtime, binding, applied, priority, command)
    }

    /// Preserves a user-selected limit price and policy while reusing the same resident Actor
    /// receipt, account WAL, lane and writer as every strategy-generated command.
    pub fn normalize_and_prepare_priced_limit(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        intent: &AccountPricedLimitIntent,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        let command = self
            .host
            .normalize_priced_limit_intent(intent)
            .map_err(AccountRuntimeHostError::Host)?;
        self.prepare_and_admit_operator(runtime, binding, applied, priority, command)
    }

    /// Compatibility wrapper for the Copy actor's specialized public receipt.
    pub fn normalize_and_prepare_copy_limit(
        &mut self,
        runtime: &mut AccountRuntime,
        binding: &StrategyBinding,
        applied: &CopyActorAppliedReceipt,
        priority: AccountLanePriority,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        let turn = runtime
            .current_copy_actor_turn(binding, applied)
            .map_err(AccountRuntimeHostError::Runtime)?;
        self.normalize_and_prepare_limit(runtime, binding, &turn, priority, intent)
    }

    pub(crate) fn has_prepared(&self, command_id: &venue_domain::domain::CommandId) -> bool {
        self.prepared.contains_key(command_id)
    }

    fn reject_queued_prepared(
        &mut self,
        reason: &str,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        for admission in self.prepared.values() {
            self.host
                .reject_prepared_without_dispatch(&admission.proof, reason)
                .map_err(AccountRuntimeHostError::Host)?;
        }
        self.prepared.clear();
        Ok(())
    }

    /// A multi-command semantic turn may fail its all-or-nothing admission before any physical
    /// dispatch.  Retire each already-fsynced Prepared record in the sole Host WAL so it cannot
    /// become a stranded reservation or be sent by a later turn.
    pub fn reject_prepared_batch(
        &mut self,
        runtime: &mut AccountRuntime,
        reason: &str,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if reason.trim().is_empty() || runtime.account() != &self.account {
            return Err(AccountRuntimeHostError::Scope);
        }
        if let Some(target) = self.managed_grid_batch_target.take().or_else(|| {
            self.managed_grid_prepared.iter().find_map(|command_id| {
                self.prepared
                    .get(command_id)
                    .map(|admission| admission.commitment.target.clone())
            })
        }) {
            runtime
                .abort_managed_grid_batch(&target)
                .map_err(AccountRuntimeHostError::Runtime)?;
        }
        let command_ids = std::mem::take(&mut self.managed_grid_prepared);
        for command_id in command_ids {
            if let Some(admission) = self.prepared.remove(&command_id) {
                self.host
                    .reject_prepared_without_dispatch(&admission.proof, reason)
                    .map_err(AccountRuntimeHostError::Host)?;
            }
        }
        runtime
            .advance_resident_wal_head(
                self.host
                    .runtime_wal_head()
                    .map_err(AccountRuntimeHostError::Host)?,
            )
            .map_err(AccountRuntimeHostError::Runtime)
    }

    /// Retires only the managed Grid batch owned by `key`.  Control transitions call this before
    /// changing Runtime lifecycle so a queued cancellation cannot remain Prepared after the
    /// matching batch authority has been revoked.
    pub fn reject_prepared_managed_grid_batch(
        &mut self,
        runtime: &mut AccountRuntime,
        key: &StrategyInstanceKey,
        reason: &str,
    ) -> Result<(), AccountRuntimeHostError<G::Error>> {
        if self
            .managed_grid_batch_target
            .as_ref()
            .is_some_and(|target| target != key)
        {
            return Err(AccountRuntimeHostError::Scope);
        }
        self.reject_prepared_batch(runtime, reason)
    }

    pub(crate) fn runtime_wal_head(
        &self,
    ) -> Result<venue_storage::DurableWalHead, AccountRuntimeHostError<G::Error>> {
        self.host
            .runtime_wal_head()
            .map_err(AccountRuntimeHostError::Host)
    }

    pub(crate) fn dispatch_prepared_for_lane(
        &mut self,
        command_id: &venue_domain::domain::CommandId,
    ) -> Result<AccountDispatchOutcome, AccountRuntimeHostError<G::Error>> {
        let prepared = self
            .prepared
            .remove(command_id)
            .ok_or(AccountRuntimeHostError::PreparedProof)?
            .proof;
        self.managed_grid_prepared.remove(command_id);
        if prepared.command_id() != command_id {
            return Err(AccountRuntimeHostError::PreparedProof);
        }
        self.host
            .dispatch_prepared(prepared)
            .map_err(AccountRuntimeHostError::Host)
    }
}

impl PreparedAdmissionCommitment {
    fn new(
        binding: &StrategyBinding,
        applied: &AppliedStrategyTurnReceipt,
        priority: AccountLanePriority,
        prepared: &HostPreparedCommand,
    ) -> Result<Self, AccountRuntimeError> {
        let token = applied.token();
        Ok(Self {
            target: binding.key.clone(),
            connection_generation: token.connection_generation(),
            private_generation: token.private_generation(),
            config_digest: token.config_digest().to_owned(),
            config_epoch: token.config_epoch(),
            turn_sequence: token.turn_sequence(),
            priority,
            command_sha256: execution_command_sha256(prepared.command())
                .map_err(|_| AccountRuntimeError::StrategyTurnAuthority)?,
            allocation_sequence: prepared.receipt_sequence(),
            allocation_record_sha256: prepared.receipt_digest(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AccountRuntimeHostError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Model(AccountModelError),
    #[error(transparent)]
    Runtime(AccountRuntimeError),
    #[error("runtime command owner is outside this exchange account")]
    Scope,
    #[error("lane command has no matching resident Host Prepared proof")]
    PreparedProof,
    #[error("replayed prepared command does not match its original lane admission")]
    PreparedAdmissionConflict,
    #[error("account gateway read failed")]
    Gateway(#[source] E),
    #[error(transparent)]
    Host(AccountHostError<E>),
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use rust_decimal::Decimal;
    use tempfile::TempDir;
    use venue_domain::domain::{
        CommandId, DomainEvent, EventId, ExecutionCommand, FieldState, Fill, NativeOrderFamily,
        OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
    };
    use venue_execution::{
        AccountGatewayResult, AccountHostValidationError, AccountRecoveryOutcome,
        AccountRecoveryReport, AccountRecoveryRequest, AccountRiskEvidence, SignedAccountOrderFact,
        SignedAccountPositionFact, SignedAccountPositionMode, SignedAccountSnapshot,
        SignedUnknownFact, SignedUnknownResult,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::account::AccountPrivateFactInput;
    use crate::{ExchangeId, StrategyBinding, StrategyInstanceKey, StrategyKind};

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    struct Gateway {
        binding: GatewayBinding,
        state: Arc<Mutex<GatewayState>>,
    }

    #[derive(Default)]
    struct GatewayState {
        dispatches: usize,
        signed_quantity: Decimal,
        external_order: bool,
        unresolved_after_dispatch: bool,
        resolve_unknown_on_snapshot: bool,
        stale_snapshot: bool,
        missing_position_leg: bool,
        private_generation: u64,
        command: Option<ExecutionCommand>,
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
            let outcomes = request
                .unresolved()
                .iter()
                .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
                .collect();
            AccountRecoveryReport::new(self.binding.clone(), 1, outcomes).map_err(io::Error::other)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            let observed_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| AccountHostValidationError::RiskEvidence)?
                .as_millis()
                .try_into()
                .map_err(|_| AccountHostValidationError::RiskEvidence)?;
            AccountRiskEvidence::complete(
                self.binding.clone(),
                observed_at_ms,
                1,
                Vec::new(),
                Vec::new(),
            )
        }

        fn signed_account_snapshot(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
            let state = self
                .state
                .lock()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let observed_at_ms: u64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?
                .as_millis()
                .try_into()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let observed_at_ms = if state.stale_snapshot {
                observed_at_ms.saturating_sub(60_001)
            } else {
                observed_at_ms
            };
            let mut positions = vec![
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Long,
                    quantity: state.signed_quantity,
                    entry_price: (!state.signed_quantity.is_zero()).then_some(Decimal::ONE),
                    mark_price: (!state.signed_quantity.is_zero()).then_some(Decimal::ONE),
                },
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Short,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: None,
                },
            ];
            if state.missing_position_leg {
                let _missing = positions.pop();
            }
            SignedAccountSnapshot::complete(
                self.binding.clone(),
                observed_at_ms,
                1,
                state.private_generation,
                1,
                SignedAccountPositionMode::Hedge,
                if state.external_order {
                    vec![SignedAccountOrderFact {
                        created_at_ms: None,
                        time_in_force: Some(Default::default()),
                        client_order_id: "external-order".to_owned(),
                        venue_order_id: Some("native-external".to_owned()),
                        symbol: self.binding.symbol.clone(),
                        family: NativeOrderFamily::UmOrder,
                        side: OrderSide::Buy,
                        position_side: PositionSide::Long,
                        quantity: Decimal::ONE,
                        limit_price: Some(Decimal::ONE),
                        reduce_only: false,
                        owner: None,
                        external: true,
                        state: None,
                        filled_quantity: None,
                    }]
                } else if let Some(ExecutionCommand::PlaceLimit(command)) = &state.command {
                    vec![SignedAccountOrderFact {
                        created_at_ms: None,
                        time_in_force: Some(Default::default()),
                        client_order_id: command.client_order_id.as_str().to_owned(),
                        venue_order_id: Some(
                            if state.unresolved_after_dispatch && state.resolve_unknown_on_snapshot
                            {
                                "resolved-order"
                            } else {
                                "order-1"
                            }
                            .to_owned(),
                        ),
                        symbol: command.owner.symbol.clone(),
                        family: NativeOrderFamily::UmOrder,
                        side: command.side,
                        position_side: command.position_side,
                        quantity: command.quantity,
                        limit_price: Some(command.limit_price.value()),
                        reduce_only: command.reduce_only,
                        owner: None,
                        external: true,
                        state: None,
                        filled_quantity: None,
                    }]
                } else {
                    Vec::new()
                },
                positions,
                "fills:0".to_owned(),
                if state.unresolved_after_dispatch {
                    request
                        .unresolved()
                        .iter()
                        .map(|command| SignedUnknownFact {
                            command_id: command.command_id().clone(),
                            result: if state.resolve_unknown_on_snapshot {
                                SignedUnknownResult::Accepted {
                                    venue_order_id: "resolved-order".to_owned(),
                                }
                            } else {
                                SignedUnknownResult::Unknown
                            },
                        })
                        .collect()
                } else {
                    Vec::new()
                },
            )
        }

        fn dispatch(
            &mut self,
            permit: venue_execution::AccountDispatchPermit,
        ) -> AccountGatewayResult {
            let Ok(mut state) = self.state.lock() else {
                return AccountGatewayResult::Unknown;
            };
            state.dispatches = state.dispatches.saturating_add(1);
            state.command = Some(permit.command().clone());
            if state.unresolved_after_dispatch {
                AccountGatewayResult::Unknown
            } else {
                AccountGatewayResult::Accepted {
                    venue_order_id: "order-1".to_owned(),
                }
            }
        }
    }

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)
    }

    fn root(temp: &TempDir) -> PathBuf {
        temp.path().join("okx").join("LIVE").join(ACCOUNT)
    }

    fn gateway(
        binding: GatewayBinding,
        signed_quantity: Decimal,
        external_order: bool,
        unresolved_after_dispatch: bool,
    ) -> (Gateway, Arc<Mutex<GatewayState>>) {
        let state = Arc::new(Mutex::new(GatewayState {
            signed_quantity,
            external_order,
            unresolved_after_dispatch,
            private_generation: 1,
            ..GatewayState::default()
        }));
        (
            Gateway {
                binding,
                state: state.clone(),
            },
            state,
        )
    }

    fn command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("runtime-host-command")?,
            client_order_id: CommandId::new("runtime-host-client")?,
            owner: OrderOwner {
                strategy_instance_id: "copy-instance".to_owned(),
                run_id: "run-1".to_owned(),
                exchange: "okx".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        }))
    }

    #[test]
    fn resident_host_keeps_the_physical_host_private() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let resident = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        assert!(!resident.has_unresolved());
        Ok(())
    }

    #[test]
    fn resident_host_exposes_only_a_durable_command_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut resident = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let command = command()?;
        let command_id = command.command_id().clone();

        assert_eq!(resident.command_snapshot(&command_id), None);
        let _prepared = resident.host.prepare_for_lane(command.clone())?;
        assert_eq!(resident.command_snapshot(&command_id), Some(command));
        Ok(())
    }

    #[test]
    fn signed_open_order_owner_comes_only_from_the_exact_wal_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut resident = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let command = command()?;
        let command_id = command.command_id().clone();
        let prepared = resident.host.prepare_for_lane(command.clone())?;
        assert!(matches!(
            resident.host.dispatch_prepared(prepared)?,
            AccountDispatchOutcome::Accepted { .. }
        ));

        let snapshot = resident.refresh_signed_snapshot()?;
        let order = snapshot.open_orders().first().ok_or("missing order")?;
        assert!(!order.external);
        assert_eq!(order.owner, command.owner().cloned());
        assert_eq!(
            resident.command_snapshot_by_venue_order_id(NativeOrderFamily::UmOrder, "order-1"),
            Some(command.clone())
        );
        assert_eq!(
            resident.command_snapshot_by_venue_order_id(NativeOrderFamily::UmAlgo, "order-1"),
            None
        );
        assert_eq!(resident.command_snapshot(&command_id), Some(command));
        Ok(())
    }

    #[test]
    fn host_fsyncs_signed_account_bootstrap_before_runtime_becomes_ready()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        assert_eq!(runtime.health(), crate::account::AccountHealth::Ready);
        assert_eq!(
            runtime.private_router_generation_for_test(),
            runtime.connection_generation()
        );
        assert!(root(&temp).join("signed-account-bootstrap.json").is_file());
        Ok(())
    }

    #[test]
    fn nonflat_signed_account_is_ready_but_keeps_the_account_risk_fence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ONE, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        assert_eq!(runtime.health(), crate::account::AccountHealth::Ready);
        assert!(runtime.production_new_risk_fenced());
        Ok(())
    }

    #[test]
    fn signed_refresh_does_not_resume_an_operator_paused_lifecycle()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ONE, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding.clone(), gateway)?;
        let account = AccountKey::new(ExchangeId::Okx, ACCOUNT)?;
        let mut runtime = AccountRuntime::new(account.clone());
        let strategy = StrategyBinding::new(
            StrategyInstanceKey::new(
                account,
                StrategyKind::Copy,
                "copy-refresh",
                binding.symbol.clone(),
            )?,
            "run-refresh",
            "config-refresh",
        )?;
        runtime.register_strategy(strategy.clone())?;
        host.bootstrap_runtime(&mut runtime)?;
        runtime.request_pause(&strategy.key)?;
        assert!(runtime.production_new_risk_fenced());

        let mut gateway_state = state.lock().map_err(|_| "lock")?;
        gateway_state.signed_quantity = Decimal::ZERO;
        gateway_state.private_generation = 2;
        drop(gateway_state);
        host.refresh_runtime_signed_snapshot(&mut runtime)?;

        assert!(!runtime.production_new_risk_fenced());
        assert_eq!(
            runtime.strategy_lifecycle(&strategy),
            Some(crate::account::InstanceLifecycle::Paused)
        );
        Ok(())
    }

    #[test]
    fn signed_refresh_accepts_a_strictly_newer_private_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        state.lock().map_err(|_| "lock")?.private_generation = 2;

        let snapshot = host.refresh_runtime_signed_snapshot(&mut runtime)?;

        assert_eq!(snapshot.private_generation(), 2);
        assert_eq!(runtime.last_reconciliation_generation, 2);
        Ok(())
    }

    #[test]
    fn same_gateway_session_cannot_relabel_a_repeated_attempt_as_new_signed_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;

        assert!(host.refresh_runtime_signed_snapshot(&mut runtime).is_err());
        assert_eq!(runtime.last_reconciliation_generation, 1);
        state.lock().map_err(|_| "lock")?.private_generation = 2;
        host.refresh_runtime_signed_snapshot(&mut runtime)?;
        assert_eq!(runtime.last_reconciliation_generation, 2);
        Ok(())
    }

    #[test]
    fn cold_restart_ratchets_a_reset_gateway_attempt_above_the_durable_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        {
            let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
            let mut host = AccountRuntimeHost::open(root(&temp), binding.clone(), gateway)?;
            let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
            host.bootstrap_runtime(&mut runtime)?;
            state.lock().map_err(|_| "lock")?.private_generation = 2;
            host.refresh_runtime_signed_snapshot(&mut runtime)?;
            assert_eq!(runtime.last_reconciliation_generation, 2);
        }

        // This is a new adapter/gateway object, not the old Arc-backed test state. Its local
        // attempt counter starts again at one just as a process restart does.
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        assert_eq!(state.lock().map_err(|_| "lock")?.private_generation, 1);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;

        assert_eq!(runtime.last_reconciliation_generation, 3);
        Ok(())
    }

    #[test]
    fn newer_signed_generation_rejects_queued_prepared_before_any_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding.clone(), gateway)?;
        let account = AccountKey::new(ExchangeId::Okx, ACCOUNT)?;
        let mut runtime = AccountRuntime::new(account.clone());
        host.bootstrap_runtime(&mut runtime)?;
        let command = command()?;
        let command_id = command.command_id().clone();
        let prepared = host.host.prepare_for_lane(command)?;
        host.prepared.insert(
            command_id.clone(),
            PreparedLaneAdmission {
                commitment: PreparedAdmissionCommitment {
                    target: StrategyInstanceKey::new(
                        account,
                        StrategyKind::Copy,
                        "queued-generation-change",
                        binding.symbol.clone(),
                    )?,
                    connection_generation: 1,
                    private_generation: 1,
                    config_digest: "queued-generation-change".to_owned(),
                    config_epoch: 1,
                    turn_sequence: 1,
                    priority: AccountLanePriority::Normal,
                    command_sha256: execution_command_sha256(prepared.command())?,
                    allocation_sequence: prepared.receipt_sequence(),
                    allocation_record_sha256: prepared.receipt_digest(),
                },
                proof: prepared,
            },
        );
        let mut gateway_state = state.lock().map_err(|_| "lock")?;
        gateway_state.private_generation = 2;
        // A complete signed refresh must account for the undispatched Prepared record rather
        // than pretending it vanished. The Host later terminates that precise proof.
        gateway_state.unresolved_after_dispatch = true;
        drop(gateway_state);

        host.refresh_runtime_signed_snapshot(&mut runtime)?;

        assert!(!host.has_prepared(&command_id));
        assert!(matches!(
            host.command_status(&command_id)?.ok_or("status missing")?.state(),
            venue_execution::CommandState::Rejected { reason }
                if reason == "private_generation_advanced_before_dispatch"
        ));
        assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 0);
        Ok(())
    }

    #[test]
    fn signed_refresh_requires_fresh_complete_position_legs()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;

        state.lock().map_err(|_| "lock")?.stale_snapshot = true;
        assert!(host.refresh_runtime_signed_snapshot(&mut runtime).is_err());
        state.lock().map_err(|_| "lock")?.stale_snapshot = false;
        state.lock().map_err(|_| "lock")?.missing_position_leg = true;
        assert!(host.refresh_runtime_signed_snapshot(&mut runtime).is_err());
        assert!(!runtime.production_new_risk_fenced());
        Ok(())
    }

    #[test]
    fn signed_refresh_settles_only_exact_host_unknowns_without_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, true);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let prepared = host.host.prepare_for_lane(command()?)?;
        assert!(matches!(
            host.host.dispatch_prepared(prepared)?,
            AccountDispatchOutcome::Unknown
        ));
        assert!(host.has_unresolved());
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        assert!(runtime.production_new_risk_fenced());

        let mut gateway_state = state.lock().map_err(|_| "lock")?;
        gateway_state.resolve_unknown_on_snapshot = true;
        gateway_state.private_generation = 2;
        drop(gateway_state);
        host.refresh_runtime_signed_snapshot(&mut runtime)?;

        assert!(!host.has_unresolved());
        // The exact WAL-owned order remains in the broad account fence. Only a separately
        // confirmed target-specific managed Grid surface can consume that fence for one batch.
        assert!(runtime.production_new_risk_fenced());
        assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
        Ok(())
    }

    #[test]
    fn unknown_to_accepted_syncs_wal_and_routes_fill_without_redispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, state) = gateway(binding.clone(), Decimal::ZERO, false, true);
        let mut host = AccountRuntimeHost::open(root(&temp), binding.clone(), gateway)?;
        let command = command()?;
        let command_id = command.command_id().clone();
        let owner = command.mutation_owner().clone();
        let prepared = host.host.prepare_for_lane(command)?;
        assert!(matches!(
            host.host.dispatch_prepared(prepared)?,
            AccountDispatchOutcome::Unknown
        ));

        let account = AccountKey::new(ExchangeId::Okx, ACCOUNT)?;
        let strategy = StrategyBinding::new(
            StrategyInstanceKey::new(
                account.clone(),
                StrategyKind::Copy,
                owner.strategy_instance_id.clone(),
                owner.symbol.clone(),
            )?,
            owner.run_id.clone(),
            "unknown-route-config",
        )?;
        let mut runtime = AccountRuntime::new(account);
        runtime.register_strategy(strategy.clone())?;
        host.bootstrap_runtime(&mut runtime)?;
        runtime.attach_private_ingress(temp.path().join("unknown-route-private-facts.jsonl"))?;
        let wal_before = runtime.actor_applied_wal_head.ok_or("missing WAL head")?;

        let mut gateway_state = state.lock().map_err(|_| "lock")?;
        gateway_state.resolve_unknown_on_snapshot = true;
        gateway_state.private_generation = 2;
        drop(gateway_state);
        let status = host
            .reconcile_runtime_command_status(&mut runtime, &command_id)?
            .ok_or("missing reconciled status")?;
        assert!(matches!(
            status.state(),
            venue_execution::CommandState::Accepted { venue_order_id }
                if venue_order_id == "resolved-order"
        ));

        let wal_after = runtime.actor_applied_wal_head.ok_or("missing WAL head")?;
        assert!(wal_after.tail_sequence() > wal_before.tail_sequence());
        assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
        let connection_generation = runtime.connection_generation();
        let report = runtime.ingest_private(AccountPrivateFactInput::new(
            EventId::new("unknown-accepted-fill")?,
            connection_generation,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
                .try_into()?,
            Some(NativeOrderFamily::UmOrder),
            DomainEvent::Fill(Fill {
                fill_id: "unknown-accepted-fill".to_owned(),
                execution_sequence: FieldState::Known(1),
                order_id: "resolved-order".to_owned(),
                symbol: owner.symbol,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: Decimal::ONE,
                price: Price::new(Decimal::ONE)?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: None,
            }),
        )?)?;
        assert_eq!(report.deliveries.len(), 1);
        assert_eq!(report.deliveries[0].target, strategy.key);
        assert!(report.reconcile.is_none());
        Ok(())
    }

    #[test]
    fn external_signed_order_keeps_account_readable_but_fences_new_risk()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, true, false);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        assert_eq!(runtime.health(), crate::account::AccountHealth::Ready);
        assert!(runtime.production_new_risk_fenced());
        Ok(())
    }

    #[test]
    fn signed_bootstrap_with_no_unresolved_does_not_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let binding = binding()?;
        let (gateway, _state) = gateway(binding.clone(), Decimal::ZERO, false, true);
        let mut host = AccountRuntimeHost::open(root(&temp), binding, gateway)?;
        let mut runtime = AccountRuntime::new(AccountKey::new(ExchangeId::Okx, ACCOUNT)?);
        host.bootstrap_runtime(&mut runtime)?;
        assert_eq!(runtime.health(), crate::account::AccountHealth::Ready);
        assert!(!host.has_unresolved());
        Ok(())
    }
}
