use std::collections::{BTreeMap, BTreeSet};

use crate::{
    domain::{AccountOrderCapabilityEvidence, AppliedStrategyTurnReceipt, StrategyTurnToken},
    execution::{
        AccountDispatchDecision, AccountExecutionIntent, AccountExecutionLane,
        AccountExecutionRequest, AccountLaneError, AccountLaneFollowUp, ExposureEffect,
        PersistedMutationOutcomeReceipt, PersistedWalPreparedReceipt, PersistedWriterLeaseReceipt,
        PreWalCandidate, UnknownReadbackProof, WalNotPreparedReceipt,
    },
    runtime::{
        account::{
            AccountFault, AccountHealth, AccountKey, AccountPositionMode, AccountReconcilerError,
            AccountReconciliationReport, AccountRecoverySnapshot, DesiredOrderSets, FlattenPlan,
            InstanceLifecycle, MarketHub, MarketHubError, PersistedOrderRouteAppendReceipt,
            PhysicalRecoveryAuthorityRoots, PhysicalRecoveryReadbackManifest, PrivateRouteReport,
            PrivateRouter, PrivateRouterError, ReconcileScope, RecoveredShutdownMode,
            RegistryError, SignedOpenOrders, SignedStopProof, StopPlan, StrategyBinding,
            StrategyInstanceKey, StrategyRegistry, reconcile_open_orders,
        },
        strategy::{
            AccountMarketEvent, PersistedPrivateFact, StrategyActorHost, StrategyControl,
            StrategyHostError, StrategyInput, StrategyTurn,
        },
    },
};

#[cfg(test)]
use crate::runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryScope,
};
use venue_gateway_api::GatewayMode;
#[cfg(test)]
use venue_gateway_api::{GatewayBinding, VenueId};

#[derive(Debug)]
pub struct AccountRuntime {
    account: AccountKey,
    capability_evidence: AccountOrderCapabilityEvidence,
    health: AccountHealth,
    fault: Option<AccountFault>,
    connection_generation: u64,
    last_applied_private_sequence: u64,
    last_reconciliation_generation: u64,
    registry: StrategyRegistry,
    market_hub: MarketHub,
    private_router: PrivateRouter,
    actors: BTreeMap<StrategyInstanceKey, StrategyActorHost>,
    execution_lane: AccountExecutionLane,
    last_instance_orders: BTreeMap<StrategyInstanceKey, (u64, usize)>,
    last_instance_flat: BTreeMap<StrategyInstanceKey, (u64, bool)>,
    stop_fences: BTreeMap<StrategyInstanceKey, (u64, u64)>,
    shutdown_modes: BTreeMap<StrategyInstanceKey, ShutdownMode>,
    turn_sequences: BTreeMap<StrategyInstanceKey, u64>,
    active_turns: BTreeMap<StrategyInstanceKey, ActiveStrategyTurn>,
    last_applied_turns: BTreeMap<StrategyInstanceKey, StrategyTurnToken>,
    strategy_state_revision: u64,
    private_route_revision: u64,
    dispatch_revision: u64,
    pending_private_applications: BTreeMap<u64, PendingPrivateApplication>,
    completed_private_sequences: BTreeSet<u64>,
    private_batch_fence_active: bool,
    owner_index_root: Option<[u8; 32]>,
    owner_index_tail_sequence: u64,
    owner_index_record_count: u64,
    durable_recovery_complete: bool,
    recovered_gateway_mode: Option<GatewayMode>,
    recovered_position_mode: Option<AccountPositionMode>,
    physical_authority_roots: Option<PhysicalRecoveryAuthorityRoots>,
    physical_private_generation_floor: u64,
    pending_physical_recovery: Option<PhysicalRecoveryReadbackManifest>,
    admitted_physical_recovery: Option<PhysicalRecoveryReadbackManifest>,
    physical_recovery_drifted: bool,
    #[cfg(test)]
    physical_recovery_test_fixture_enabled: bool,
    #[cfg(test)]
    physical_profile_version_override: Option<u64>,
}
#[derive(Clone, Debug)]
struct ActiveStrategyTurn {
    token: StrategyTurnToken,
    input: StrategyInput,
}

#[derive(Clone, Debug)]
struct PendingPrivateApplication {
    expected: BTreeSet<(StrategyInstanceKey, u32)>,
}

#[derive(Clone, Debug)]
pub struct PrivateRoutePlan {
    base_revision: u64,
    strategy_state_revision: u64,
    connection_generation: u64,
    evidence_sequence: u64,
    next_router: PrivateRouter,
    report: PrivateRouteReport,
}

impl PrivateRoutePlan {
    #[must_use]
    pub const fn report(&self) -> &PrivateRouteReport {
        &self.report
    }
}

/// Opaque acknowledgement that every delivery in a route plan was appended to the durable actor
/// inbox transaction. Only then may AccountRuntime commit router cursor and in-memory mailboxes.
#[derive(Clone, Debug)]
pub struct PersistedPrivateDispatchReceipt {
    plan: PrivateRoutePlan,
}

impl PersistedPrivateDispatchReceipt {
    pub(super) fn persisted(plan: PrivateRoutePlan) -> Self {
        Self { plan }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownMode {
    Stop,
    Flatten,
}

impl AccountRuntime {
    #[must_use]
    pub fn new(account: AccountKey) -> Self {
        let capability_evidence = AccountOrderCapabilityEvidence::for_account(account.clone());
        Self {
            registry: StrategyRegistry::new(account.clone()),
            market_hub: MarketHub::new(),
            private_router: PrivateRouter::new(account.clone()),
            actors: BTreeMap::new(),
            execution_lane: AccountExecutionLane::new(account.clone()),
            last_instance_orders: BTreeMap::new(),
            last_instance_flat: BTreeMap::new(),
            stop_fences: BTreeMap::new(),
            shutdown_modes: BTreeMap::new(),
            turn_sequences: BTreeMap::new(),
            active_turns: BTreeMap::new(),
            last_applied_turns: BTreeMap::new(),
            strategy_state_revision: 0,
            private_route_revision: 0,
            dispatch_revision: 1,
            pending_private_applications: BTreeMap::new(),
            completed_private_sequences: BTreeSet::new(),
            private_batch_fence_active: false,
            owner_index_root: None,
            owner_index_tail_sequence: 0,
            owner_index_record_count: 0,
            durable_recovery_complete: false,
            recovered_gateway_mode: None,
            recovered_position_mode: None,
            physical_authority_roots: None,
            physical_private_generation_floor: 0,
            pending_physical_recovery: None,
            admitted_physical_recovery: None,
            physical_recovery_drifted: false,
            #[cfg(test)]
            physical_recovery_test_fixture_enabled: false,
            #[cfg(test)]
            physical_profile_version_override: None,
            capability_evidence,
            account,
            health: AccountHealth::Starting,
            fault: None,
            connection_generation: 0,
            last_applied_private_sequence: 0,
            last_reconciliation_generation: 0,
        }
    }

    #[must_use]
    pub const fn account(&self) -> &AccountKey {
        &self.account
    }

    #[must_use]
    pub(crate) const fn capability_evidence(&self) -> &AccountOrderCapabilityEvidence {
        &self.capability_evidence
    }

    fn ensure_supported_order_family(
        &self,
        family: crate::domain::NativeOrderFamily,
    ) -> Result<(), AccountRuntimeError> {
        self.capability_evidence
            .supports(family)
            .then_some(())
            .ok_or(AccountRuntimeError::UnsupportedOrderFamily)
    }

    fn physical_family_support(&self) -> BTreeMap<crate::domain::NativeOrderFamily, bool> {
        [
            crate::domain::NativeOrderFamily::UmOrder,
            crate::domain::NativeOrderFamily::UmConditional,
            crate::domain::NativeOrderFamily::UmAlgo,
        ]
        .into_iter()
        .map(|family| (family, self.capability_evidence.supports(family)))
        .collect()
    }

    fn physical_profile_version(&self) -> u64 {
        #[cfg(test)]
        if let Some(version) = self.physical_profile_version_override {
            return version;
        }
        1_u64
    }

    #[must_use]
    pub const fn health(&self) -> AccountHealth {
        self.health
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    /// Returns the exact recovered Owner, WAL, and Unknown projection roots that a physical
    /// readback scope must bind. These digests are evidence anchors only and grant no I/O or
    /// mutation authority.
    #[must_use]
    pub const fn physical_recovery_authority_roots(
        &self,
    ) -> Option<&PhysicalRecoveryAuthorityRoots> {
        self.physical_authority_roots.as_ref()
    }

    #[must_use]
    pub const fn physical_recovery_private_generation_floor(&self) -> u64 {
        self.physical_private_generation_floor
    }

    /// Attempts to install one complete in-memory physical readback for the next connection.
    /// Production rejects every caller-built manifest until an opaque fresh collector and runtime
    /// authority-root refresh path are integrated. The value type remains a future contract only.
    pub fn install_physical_recovery_manifest(
        &mut self,
        manifest: PhysicalRecoveryReadbackManifest,
    ) -> Result<(), AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        if !self.physical_recovery_integration_available() {
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
        }
        if self.pending_physical_recovery.is_some() {
            return Err(AccountRuntimeError::PhysicalRecoveryAlreadyInstalled);
        }
        self.validate_physical_recovery_manifest(&manifest)?;
        self.pending_physical_recovery = Some(manifest);
        Ok(())
    }

    fn validate_physical_recovery_manifest(
        &self,
        manifest: &PhysicalRecoveryReadbackManifest,
    ) -> Result<(), AccountRuntimeError> {
        let expected_connection_generation = self
            .connection_generation
            .checked_add(1)
            .ok_or(AccountRuntimeError::ConnectionGenerationExhausted)?;
        let scope = manifest.scope();
        let roots_match = self
            .physical_authority_roots
            .as_ref()
            .is_some_and(|expected| expected == scope.authority_roots());
        let authority_matches = self
            .recovered_gateway_mode
            .zip(self.recovered_position_mode)
            .is_some_and(|(mode, position_mode)| {
                scope.matches_account_authority(
                    &self.account,
                    mode,
                    self.registry.registrations().map(|registration| {
                        (
                            registration.binding.key.symbol.clone(),
                            registration.binding.config_digest.clone(),
                            registration.config_epoch,
                        )
                    }),
                    position_mode,
                    &self.physical_family_support(),
                    self.physical_profile_version(),
                )
            });
        if !roots_match
            || !authority_matches
            || scope.connection_generation() != expected_connection_generation
            || scope.recovered_private_generation() != self.physical_private_generation_floor
            || manifest.private_generation() <= self.physical_private_generation_floor
        {
            return Err(AccountRuntimeError::PhysicalRecoveryScopeMismatch);
        }
        Ok(())
    }

    fn physical_recovery_integration_available(&self) -> bool {
        #[cfg(test)]
        {
            self.physical_recovery_test_fixture_enabled
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn physical_turn_authorized(&self) -> bool {
        if !self.physical_recovery_integration_available() {
            return false;
        }
        let current_admitted = self
            .admitted_physical_recovery
            .as_ref()
            .is_some_and(|manifest| {
                manifest.scope().connection_generation() == self.connection_generation
            });
        let recovery_staged = self
            .pending_physical_recovery
            .as_ref()
            .is_some_and(|manifest| {
                self.connection_generation.checked_add(1)
                    == Some(manifest.scope().connection_generation())
            });
        current_admitted || recovery_staged
    }

    fn admitted_physical_authority_current(&self) -> bool {
        self.admitted_physical_recovery
            .as_ref()
            .is_some_and(|manifest| {
                let scope = manifest.scope();
                scope.connection_generation() == self.connection_generation
                    && manifest.private_generation() == self.physical_private_generation_floor
                    && self.physical_authority_roots.as_ref() == Some(scope.authority_roots())
                    && self
                        .recovered_gateway_mode
                        .zip(self.recovered_position_mode)
                        .is_some_and(|(mode, position_mode)| {
                            scope.matches_account_authority(
                                &self.account,
                                mode,
                                self.registry.registrations().map(|registration| {
                                    (
                                        registration.binding.key.symbol.clone(),
                                        registration.binding.config_digest.clone(),
                                        registration.config_epoch,
                                    )
                                }),
                                position_mode,
                                &self.physical_family_support(),
                                self.physical_profile_version(),
                            )
                        })
            })
    }

    fn revoke_physical_authority(&mut self) {
        self.pending_physical_recovery = None;
        self.admitted_physical_recovery = None;
        self.active_turns.clear();
        self.last_applied_turns.clear();
        self.invalidate_dispatch_authority_fail_closed();
        self.health = AccountHealth::Starting;
        self.fault = None;
        self.physical_recovery_drifted = true;
    }

    fn reject_drifted_physical_authority(&mut self) -> Result<(), AccountRuntimeError> {
        if self.admitted_physical_recovery.is_some() && !self.admitted_physical_authority_current()
        {
            self.revoke_physical_authority();
            return Err(AccountRuntimeError::PhysicalRecoveryRequired);
        }
        Ok(())
    }

    /// Explicit compatibility hook for account-kernel tests that do not own a physical collector.
    #[cfg(test)]
    pub(crate) fn enable_physical_recovery_test_fixture(&mut self) {
        self.physical_recovery_test_fixture_enabled = true;
    }

    #[cfg(test)]
    pub(crate) fn set_physical_profile_version_for_test(&mut self, version: u64) {
        self.physical_profile_version_override = Some(version);
        if self.admitted_physical_recovery.is_some() {
            self.revoke_physical_authority();
        }
    }

    #[cfg(test)]
    fn stage_physical_recovery_test_fixture(&mut self) -> Result<(), AccountRuntimeError> {
        if !self.physical_recovery_test_fixture_enabled
            || !self.durable_recovery_complete
            || self.pending_physical_recovery.is_some()
        {
            return Ok(());
        }
        let connection_generation = self
            .connection_generation
            .checked_add(1)
            .ok_or(AccountRuntimeError::ConnectionGenerationExhausted)?;
        let private_generation = self
            .physical_private_generation_floor
            .checked_add(1)
            .ok_or(AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        let symbol = self
            .registry
            .registrations()
            .map(|registration| registration.binding.key.symbol.clone())
            .min()
            .unwrap_or(
                "BTC/USDT"
                    .parse()
                    .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?,
            );
        let venue = match self.account.exchange {
            crate::domain::ExchangeId::Binance => VenueId::Binance,
            crate::domain::ExchangeId::Gate => VenueId::Gate,
            crate::domain::ExchangeId::Bitget => VenueId::Bitget,
            crate::domain::ExchangeId::Bybit => VenueId::Bybit,
            crate::domain::ExchangeId::Hyperliquid => VenueId::Hyperliquid,
            crate::domain::ExchangeId::Okx => VenueId::Okx,
        };
        let roots = self
            .physical_authority_roots
            .clone()
            .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?;
        let scope = PhysicalRecoveryScope::verified_account(
            GatewayBinding::new(
                venue,
                self.recovered_gateway_mode
                    .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?,
                self.account.account.clone(),
                symbol,
            )
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?,
            self.account.clone(),
            self.registry.registrations().map(|registration| {
                (
                    registration.binding.key.symbol.clone(),
                    registration.binding.config_digest.clone(),
                    registration.config_epoch,
                )
            }),
            self.recovered_position_mode
                .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?,
            self.physical_family_support(),
            self.physical_profile_version(),
            connection_generation,
            self.physical_private_generation_floor,
            roots,
        )
        .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        let surfaces = [
            PhysicalReadbackSurface::Account,
            PhysicalReadbackSurface::Positions,
            PhysicalReadbackSurface::UmOrder,
            PhysicalReadbackSurface::UmConditional,
            PhysicalReadbackSurface::UmAlgo,
            PhysicalReadbackSurface::FillsCursor,
        ];
        let receipts = surfaces
            .into_iter()
            .enumerate()
            .map(|(index, surface)| {
                let seed = u8::try_from(index).unwrap_or(u8::MAX).saturating_add(1);
                let unsupported = match surface {
                    PhysicalReadbackSurface::UmOrder => !self
                        .capability_evidence
                        .supports(crate::domain::NativeOrderFamily::UmOrder),
                    PhysicalReadbackSurface::UmConditional => !self
                        .capability_evidence
                        .supports(crate::domain::NativeOrderFamily::UmConditional),
                    PhysicalReadbackSurface::UmAlgo => !self
                        .capability_evidence
                        .supports(crate::domain::NativeOrderFamily::UmAlgo),
                    _ => false,
                };
                if unsupported {
                    PhysicalReadbackReceipt::verified_unsupported_order_family_account(
                        &scope,
                        surface,
                        connection_generation,
                        private_generation,
                        [seed; 32],
                    )
                } else {
                    PhysicalReadbackReceipt::verified_complete_account(
                        &scope,
                        surface,
                        connection_generation,
                        private_generation,
                        [seed; 32],
                        0,
                    )
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        self.pending_physical_recovery = Some(
            PhysicalRecoveryReadbackManifest::verified(scope, receipts)
                .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?,
        );
        Ok(())
    }

    #[must_use]
    pub(crate) const fn applied_private_sequence(&self) -> u64 {
        self.last_applied_private_sequence
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn owner_index_boundary_for_test(&self) -> Option<([u8; 32], u64, u64)> {
        match self.owner_index_root {
            Some(root) => Some((
                root,
                self.owner_index_tail_sequence,
                self.owner_index_record_count,
            )),
            None => None,
        }
    }

    #[must_use]
    pub const fn fault_reason(&self) -> Option<AccountFault> {
        self.fault
    }

    #[must_use]
    pub(crate) const fn registry(&self) -> &StrategyRegistry {
        &self.registry
    }

    fn next_strategy_state_revision(&self) -> Result<u64, AccountRuntimeError> {
        self.strategy_state_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::StrategyStateRevisionExhausted)
    }

    fn next_dispatch_revision(&self) -> Result<u64, AccountRuntimeError> {
        self.dispatch_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::DispatchRevisionExhausted)
    }

    fn install_dispatch_revision(&mut self, revision: u64) {
        self.execution_lane.revoke_pre_wal_candidate();
        self.dispatch_revision = revision;
    }

    fn invalidate_dispatch_authority_fail_closed(&mut self) {
        self.execution_lane.revoke_pre_wal_candidate();
        self.dispatch_revision = self.dispatch_revision.checked_add(1).unwrap_or(0);
    }

    fn has_pending_private_delivery(&self, key: &StrategyInstanceKey) -> bool {
        self.pending_private_applications
            .values()
            .any(|application| application.expected.iter().any(|(target, _)| target == key))
    }

    fn discard_queued_risk_increases(
        lane: &mut AccountExecutionLane,
        registry: &StrategyRegistry,
    ) -> Result<(), AccountRuntimeError> {
        for request in lane.discard_all_queued() {
            if request.exposure() == ExposureEffect::Increase {
                continue;
            }
            let binding = registry
                .registration(request.target())
                .ok_or(AccountRuntimeError::ActorMissing)?
                .binding
                .clone();
            lane.enqueue(request, &binding)?;
        }
        Ok(())
    }

    pub fn register_strategy(
        &mut self,
        binding: StrategyBinding,
    ) -> Result<(), AccountRuntimeError> {
        let next_state_revision = self.next_strategy_state_revision()?;
        let mut next_registry = self.registry.clone();
        next_registry.register(binding.clone())?;
        self.registry = next_registry;
        self.actors
            .insert(binding.key.clone(), StrategyActorHost::new(binding));
        self.strategy_state_revision = next_state_revision;
        if self.admitted_physical_recovery.is_some() {
            self.revoke_physical_authority();
        }
        Ok(())
    }

    /// Installs the complete durable ownership and unresolved-WAL recovery result. Runtime
    /// connectivity cannot become Ready until this succeeds, including for an empty new account.
    pub(crate) fn restore_durable_state(
        &mut self,
        snapshot: AccountRecoverySnapshot,
    ) -> Result<(), AccountRuntimeError> {
        if self.durable_recovery_complete || self.health != AccountHealth::Starting {
            return Err(AccountRuntimeError::DurableRecoveryAlreadyInstalled);
        }
        let (
            account,
            gateway_mode,
            position_mode,
            journal_roots,
            _manifest_commitment,
            last_connection_generation,
            applied_private_cursor,
            strategy_states,
            pending_private_batches,
            routes,
            unresolved_mutations,
            physical_authority_roots,
        ) = snapshot.into_parts();
        if account != self.account {
            return Err(AccountRuntimeError::RecoveryAccountMismatch);
        }
        let next_state_revision = self.next_strategy_state_revision()?;
        let recovered_private_sequence = applied_private_cursor.sequence();
        let recovered_private_generation = applied_private_cursor.generation();
        if strategy_states.len() != self.registry.registrations().count()
            || (!pending_private_batches.is_empty() && last_connection_generation == 0)
            || unresolved_mutations.iter().any(|request| {
                request.admission_connection_generation() > last_connection_generation
            })
        {
            return Err(AccountRuntimeError::RecoveryStateMismatch);
        }
        let mut next_router = self.private_router.clone();
        let mut next_lane = self.execution_lane.clone();
        let mut next_registry = self.registry.clone();
        let mut next_actors = self.actors.clone();
        let mut next_stop_fences = BTreeMap::new();
        let mut next_shutdown_modes = BTreeMap::new();
        let mut next_pending_private: BTreeMap<u64, PendingPrivateApplication> = BTreeMap::new();
        let mut next_completed_private = BTreeSet::new();
        let mut recovered_strategy_keys = BTreeSet::new();
        for state in strategy_states {
            let (binding, config_epoch, lifecycle, shutdown) = state.into_parts();
            if !recovered_strategy_keys.insert(binding.key.clone()) {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            }
            next_registry.restore_state(&binding, config_epoch, lifecycle)?;
            next_actors
                .get_mut(&binding.key)
                .ok_or(AccountRuntimeError::ActorMissing)?
                .restore_configuration(binding.clone(), config_epoch)?;
            if let Some(shutdown) = shutdown {
                let (mode, connection_fence, private_fence) = shutdown.parts();
                next_stop_fences.insert(binding.key.clone(), (connection_fence, private_fence));
                next_shutdown_modes.insert(
                    binding.key,
                    match mode {
                        RecoveredShutdownMode::Stop => ShutdownMode::Stop,
                        RecoveredShutdownMode::Flatten => ShutdownMode::Flatten,
                    },
                );
            }
        }
        if next_registry
            .registrations()
            .any(|registration| !recovered_strategy_keys.contains(&registration.binding.key))
        {
            return Err(AccountRuntimeError::RecoveryStateMismatch);
        }
        for route in routes {
            let (family, command_id, client_order_id, venue_order_id, owner) = route.into_parts();
            self.ensure_supported_order_family(family)?;
            next_router.bind_order(
                family,
                client_order_id,
                venue_order_id,
                command_id,
                owner,
                &next_registry,
            )?;
        }
        if last_connection_generation > 0 {
            next_router
                .activate_generation(last_connection_generation, recovered_private_sequence)?;
        }
        for batch in pending_private_batches {
            let (facts, deliveries, applied) = batch.into_parts();
            let Some(first) = facts.first() else {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            };
            let sequence = first.evidence().sequence();
            if sequence <= recovered_private_sequence
                || first.evidence().generation() != last_connection_generation
            {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            }
            let mut completed_report = None;
            for fact in facts {
                let report = next_router.route(fact, &next_registry);
                if report.reconcile.is_some() || report.duplicate {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
                if report.pending_batch {
                    if !report.deliveries.is_empty() {
                        return Err(AccountRuntimeError::RecoveryStateMismatch);
                    }
                } else if completed_report.replace(report).is_some() {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
            }
            let report = completed_report.ok_or(AccountRuntimeError::RecoveryStateMismatch)?;
            let mut routed = BTreeMap::new();
            for delivery in report.deliveries {
                let identity = (delivery.target, delivery.fact.fact_index());
                if routed.insert(identity, delivery.fact).is_some() {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
            }
            let mut recovered = BTreeMap::new();
            for delivery in deliveries {
                let (target, fact) = delivery.into_parts();
                let identity = (target, fact.fact_index());
                if recovered.insert(identity, fact).is_some() {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
            }
            if routed != recovered || !applied.is_subset(&routed.keys().cloned().collect()) {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            }
            let mut expected = BTreeSet::new();
            for ((target, fact_index), fact) in routed {
                if applied.contains(&(target.clone(), fact_index)) {
                    continue;
                }
                next_actors
                    .get_mut(&target)
                    .ok_or(AccountRuntimeError::RecoveryStateMismatch)?
                    .push_private(fact)?;
                if !expected.insert((target, fact_index)) {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
            }
            if expected.is_empty() {
                if !next_completed_private.insert(sequence) {
                    return Err(AccountRuntimeError::RecoveryStateMismatch);
                }
            } else if next_pending_private
                .insert(sequence, PendingPrivateApplication { expected })
                .is_some()
            {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            }
        }
        for request in unresolved_mutations {
            self.ensure_supported_order_family(request.native_order_family())?;
            let is_cancel = matches!(
                request.command(),
                crate::domain::ExecutionCommand::Cancel(_)
            );
            if !next_router.recovered_mutation_has_exact_route(
                request.native_order_family(),
                request.native_client_id().as_str(),
                request.command_id(),
                request.command().mutation_owner(),
                is_cancel,
            ) {
                return Err(AccountRuntimeError::RecoveryStateMismatch);
            }
            let binding = next_registry
                .registration(request.target())
                .ok_or(RegistryError::Missing)?
                .binding
                .clone();
            next_lane.recover_unknown(request, &binding)?;
        }
        self.private_router = next_router;
        self.execution_lane = next_lane;
        self.registry = next_registry;
        self.actors = next_actors;
        self.stop_fences = next_stop_fences;
        self.shutdown_modes = next_shutdown_modes;
        self.pending_private_applications = next_pending_private;
        self.completed_private_sequences = next_completed_private;
        self.connection_generation = last_connection_generation;
        self.last_applied_private_sequence = recovered_private_sequence;
        self.owner_index_root = Some(journal_roots.owner_index());
        self.owner_index_tail_sequence = journal_roots.owner_index_tail_sequence();
        self.owner_index_record_count = journal_roots.owner_index_record_count();
        self.physical_authority_roots = Some(physical_authority_roots);
        self.recovered_gateway_mode = Some(gateway_mode);
        self.recovered_position_mode = Some(position_mode);
        self.physical_private_generation_floor = recovered_private_generation;
        self.pending_physical_recovery = None;
        self.admitted_physical_recovery = None;
        self.physical_recovery_drifted = false;
        self.strategy_state_revision = next_state_revision;
        self.durable_recovery_complete = true;
        self.advance_applied_private_cursor();
        Ok(())
    }

    pub fn mark_account_ready(&mut self) -> Result<Vec<StrategyInstanceKey>, AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        if !self.physical_recovery_integration_available() {
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
        }
        #[cfg(test)]
        self.stage_physical_recovery_test_fixture()?;
        let manifest = self
            .pending_physical_recovery
            .as_ref()
            .cloned()
            .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?;
        self.validate_physical_recovery_manifest(&manifest)?;
        if self.execution_lane.has_in_flight() {
            return Err(AccountRuntimeError::ReconnectWithInFlight);
        }
        if !self.active_turns.is_empty() || !self.pending_private_applications.is_empty() {
            return Err(AccountRuntimeError::ReconnectWithUnappliedActorState);
        }
        let connection_generation = self
            .connection_generation
            .checked_add(1)
            .ok_or(AccountRuntimeError::ConnectionGenerationExhausted)?;
        let mut next_router = self.private_router.clone();
        next_router
            .activate_generation(connection_generation, self.last_applied_private_sequence)?;
        let mut next_actors = self.actors.clone();
        for (key, actor) in &mut next_actors {
            actor.clear_transient_inputs();
            if let Some(mode) = self.shutdown_modes.get(key) {
                actor.push_control(match mode {
                    ShutdownMode::Stop => StrategyControl::Stop,
                    ShutdownMode::Flatten => StrategyControl::Flatten,
                })?;
            }
        }
        let mut next_registry = self.registry.clone();
        let recovering = next_registry.begin_recovery_all();
        let next_route_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        let _discarded_stale_intents = self.execution_lane.discard_all_queued();
        self.active_turns.clear();
        self.last_applied_turns.clear();
        self.private_router = next_router;
        self.actors = next_actors;
        self.registry = next_registry;
        self.pending_private_applications.clear();
        self.completed_private_sequences.clear();
        self.private_route_revision = next_route_revision;
        self.strategy_state_revision = next_state_revision;
        self.install_dispatch_revision(next_dispatch_revision);
        self.private_batch_fence_active = false;
        self.connection_generation = connection_generation;
        self.last_reconciliation_generation = 0;
        self.last_instance_orders.clear();
        self.last_instance_flat.clear();
        self.health = AccountHealth::Ready;
        self.fault = None;
        self.pending_physical_recovery = None;
        self.physical_private_generation_floor = manifest.private_generation();
        self.admitted_physical_recovery = Some(manifest);
        self.physical_recovery_drifted = false;
        Ok(recovering)
    }

    pub fn freeze_account(&mut self, fault: AccountFault) {
        self.invalidate_dispatch_authority_fail_closed();
        let _revoked_risk = self.execution_lane.discard_queued_risk_increases();
        self.health = AccountHealth::Frozen;
        self.fault = Some(fault);
    }

    pub fn publish_market(
        &mut self,
        event: AccountMarketEvent,
    ) -> Result<bool, AccountRuntimeError> {
        let mut next_hub = self.market_hub.clone();
        let publish = next_hub.publish(event)?;
        let Some(binding) = self
            .registry
            .binding_by_symbol(publish.event.symbol())
            .cloned()
        else {
            self.market_hub = next_hub;
            return Ok(false);
        };
        let mut next_actor = self
            .actors
            .get(&binding.key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        if let Err(error) = next_actor.push_market(publish.event) {
            let lifecycle = self
                .registry
                .registration(&binding.key)
                .ok_or(AccountRuntimeError::ActorMissing)?
                .lifecycle;
            if !matches!(
                lifecycle,
                InstanceLifecycle::Stopping
                    | InstanceLifecycle::Faulted
                    | InstanceLifecycle::NeedsAttention
            ) {
                self.registry.needs_attention(&binding.key)?;
            }
            return Err(error.into());
        }
        self.market_hub = next_hub;
        self.actors.insert(binding.key.clone(), next_actor);
        Ok(true)
    }

    pub(crate) fn install_order_route(
        &mut self,
        receipt: PersistedOrderRouteAppendReceipt,
    ) -> Result<(), AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        let (route, previous_root, next_root, append_sequence, record_sha256) =
            receipt.into_parts();
        let expected_append_sequence = self
            .owner_index_tail_sequence
            .checked_add(1)
            .ok_or(AccountRuntimeError::OrderRouteReceipt)?;
        let next_record_count = self
            .owner_index_record_count
            .checked_add(1)
            .ok_or(AccountRuntimeError::OrderRouteReceipt)?;
        if self.owner_index_root != Some(previous_root)
            || append_sequence != expected_append_sequence
            || record_sha256.iter().all(|byte| *byte == 0)
            || next_root.iter().all(|byte| *byte == 0)
        {
            return Err(AccountRuntimeError::OrderRouteReceipt);
        }
        let (family, command_id, client_order_id, venue_order_id, owner) = route.into_parts();
        self.ensure_supported_order_family(family)?;
        let mut next_router = self.private_router.clone();
        next_router.bind_order(
            family,
            client_order_id,
            venue_order_id,
            command_id,
            owner,
            &self.registry,
        )?;
        let next_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        let current_roots = self
            .physical_authority_roots
            .as_ref()
            .ok_or(AccountRuntimeError::PhysicalRecoveryRequired)?;
        let next_physical_roots = current_roots
            .refreshed_owner(next_root)
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        self.private_router = next_router;
        self.private_route_revision = next_revision;
        self.owner_index_root = Some(next_root);
        self.owner_index_tail_sequence = append_sequence;
        self.owner_index_record_count = next_record_count;
        self.physical_authority_roots = Some(next_physical_roots);
        if self.admitted_physical_recovery.is_some() {
            self.revoke_physical_authority();
        }
        Ok(())
    }

    pub(crate) fn plan_private_route(
        &self,
        fact: PersistedPrivateFact,
    ) -> Result<PrivateRoutePlan, AccountRuntimeError> {
        let evidence_sequence = fact.evidence().sequence();
        let mut next_router = self.private_router.clone();
        let report = next_router.route(fact, &self.registry);
        Ok(PrivateRoutePlan {
            base_revision: self.private_route_revision,
            strategy_state_revision: self.strategy_state_revision,
            connection_generation: self.connection_generation,
            evidence_sequence,
            next_router,
            report,
        })
    }

    pub(crate) fn commit_private_route(
        &mut self,
        receipt: PersistedPrivateDispatchReceipt,
    ) -> Result<PrivateRouteReport, AccountRuntimeError> {
        let plan = receipt.plan;
        if plan.base_revision != self.private_route_revision
            || plan.strategy_state_revision != self.strategy_state_revision
            || plan.connection_generation != self.connection_generation
        {
            return Err(AccountRuntimeError::StalePrivateRoutePlan);
        }
        let report = plan.report;
        let mut next_registry = self.registry.clone();
        let mut next_actors = self.actors.clone();
        let mut next_health = self.health;
        let mut next_fault = self.fault;
        let mut next_batch_fence_active = self.private_batch_fence_active;
        let mut registry_state_changed = false;
        if report.pending_batch {
            next_batch_fence_active = true;
            if next_health == AccountHealth::Ready && next_fault.is_none() {
                next_health = AccountHealth::Frozen;
                next_fault = Some(AccountFault::PrivateEvidenceBatchIncomplete);
            }
        } else if next_batch_fence_active {
            next_batch_fence_active = false;
            if next_fault == Some(AccountFault::PrivateEvidenceBatchIncomplete) {
                next_health = AccountHealth::Ready;
                next_fault = None;
            }
        }
        if let Some(reconcile) = &report.reconcile {
            let account_wide = matches!(reconcile.scope, ReconcileScope::Account)
                || matches!(
                    reconcile.reason,
                    crate::runtime::account::ReconcileReason::IdentityConflict
                        | crate::runtime::account::ReconcileReason::OwnerNoLongerRegistered
                        | crate::runtime::account::ReconcileReason::SymbolMismatch
                );
            if account_wide {
                let fault = match reconcile.reason {
                    crate::runtime::account::ReconcileReason::PrivateGenerationMismatch => {
                        AccountFault::PrivateGenerationMismatch
                    }
                    crate::runtime::account::ReconcileReason::PrivateEvidenceGap => {
                        AccountFault::PrivateEvidenceGap
                    }
                    _ => AccountFault::ReconciliationFailed,
                };
                next_health = AccountHealth::Frozen;
                next_fault = Some(fault);
            } else {
                match &reconcile.scope {
                    ReconcileScope::Account => {
                        next_health = AccountHealth::Frozen;
                        next_fault = Some(AccountFault::ReconciliationFailed);
                    }
                    ReconcileScope::Symbol(symbol) => {
                        if let Some(binding) = next_registry.binding_by_symbol(symbol).cloned() {
                            let lifecycle = next_registry
                                .registration(&binding.key)
                                .ok_or(AccountRuntimeError::ActorMissing)?
                                .lifecycle;
                            if !matches!(
                                lifecycle,
                                InstanceLifecycle::Stopping
                                    | InstanceLifecycle::Faulted
                                    | InstanceLifecycle::NeedsAttention
                            ) {
                                next_registry.needs_attention(&binding.key)?;
                                registry_state_changed = true;
                            }
                        }
                    }
                }
            }
        }
        for delivery in &report.deliveries {
            let actor = next_actors
                .get_mut(&delivery.target)
                .ok_or(AccountRuntimeError::ActorMissing)?;
            if let Err(error) = actor.push_private(delivery.fact.clone()) {
                self.freeze_account(AccountFault::ReconciliationFailed);
                let _discarded = self.execution_lane.discard_all_queued();
                return Err(error.into());
            }
        }
        let mut next_pending = self.pending_private_applications.clone();
        let mut next_completed = self.completed_private_sequences.clone();
        if report.reconcile.is_none() && !report.duplicate && !report.pending_batch {
            let expected: BTreeSet<_> = report
                .deliveries
                .iter()
                .map(|delivery| (delivery.target.clone(), delivery.fact.fact_index()))
                .collect();
            if expected.len() != report.deliveries.len()
                || next_pending.contains_key(&plan.evidence_sequence)
                || next_completed.contains(&plan.evidence_sequence)
            {
                return Err(AccountRuntimeError::PrivateApplicationState);
            }
            if expected.is_empty() {
                next_completed.insert(plan.evidence_sequence);
            } else {
                next_pending.insert(
                    plan.evidence_sequence,
                    PendingPrivateApplication { expected },
                );
            }
        }
        let pending_targets: BTreeSet<_> = next_pending
            .values()
            .flat_map(|application| {
                application
                    .expected
                    .iter()
                    .map(|(target, _)| target.clone())
            })
            .collect();
        let mut next_applied_turns = self.last_applied_turns.clone();
        for target in &pending_targets {
            next_applied_turns.remove(target);
        }
        let mut next_lane = self.execution_lane.clone();
        if !next_pending.is_empty() {
            Self::discard_queued_risk_increases(&mut next_lane, &next_registry)?;
        }
        let next_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        let next_state_revision = if registry_state_changed {
            Some(self.next_strategy_state_revision()?)
        } else {
            None
        };
        let next_dispatch_revision = (!report.duplicate)
            .then(|| self.next_dispatch_revision())
            .transpose()?;
        self.private_router = plan.next_router;
        self.registry = next_registry;
        self.actors = next_actors;
        self.execution_lane = next_lane;
        self.last_applied_turns = next_applied_turns;
        self.health = next_health;
        self.fault = next_fault;
        self.private_batch_fence_active = next_batch_fence_active;
        self.pending_private_applications = next_pending;
        self.completed_private_sequences = next_completed;
        self.private_route_revision = next_revision;
        if let Some(revision) = next_state_revision {
            self.strategy_state_revision = revision;
        }
        if let Some(revision) = next_dispatch_revision {
            self.install_dispatch_revision(revision);
        }
        self.advance_applied_private_cursor();
        if self.health != AccountHealth::Ready {
            let _discarded = self.execution_lane.discard_all_queued();
        }
        Ok(report)
    }

    fn advance_applied_private_cursor(&mut self) {
        while self
            .last_applied_private_sequence
            .checked_add(1)
            .is_some_and(|next| self.completed_private_sequences.remove(&next))
        {
            self.last_applied_private_sequence =
                self.last_applied_private_sequence.saturating_add(1);
        }
    }

    pub fn reconcile(
        &mut self,
        desired: &DesiredOrderSets,
        signed: SignedOpenOrders,
    ) -> Result<AccountReconciliationReport, AccountRuntimeError> {
        if !self.active_turns.is_empty() {
            return Err(AccountRuntimeError::StrategyTurnActive);
        }
        if self.execution_lane.has_in_flight() || !self.pending_private_applications.is_empty() {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        if self.health != AccountHealth::Ready
            || signed.connection_generation() != self.connection_generation
        {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        if signed.private_generation() <= self.last_reconciliation_generation {
            return Err(AccountRuntimeError::StaleReconciliation);
        }
        let desired_authority = desired.verify_runtime_authority(
            &self.registry,
            &self.last_applied_turns,
            signed.connection_generation(),
            signed.private_generation(),
            self.last_reconciliation_generation == 0,
        );
        let report = match desired_authority.and_then(|()| {
            reconcile_open_orders(
                &self.private_router,
                &self.registry,
                &self.capability_evidence,
                desired,
                signed,
            )
        }) {
            Ok(report) => report,
            Err(error) => {
                self.freeze_account(AccountFault::ReconciliationFailed);
                let _discarded = self.execution_lane.discard_all_queued();
                self.active_turns.clear();
                self.last_applied_turns.clear();
                return Err(error.into());
            }
        };
        let mut next_registry = self.registry.clone();
        let mut next_actors = self.actors.clone();
        let mut next_orders = self.last_instance_orders.clone();
        let mut next_flat = self.last_instance_flat.clone();
        let mut next_health = self.health;
        let mut next_fault = self.fault;
        for unresolved in &report.unresolved {
            if matches!(
                unresolved.reason,
                crate::runtime::account::ReconcileReason::IdentityConflict
                    | crate::runtime::account::ReconcileReason::OwnerNoLongerRegistered
                    | crate::runtime::account::ReconcileReason::SymbolMismatch
            ) {
                next_health = AccountHealth::Frozen;
                next_fault = Some(AccountFault::ReconciliationFailed);
            } else if let Some(binding) =
                self.registry.binding_by_symbol(&unresolved.symbol).cloned()
            {
                let lifecycle = next_registry
                    .registration(&binding.key)
                    .ok_or(AccountRuntimeError::ActorMissing)?
                    .lifecycle;
                if !matches!(
                    lifecycle,
                    InstanceLifecycle::Stopping
                        | InstanceLifecycle::Faulted
                        | InstanceLifecycle::NeedsAttention
                ) {
                    next_registry.needs_attention(&binding.key)?;
                }
            } else {
                next_health = AccountHealth::Frozen;
                next_fault = Some(AccountFault::ReconciliationFailed);
            }
        }
        let clean_owner_assignment = report.unresolved.is_empty();
        if !clean_owner_assignment {
            next_orders.clear();
            next_flat.clear();
        }
        for instance in &report.instances {
            let registration = next_registry
                .registration(&instance.target)
                .ok_or(AccountRuntimeError::ActorMissing)?;
            if registration.binding.config_digest != instance.config_digest
                || registration.config_epoch != instance.config_epoch
            {
                return Err(AccountRuntimeError::StaleConfiguration);
            }
            if clean_owner_assignment {
                next_orders.insert(
                    instance.target.clone(),
                    (
                        instance.notice.private_generation,
                        instance.notice.actual_open_orders,
                    ),
                );
                let flat = report
                    .flat_by_symbol
                    .get(&instance.target.symbol)
                    .copied()
                    .unwrap_or(false);
                next_flat.insert(
                    instance.target.clone(),
                    (instance.notice.private_generation, flat),
                );
            }
            let lifecycle = next_registry
                .registration(&instance.target)
                .ok_or(AccountRuntimeError::ActorMissing)?
                .lifecycle;
            next_actors
                .get_mut(&instance.target)
                .ok_or(AccountRuntimeError::ActorMissing)?
                .push_reconciliation(instance.notice.clone())?;
            if !(clean_owner_assignment
                && next_health == AccountHealth::Ready
                && instance.notice.exact())
                && !matches!(
                    lifecycle,
                    InstanceLifecycle::Paused
                        | InstanceLifecycle::Stopping
                        | InstanceLifecycle::Faulted
                        | InstanceLifecycle::NeedsAttention
                )
            {
                next_registry.mark_recovering(&instance.target)?;
            }
        }
        let mut next_applied_turns = self.last_applied_turns.clone();
        for instance in &report.instances {
            next_applied_turns.remove(&instance.target);
        }
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors = next_actors;
        self.last_instance_orders = next_orders;
        self.last_instance_flat = next_flat;
        self.last_applied_turns = next_applied_turns;
        self.health = next_health;
        self.fault = next_fault;
        self.last_reconciliation_generation = report.private_generation;
        self.strategy_state_revision = next_state_revision;
        let _discarded_stale_intents = self.execution_lane.discard_all_queued();
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(report)
    }

    pub fn request_pause(&mut self, key: &StrategyInstanceKey) -> Result<(), AccountRuntimeError> {
        let mut next_registry = self.registry.clone();
        next_registry.pause(key)?;
        let mut next_actor = self
            .actors
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        next_actor.push_control(StrategyControl::Pause)?;
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors.insert(key.clone(), next_actor);
        self.strategy_state_revision = next_state_revision;
        let _revoked = self
            .execution_lane
            .discard_queued_instance_risk_increases(key);
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(())
    }

    pub fn request_resume(&mut self, key: &StrategyInstanceKey) -> Result<(), AccountRuntimeError> {
        let mut next_registry = self.registry.clone();
        next_registry.resume(key)?;
        let mut next_actor = self
            .actors
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        next_actor.push_control(StrategyControl::Resume)?;
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors.insert(key.clone(), next_actor);
        self.strategy_state_revision = next_state_revision;
        let _revoked = self
            .execution_lane
            .discard_queued_instance_risk_increases(key);
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(())
    }

    pub fn change_parameters(
        &mut self,
        key: &StrategyInstanceKey,
        config_digest: String,
    ) -> Result<(), AccountRuntimeError> {
        let current = self
            .registry
            .registration(key)
            .ok_or(RegistryError::Missing)?;
        if current.binding.config_digest == config_digest {
            return Ok(());
        }
        if self.execution_lane.instance_has_dispatched_or_unknown(key)
            || self.active_turns.contains_key(key)
        {
            return Err(AccountRuntimeError::ParameterChangeBusy);
        }
        let mut next_registry = self.registry.clone();
        next_registry.replace_config_digest(key, config_digest)?;
        let next_registration = next_registry
            .registration(key)
            .ok_or(RegistryError::Missing)?
            .clone();
        let mut next_actor = self
            .actors
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        next_actor.install_configuration(
            next_registration.binding.clone(),
            next_registration.config_epoch,
        )?;
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors.insert(key.clone(), next_actor);
        self.strategy_state_revision = next_state_revision;
        let _discarded_old_configuration = self.execution_lane.discard_queued_instance(key);
        self.install_dispatch_revision(next_dispatch_revision);
        if self.admitted_physical_recovery.is_some() {
            self.revoke_physical_authority();
        }
        Ok(())
    }

    pub(crate) fn pop_strategy_input(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<Option<StrategyTurn>, AccountRuntimeError> {
        self.reject_drifted_physical_authority()?;
        if !self.physical_recovery_integration_available() {
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
        }
        if self.physical_recovery_drifted {
            return Err(AccountRuntimeError::PhysicalRecoveryRequired);
        }
        #[cfg(test)]
        if !self.physical_recovery_drifted && !self.physical_turn_authorized() {
            self.stage_physical_recovery_test_fixture()?;
        }
        if !self.physical_turn_authorized() {
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
        }
        if self.active_turns.contains_key(key) {
            return Err(AccountRuntimeError::StrategyTurnActive);
        }
        let Some(input) = self
            .actors
            .get_mut(key)
            .ok_or(AccountRuntimeError::ActorMissing)?
            .pop_next()
        else {
            return Ok(None);
        };
        let registration = self
            .registry
            .registration(key)
            .ok_or(AccountRuntimeError::ActorMissing)?;
        let turn_sequence = self
            .turn_sequences
            .get(key)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let token = StrategyTurnToken::issue(
            key.clone(),
            self.connection_generation,
            self.last_reconciliation_generation,
            registration.binding.config_digest.clone(),
            registration.config_epoch,
            turn_sequence,
        )
        .map_err(|_| AccountRuntimeError::StrategyTurnAuthority)?;
        self.turn_sequences.insert(key.clone(), turn_sequence);
        self.active_turns.insert(
            key.clone(),
            ActiveStrategyTurn {
                token: token.clone(),
                input: input.clone(),
            },
        );
        Ok(Some(StrategyTurn::issued(token, input)))
    }

    /// Applies an actor turn only after its inbox/checkpoint transaction produced an opaque durable
    /// receipt. Exact reconciliation therefore cannot make an instance Running merely by enqueue.
    pub(crate) fn acknowledge_strategy_turn(
        &mut self,
        receipt: AppliedStrategyTurnReceipt,
    ) -> Result<(), AccountRuntimeError> {
        self.reject_drifted_physical_authority()?;
        let token = receipt.token();
        let active = self
            .active_turns
            .get(token.target())
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        let registration = self
            .registry
            .registration(token.target())
            .ok_or(AccountRuntimeError::ActorMissing)?;
        if active.token != *token
            || token.connection_generation() != self.connection_generation
            || token.private_generation() != self.last_reconciliation_generation
            || token.config_digest() != registration.binding.config_digest
            || token.config_epoch() != registration.config_epoch
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        if let StrategyInput::Private(fact) = &active.input {
            let application = self
                .pending_private_applications
                .get(&fact.evidence().sequence())
                .ok_or(AccountRuntimeError::PrivateApplicationState)?;
            if !application
                .expected
                .contains(&(token.target().clone(), fact.fact_index()))
            {
                return Err(AccountRuntimeError::PrivateApplicationState);
            }
        }
        let promotes_running = matches!(
            &active.input,
            StrategyInput::Reconciliation(notice)
                if notice.private_generation == self.last_reconciliation_generation
                    && notice.exact()
                    && self.health == AccountHealth::Ready
                    && matches!(
                        registration.lifecycle,
                        InstanceLifecycle::Registered | InstanceLifecycle::Recovering
                    )
        );
        let next_state_revision = promotes_running
            .then(|| self.next_strategy_state_revision())
            .transpose()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        let active = self
            .active_turns
            .remove(token.target())
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        if let Some(next_state_revision) = next_state_revision {
            self.registry.mark_running(token.target())?;
            self.strategy_state_revision = next_state_revision;
        }
        if let StrategyInput::Private(fact) = &active.input {
            let sequence = fact.evidence().sequence();
            let should_complete = {
                let application = self
                    .pending_private_applications
                    .get_mut(&sequence)
                    .ok_or(AccountRuntimeError::PrivateApplicationState)?;
                if !application
                    .expected
                    .remove(&(token.target().clone(), fact.fact_index()))
                {
                    return Err(AccountRuntimeError::PrivateApplicationState);
                }
                application.expected.is_empty()
            };
            if should_complete {
                self.pending_private_applications.remove(&sequence);
                self.completed_private_sequences.insert(sequence);
                self.advance_applied_private_cursor();
            }
        }
        if !self.has_pending_private_delivery(token.target()) {
            self.last_applied_turns
                .insert(token.target().clone(), token.clone());
        }
        let _revoked = self
            .execution_lane
            .discard_queued_instance_risk_increases(token.target());
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(())
    }

    pub(crate) fn latest_applied_turn_receipt(
        &self,
        key: &StrategyInstanceKey,
    ) -> Option<AppliedStrategyTurnReceipt> {
        self.last_applied_turns
            .get(key)
            .cloned()
            .map(AppliedStrategyTurnReceipt::persisted)
    }

    pub fn enqueue_execution(
        &mut self,
        intent: AccountExecutionIntent,
    ) -> Result<(), AccountRuntimeError> {
        let registration = self
            .registry
            .registration(intent.target())
            .ok_or(AccountRuntimeError::ActorMissing)?;
        if self.connection_generation == 0 || self.last_reconciliation_generation == 0 {
            return Err(AccountRuntimeError::StaleExecutionAuthority);
        }
        self.ensure_supported_order_family(intent.native_order_family())?;
        if intent.config_digest() != registration.binding.config_digest
            || intent.config_epoch() != registration.config_epoch
        {
            return Err(AccountRuntimeError::StaleConfiguration);
        }
        let applied = self
            .last_applied_turns
            .get(intent.target())
            .ok_or(AccountRuntimeError::StrategyTurnAuthority)?;
        if intent.admission_connection_generation() != applied.connection_generation()
            || intent.admission_private_generation() != applied.private_generation()
            || intent.turn_sequence() != applied.turn_sequence()
            || intent.config_digest() != applied.config_digest()
            || intent.config_epoch() != applied.config_epoch()
        {
            return Err(AccountRuntimeError::StrategyTurnAuthority);
        }
        let exposure = intent.exposure();
        let lifecycle_allows_new_risk = registration.lifecycle.accepts_new_risk();
        let binding = registration.binding.clone();
        if exposure == ExposureEffect::Increase
            && (self.health != AccountHealth::Ready
                || !lifecycle_allows_new_risk
                || !self.pending_private_applications.is_empty())
        {
            return Err(AccountRuntimeError::RiskFenced);
        }
        let mut next_router = self.private_router.clone();
        next_router.reserve_execution_identity(&intent, &self.registry)?;
        let request = AccountExecutionRequest::authorize(intent)?;
        let mut next_lane = self.execution_lane.clone();
        next_lane.enqueue(request, &binding)?;
        let next_route_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        self.private_router = next_router;
        self.execution_lane = next_lane;
        self.private_route_revision = next_route_revision;
        Ok(())
    }

    pub(crate) fn next_execution_for_wal(
        &mut self,
    ) -> Result<Option<PreWalCandidate<'_>>, AccountRuntimeError> {
        let health = self.health;
        let connection_generation = self.connection_generation;
        let private_generation = self.last_reconciliation_generation;
        let private_application_pending = !self.pending_private_applications.is_empty();
        let private_batch_fence_active = self.private_batch_fence_active;
        let registry = &self.registry;
        let applied_turns = &self.last_applied_turns;
        let active_turns = &self.active_turns;
        Ok(self
            .execution_lane
            .next_for_wal_matching(self.dispatch_revision, |request| {
                registry
                    .registration(request.target())
                    .is_some_and(|registration| {
                        let turn_matches =
                            applied_turns.get(request.target()).is_some_and(|applied| {
                                request.admission_connection_generation()
                                    == applied.connection_generation()
                                    && request.admission_private_generation()
                                        == applied.private_generation()
                                    && request.config_digest() == applied.config_digest()
                                    && request.config_epoch() == applied.config_epoch()
                                    && request.turn_sequence() == applied.turn_sequence()
                            });
                        if request.admission_connection_generation() != connection_generation
                            || request.admission_private_generation() != private_generation
                            || request.config_digest() != registration.binding.config_digest
                            || request.config_epoch() != registration.config_epoch
                            || !turn_matches
                        {
                            return false;
                        }
                        if request.exposure() != ExposureEffect::Increase {
                            return true;
                        }
                        !private_application_pending
                            && !private_batch_fence_active
                            && !active_turns.contains_key(request.target())
                            && health == AccountHealth::Ready
                            && registration.lifecycle.accepts_new_risk()
                    })
            })?)
    }

    /// A pre-WAL candidate becomes physically dispatchable only here, after exact durable WAL and
    /// writer receipts are consumed and every runtime fence is checked a second time.
    pub(crate) fn authorize_execution_dispatch(
        &mut self,
        wal: PersistedWalPreparedReceipt,
        writer: PersistedWriterLeaseReceipt,
    ) -> Result<AccountDispatchDecision, AccountRuntimeError> {
        let health = self.health;
        let connection_generation = self.connection_generation;
        let private_generation = self.last_reconciliation_generation;
        let private_application_pending = !self.pending_private_applications.is_empty();
        let private_batch_fence_active = self.private_batch_fence_active;
        let registry = &self.registry;
        let applied_turns = &self.last_applied_turns;
        let active_turns = &self.active_turns;
        let decision = self.execution_lane.authorize_dispatch(
            wal,
            writer,
            self.dispatch_revision,
            |request| {
                registry
                    .registration(request.target())
                    .is_some_and(|registration| {
                        let turn_matches =
                            applied_turns.get(request.target()).is_some_and(|applied| {
                                request.admission_connection_generation()
                                    == applied.connection_generation()
                                    && request.admission_private_generation()
                                        == applied.private_generation()
                                    && request.config_digest() == applied.config_digest()
                                    && request.config_epoch() == applied.config_epoch()
                                    && request.turn_sequence() == applied.turn_sequence()
                            });
                        if request.admission_connection_generation() != connection_generation
                            || request.admission_private_generation() != private_generation
                            || request.config_digest() != registration.binding.config_digest
                            || request.config_epoch() != registration.config_epoch
                            || !turn_matches
                        {
                            return false;
                        }
                        if request.exposure() != ExposureEffect::Increase {
                            return true;
                        }
                        !private_application_pending
                            && !private_batch_fence_active
                            && !active_turns.contains_key(request.target())
                            && health == AccountHealth::Ready
                            && registration.lifecycle.accepts_new_risk()
                    })
            },
        );
        self.physical_authority_roots = None;
        self.revoke_physical_authority();
        let decision = decision?;
        match decision {
            AccountDispatchDecision::Fenced(fence) => Ok(AccountDispatchDecision::Fenced(fence)),
            AccountDispatchDecision::Permit(_) => {
                Err(AccountRuntimeError::PhysicalRecoveryRequired)
            }
        }
    }

    pub(crate) fn record_execution_outcome(
        &mut self,
        receipt: PersistedMutationOutcomeReceipt,
    ) -> Result<AccountLaneFollowUp, AccountRuntimeError> {
        let follow_up = self.execution_lane.record_outcome(receipt);
        self.physical_authority_roots = None;
        self.revoke_physical_authority();
        let follow_up = follow_up?;
        Ok(follow_up)
    }

    pub(crate) fn abort_execution_before_wal(
        &mut self,
        receipt: WalNotPreparedReceipt,
    ) -> Result<AccountLaneFollowUp, AccountRuntimeError> {
        Ok(self.execution_lane.abort_before_wal(receipt)?)
    }

    pub fn resolve_unknown_execution(
        &mut self,
        proof: UnknownReadbackProof,
    ) -> Result<AccountLaneFollowUp, AccountRuntimeError> {
        if self.health != AccountHealth::Ready
            || proof.connection_generation() != self.connection_generation
            || proof.readback_generation() != self.last_reconciliation_generation
        {
            return Err(AccountRuntimeError::StaleExecutionAuthority);
        }
        let follow_up = self.execution_lane.resolve_unknown(proof);
        self.physical_authority_roots = None;
        self.revoke_physical_authority();
        let follow_up = follow_up?;
        Ok(follow_up)
    }

    pub fn request_stop(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<StopPlan, AccountRuntimeError> {
        if self.health != AccountHealth::Ready || self.last_reconciliation_generation == 0 {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        let registration = self
            .registry
            .registration(key)
            .ok_or(RegistryError::Missing)?;
        if !self.actors.contains_key(key)
            || self.shutdown_modes.get(key) == Some(&ShutdownMode::Flatten)
        {
            return Err(AccountRuntimeError::ShutdownMode);
        }
        let already_stopping = registration.lifecycle == InstanceLifecycle::Stopping;
        let fence = self.stop_fences.get(key).copied().unwrap_or((
            self.connection_generation,
            self.last_reconciliation_generation,
        ));
        let mut next_registry = self.registry.clone();
        let plan = next_registry.request_stop(key, fence.0, fence.1)?;
        let mut next_actor = self
            .actors
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        if !already_stopping {
            next_actor.push_control(StrategyControl::Stop)?;
        }
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors.insert(key.clone(), next_actor);
        self.strategy_state_revision = next_state_revision;
        self.stop_fences.entry(key.clone()).or_insert(fence);
        self.shutdown_modes.insert(key.clone(), ShutdownMode::Stop);
        let _revoked = self
            .execution_lane
            .discard_queued_instance_risk_increases(key);
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(plan)
    }

    pub fn request_flatten(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<FlattenPlan, AccountRuntimeError> {
        if self.health != AccountHealth::Ready || self.last_reconciliation_generation == 0 {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        if self.registry.registration(key).is_none() || !self.actors.contains_key(key) {
            return Err(RegistryError::Missing.into());
        }
        let already_flattening = self.shutdown_modes.get(key) == Some(&ShutdownMode::Flatten);
        let fence = self.stop_fences.get(key).copied().unwrap_or((
            self.connection_generation,
            self.last_reconciliation_generation,
        ));
        let mut next_registry = self.registry.clone();
        let plan = next_registry.request_flatten(key, fence.0, fence.1)?;
        let mut next_actor = self
            .actors
            .get(key)
            .cloned()
            .ok_or(AccountRuntimeError::ActorMissing)?;
        if !already_flattening {
            next_actor.push_control(StrategyControl::Flatten)?;
        }
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        self.registry = next_registry;
        self.actors.insert(key.clone(), next_actor);
        self.strategy_state_revision = next_state_revision;
        self.stop_fences.entry(key.clone()).or_insert(fence);
        self.shutdown_modes
            .insert(key.clone(), ShutdownMode::Flatten);
        let _revoked = self
            .execution_lane
            .discard_queued_instance_risk_increases(key);
        self.install_dispatch_revision(next_dispatch_revision);
        Ok(plan)
    }

    pub fn complete_stop(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<StrategyBinding, AccountRuntimeError> {
        if self.shutdown_modes.get(key) != Some(&ShutdownMode::Stop) {
            return Err(AccountRuntimeError::ShutdownMode);
        }
        self.complete_shutdown(key, false)
    }

    pub fn complete_flatten(
        &mut self,
        key: &StrategyInstanceKey,
    ) -> Result<StrategyBinding, AccountRuntimeError> {
        if self.shutdown_modes.get(key) != Some(&ShutdownMode::Flatten) {
            return Err(AccountRuntimeError::ShutdownMode);
        }
        self.complete_shutdown(key, true)
    }

    fn complete_shutdown(
        &mut self,
        key: &StrategyInstanceKey,
        require_flat: bool,
    ) -> Result<StrategyBinding, AccountRuntimeError> {
        if self.health != AccountHealth::Ready {
            return Err(AccountRuntimeError::AccountUnavailable);
        }
        if self.active_turns.contains_key(key) || self.has_pending_private_delivery(key) {
            return Err(AccountRuntimeError::ShutdownActorStatePending);
        }
        let fence = self
            .stop_fences
            .get(key)
            .copied()
            .ok_or(RegistryError::StopNotProven)?;
        let (private_generation, owned_open_orders) = self
            .last_instance_orders
            .get(key)
            .copied()
            .ok_or(RegistryError::StopNotProven)?;
        if (self.connection_generation, private_generation) <= fence || owned_open_orders != 0 {
            return Err(RegistryError::StopNotProven.into());
        }
        let (position_generation, flat) =
            self.last_instance_flat
                .get(key)
                .copied()
                .ok_or(if require_flat {
                    AccountRuntimeError::FlattenNotProven
                } else {
                    AccountRuntimeError::ResidualPositionCustody
                })?;
        if position_generation != private_generation || !flat {
            return Err(if require_flat {
                AccountRuntimeError::FlattenNotProven
            } else {
                AccountRuntimeError::ResidualPositionCustody
            });
        }
        let proof =
            SignedStopProof::new(key.clone(), self.connection_generation, private_generation);
        let next_route_revision = self
            .private_route_revision
            .checked_add(1)
            .ok_or(AccountRuntimeError::PrivateApplicationState)?;
        let next_state_revision = self.next_strategy_state_revision()?;
        let next_dispatch_revision = self.next_dispatch_revision()?;
        let mut next_lane = self.execution_lane.clone();
        let _discarded = next_lane.retire_instance(key)?;
        let mut next_registry = self.registry.clone();
        let binding = next_registry.complete_stop(key, proof)?;
        let mut next_router = self.private_router.clone();
        next_router.release_instance(key);
        self.execution_lane = next_lane;
        self.registry = next_registry;
        self.private_router = next_router;
        self.private_route_revision = next_route_revision;
        self.strategy_state_revision = next_state_revision;
        self.install_dispatch_revision(next_dispatch_revision);
        self.actors.remove(key);
        self.last_applied_turns.remove(key);
        self.turn_sequences.remove(key);
        self.last_instance_orders.remove(key);
        self.last_instance_flat.remove(key);
        self.stop_fences.remove(key);
        self.shutdown_modes.remove(key);
        self.revoke_physical_authority();
        Ok(binding)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AccountRuntimeError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    PrivateRouter(#[from] PrivateRouterError),
    #[error(transparent)]
    MarketHub(#[from] MarketHubError),
    #[error(transparent)]
    StrategyHost(#[from] StrategyHostError),
    #[error(transparent)]
    Reconciler(#[from] AccountReconcilerError),
    #[error(transparent)]
    ExecutionLane(#[from] AccountLaneError),
    #[error("account is not ready on the requested private generation")]
    AccountUnavailable,
    #[error("strategy actor host is missing")]
    ActorMissing,
    #[error("signed reconciliation generation is stale or duplicated")]
    StaleReconciliation,
    #[error("risk-increasing execution is fenced for this account or instance")]
    RiskFenced,
    #[error("native order family is unsupported by this account capability evidence")]
    UnsupportedOrderFamily,
    #[error("execution request is not bound to a current signed private generation")]
    StaleExecutionAuthority,
    #[error("Stop and Flatten lifecycle modes cannot be confused or downgraded")]
    ShutdownMode,
    #[error("Stop or Flatten cannot complete while its actor has an active or unapplied turn")]
    ShutdownActorStatePending,
    #[error("Flatten requires a same-generation signed zero-position proof")]
    FlattenNotProven,
    #[error(
        "Stop preserves a residual position, so the instance retains symbol custody until flat"
    )]
    ResidualPositionCustody,
    #[error("durable account recovery must be installed before private connectivity becomes ready")]
    DurableRecoveryRequired,
    #[error(
        "production physical recovery integration is unavailable; caller manifests cannot authorize connectivity or actor turns"
    )]
    PhysicalRecoveryIntegrationUnavailable,
    #[error("a complete physical recovery readback manifest is required for the next connection")]
    PhysicalRecoveryRequired,
    #[error("a physical recovery manifest is already staged for the next connection")]
    PhysicalRecoveryAlreadyInstalled,
    #[error("physical recovery binding, generation, configuration, or authority roots drifted")]
    PhysicalRecoveryScopeMismatch,
    #[error("durable account recovery was already installed or startup already advanced")]
    DurableRecoveryAlreadyInstalled,
    #[error("durable recovery snapshot belongs to another exchange account")]
    RecoveryAccountMismatch,
    #[error("durable recovery does not exactly cover configured strategies and mutation epochs")]
    RecoveryStateMismatch,
    #[error("reconciliation or execution authority belongs to a stale configuration epoch")]
    StaleConfiguration,
    #[error("configuration cannot change while this instance has an in-flight or UNKNOWN mutation")]
    ParameterChangeBusy,
    #[error("an in-flight mutation must be classified as UNKNOWN before reconnect becomes ready")]
    ReconnectWithInFlight,
    #[error("durable actor inbox turns must be applied or recovered before reconnect advances")]
    ReconnectWithUnappliedActorState,
    #[error("connection generation counter is exhausted")]
    ConnectionGenerationExhausted,
    #[error("a strategy instance already has an unacknowledged actor turn")]
    StrategyTurnActive,
    #[error("strategy turn receipt is stale, unpersisted, or belongs to another authority epoch")]
    StrategyTurnAuthority,
    #[error("private route plan is stale relative to the committed durable inbox revision")]
    StalePrivateRoutePlan,
    #[error("order route append receipt does not extend the installed durable owner-index head")]
    OrderRouteReceipt,
    #[error("private actor application cursor or delivery acknowledgement is inconsistent")]
    PrivateApplicationState,
    #[error("strategy registry/lifecycle/config state revision counter is exhausted")]
    StrategyStateRevisionExhausted,
    #[error("execution dispatch authority revision counter is exhausted")]
    DispatchRevisionExhausted,
}

#[cfg(test)]
mod capability_tests {
    use super::*;
    use crate::domain::{ExchangeId, NativeOrderFamily};

    #[test]
    fn one_capability_gate_rejects_unsupported_admission_and_recovery_families()
    -> Result<(), Box<dyn std::error::Error>> {
        for exchange in [ExchangeId::Gate, ExchangeId::Bitget] {
            let runtime = AccountRuntime::new(AccountKey::new(exchange, "main")?);
            assert!(
                runtime
                    .ensure_supported_order_family(NativeOrderFamily::UmOrder)
                    .is_ok()
            );
            assert!(matches!(
                runtime.ensure_supported_order_family(NativeOrderFamily::UmConditional),
                Err(AccountRuntimeError::UnsupportedOrderFamily)
            ));
            assert!(matches!(
                runtime.ensure_supported_order_family(NativeOrderFamily::UmAlgo),
                Err(AccountRuntimeError::UnsupportedOrderFamily)
            ));
        }
        Ok(())
    }
}
