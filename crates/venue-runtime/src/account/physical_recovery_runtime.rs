use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use super::{AccountRuntime, AccountRuntimeError};
use crate::account::recovery_session::{
    PhysicalRecoveryRootRefresh, PhysicalRecoverySession, PhysicalRecoverySessionParameters,
};
#[cfg(test)]
use crate::runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryScope,
};
use crate::{
    domain::NativeOrderFamily,
    runtime::account::{PhysicalRecoveryAuthorityRoots, PhysicalRecoveryReadbackManifest},
};
use venue_gateway_api::GatewayBinding;
#[cfg(test)]
use venue_gateway_api::VenueId;

impl AccountRuntime {
    fn physical_recovery_runtime_authority_sha256(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        commit_bytes(&mut digest, b"venue-physical-recovery-runtime-authority-v1");
        commit_u64(&mut digest, self.strategy_state_revision);
        commit_u64(&mut digest, self.market_actor_revision);
        commit_u64(&mut digest, self.private_route_revision);
        commit_u64(&mut digest, self.dispatch_revision);
        commit_u64(&mut digest, self.connection_generation);
        commit_u64(&mut digest, self.last_applied_private_sequence);
        commit_u64(&mut digest, self.last_reconciliation_generation);
        commit_bytes(&mut digest, &[account_health_tag(self.health)]);
        commit_bytes(&mut digest, &[self.fault.map_or(0, account_fault_tag)]);
        commit_bytes(&mut digest, &[u8::from(self.private_batch_fence_active)]);
        commit_u64(&mut digest, self.registry.registrations().count() as u64);
        for registration in self.registry.registrations() {
            commit_strategy_key(&mut digest, &registration.binding.key);
            commit_str(&mut digest, &registration.binding.run_id);
            commit_str(&mut digest, &registration.binding.config_digest);
            commit_u64(&mut digest, registration.config_epoch);
            commit_bytes(&mut digest, &[lifecycle_tag(registration.lifecycle)]);
        }
        commit_u64(&mut digest, self.turn_sequences.len() as u64);
        for (key, sequence) in &self.turn_sequences {
            commit_strategy_key(&mut digest, key);
            commit_u64(&mut digest, *sequence);
        }
        commit_u64(&mut digest, self.active_turns.len() as u64);
        for (key, active) in &self.active_turns {
            commit_strategy_key(&mut digest, key);
            commit_turn_token(&mut digest, &active.token);
        }
        commit_u64(&mut digest, self.last_applied_turns.len() as u64);
        for (key, token) in &self.last_applied_turns {
            commit_strategy_key(&mut digest, key);
            commit_turn_token(&mut digest, token);
        }
        commit_u64(&mut digest, self.pending_private_applications.len() as u64);
        for (sequence, application) in &self.pending_private_applications {
            commit_u64(&mut digest, *sequence);
            commit_u64(&mut digest, application.expected.len() as u64);
            for (key, fact_index) in &application.expected {
                commit_strategy_key(&mut digest, key);
                commit_u64(&mut digest, u64::from(*fact_index));
            }
        }
        commit_u64(&mut digest, self.completed_private_sequences.len() as u64);
        for sequence in &self.completed_private_sequences {
            commit_u64(&mut digest, *sequence);
        }
        commit_u64(&mut digest, self.stop_fences.len() as u64);
        for (key, (connection_generation, private_generation)) in &self.stop_fences {
            commit_strategy_key(&mut digest, key);
            commit_u64(&mut digest, *connection_generation);
            commit_u64(&mut digest, *private_generation);
        }
        commit_u64(&mut digest, self.shutdown_modes.len() as u64);
        for (key, mode) in &self.shutdown_modes {
            commit_strategy_key(&mut digest, key);
            commit_bytes(
                &mut digest,
                &[match mode {
                    super::ShutdownMode::Stop => 1,
                    super::ShutdownMode::Flatten => 2,
                }],
            );
        }
        digest.finalize().into()
    }

    fn recovery_session_parameters(
        &self,
        durable_roots: super::PhysicalRecoveryDurableRoots,
    ) -> Result<PhysicalRecoverySessionParameters, AccountRuntimeError> {
        let connection_generation = self
            .connection_generation
            .checked_add(1)
            .ok_or(AccountRuntimeError::ConnectionGenerationExhausted)?;
        let expected_private_generation = self
            .physical_private_generation_floor
            .checked_add(1)
            .ok_or(AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        let registrations = self
            .registry
            .registrations()
            .map(|registration| (registration.binding.clone(), registration.config_epoch))
            .collect::<Vec<_>>();
        let anchor = registrations
            .iter()
            .map(|(binding, _)| &binding.key.symbol)
            .min()
            .cloned()
            .ok_or(AccountRuntimeError::PhysicalRecoveryUniverseIncomplete)?;
        let mode = self
            .recovered_gateway_mode
            .ok_or(AccountRuntimeError::DurableRecoveryRequired)?;
        let position_mode = self
            .recovered_position_mode
            .ok_or(AccountRuntimeError::DurableRecoveryRequired)?;
        let binding = GatewayBinding::new(
            self.account.exchange,
            mode,
            self.account.account.clone(),
            anchor,
        )
        .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        Ok(PhysicalRecoverySessionParameters {
            binding,
            account: self.account.clone(),
            registrations,
            position_mode,
            family_support: self.physical_family_support(),
            profile_version: self.physical_profile_version(),
            connection_generation,
            recovered_private_generation: self.physical_private_generation_floor,
            expected_private_generation,
            runtime_authority_sha256: self.physical_recovery_runtime_authority_sha256(),
            durable_roots,
        })
    }

    /// Issues an opaque, authenticated lease for one read-only recovery attempt. The issuer uses
    /// only runtime-owned recovered roots and registry state; callers cannot supply a digest,
    /// generation, universe, or profile to be signed.
    pub fn issue_physical_recovery_session(
        &mut self,
    ) -> Result<PhysicalRecoverySession, AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        if self.active_physical_recovery_session.is_some()
            || self.pending_physical_recovery.is_some()
        {
            return Err(AccountRuntimeError::PhysicalRecoverySessionActive);
        }
        let durable_roots = self
            .physical_durable_roots
            .clone()
            .ok_or(AccountRuntimeError::PhysicalRecoveryDurableRootsRequired)?;
        if self.physical_authority_roots.as_ref() != Some(durable_roots.physical_roots()) {
            return Err(AccountRuntimeError::PhysicalRecoveryDurableRootDrift);
        }
        let parameters = self.recovery_session_parameters(durable_roots)?;
        let session = self
            .physical_recovery_session_issuer
            .issue(parameters)
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        self.active_physical_recovery_session = Some(session.clone());
        Ok(session)
    }

    #[cfg(test)]
    pub(crate) fn issue_expired_physical_recovery_session_for_test(
        &mut self,
    ) -> Result<PhysicalRecoverySession, AccountRuntimeError> {
        if !self.durable_recovery_complete || self.active_physical_recovery_session.is_some() {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        let durable_roots = self
            .physical_durable_roots
            .clone()
            .ok_or(AccountRuntimeError::PhysicalRecoveryDurableRootsRequired)?;
        let parameters = self.recovery_session_parameters(durable_roots)?;
        let session = self
            .physical_recovery_session_issuer
            .issue_expired_for_test(parameters)
            .map_err(|_| AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        self.active_physical_recovery_session = Some(session.clone());
        Ok(session)
    }

    fn validate_physical_recovery_session(
        &self,
        session: &PhysicalRecoverySession,
    ) -> Result<(), AccountRuntimeError> {
        if !self.durable_recovery_complete {
            return Err(AccountRuntimeError::DurableRecoveryRequired);
        }
        let active = self
            .active_physical_recovery_session
            .as_ref()
            .ok_or(AccountRuntimeError::PhysicalRecoverySessionInvalid)?;
        if !active.same_authority(session)
            || !self.physical_recovery_session_issuer.authenticates(session)
        {
            return Err(AccountRuntimeError::PhysicalRecoverySessionInvalid);
        }
        if session.is_expired() {
            return Err(AccountRuntimeError::PhysicalRecoverySessionExpired);
        }
        let durable_roots = self
            .physical_durable_roots
            .as_ref()
            .ok_or(AccountRuntimeError::PhysicalRecoveryDurableRootsRequired)?;
        let physical_roots_current = self.physical_authority_roots.as_ref()
            == Some(durable_roots.physical_roots())
            && session.durable_roots() == durable_roots;
        let expected_connection_generation = self
            .connection_generation
            .checked_add(1)
            .ok_or(AccountRuntimeError::ConnectionGenerationExhausted)?;
        let expected_private_generation = self
            .physical_private_generation_floor
            .checked_add(1)
            .ok_or(AccountRuntimeError::PhysicalRecoveryScopeMismatch)?;
        let scope = session.scope();
        let account_authority_current = self
            .recovered_gateway_mode
            .zip(self.recovered_position_mode)
            .is_some_and(|(mode, position_mode)| {
                scope.matches_account_authority(
                    &self.account,
                    mode,
                    self.registry.registrations().map(|registration| {
                        (registration.binding.clone(), registration.config_epoch)
                    }),
                    position_mode,
                    &self.physical_family_support(),
                    self.physical_profile_version(),
                )
            });
        if !physical_roots_current
            || !account_authority_current
            || session.runtime_authority_sha256()
                != &self.physical_recovery_runtime_authority_sha256()
            || scope.authority_roots() != durable_roots.physical_roots()
            || scope.connection_generation() != expected_connection_generation
            || scope.recovered_private_generation() != self.physical_private_generation_floor
            || session.private_generation() != expected_private_generation
        {
            return Err(AccountRuntimeError::PhysicalRecoveryDurableRootDrift);
        }
        Ok(())
    }

    fn revoke_failed_recovery_session(
        &mut self,
        error: AccountRuntimeError,
    ) -> AccountRuntimeError {
        self.revoke_physical_authority();
        error
    }

    /// Must be called after every collector await. It rechecks the unexpired lease, authenticated
    /// attempt/session epoch, complete registry/profile authority, and every current durable root.
    pub fn validate_physical_recovery_session_after_await(
        &mut self,
        session: &PhysicalRecoverySession,
    ) -> Result<(), AccountRuntimeError> {
        self.validate_physical_recovery_session(session)
            .map_err(|error| self.revoke_failed_recovery_session(error))
    }

    /// Consumes one complete durable replay refresh and returns the next authenticated epoch.
    /// Refresh never extends the original lease. Any omission, regression, or cross-attempt
    /// receipt revokes the entire recovery authority.
    pub fn refresh_physical_recovery_session(
        &mut self,
        session: &PhysicalRecoverySession,
        refresh: PhysicalRecoveryRootRefresh,
    ) -> Result<PhysicalRecoverySession, AccountRuntimeError> {
        if let Err(error) = self.validate_physical_recovery_session(session) {
            return Err(self.revoke_failed_recovery_session(error));
        }
        if !refresh.matches(session)
            || !refresh
                .roots()
                .monotonic_successor_of(session.durable_roots())
        {
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoveryDurableRootRegression,
            ));
        }
        let roots = refresh.roots().clone();
        let parameters = match self.recovery_session_parameters(roots.clone()) {
            Ok(parameters) => parameters,
            Err(error) => return Err(self.revoke_failed_recovery_session(error)),
        };
        let refreshed = match self
            .physical_recovery_session_issuer
            .refresh(session, parameters)
        {
            Ok(refreshed) => refreshed,
            Err(_) => {
                return Err(self.revoke_failed_recovery_session(
                    AccountRuntimeError::PhysicalRecoveryScopeMismatch,
                ));
            }
        };
        self.physical_authority_roots = Some(roots.physical_roots().clone());
        self.physical_durable_roots = Some(roots);
        self.active_physical_recovery_session = Some(refreshed.clone());
        Ok(refreshed)
    }

    /// Stages a manifest only when it is bound to the exact current authenticated session. The
    /// existing integration gate remains closed in production until a gateway collector is wired.
    pub fn install_authenticated_physical_recovery_manifest(
        &mut self,
        session: &PhysicalRecoverySession,
        manifest: PhysicalRecoveryReadbackManifest,
    ) -> Result<(), AccountRuntimeError> {
        if !self.physical_recovery_integration_available() {
            self.revoke_physical_authority();
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
        }
        if let Err(error) = self.validate_physical_recovery_session(session) {
            return Err(self.revoke_failed_recovery_session(error));
        }
        if session.session_epoch() < 2 || session.durable_roots().authority_epoch() < 2 {
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoveryPostAwaitRefreshRequired,
            ));
        }
        if self.pending_physical_recovery.is_some() {
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoveryAlreadyInstalled,
            ));
        }
        if manifest.scope() != session.scope()
            || manifest.attempt_id() != session.attempt_id()
            || manifest.private_generation() != session.private_generation()
        {
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoverySessionInvalid,
            ));
        }
        if let Err(error) = self.validate_physical_recovery_manifest(&manifest) {
            return Err(self.revoke_failed_recovery_session(error));
        }
        self.pending_physical_recovery = Some(manifest);
        self.active_physical_recovery_session = None;
        Ok(())
    }

    fn physical_family_support(&self) -> BTreeMap<NativeOrderFamily, bool> {
        [
            NativeOrderFamily::UmOrder,
            NativeOrderFamily::UmConditional,
            NativeOrderFamily::UmAlgo,
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
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable,
            ));
        }
        if self.active_physical_recovery_session.is_some() {
            return Err(self.revoke_failed_recovery_session(
                AccountRuntimeError::PhysicalRecoverySessionInvalid,
            ));
        }
        if self.pending_physical_recovery.is_some() {
            return Err(AccountRuntimeError::PhysicalRecoveryAlreadyInstalled);
        }
        self.validate_physical_recovery_manifest(&manifest)?;
        self.pending_physical_recovery = Some(manifest);
        Ok(())
    }

    pub(super) fn validate_physical_recovery_manifest(
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
                        (registration.binding.clone(), registration.config_epoch)
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

    pub(super) fn physical_recovery_integration_available(&self) -> bool {
        #[cfg(test)]
        {
            self.physical_recovery_test_fixture_enabled
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    pub(super) fn physical_turn_authorized(&self) -> bool {
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
                                    (registration.binding.clone(), registration.config_epoch)
                                }),
                                position_mode,
                                &self.physical_family_support(),
                                self.physical_profile_version(),
                            )
                        })
            })
    }

    pub(super) fn revoke_physical_authority(&mut self) {
        self.active_physical_recovery_session = None;
        self.pending_physical_recovery = None;
        self.admitted_physical_recovery = None;
        self.active_turns.clear();
        self.last_applied_turns.clear();
        self.invalidate_dispatch_authority_fail_closed();
        if self.health != crate::runtime::account::AccountHealth::Frozen {
            self.health = crate::runtime::account::AccountHealth::Starting;
            self.fault = None;
        }
        self.physical_recovery_drifted = true;
    }

    pub(super) fn reject_drifted_physical_authority(&mut self) -> Result<(), AccountRuntimeError> {
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
        if self.admitted_physical_recovery.is_some()
            || self.active_physical_recovery_session.is_some()
        {
            self.revoke_physical_authority();
        }
    }

    #[cfg(test)]
    pub(super) fn stage_physical_recovery_test_fixture(
        &mut self,
    ) -> Result<(), AccountRuntimeError> {
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
            self.registry
                .registrations()
                .map(|registration| (registration.binding.clone(), registration.config_epoch)),
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
                        .supports(NativeOrderFamily::UmOrder),
                    PhysicalReadbackSurface::UmConditional => !self
                        .capability_evidence
                        .supports(NativeOrderFamily::UmConditional),
                    PhysicalReadbackSurface::UmAlgo => {
                        !self.capability_evidence.supports(NativeOrderFamily::UmAlgo)
                    }
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
}

const fn account_health_tag(health: super::AccountHealth) -> u8 {
    match health {
        super::AccountHealth::Starting => 1,
        super::AccountHealth::Ready => 2,
        super::AccountHealth::Frozen => 3,
    }
}

const fn account_fault_tag(fault: super::AccountFault) -> u8 {
    match fault {
        super::AccountFault::PrivateStreamDisconnected => 1,
        super::AccountFault::PrivateGenerationMismatch => 2,
        super::AccountFault::PrivateEvidenceGap => 3,
        super::AccountFault::PrivateEvidenceBatchIncomplete => 4,
        super::AccountFault::ReconciliationFailed => 5,
        super::AccountFault::WriterUnavailable => 6,
    }
}

const fn lifecycle_tag(lifecycle: super::InstanceLifecycle) -> u8 {
    match lifecycle {
        super::InstanceLifecycle::Registered => 1,
        super::InstanceLifecycle::Recovering => 2,
        super::InstanceLifecycle::Running => 3,
        super::InstanceLifecycle::Paused => 4,
        super::InstanceLifecycle::Stopping => 5,
        super::InstanceLifecycle::Faulted => 6,
        super::InstanceLifecycle::NeedsAttention => 7,
    }
}

fn commit_strategy_key(digest: &mut Sha256, key: &crate::StrategyInstanceKey) {
    commit_bytes(digest, &[strategy_kind_tag(key.strategy_kind)]);
    commit_str(digest, key.account.exchange.as_str());
    commit_str(digest, &key.account.account);
    commit_str(digest, &key.instance_id);
    commit_str(digest, &key.symbol.to_string());
}

const fn strategy_kind_tag(kind: crate::StrategyKind) -> u8 {
    match kind {
        crate::StrategyKind::HedgedGrid => 1,
        crate::StrategyKind::Scalping => 2,
        crate::StrategyKind::Copy => 3,
        crate::StrategyKind::Manual => 4,
    }
}

fn commit_turn_token(digest: &mut Sha256, token: &crate::StrategyTurnToken) {
    commit_strategy_key(digest, token.target());
    commit_u64(digest, token.connection_generation());
    commit_u64(digest, token.private_generation());
    commit_str(digest, token.config_digest());
    commit_u64(digest, token.config_epoch());
    commit_u64(digest, token.turn_sequence());
}

fn commit_str(digest: &mut Sha256, value: &str) {
    commit_bytes(digest, value.as_bytes());
}

fn commit_u64(digest: &mut Sha256, value: u64) {
    commit_bytes(digest, &value.to_be_bytes());
}

fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}
