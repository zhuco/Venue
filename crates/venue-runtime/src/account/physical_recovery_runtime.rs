use std::collections::BTreeMap;

use super::{AccountRuntime, AccountRuntimeError};
#[cfg(test)]
use crate::runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryScope,
};
use crate::{
    domain::NativeOrderFamily,
    runtime::account::{PhysicalRecoveryAuthorityRoots, PhysicalRecoveryReadbackManifest},
};
#[cfg(test)]
use venue_gateway_api::{GatewayBinding, VenueId};

impl AccountRuntime {
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
            return Err(AccountRuntimeError::PhysicalRecoveryIntegrationUnavailable);
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

    pub(super) fn revoke_physical_authority(&mut self) {
        self.pending_physical_recovery = None;
        self.admitted_physical_recovery = None;
        self.active_turns.clear();
        self.last_applied_turns.clear();
        self.invalidate_dispatch_authority_fail_closed();
        self.health = crate::runtime::account::AccountHealth::Starting;
        self.fault = None;
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
        if self.admitted_physical_recovery.is_some() {
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
