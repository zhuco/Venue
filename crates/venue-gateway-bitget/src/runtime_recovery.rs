//! Runtime-owned facts sealed around one Bitget recovery turn.
//!
//! The gateway deliberately accepts only an immutable projection here. It never opens durable
//! state or acquires a writer; Runtime remains the authority for Owner, WAL, and Unknown facts.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use venue_domain::domain::{CommandId, NativeOrderFamily, Symbol};
use venue_gateway_api::GatewayMode;

use crate::{
    BITGET_ORDER_PROFILE_VERSION, BitgetAuthenticatedRecoverySession, BitgetFreshRecoveryCandidate,
    BitgetFreshRecoveryCollector, BitgetFreshRecoveryScope, BitgetRecoveryCollectionOutcome,
    BitgetRecoveryCoverage, BitgetRecoveryError, BitgetRecoveryOwnerRoute, BitgetRecoverySurface,
};

const MAX_REGISTRATIONS: usize = 256;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BitgetRuntimeRecoveryRegistration {
    symbol: Symbol,
    strategy_instance_id: String,
    config_digest: String,
    config_epoch: u64,
}

impl BitgetRuntimeRecoveryRegistration {
    pub fn verified(
        symbol: Symbol,
        strategy_instance_id: impl Into<String>,
        config_digest: impl Into<String>,
        config_epoch: u64,
    ) -> Result<Self, BitgetRuntimeRecoveryError> {
        let strategy_instance_id = strategy_instance_id.into();
        let config_digest = config_digest.into();
        if !valid_label(&strategy_instance_id) || !valid_digest(&config_digest) || config_epoch == 0
        {
            return Err(BitgetRuntimeRecoveryError::Scope);
        }
        Ok(Self {
            symbol,
            strategy_instance_id,
            config_digest,
            config_epoch,
        })
    }

    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }
    #[must_use]
    pub fn strategy_instance_id(&self) -> &str {
        &self.strategy_instance_id
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BitgetRuntimeStructuredUnknown {
    command_id: CommandId,
    native_client_id: CommandId,
    family: NativeOrderFamily,
    symbol: Symbol,
}

impl BitgetRuntimeStructuredUnknown {
    pub fn verified(
        command_id: CommandId,
        native_client_id: CommandId,
        family: NativeOrderFamily,
        symbol: Symbol,
    ) -> Result<Self, BitgetRuntimeRecoveryError> {
        if family != NativeOrderFamily::UmOrder {
            return Err(BitgetRuntimeRecoveryError::Unknown);
        }
        Ok(Self {
            command_id,
            native_client_id,
            family,
            symbol,
        })
    }

    #[must_use]
    pub const fn command_id(&self) -> &CommandId {
        &self.command_id
    }
    #[must_use]
    pub const fn native_client_id(&self) -> &CommandId {
        &self.native_client_id
    }
    #[must_use]
    pub const fn family(&self) -> NativeOrderFamily {
        self.family
    }
    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRuntimeRecoveryScopeInput {
    pub fresh_scope: BitgetFreshRecoveryScope,
    pub recovery_session_sha256: [u8; 32],
    pub registrations: Vec<BitgetRuntimeRecoveryRegistration>,
    pub owner_routes: Vec<BitgetRecoveryOwnerRoute>,
    pub structured_unknowns: Vec<BitgetRuntimeStructuredUnknown>,
}

/// Runtime facts frozen before the first signed recovery request. It carries evidence only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRuntimeRecoveryScope {
    fresh_scope: BitgetFreshRecoveryScope,
    recovery_session_sha256: [u8; 32],
    registrations: Vec<BitgetRuntimeRecoveryRegistration>,
    owner_routes: Vec<BitgetRecoveryOwnerRoute>,
    structured_unknowns: Vec<BitgetRuntimeStructuredUnknown>,
    commitment_sha256: [u8; 32],
}

impl BitgetRuntimeRecoveryScope {
    pub fn verified(
        mut input: BitgetRuntimeRecoveryScopeInput,
    ) -> Result<Self, BitgetRuntimeRecoveryError> {
        let scope = &input.fresh_scope;
        if scope.endpoint().mode() != GatewayMode::Live
            || zero(&input.recovery_session_sha256)
            || input.registrations.is_empty()
            || input.registrations.len() > MAX_REGISTRATIONS
        {
            return Err(BitgetRuntimeRecoveryError::Scope);
        }
        input.registrations.sort();
        let registration_symbols = input
            .registrations
            .iter()
            .map(|value| value.symbol.clone())
            .collect::<BTreeSet<_>>();
        if registration_symbols != *scope.symbols()
            || input
                .registrations
                .windows(2)
                .any(|values| values[0].symbol == values[1].symbol)
            || input.registrations.iter().any(|value| {
                value.config_digest != scope.config_digest()
                    || value.config_epoch != scope.config_epoch()
            })
        {
            return Err(BitgetRuntimeRecoveryError::Universe);
        }
        let registration_by_symbol = input
            .registrations
            .iter()
            .map(|value| (value.symbol.clone(), value))
            .collect::<BTreeMap<_, _>>();
        input.owner_routes.sort_by(|left, right| {
            left.owner()
                .symbol
                .cmp(&right.owner().symbol)
                .then_with(|| left.client_order_id().cmp(right.client_order_id()))
                .then_with(|| left.venue_order_id().cmp(right.venue_order_id()))
        });
        let mut route_keys = BTreeSet::new();
        for route in &input.owner_routes {
            let owner = route.owner();
            let Some(registration) = registration_by_symbol.get(&owner.symbol) else {
                return Err(BitgetRuntimeRecoveryError::Owner);
            };
            if owner.exchange != "bitget"
                || owner.account != scope.trading_account_id()
                || owner.strategy_instance_id != registration.strategy_instance_id
                || !route_keys.insert((
                    owner.symbol.clone(),
                    route.client_order_id().to_owned(),
                    route.venue_order_id().to_owned(),
                ))
            {
                return Err(BitgetRuntimeRecoveryError::Owner);
            }
        }
        input.structured_unknowns.sort();
        let mut commands = BTreeSet::new();
        let mut native_ids = BTreeSet::new();
        for unknown in &input.structured_unknowns {
            if !registration_by_symbol.contains_key(&unknown.symbol)
                || unknown.family != NativeOrderFamily::UmOrder
                || !commands.insert(unknown.command_id.clone())
                || !native_ids.insert((unknown.symbol.clone(), unknown.native_client_id.clone()))
            {
                return Err(BitgetRuntimeRecoveryError::Unknown);
            }
        }
        let expected_unknowns = scope
            .unresolved_commands()
            .iter()
            .map(|value| {
                (
                    value.command_id().clone(),
                    value.client_order_id().clone(),
                    value.family(),
                    value.symbol().clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let supplied_unknowns = input
            .structured_unknowns
            .iter()
            .map(|value| {
                (
                    value.command_id.clone(),
                    value.native_client_id.clone(),
                    value.family,
                    value.symbol.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        if supplied_unknowns != expected_unknowns {
            return Err(BitgetRuntimeRecoveryError::Unknown);
        }
        let commitment_sha256 = scope_commitment(&input);
        Ok(Self {
            fresh_scope: input.fresh_scope,
            recovery_session_sha256: input.recovery_session_sha256,
            registrations: input.registrations,
            owner_routes: input.owner_routes,
            structured_unknowns: input.structured_unknowns,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn fresh_scope(&self) -> &BitgetFreshRecoveryScope {
        &self.fresh_scope
    }
    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
    #[must_use]
    pub fn owner_routes(&self) -> &[BitgetRecoveryOwnerRoute] {
        &self.owner_routes
    }
    #[must_use]
    pub fn structured_unknowns(&self) -> &[BitgetRuntimeStructuredUnknown] {
        &self.structured_unknowns
    }

    pub(crate) fn validate_session(
        &self,
        session: &BitgetAuthenticatedRecoverySession,
    ) -> Result<(), BitgetRuntimeRecoveryError> {
        let scope = &self.fresh_scope;
        if session.mode() != scope.endpoint().mode()
            || session.trading_account_id() != scope.trading_account_id()
            || session.symbols() != scope.symbols()
            || session.connection_generation() != scope.connection_generation()
            || session.private_generation() != scope.private_generation()
            || session.attempt_id() != scope.private_generation()
            || session.started_at_ms() != scope.started_at_ms()
            || session.deadline_at_ms() != scope.deadline_ms()
            || session.request_universe_sha256() != &self.recovery_session_sha256
            || session.rest_origin() != scope.endpoint().rest_origin()
            || session.private_ws_endpoint() != scope.endpoint().private_ws()
        {
            return Err(BitgetRuntimeRecoveryError::Session);
        }
        Ok(())
    }
}

/// Runtime provides the current immutable scope commitment. A matching value proves only scope
/// stability; it never grants a writer, capability, or journal handle.
pub trait BitgetRuntimeRecoveryRevalidator {
    fn current_scope_sha256(&self) -> Option<[u8; 32]>;
}

#[derive(Clone, Copy)]
pub(crate) struct BitgetRuntimeRecoveryAwaitGuard<'a> {
    expected: [u8; 32],
    revalidator: &'a dyn BitgetRuntimeRecoveryRevalidator,
}

impl<'a> BitgetRuntimeRecoveryAwaitGuard<'a> {
    pub(crate) fn new(
        scope: &BitgetRuntimeRecoveryScope,
        revalidator: &'a dyn BitgetRuntimeRecoveryRevalidator,
    ) -> Result<Self, BitgetRuntimeRecoveryError> {
        let guard = Self {
            expected: *scope.commitment_sha256(),
            revalidator,
        };
        guard.revalidate()?;
        Ok(guard)
    }

    pub(crate) fn revalidate(self) -> Result<(), BitgetRuntimeRecoveryError> {
        if self.revalidator.current_scope_sha256() == Some(self.expected) {
            Ok(())
        } else {
            Err(BitgetRuntimeRecoveryError::ScopeDrift)
        }
    }
}

/// Complete, read-only Bitget evidence that Runtime may admit only after its own reconciliation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRuntimeRecoveryBundle {
    runtime_scope: BitgetRuntimeRecoveryScope,
    candidate: BitgetFreshRecoveryCandidate,
    commitment_sha256: [u8; 32],
}

impl BitgetRuntimeRecoveryBundle {
    fn from_candidate(
        runtime_scope: BitgetRuntimeRecoveryScope,
        candidate: BitgetFreshRecoveryCandidate,
    ) -> Result<Self, BitgetRuntimeRecoveryError> {
        if candidate.scope() != runtime_scope.fresh_scope()
            || candidate.owner_routes() != runtime_scope.owner_routes()
        {
            return Err(BitgetRuntimeRecoveryError::ScopeDrift);
        }
        for surface in [
            BitgetRecoverySurface::Account,
            BitgetRecoverySurface::Positions,
            BitgetRecoverySurface::UmOrder,
            BitgetRecoverySurface::FillsCursor,
        ] {
            if !matches!(
                candidate.coverage(surface),
                BitgetRecoveryCoverage::Complete { .. }
            ) {
                return Err(BitgetRuntimeRecoveryError::Coverage);
            }
        }
        for surface in [
            BitgetRecoverySurface::UmConditional,
            BitgetRecoverySurface::UmAlgo,
        ] {
            if !matches!(candidate.coverage(surface), BitgetRecoveryCoverage::Unsupported { profile_version, .. } if *profile_version == BITGET_ORDER_PROFILE_VERSION)
            {
                return Err(BitgetRuntimeRecoveryError::Coverage);
            }
        }
        let mut digest = Sha256::new();
        digest.update(b"venue-bitget-runtime-recovery-bundle-v1");
        digest.update(runtime_scope.commitment_sha256());
        digest.update(candidate.commitment_sha256());
        digest.update(candidate.completed_at_ms().to_be_bytes());
        Ok(Self {
            commitment_sha256: digest.finalize().into(),
            runtime_scope,
            candidate,
        })
    }

    #[must_use]
    pub const fn runtime_scope(&self) -> &BitgetRuntimeRecoveryScope {
        &self.runtime_scope
    }
    #[must_use]
    pub const fn candidate(&self) -> &BitgetFreshRecoveryCandidate {
        &self.candidate
    }
    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

impl BitgetFreshRecoveryCollector {
    /// Finishes exactly one Runtime-bound turn. A failed revalidation or non-complete physical
    /// fold has no resumable result, so callers cannot stitch evidence across attempts.
    pub fn finish_runtime(
        self,
        runtime_scope: BitgetRuntimeRecoveryScope,
        observed_endpoint: &crate::BitgetRecoveryEndpoint,
        generation: crate::BitgetRecoveryGenerationWitness,
        now_ms: u64,
        revalidator: &dyn BitgetRuntimeRecoveryRevalidator,
    ) -> Result<BitgetRuntimeRecoveryBundle, BitgetRuntimeRecoveryError> {
        BitgetRuntimeRecoveryAwaitGuard::new(&runtime_scope, revalidator)?.revalidate()?;
        let outcome = self.finish(
            observed_endpoint,
            generation,
            runtime_scope.owner_routes.clone(),
            now_ms,
        )?;
        BitgetRuntimeRecoveryAwaitGuard::new(&runtime_scope, revalidator)?.revalidate()?;
        let BitgetRecoveryCollectionOutcome::Complete(candidate) = outcome else {
            return Err(BitgetRuntimeRecoveryError::Unknown);
        };
        BitgetRuntimeRecoveryBundle::from_candidate(runtime_scope, *candidate)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetRuntimeRecoveryError {
    #[error("Bitget Runtime recovery scope is invalid")]
    Scope,
    #[error("Bitget Runtime recovery registration universe differs from the sealed session")]
    Universe,
    #[error("Bitget Runtime recovery Owner projection is incomplete or invalid")]
    Owner,
    #[error("Bitget Runtime recovery Unknown projection is incomplete or invalid")]
    Unknown,
    #[error("Bitget Runtime recovery session does not match its sealed scope")]
    Session,
    #[error("Bitget Runtime recovery scope changed while collection was in flight")]
    ScopeDrift,
    #[error("Bitget Runtime recovery did not close all six canonical surfaces")]
    Coverage,
    #[error("Bitget physical recovery collection failed")]
    Physical(#[from] BitgetRecoveryError),
}

fn scope_commitment(input: &BitgetRuntimeRecoveryScopeInput) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue-bitget-runtime-recovery-scope-v1");
    digest.update(input.fresh_scope.commitment_sha256());
    digest.update(input.recovery_session_sha256);
    for registration in &input.registrations {
        part(&mut digest, registration.symbol.to_string().as_bytes());
        part(&mut digest, registration.strategy_instance_id.as_bytes());
        part(&mut digest, registration.config_digest.as_bytes());
        digest.update(registration.config_epoch.to_be_bytes());
    }
    for route in &input.owner_routes {
        part(&mut digest, route.client_order_id().as_bytes());
        part(&mut digest, route.venue_order_id().as_bytes());
        part(&mut digest, route.owner().strategy_instance_id.as_bytes());
    }
    for unknown in &input.structured_unknowns {
        part(&mut digest, unknown.command_id.as_str().as_bytes());
        part(&mut digest, unknown.native_client_id.as_str().as_bytes());
        part(&mut digest, unknown.symbol.to_string().as_bytes());
    }
    digest.finalize().into()
}

fn part(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
        })
}

fn valid_digest(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn zero(value: &[u8; 32]) -> bool {
    value.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct Drift {
        changed: AtomicBool,
    }

    impl BitgetRuntimeRecoveryRevalidator for Drift {
        fn current_scope_sha256(&self) -> Option<[u8; 32]> {
            Some(if self.changed.load(Ordering::Acquire) {
                [2; 32]
            } else {
                [1; 32]
            })
        }
    }

    #[tokio::test]
    async fn await_guard_rejects_runtime_scope_drift() {
        let revalidator = Drift {
            changed: AtomicBool::new(false),
        };
        let guard = BitgetRuntimeRecoveryAwaitGuard {
            expected: [1; 32],
            revalidator: &revalidator,
        };
        assert_eq!(guard.revalidate(), Ok(()));
        tokio::task::yield_now().await;
        revalidator.changed.store(true, Ordering::Release);
        assert_eq!(
            guard.revalidate(),
            Err(BitgetRuntimeRecoveryError::ScopeDrift)
        );
    }
}
