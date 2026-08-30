use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use venue_domain::domain::{CommandId, NativeOrderFamily, PositionSide, Symbol};
use venue_gateway_api::GatewayMode;

use crate::{
    GATE_STAGE7_ORDER_PROFILE_VERSION, GateFreshRecoveryCandidate, GateFreshRecoveryError,
    GateRecoveryAuthorityRoots, GateRecoveryCoverage, GateRecoveryOwnerRoute, GateRecoverySurface,
};

const MAX_CONFIG_DIGEST_LEN: usize = 128;
const MAX_RUNTIME_REGISTRATIONS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateRuntimePositionMode {
    Hedge,
    Net,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateRuntimeOrderProfile {
    pub profile_version: u64,
    pub regular_supported: bool,
    pub conditional_supported: bool,
    pub algo_supported: bool,
}

impl GateRuntimeOrderProfile {
    #[must_use]
    pub const fn stage7_regular_only() -> Self {
        Self {
            profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
            regular_supported: true,
            conditional_supported: false,
            algo_supported: false,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GateRuntimeRecoveryRegistration {
    symbol: Symbol,
    strategy_kind: String,
    strategy_instance_id: String,
    config_digest: String,
    config_epoch: u64,
}

impl GateRuntimeRecoveryRegistration {
    pub fn verified(
        symbol: Symbol,
        strategy_kind: impl Into<String>,
        strategy_instance_id: impl Into<String>,
        config_digest: impl Into<String>,
        config_epoch: u64,
    ) -> Result<Self, GateFreshRecoveryError> {
        let strategy_kind = strategy_kind.into();
        let strategy_instance_id = strategy_instance_id.into();
        let config_digest = config_digest.into();
        if !valid_label(&strategy_kind)
            || !valid_label(&strategy_instance_id)
            || !valid_config_digest(&config_digest)
            || config_epoch == 0
        {
            return Err(GateFreshRecoveryError::RuntimeScope);
        }
        Ok(Self {
            symbol,
            strategy_kind,
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
    pub fn strategy_kind(&self) -> &str {
        &self.strategy_kind
    }

    #[must_use]
    pub fn strategy_instance_id(&self) -> &str {
        &self.strategy_instance_id
    }

    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GateRuntimeStructuredUnknown {
    command_id: CommandId,
    native_client_id: CommandId,
    family: NativeOrderFamily,
    symbol: Symbol,
}

impl GateRuntimeStructuredUnknown {
    pub fn verified(
        command_id: CommandId,
        native_client_id: CommandId,
        family: NativeOrderFamily,
        symbol: Symbol,
    ) -> Result<Self, GateFreshRecoveryError> {
        if family != NativeOrderFamily::UmOrder {
            return Err(GateFreshRecoveryError::RuntimeUnknown);
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
pub struct GateRuntimeRecoveryScopeInput {
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub config_digest: String,
    pub config_epoch: u64,
    pub connection_generation: u64,
    pub recovered_private_generation: u64,
    pub position_mode: GateRuntimePositionMode,
    pub order_profile: GateRuntimeOrderProfile,
    pub recovery_session_sha256: [u8; 32],
    pub authority_roots: GateRecoveryAuthorityRoots,
    pub registrations: Vec<GateRuntimeRecoveryRegistration>,
    pub owner_routes: Vec<GateRecoveryOwnerRoute>,
    pub structured_unknowns: Vec<GateRuntimeStructuredUnknown>,
}

/// Runtime facts frozen before the authenticated Gate collection begins. This value only commits
/// readback scope; it cannot issue capability, WAL, writer, or dispatch authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRuntimeRecoveryScope {
    mode: GatewayMode,
    trading_account_id: String,
    config_digest: String,
    config_epoch: u64,
    connection_generation: u64,
    recovered_private_generation: u64,
    private_generation: u64,
    position_mode: GateRuntimePositionMode,
    order_profile: GateRuntimeOrderProfile,
    recovery_session_sha256: [u8; 32],
    authority_roots: GateRecoveryAuthorityRoots,
    registrations: Vec<GateRuntimeRecoveryRegistration>,
    owner_routes: Vec<GateRecoveryOwnerRoute>,
    structured_unknowns: Vec<GateRuntimeStructuredUnknown>,
    commitment_sha256: [u8; 32],
}

impl GateRuntimeRecoveryScope {
    pub fn verified(
        mut input: GateRuntimeRecoveryScopeInput,
    ) -> Result<Self, GateFreshRecoveryError> {
        let private_generation = input
            .recovered_private_generation
            .checked_add(1)
            .ok_or(GateFreshRecoveryError::Generation)?;
        if input.mode != GatewayMode::Live
            || !valid_label(&input.trading_account_id)
            || !valid_config_digest(&input.config_digest)
            || input.config_epoch == 0
            || input.connection_generation == 0
            || input.position_mode != GateRuntimePositionMode::Hedge
            || input.order_profile != GateRuntimeOrderProfile::stage7_regular_only()
            || is_zero_digest(&input.recovery_session_sha256)
            || input.authority_roots == GateRecoveryAuthorityRoots::unbound()
            || input.registrations.is_empty()
            || input.registrations.len() > MAX_RUNTIME_REGISTRATIONS
        {
            return Err(GateFreshRecoveryError::RuntimeScope);
        }

        input.registrations.sort();
        if input
            .registrations
            .windows(2)
            .any(|pair| pair[0].symbol == pair[1].symbol)
        {
            return Err(GateFreshRecoveryError::RuntimeUniverse);
        }
        let mut strategy_instances = BTreeSet::new();
        if input.registrations.iter().any(|registration| {
            !strategy_instances.insert(registration.strategy_instance_id.clone())
                || registration.config_digest != input.config_digest
                || registration.config_epoch != input.config_epoch
        }) {
            return Err(GateFreshRecoveryError::RuntimeUniverse);
        }
        let by_symbol = input
            .registrations
            .iter()
            .map(|registration| (registration.symbol.clone(), registration))
            .collect::<BTreeMap<_, _>>();

        input.owner_routes.sort_by(|left, right| {
            left.owner()
                .symbol
                .cmp(&right.owner().symbol)
                .then_with(|| left.client_order_id().cmp(right.client_order_id()))
        });
        let mut owner_keys = BTreeSet::new();
        let mut owner_venue_ids = BTreeSet::new();
        for route in &input.owner_routes {
            let owner = route.owner();
            let Some(registration) = by_symbol.get(&owner.symbol) else {
                return Err(GateFreshRecoveryError::OwnerRoute);
            };
            if owner.account != input.trading_account_id
                || owner.strategy_instance_id != registration.strategy_instance_id
                || !owner_keys.insert((owner.symbol.clone(), route.client_order_id().clone()))
                || !owner_venue_ids.insert(route.venue_order_id().to_owned())
            {
                return Err(GateFreshRecoveryError::OwnerRoute);
            }
        }

        input.structured_unknowns.sort();
        let mut unknown_commands = BTreeSet::new();
        let mut unknown_native_ids = BTreeSet::new();
        for unknown in &input.structured_unknowns {
            if !by_symbol.contains_key(&unknown.symbol)
                || !unknown_commands.insert(unknown.command_id.clone())
                || !unknown_native_ids
                    .insert((unknown.symbol.clone(), unknown.native_client_id.clone()))
            {
                return Err(GateFreshRecoveryError::RuntimeUnknown);
            }
        }

        let commitment_sha256 = runtime_scope_commitment(
            &input,
            private_generation,
            &input.registrations,
            &input.owner_routes,
            &input.structured_unknowns,
        );
        Ok(Self {
            mode: input.mode,
            trading_account_id: input.trading_account_id,
            config_digest: input.config_digest,
            config_epoch: input.config_epoch,
            connection_generation: input.connection_generation,
            recovered_private_generation: input.recovered_private_generation,
            private_generation,
            position_mode: input.position_mode,
            order_profile: input.order_profile,
            recovery_session_sha256: input.recovery_session_sha256,
            authority_roots: input.authority_roots,
            registrations: input.registrations,
            owner_routes: input.owner_routes,
            structured_unknowns: input.structured_unknowns,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub fn trading_account_id(&self) -> &str {
        &self.trading_account_id
    }

    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn recovered_private_generation(&self) -> u64 {
        self.recovered_private_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn position_mode(&self) -> GateRuntimePositionMode {
        self.position_mode
    }

    #[must_use]
    pub const fn order_profile(&self) -> GateRuntimeOrderProfile {
        self.order_profile
    }

    #[must_use]
    pub const fn recovery_session_sha256(&self) -> &[u8; 32] {
        &self.recovery_session_sha256
    }

    #[must_use]
    pub const fn authority_roots(&self) -> &GateRecoveryAuthorityRoots {
        &self.authority_roots
    }

    #[must_use]
    pub fn registrations(&self) -> &[GateRuntimeRecoveryRegistration] {
        &self.registrations
    }

    #[must_use]
    pub fn owner_routes(&self) -> &[GateRecoveryOwnerRoute] {
        &self.owner_routes
    }

    #[must_use]
    pub fn structured_unknowns(&self) -> &[GateRuntimeStructuredUnknown] {
        &self.structured_unknowns
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }

    pub(crate) fn validate_authenticated_universe<'a, I>(
        &self,
        mode: GatewayMode,
        trading_account_id: &str,
        symbols: I,
    ) -> Result<(), GateFreshRecoveryError>
    where
        I: IntoIterator<Item = &'a Symbol>,
    {
        let symbols = symbols.into_iter().cloned().collect::<BTreeSet<_>>();
        let registrations = self
            .registrations
            .iter()
            .map(|registration| registration.symbol.clone())
            .collect::<BTreeSet<_>>();
        if self.mode != mode
            || self.trading_account_id != trading_account_id
            || symbols != registrations
        {
            return Err(GateFreshRecoveryError::RuntimeUniverse);
        }
        Ok(())
    }
}

/// Runtime supplies a fresh commitment lookup. The Gate adapter calls it around network awaits;
/// matching this public trait alone never grants recovery or mutation authority.
pub trait GateRuntimeRecoveryRevalidator {
    fn current_scope_sha256(&self) -> Option<[u8; 32]>;
}

#[derive(Clone, Copy)]
pub(crate) struct GateRuntimeRecoveryAwaitGuard<'a> {
    expected: [u8; 32],
    revalidator: &'a dyn GateRuntimeRecoveryRevalidator,
}

impl<'a> GateRuntimeRecoveryAwaitGuard<'a> {
    pub(crate) fn new(
        scope: &GateRuntimeRecoveryScope,
        revalidator: &'a dyn GateRuntimeRecoveryRevalidator,
    ) -> Result<Self, GateFreshRecoveryError> {
        let guard = Self {
            expected: *scope.commitment_sha256(),
            revalidator,
        };
        guard.revalidate()?;
        Ok(guard)
    }

    pub(crate) fn revalidate(self) -> Result<(), GateFreshRecoveryError> {
        if self.revalidator.current_scope_sha256() == Some(self.expected) {
            Ok(())
        } else {
            Err(GateFreshRecoveryError::RuntimeScopeDrift)
        }
    }
}

/// Complete Gate-local bridge value consumed by the later Node/runtime integration goal.
/// It contains read-only evidence and exposes no transport, credential, writer, or permit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRuntimeRecoveryBundle {
    runtime_scope: GateRuntimeRecoveryScope,
    candidate: GateFreshRecoveryCandidate,
    commitment_sha256: [u8; 32],
}

impl GateRuntimeRecoveryBundle {
    pub(crate) fn from_candidate(
        runtime_scope: GateRuntimeRecoveryScope,
        candidate: GateFreshRecoveryCandidate,
    ) -> Result<Self, GateFreshRecoveryError> {
        let scope = candidate.scope();
        if scope.mode() != runtime_scope.mode
            || scope.trading_account_id() != runtime_scope.trading_account_id
            || scope.config_digest() != runtime_scope.config_digest
            || scope.config_epoch() != runtime_scope.config_epoch
            || scope.connection_generation() != runtime_scope.connection_generation
            || scope.recovered_private_generation() != runtime_scope.recovered_private_generation
            || scope.private_generation() != runtime_scope.private_generation
            || scope.authority_roots() != &runtime_scope.authority_roots
            || scope.runtime_scope_sha256() != Some(runtime_scope.commitment_sha256())
            || scope.symbol_universe()
                != runtime_scope
                    .registrations
                    .iter()
                    .map(|registration| registration.symbol.clone())
                    .collect::<Vec<_>>()
        {
            return Err(GateFreshRecoveryError::RuntimeScopeDrift);
        }
        for readback in candidate.symbol_readbacks() {
            let private = readback.candidate();
            if private.order_families.scope().profile_version
                != runtime_scope.order_profile.profile_version
                || private.positions[0].side != PositionSide::Long
                || private.positions[1].side != PositionSide::Short
                || private
                    .positions
                    .iter()
                    .any(|position| position.symbol != *readback.symbol())
            {
                return Err(GateFreshRecoveryError::RuntimeProfile);
            }
        }
        validate_bundle_surfaces(&candidate, runtime_scope.order_profile)?;
        let commitment_sha256 = bundle_commitment(&runtime_scope, &candidate);
        Ok(Self {
            runtime_scope,
            candidate,
            commitment_sha256,
        })
    }

    #[must_use]
    pub const fn runtime_scope(&self) -> &GateRuntimeRecoveryScope {
        &self.runtime_scope
    }

    #[must_use]
    pub const fn candidate(&self) -> &GateFreshRecoveryCandidate {
        &self.candidate
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }
}

fn validate_bundle_surfaces(
    candidate: &GateFreshRecoveryCandidate,
    profile: GateRuntimeOrderProfile,
) -> Result<(), GateFreshRecoveryError> {
    for surface in [
        GateRecoverySurface::Account,
        GateRecoverySurface::Positions,
        GateRecoverySurface::RegularOrders,
        GateRecoverySurface::ConditionalOrders,
        GateRecoverySurface::AlgoOrders,
        GateRecoverySurface::FillsCursor,
    ] {
        let coverage = candidate
            .surface(surface)
            .ok_or(GateFreshRecoveryError::MissingSurface)?
            .coverage();
        let valid = match surface {
            GateRecoverySurface::ConditionalOrders | GateRecoverySurface::AlgoOrders => matches!(
                coverage,
                GateRecoveryCoverage::Unsupported { profile_version }
                    if *profile_version == profile.profile_version
            ),
            _ => matches!(coverage, GateRecoveryCoverage::Complete { .. }),
        };
        if !valid {
            return Err(GateFreshRecoveryError::RuntimeProfile);
        }
    }
    Ok(())
}

fn runtime_scope_commitment(
    input: &GateRuntimeRecoveryScopeInput,
    private_generation: u64,
    registrations: &[GateRuntimeRecoveryRegistration],
    owner_routes: &[GateRecoveryOwnerRoute],
    structured_unknowns: &[GateRuntimeStructuredUnknown],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue-gate-runtime-recovery-scope-v1");
    update_part(&mut digest, input.mode.as_str().as_bytes());
    update_part(&mut digest, input.trading_account_id.as_bytes());
    update_part(&mut digest, input.config_digest.as_bytes());
    digest.update(input.config_epoch.to_be_bytes());
    digest.update(input.connection_generation.to_be_bytes());
    digest.update(input.recovered_private_generation.to_be_bytes());
    digest.update(private_generation.to_be_bytes());
    digest.update([1]);
    digest.update(input.order_profile.profile_version.to_be_bytes());
    digest.update([
        u8::from(input.order_profile.regular_supported),
        u8::from(input.order_profile.conditional_supported),
        u8::from(input.order_profile.algo_supported),
    ]);
    digest.update(input.recovery_session_sha256);
    digest.update(input.authority_roots.owner());
    digest.update(input.authority_roots.wal());
    digest.update(input.authority_roots.unknown());
    for registration in registrations {
        update_part(&mut digest, registration.symbol.to_string().as_bytes());
        update_part(&mut digest, registration.strategy_kind.as_bytes());
        update_part(&mut digest, registration.strategy_instance_id.as_bytes());
        update_part(&mut digest, registration.config_digest.as_bytes());
        digest.update(registration.config_epoch.to_be_bytes());
    }
    for route in owner_routes {
        update_part(&mut digest, route.client_order_id().as_str().as_bytes());
        update_part(&mut digest, route.venue_order_id().as_bytes());
        update_part(&mut digest, route.owner().strategy_instance_id.as_bytes());
        update_part(&mut digest, route.owner().run_id.as_bytes());
        update_part(&mut digest, route.owner().symbol.to_string().as_bytes());
    }
    for unknown in structured_unknowns {
        update_part(&mut digest, unknown.command_id.as_str().as_bytes());
        update_part(&mut digest, unknown.native_client_id.as_str().as_bytes());
        digest.update([native_family_tag(unknown.family)]);
        update_part(&mut digest, unknown.symbol.to_string().as_bytes());
    }
    digest.finalize().into()
}

fn bundle_commitment(
    scope: &GateRuntimeRecoveryScope,
    candidate: &GateFreshRecoveryCandidate,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"venue-gate-runtime-recovery-bundle-v1");
    digest.update(scope.commitment_sha256());
    digest.update(candidate.commitment_sha256());
    digest.update(candidate.scope().attempt_id().to_be_bytes());
    digest.update(candidate.scope().private_generation().to_be_bytes());
    digest.update(candidate.scope().deadline_at_ms().to_be_bytes());
    digest.finalize().into()
}

fn update_part(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

const fn native_family_tag(family: NativeOrderFamily) -> u8 {
    match family {
        NativeOrderFamily::UmOrder => 1,
        NativeOrderFamily::UmConditional => 2,
        NativeOrderFamily::UmAlgo => 3,
    }
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
        })
}

fn valid_config_digest(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONFIG_DIGEST_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn is_zero_digest(value: &[u8; 32]) -> bool {
    value.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct AwaitDrift {
        drifted: AtomicBool,
    }

    impl GateRuntimeRecoveryRevalidator for AwaitDrift {
        fn current_scope_sha256(&self) -> Option<[u8; 32]> {
            Some(if self.drifted.load(Ordering::Acquire) {
                [2; 32]
            } else {
                [1; 32]
            })
        }
    }

    #[tokio::test]
    async fn await_guard_rejects_scope_that_changes_while_an_await_is_in_flight() {
        let revalidator = AwaitDrift {
            drifted: AtomicBool::new(false),
        };
        let guard = GateRuntimeRecoveryAwaitGuard {
            expected: [1; 32],
            revalidator: &revalidator,
        };
        assert_eq!(guard.revalidate(), Ok(()));
        async {
            tokio::task::yield_now().await;
            revalidator.drifted.store(true, Ordering::Release);
        }
        .await;
        assert_eq!(
            guard.revalidate(),
            Err(GateFreshRecoveryError::RuntimeScopeDrift)
        );
    }
}
