use std::process::ExitCode;

#[cfg(test)]
use std::collections::BTreeSet;

#[cfg(test)]
use serde::Serialize;
#[cfg(test)]
use sha2::{Digest, Sha256};
#[cfg(test)]
use venue_gateway_api::GatewayMode;
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};
#[cfg(test)]
use venue_gateway_hyperliquid::{
    HYPERLIQUID_CAPABILITY_PROBE_SCHEMA, HyperliquidAccountSnapshot, HyperliquidActionKind,
    HyperliquidFillWindowEvidence, HyperliquidOpenOrdersSnapshot, HyperliquidOrderFamily,
    HyperliquidOrderFamilyCoverage, HyperliquidOrderLookup, HyperliquidOrderStatus,
    HyperliquidPayloadScope,
};
use venue_gateway_hyperliquid::{
    HyperliquidConfig, HyperliquidGatewayBinding, HyperliquidNodeCandidate, capabilities,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_runtime, report_result,
};
use venue_runtime::account::PhysicalRecoveryReadbackManifest;
#[cfg(test)]
use venue_runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryManifestError,
    PhysicalRecoveryScope,
};

const PROGRAM: &str = "venue-node-hyperliquid";
#[cfg(test)]
const HYPERLIQUID_RECOVERY_ORDER_PROFILE_VERSION: u64 = 1;

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Hyperliquid)?;
    launch.require_no_runtime_arguments()?;
    let binding = HyperliquidGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Hyperliquid))?;
    let adapter = HyperliquidConfig::for_binding(&binding);
    AdapterIsolation {
        venue: VenueId::Hyperliquid,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.websocket()],
        credential_environment: &[
            "HYPERLIQUID_MASTER_ADDRESS",
            "HYPERLIQUID_USER_ADDRESS",
            "HYPERLIQUID_VAULT_ADDRESS",
            "HYPERLIQUID_AGENT_NAME",
            "HYPERLIQUID_AGENT_ADDRESS",
            "HYPERLIQUID_AGENT_PRIVATE_KEY",
        ],
        credential_prefix: "HYPERLIQUID_",
        account_binding: "usdc_perpetual_agent",
    }
    .validate(launch.binding())?;
    let _isolated_artifacts_root = launch.artifacts_root();
    let candidate_bridge = HyperliquidPhysicalGatewayCandidate::new(launch.binding().clone(), None)
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Hyperliquid))?;
    let advertised = candidate_bridge.capability_snapshot();
    if !advertised.flags.is_empty() || candidate_bridge.persisted_probe_candidate().is_some() {
        return Err(NodeError::UnexpectedAdapterCapability(VenueId::Hyperliquid));
    }
    reject_unintegrated_runtime(VenueId::Hyperliquid, launch.binding().mode, capabilities())
}

/// Fixed-binary adapter boundary. A persisted probe can be attached for validation and action
/// preparation, but the `PhysicalGateway` view remains closed until the shared host can explicitly
/// promote candidate evidence and drive the async readback/action surfaces without bypassing its
/// Control, Owner, WAL, writer and Canary sequence.
struct HyperliquidPhysicalGatewayCandidate {
    binding: GatewayBinding,
    persisted_probe: Option<HyperliquidNodeCandidate>,
}

impl HyperliquidPhysicalGatewayCandidate {
    fn new(
        binding: GatewayBinding,
        persisted_probe: Option<HyperliquidNodeCandidate>,
    ) -> Result<Self, HyperliquidBridgeError> {
        if binding.venue != VenueId::Hyperliquid
            || persisted_probe.as_ref().is_some_and(|candidate| {
                candidate.candidate_capability_snapshot().binding != binding
            })
        {
            return Err(HyperliquidBridgeError::Binding);
        }
        Ok(Self {
            binding,
            persisted_probe,
        })
    }

    fn persisted_probe_candidate(&self) -> Option<&CapabilitySnapshot> {
        self.persisted_probe
            .as_ref()
            .map(HyperliquidNodeCandidate::candidate_capability_snapshot)
    }

    fn closed_capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.binding.clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: CapabilityFlags::empty(),
        }
    }

    /// Persisted probe evidence cannot be rebound to a fresh recovery scope. Until an opaque
    /// collector binds expiry, raw replay, Unknown and Owner proof to the exact attempt and roots,
    /// production recovery is unavailable rather than accepting a caller-assembled manifest.
    #[allow(dead_code)]
    fn recovery_readback_manifest(
        &self,
    ) -> Result<PhysicalRecoveryReadbackManifest, HyperliquidBridgeError> {
        Err(HyperliquidBridgeError::RecoveryUnavailable)
    }
}

/// Exact read-only `orderStatus` response retained with its request identity and raw response hash.
/// `Unknown` remains unresolved evidence; it is never converted into an empty order-family page.
#[cfg(test)]
struct HyperliquidOrderStatusReadback<'a> {
    private_generation: u64,
    observed_ms: u64,
    lookup: &'a HyperliquidOrderLookup,
    status: &'a HyperliquidOrderStatus,
    response_sha256: [u8; 32],
}

/// All native read responses needed by the mapping. Required traditional faces are `Option`s so
/// omission reaches an explicit fail-closed error instead of becoming a fabricated empty result.
#[cfg(test)]
struct HyperliquidRecoveryReadback<'a> {
    account: Option<&'a HyperliquidAccountSnapshot>,
    orders: Option<&'a HyperliquidOpenOrdersSnapshot>,
    fills: Option<&'a HyperliquidFillWindowEvidence>,
    order_status: &'a [HyperliquidOrderStatusReadback<'a>],
    owner_order_ids: Option<&'a BTreeSet<String>>,
    raw_meta: &'a [u8],
    raw_account: &'a [u8],
    collected_at_ms: u64,
}

#[cfg(test)]
#[derive(serde::Deserialize)]
struct HyperliquidProbeRecoveryAnchor {
    payload: HyperliquidProbeRecoveryPayload,
    commitment_keccak256: String,
    collector_scope_sha256: [u8; 32],
    collector_attempt_id: u64,
    collector_meta_sha256: [u8; 32],
    collector_account_sha256: [u8; 32],
    collector_orders_sha256: [u8; 32],
}

#[cfg(test)]
#[derive(serde::Deserialize)]
struct HyperliquidProbeRecoveryPayload {
    schema_version: u16,
    binding: GatewayBinding,
    observed_ms: u64,
    expires_ms: u64,
    private_generation: u64,
    master_address: String,
    user_address: String,
    vault_address: Option<String>,
    native_coin: String,
    account_exchange_time_ms: u64,
    orders_observed_ms: u64,
    meta_commitment_keccak256: String,
    account_commitment_keccak256: String,
    orders_commitment_keccak256: String,
    fill_window: serde_json::Value,
    actions: [HyperliquidProbeRecoveryAction; 3],
}

#[cfg(test)]
#[derive(serde::Deserialize)]
struct HyperliquidProbeRecoveryAction {
    kind: HyperliquidActionKind,
}

#[cfg(test)]
fn map_recovery_readback(
    binding: &GatewayBinding,
    scope: PhysicalRecoveryScope,
    attempt_id: u64,
    anchor: &HyperliquidProbeRecoveryAnchor,
    readback: HyperliquidRecoveryReadback<'_>,
) -> Result<PhysicalRecoveryReadbackManifest, HyperliquidBridgeError> {
    validate_recovery_anchor(
        binding,
        &scope,
        attempt_id,
        readback.collected_at_ms,
        anchor,
    )?;
    let account = readback
        .account
        .ok_or(HyperliquidBridgeError::RecoveryEvidence)?;
    let orders = readback
        .orders
        .ok_or(HyperliquidBridgeError::RecoveryEvidence)?;
    let fills = readback
        .fills
        .ok_or(HyperliquidBridgeError::RecoveryEvidence)?;
    validate_payload_scope(&account.scope, anchor)?;
    validate_payload_scope(&orders.scope, anchor)?;
    if account.exchange_time_ms != anchor.payload.account_exchange_time_ms
        || orders.observed_at_ms != anchor.payload.orders_observed_ms
        || serde_json::to_value(fills).map_err(|_| HyperliquidBridgeError::RecoveryEvidence)?
            != anchor.payload.fill_window
        || orders.regular_coverage != HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot
        || orders.conditional_coverage != HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot
        || orders.algo_coverage != HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders
        || sha256(readback.raw_meta) != anchor.collector_meta_sha256
        || sha256(readback.raw_account) != anchor.collector_account_sha256
        || sha256(&orders.raw_payload) != anchor.collector_orders_sha256
    {
        return Err(HyperliquidBridgeError::RecoveryEvidence);
    }

    let status_sha256 = order_status_commitment(anchor, readback.order_status)?;
    let common_sha256 = recovery_common_commitment(scope.commitment_sha256(), anchor)?;
    let account_sha256 = evidence_commitment(
        b"hyperliquid-recovery-account-v1",
        &common_sha256,
        &(account.exchange_time_ms, &account.balance),
    )?;
    let positions_sha256 = evidence_commitment(
        b"hyperliquid-recovery-positions-v1",
        &common_sha256,
        &(account.exchange_time_ms, &account.position),
    )?;
    let order_payload_sha256 = sha256(&orders.raw_payload);
    let regular_count = family_count(orders, HyperliquidOrderFamily::Regular)?;
    let conditional_count = family_count(orders, HyperliquidOrderFamily::Conditional)?;
    let actual_order_ids = orders
        .orders
        .iter()
        .map(|order| order.order.order_id.clone())
        .collect::<BTreeSet<_>>();
    if readback.owner_order_ids != Some(&actual_order_ids)
        || (regular_count == 0
            && readback
                .order_status
                .iter()
                .any(|readback| matches!(readback.status, HyperliquidOrderStatus::Unknown { .. })))
    {
        return Err(HyperliquidBridgeError::RecoveryEvidence);
    }
    let fills_count =
        u64::try_from(fills.fill_count()).map_err(|_| HyperliquidBridgeError::RecoveryEvidence)?;
    let generation = anchor.payload.private_generation;

    let mut receipts = vec![
        PhysicalReadbackReceipt::verified_complete(
            &scope,
            PhysicalReadbackSurface::Account,
            attempt_id,
            generation,
            account_sha256,
            1,
        )?,
        PhysicalReadbackReceipt::verified_complete(
            &scope,
            PhysicalReadbackSurface::Positions,
            attempt_id,
            generation,
            positions_sha256,
            u64::from(account.position.is_some()),
        )?,
        order_family_receipt(
            &scope,
            attempt_id,
            generation,
            PhysicalReadbackSurface::UmOrder,
            orders.regular_coverage,
            regular_count,
            &common_sha256,
            &order_payload_sha256,
            &status_sha256,
        )?,
        order_family_receipt(
            &scope,
            attempt_id,
            generation,
            PhysicalReadbackSurface::UmConditional,
            orders.conditional_coverage,
            conditional_count,
            &common_sha256,
            &order_payload_sha256,
            &status_sha256,
        )?,
        order_family_receipt(
            &scope,
            attempt_id,
            generation,
            PhysicalReadbackSurface::UmAlgo,
            orders.algo_coverage,
            0,
            &common_sha256,
            &order_payload_sha256,
            &status_sha256,
        )?,
    ];
    receipts.push(PhysicalReadbackReceipt::verified_complete(
        &scope,
        PhysicalReadbackSurface::FillsCursor,
        attempt_id,
        generation,
        evidence_commitment(
            b"hyperliquid-recovery-fills-window-v1",
            &common_sha256,
            fills,
        )?,
        fills_count,
    )?);

    Ok(PhysicalRecoveryReadbackManifest::verified(scope, receipts)?)
}

#[cfg(test)]
fn validate_recovery_anchor(
    binding: &GatewayBinding,
    scope: &PhysicalRecoveryScope,
    attempt_id: u64,
    collected_at_ms: u64,
    anchor: &HyperliquidProbeRecoveryAnchor,
) -> Result<(), HyperliquidBridgeError> {
    let expected_actions = [
        HyperliquidActionKind::AloPlace,
        HyperliquidActionKind::Cancel,
        HyperliquidActionKind::IocReduceOnly,
    ];
    let expected_user = anchor
        .payload
        .vault_address
        .as_deref()
        .unwrap_or(&anchor.payload.master_address);
    if binding != scope.binding()
        || binding != &anchor.payload.binding
        || anchor.collector_scope_sha256 != *scope.commitment_sha256()
        || anchor.collector_attempt_id != attempt_id
        || anchor.payload.schema_version != HYPERLIQUID_CAPABILITY_PROBE_SCHEMA
        || anchor.payload.binding.venue != VenueId::Hyperliquid
        || anchor.payload.user_address != expected_user
        || anchor.payload.native_coin != binding.symbol.base()
        || anchor.payload.private_generation == 0
        || collected_at_ms < anchor.payload.observed_ms
        || collected_at_ms >= anchor.payload.expires_ms
        || anchor.payload.expires_ms <= anchor.payload.observed_ms
        || !valid_hex_commitment(&anchor.payload.meta_commitment_keccak256)
        || !valid_hex_commitment(&anchor.payload.account_commitment_keccak256)
        || !valid_hex_commitment(&anchor.payload.orders_commitment_keccak256)
        || anchor.commitment_keccak256.len() != 64
        || !anchor
            .commitment_keccak256
            .as_bytes()
            .iter()
            .all(u8::is_ascii_hexdigit)
        || anchor
            .payload
            .actions
            .iter()
            .map(|action| action.kind)
            .ne(expected_actions)
    {
        return Err(HyperliquidBridgeError::RecoveryEvidence);
    }
    Ok(())
}

#[cfg(test)]
fn valid_hex_commitment(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

#[cfg(test)]
fn validate_payload_scope(
    scope: &HyperliquidPayloadScope,
    anchor: &HyperliquidProbeRecoveryAnchor,
) -> Result<(), HyperliquidBridgeError> {
    if scope.binding().gateway().gateway_binding() != &anchor.payload.binding
        || scope.user_address() != anchor.payload.user_address
        || scope.native_coin() != anchor.payload.native_coin
    {
        return Err(HyperliquidBridgeError::RecoveryEvidence);
    }
    Ok(())
}

#[cfg(test)]
fn family_count(
    orders: &HyperliquidOpenOrdersSnapshot,
    family: HyperliquidOrderFamily,
) -> Result<u64, HyperliquidBridgeError> {
    u64::try_from(
        orders
            .orders
            .iter()
            .filter(|order| order.family == family)
            .count(),
    )
    .map_err(|_| HyperliquidBridgeError::RecoveryEvidence)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn order_family_receipt(
    scope: &PhysicalRecoveryScope,
    attempt_id: u64,
    generation: u64,
    surface: PhysicalReadbackSurface,
    coverage: HyperliquidOrderFamilyCoverage,
    record_count: u64,
    common_sha256: &[u8; 32],
    order_payload_sha256: &[u8; 32],
    status_sha256: &[u8; 32],
) -> Result<PhysicalReadbackReceipt, HyperliquidBridgeError> {
    let mut evidence = Sha256::new();
    commit_bytes(&mut evidence, b"hyperliquid-recovery-order-family-v1");
    commit_bytes(&mut evidence, common_sha256);
    commit_bytes(&mut evidence, &[order_surface_tag(surface)?]);
    commit_bytes(&mut evidence, order_payload_sha256);
    commit_bytes(&mut evidence, status_sha256);
    commit_bytes(&mut evidence, &record_count.to_be_bytes());
    let evidence_sha256 = evidence.finalize().into();
    match coverage {
        HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot => {
            PhysicalReadbackReceipt::verified_complete(
                scope,
                surface,
                attempt_id,
                generation,
                evidence_sha256,
                record_count,
            )
            .map_err(Into::into)
        }
        HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders => {
            if surface == PhysicalReadbackSurface::UmOrder {
                return Err(HyperliquidBridgeError::RecoveryEvidence);
            }
            PhysicalReadbackReceipt::verified_unsupported_order_family(
                scope,
                surface,
                attempt_id,
                generation,
                evidence_sha256,
                HYPERLIQUID_RECOVERY_ORDER_PROFILE_VERSION,
            )
            .map_err(Into::into)
        }
    }
}

#[cfg(test)]
fn order_surface_tag(surface: PhysicalReadbackSurface) -> Result<u8, HyperliquidBridgeError> {
    match surface {
        PhysicalReadbackSurface::UmOrder => Ok(1),
        PhysicalReadbackSurface::UmConditional => Ok(2),
        PhysicalReadbackSurface::UmAlgo => Ok(3),
        PhysicalReadbackSurface::Account
        | PhysicalReadbackSurface::Positions
        | PhysicalReadbackSurface::FillsCursor => Err(HyperliquidBridgeError::RecoveryEvidence),
    }
}

#[cfg(test)]
fn order_status_commitment(
    anchor: &HyperliquidProbeRecoveryAnchor,
    readbacks: &[HyperliquidOrderStatusReadback<'_>],
) -> Result<[u8; 32], HyperliquidBridgeError> {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"hyperliquid-recovery-order-status-v1");
    commit_bytes(
        &mut digest,
        &u64::try_from(readbacks.len())
            .map_err(|_| HyperliquidBridgeError::RecoveryEvidence)?
            .to_be_bytes(),
    );
    for readback in readbacks {
        if readback.private_generation != anchor.payload.private_generation
            || readback.observed_ms == 0
            || readback.response_sha256.iter().all(|byte| *byte == 0)
        {
            return Err(HyperliquidBridgeError::RecoveryEvidence);
        }
        validate_order_status(readback, anchor)?;
        commit_bytes(&mut digest, &readback.private_generation.to_be_bytes());
        commit_bytes(&mut digest, &readback.observed_ms.to_be_bytes());
        commit_lookup(&mut digest, readback.lookup);
        commit_bytes(&mut digest, &readback.response_sha256);
        commit_order_status(&mut digest, readback.status)?;
    }
    Ok(digest.finalize().into())
}

#[cfg(test)]
fn validate_order_status(
    readback: &HyperliquidOrderStatusReadback<'_>,
    anchor: &HyperliquidProbeRecoveryAnchor,
) -> Result<(), HyperliquidBridgeError> {
    let (scope, matches_lookup, exchange_time_ms) = match readback.status {
        HyperliquidOrderStatus::Unknown { scope, lookup } => {
            (scope, lookup == readback.lookup, None)
        }
        HyperliquidOrderStatus::Known {
            scope,
            order_id,
            client_order_id,
            exchange_time_ms,
            ..
        } => {
            let matches_lookup = match readback.lookup {
                HyperliquidOrderLookup::OrderId(expected) => expected == order_id,
                HyperliquidOrderLookup::ClientOrderId(expected) => {
                    matches!(client_order_id, venue_domain::domain::FieldState::Known(actual) if actual.eq_ignore_ascii_case(expected))
                }
            };
            (scope, matches_lookup, Some(*exchange_time_ms))
        }
    };
    validate_payload_scope(scope, anchor)?;
    if !matches_lookup || exchange_time_ms.is_some_and(|value| value > readback.observed_ms) {
        return Err(HyperliquidBridgeError::RecoveryEvidence);
    }
    Ok(())
}

#[cfg(test)]
fn recovery_common_commitment(
    scope_sha256: &[u8; 32],
    anchor: &HyperliquidProbeRecoveryAnchor,
) -> Result<[u8; 32], HyperliquidBridgeError> {
    evidence_commitment(
        b"hyperliquid-recovery-common-v1",
        scope_sha256,
        &(
            &anchor.payload.binding,
            mode_tag(anchor.payload.binding.mode),
            anchor.payload.observed_ms,
            anchor.payload.expires_ms,
            anchor.payload.private_generation,
            &anchor.payload.user_address,
            &anchor.payload.vault_address,
            &anchor.payload.native_coin,
            (
                &anchor.payload.meta_commitment_keccak256,
                &anchor.payload.account_commitment_keccak256,
                &anchor.payload.orders_commitment_keccak256,
                &anchor.commitment_keccak256,
            ),
            (
                &anchor.collector_scope_sha256,
                anchor.collector_attempt_id,
                &anchor.collector_meta_sha256,
                &anchor.collector_account_sha256,
                &anchor.collector_orders_sha256,
            ),
            anchor
                .payload
                .actions
                .iter()
                .map(|action| action.kind)
                .collect::<Vec<_>>(),
        ),
    )
}

#[cfg(test)]
fn mode_tag(mode: GatewayMode) -> u8 {
    match mode {
        GatewayMode::Test => 1,
        GatewayMode::Live => 2,
    }
}

#[cfg(test)]
fn evidence_commitment<T: Serialize>(
    domain: &[u8],
    common_sha256: &[u8; 32],
    value: &T,
) -> Result<[u8; 32], HyperliquidBridgeError> {
    let encoded =
        serde_json::to_vec(value).map_err(|_| HyperliquidBridgeError::RecoveryEvidence)?;
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, domain);
    commit_bytes(&mut digest, common_sha256);
    commit_bytes(&mut digest, &encoded);
    Ok(digest.finalize().into())
}

#[cfg(test)]
fn commit_order_status(
    digest: &mut Sha256,
    status: &HyperliquidOrderStatus,
) -> Result<(), HyperliquidBridgeError> {
    match status {
        HyperliquidOrderStatus::Unknown { .. } => commit_bytes(digest, &[0]),
        HyperliquidOrderStatus::Known {
            order_id,
            client_order_id,
            side,
            limit_price,
            original_quantity,
            remaining_quantity,
            reduce_only,
            native_order_type,
            time_in_force,
            state,
            exchange_time_ms,
            ..
        } => {
            commit_bytes(digest, &[1]);
            let encoded = serde_json::to_vec(&(
                order_id,
                client_order_id,
                side,
                limit_price,
                original_quantity,
                remaining_quantity,
                reduce_only,
                native_order_type,
                time_in_force,
                state,
                exchange_time_ms,
            ))
            .map_err(|_| HyperliquidBridgeError::RecoveryEvidence)?;
            commit_bytes(digest, &encoded);
        }
    }
    Ok(())
}

#[cfg(test)]
fn commit_lookup(digest: &mut Sha256, lookup: &HyperliquidOrderLookup) {
    match lookup {
        HyperliquidOrderLookup::OrderId(value) => {
            commit_bytes(digest, &[1]);
            commit_bytes(digest, &value.to_be_bytes());
        }
        HyperliquidOrderLookup::ClientOrderId(value) => {
            commit_bytes(digest, &[2]);
            commit_bytes(digest, value.as_bytes());
        }
    }
}

#[cfg(test)]
fn sha256(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

#[cfg(test)]
fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

impl PhysicalGateway for HyperliquidPhysicalGatewayCandidate {
    type Error = HyperliquidBridgeError;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.closed_capability_snapshot()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        if request.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        if receipt.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        if permit.binding() != &self.binding {
            return GatewayDispatchResult::Unknown;
        }
        GatewayDispatchResult::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum HyperliquidBridgeError {
    #[error("Hyperliquid fixed-node bridge binding does not match the launch scope")]
    Binding,
    #[error(
        "Hyperliquid candidate bridge lacks shared async runtime, readback promotion and command mapping"
    )]
    SharedIntegration,
    #[error(
        "Hyperliquid recovery is unavailable without a fresh scope-bound collector and structured Unknown/Owner proof"
    )]
    RecoveryUnavailable,
    #[cfg(test)]
    #[error("Hyperliquid recovery readback evidence is missing, stale, or scope-inconsistent")]
    RecoveryEvidence,
    #[cfg(test)]
    #[error(transparent)]
    RecoveryManifest(#[from] PhysicalRecoveryManifestError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::GatewayMode;
    use venue_gateway_hyperliquid::{
        HyperliquidReadBinding, parse_clearinghouse_snapshot, parse_frontend_open_orders_snapshot,
        parse_order_status, parse_perp_meta,
    };
    use venue_runtime::account::{PhysicalReadbackCoverage, PhysicalRecoveryAuthorityRoots};

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
    const USER: &str = "0x0000000000000000000000000000000000000001";
    const PROBE_OBSERVED_MS: u64 = 1_724_361_547_000;
    const COLLECTED_AT_MS: u64 = PROBE_OBSERVED_MS + 1;
    const PROBE_EXPIRES_MS: u64 = PROBE_OBSERVED_MS + 60_000;
    const META: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-hyperliquid/fixtures/perp-meta.json");
    const ACCOUNT_SNAPSHOT: &[u8] = include_bytes!(
        "../../../../crates/venue-gateway-hyperliquid/fixtures/clearinghouse-state.json"
    );
    const ORDERS: &[u8] = include_bytes!(
        "../../../../crates/venue-gateway-hyperliquid/fixtures/frontend-open-orders-family.json"
    );
    const ORDER_STATUS: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-hyperliquid/fixtures/order-status.json");

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            ACCOUNT,
            "BTC/USDC".parse()?,
        )?)
    }

    fn assert_physical_gateway<T: PhysicalGateway>() {}

    struct RecoveryFixture {
        scope: PhysicalRecoveryScope,
        anchor: HyperliquidProbeRecoveryAnchor,
        account: HyperliquidAccountSnapshot,
        orders: HyperliquidOpenOrdersSnapshot,
        fills: HyperliquidFillWindowEvidence,
        lookup: HyperliquidOrderLookup,
        status: HyperliquidOrderStatus,
        owner_order_ids: BTreeSet<String>,
    }

    fn recovery_fixture() -> Result<RecoveryFixture, Box<dyn std::error::Error>> {
        let selected = binding(GatewayMode::Test)?;
        let read_binding =
            HyperliquidReadBinding::new(HyperliquidGatewayBinding::new(selected.clone())?, USER)?;
        let meta = parse_perp_meta(META, &read_binding)?;
        let account = parse_clearinghouse_snapshot(ACCOUNT_SNAPSHOT, &meta)?;
        let observed_ms = PROBE_OBSERVED_MS;
        let orders = parse_frontend_open_orders_snapshot(ORDERS, &meta, observed_ms)?;
        let owner_order_ids = orders
            .orders
            .iter()
            .map(|order| order.order.order_id.clone())
            .collect();
        let fill_window_json = serde_json::json!({
            "gateway_binding": selected,
            "user_address": USER,
            "native_coin": "BTC",
            "private_generation": 11,
            "begin_ms": 1_724_361_546_000_u64,
            "end_ms": observed_ms,
            "fill_count": 2,
            "private_overlap_count": 1,
            "maximum_retained_fills": 10_000,
            "complete": true,
            "fill_commitment_keccak256": "11".repeat(32),
        });
        let fills = serde_json::from_value(fill_window_json.clone())?;
        let lookup = HyperliquidOrderLookup::order_id(91_490_942)?;
        let status = parse_order_status(ORDER_STATUS, &meta, &lookup)?;
        let scope = PhysicalRecoveryScope::verified(
            selected.clone(),
            "config_1",
            7,
            1,
            10,
            PhysicalRecoveryAuthorityRoots::verified([1; 32], [2; 32], [3; 32])?,
        )?;
        let collector_scope_sha256 = *scope.commitment_sha256();
        Ok(RecoveryFixture {
            scope,
            anchor: HyperliquidProbeRecoveryAnchor {
                payload: HyperliquidProbeRecoveryPayload {
                    schema_version: HYPERLIQUID_CAPABILITY_PROBE_SCHEMA,
                    binding: selected,
                    observed_ms: PROBE_OBSERVED_MS,
                    expires_ms: PROBE_EXPIRES_MS,
                    private_generation: 11,
                    master_address: USER.to_owned(),
                    user_address: USER.to_owned(),
                    vault_address: None,
                    native_coin: "BTC".to_owned(),
                    account_exchange_time_ms: account.exchange_time_ms,
                    orders_observed_ms: observed_ms,
                    meta_commitment_keccak256: "bb".repeat(32),
                    account_commitment_keccak256: "cc".repeat(32),
                    orders_commitment_keccak256: "dd".repeat(32),
                    fill_window: fill_window_json,
                    actions: [
                        HyperliquidProbeRecoveryAction {
                            kind: HyperliquidActionKind::AloPlace,
                        },
                        HyperliquidProbeRecoveryAction {
                            kind: HyperliquidActionKind::Cancel,
                        },
                        HyperliquidProbeRecoveryAction {
                            kind: HyperliquidActionKind::IocReduceOnly,
                        },
                    ],
                },
                commitment_keccak256: "aa".repeat(32),
                collector_scope_sha256,
                collector_attempt_id: 41,
                collector_meta_sha256: sha256(META),
                collector_account_sha256: sha256(ACCOUNT_SNAPSHOT),
                collector_orders_sha256: sha256(ORDERS),
            },
            account,
            orders,
            fills,
            lookup,
            status,
            owner_order_ids,
        })
    }

    fn status_readback(fixture: &RecoveryFixture) -> HyperliquidOrderStatusReadback<'_> {
        HyperliquidOrderStatusReadback {
            private_generation: 11,
            observed_ms: PROBE_OBSERVED_MS,
            lookup: &fixture.lookup,
            status: &fixture.status,
            response_sha256: sha256(ORDER_STATUS),
        }
    }

    #[test]
    fn fixed_candidate_bridge_is_physical_but_never_auto_authorizes()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_physical_gateway::<HyperliquidPhysicalGatewayCandidate>();
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let selected = binding(mode)?;
            let bridge = HyperliquidPhysicalGatewayCandidate::new(selected.clone(), None)?;
            assert_eq!(bridge.binding(), &selected);
            assert!(bridge.capability_snapshot().flags.is_empty());
            assert_eq!(bridge.capability_snapshot().version, 0);
            assert!(bridge.persisted_probe_candidate().is_none());
        }
        Ok(())
    }

    #[test]
    fn fixed_candidate_bridge_rejects_other_venue() -> Result<(), Box<dyn std::error::Error>> {
        let wrong = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Test,
            ACCOUNT,
            "BTC/USDC".parse()?,
        )?;
        assert!(matches!(
            HyperliquidPhysicalGatewayCandidate::new(wrong, None),
            Err(HyperliquidBridgeError::Binding)
        ));
        Ok(())
    }

    #[test]
    fn production_recovery_manifest_is_unconditionally_unavailable()
    -> Result<(), Box<dyn std::error::Error>> {
        let bridge = HyperliquidPhysicalGatewayCandidate::new(binding(GatewayMode::Test)?, None)?;
        assert_eq!(
            bridge.recovery_readback_manifest(),
            Err(HyperliquidBridgeError::RecoveryUnavailable)
        );
        Ok(())
    }

    #[test]
    fn hyperliquid_readback_maps_all_six_faces_without_authorizing_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = recovery_fixture()?;
        let statuses = [status_readback(&fixture)];
        let manifest = map_recovery_readback(
            &fixture.anchor.payload.binding,
            fixture.scope.clone(),
            41,
            &fixture.anchor,
            HyperliquidRecoveryReadback {
                account: Some(&fixture.account),
                orders: Some(&fixture.orders),
                fills: Some(&fixture.fills),
                order_status: &statuses,
                owner_order_ids: Some(&fixture.owner_order_ids),
                raw_meta: META,
                raw_account: ACCOUNT_SNAPSHOT,
                collected_at_ms: COLLECTED_AT_MS,
            },
        )?;

        assert_eq!(manifest.attempt_id(), 41);
        assert_eq!(manifest.private_generation(), 11);
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::Account),
            PhysicalReadbackCoverage::Complete {
                record_count: 1,
                ..
            }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::Positions),
            PhysicalReadbackCoverage::Complete {
                record_count: 1,
                ..
            }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmOrder),
            PhysicalReadbackCoverage::Complete { .. }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmConditional),
            PhysicalReadbackCoverage::Complete { .. }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmAlgo),
            PhysicalReadbackCoverage::Unsupported {
                profile_version: HYPERLIQUID_RECOVERY_ORDER_PROFILE_VERSION,
                ..
            }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::FillsCursor),
            PhysicalReadbackCoverage::Complete {
                record_count: 2,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn missing_traditional_face_or_regular_family_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = recovery_fixture()?;
        let statuses = [status_readback(&fixture)];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: None,
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );

        fixture.orders.regular_coverage =
            HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders;
        let statuses = [status_readback(&fixture)];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn uncovered_conditional_family_fails_closed_instead_of_faking_empty()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = recovery_fixture()?;
        fixture.orders.conditional_coverage =
            HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders;
        let statuses = [status_readback(&fixture)];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn mismatched_order_status_identity_never_proves_family_absence()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = recovery_fixture()?;
        let wrong_lookup = HyperliquidOrderLookup::order_id(91_490_943)?;
        let statuses = [HyperliquidOrderStatusReadback {
            private_generation: 11,
            observed_ms: 1_724_361_547_000,
            lookup: &wrong_lookup,
            status: &fixture.status,
            response_sha256: sha256(ORDER_STATUS),
        }];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn old_probe_cannot_be_relabelled_to_new_attempt_scope_or_roots()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = recovery_fixture()?;
        let relabelled_scope = PhysicalRecoveryScope::verified(
            fixture.scope.binding().clone(),
            fixture.scope.config_digest(),
            fixture.scope.config_epoch(),
            fixture.scope.connection_generation(),
            fixture.scope.recovered_private_generation(),
            PhysicalRecoveryAuthorityRoots::verified([4; 32], [5; 32], [6; 32])?,
        )?;
        let statuses = [status_readback(&fixture)];
        let readback = || HyperliquidRecoveryReadback {
            account: Some(&fixture.account),
            orders: Some(&fixture.orders),
            fills: Some(&fixture.fills),
            order_status: &statuses,
            owner_order_ids: Some(&fixture.owner_order_ids),
            raw_meta: META,
            raw_account: ACCOUNT_SNAPSHOT,
            collected_at_ms: COLLECTED_AT_MS,
        };
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                relabelled_scope,
                41,
                &fixture.anchor,
                readback(),
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                42,
                &fixture.anchor,
                readback(),
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn unknown_order_status_cannot_turn_an_unreplayed_family_into_complete_empty()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = recovery_fixture()?;
        let status_scope = match &fixture.status {
            HyperliquidOrderStatus::Unknown { scope, .. }
            | HyperliquidOrderStatus::Known { scope, .. } => scope.clone(),
        };
        fixture.orders.orders.clear();
        fixture.owner_order_ids.clear();
        fixture.anchor.collector_orders_sha256 = sha256(&fixture.orders.raw_payload);
        let unknown = HyperliquidOrderStatus::Unknown {
            scope: status_scope,
            lookup: fixture.lookup.clone(),
        };
        let statuses = [HyperliquidOrderStatusReadback {
            private_generation: 11,
            observed_ms: PROBE_OBSERVED_MS,
            lookup: &fixture.lookup,
            status: &unknown,
            response_sha256: sha256(b"unknownOid"),
        }];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn nonempty_orders_without_structured_owner_proof_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = recovery_fixture()?;
        assert!(!fixture.orders.orders.is_empty());
        let statuses = [status_readback(&fixture)];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: None,
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: COLLECTED_AT_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }

    #[test]
    fn expired_probe_fixture_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = recovery_fixture()?;
        let statuses = [status_readback(&fixture)];
        assert_eq!(
            map_recovery_readback(
                &fixture.anchor.payload.binding,
                fixture.scope.clone(),
                41,
                &fixture.anchor,
                HyperliquidRecoveryReadback {
                    account: Some(&fixture.account),
                    orders: Some(&fixture.orders),
                    fills: Some(&fixture.fills),
                    order_status: &statuses,
                    owner_order_ids: Some(&fixture.owner_order_ids),
                    raw_meta: META,
                    raw_account: ACCOUNT_SNAPSHOT,
                    collected_at_ms: PROBE_EXPIRES_MS,
                },
            ),
            Err(HyperliquidBridgeError::RecoveryEvidence)
        );
        Ok(())
    }
}
