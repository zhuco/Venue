use std::{
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};

#[cfg(test)]
use serde::Serialize;
#[cfg(test)]
use sha2::{Digest, Sha256};
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};
#[cfg(test)]
use venue_gateway_bybit::capabilities;
use venue_gateway_bybit::{
    BybitAccountGateway, BybitGatewayBinding, BybitSynchronousPhysicalSession, BybitTransportLimits,
};
#[cfg(test)]
use venue_gateway_bybit::{
    BybitCapabilityCandidate, BybitCompleteOrderFamilyEvidence, BybitError,
    BybitOrderFamilyEvidence, BybitPositionPage, BybitPrivateSource,
    BybitPrivateStreamProbeEvidence, BybitRawPrivatePayload, complete_account_readback,
    complete_position_pages, parse_position_page, validate_capability_candidate,
    validate_order_family_candidate,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest, error_chain,
    load_root_dotenv, report_result, run_live_mvp,
};
#[cfg(test)]
use venue_runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryManifestError,
};
use venue_runtime::account::{PhysicalRecoveryReadbackManifest, PhysicalRecoveryScope};

const PROGRAM: &str = "venue-node-bybit";
const PROBE_FILE: &str = "bybit_capability_probe.json";
const INSTRUMENT_FILE: &str = "bybit_linear_instrument.json";

/// The fixed node cannot issue a production manifest until the adapter exposes an opaque fresh-turn
/// authority that proves API-key replay, private-stream authentication/freshness, and one complete
/// same-attempt REST collection. Persisted probes and caller-constructed candidates stay inert.
pub fn map_bybit_physical_recovery_manifest(
    _scope: PhysicalRecoveryScope,
) -> Result<PhysicalRecoveryReadbackManifest, BybitRecoveryManifestMappingError> {
    Err(BybitRecoveryManifestMappingError::FreshTurnAuthorityUnavailable)
}

#[cfg(test)]
fn map_bybit_physical_recovery_fixture_manifest(
    scope: PhysicalRecoveryScope,
    candidate: &BybitCapabilityCandidate,
    private_stream: &BybitPrivateStreamProbeEvidence,
    validated_at_ms: u64,
) -> Result<PhysicalRecoveryReadbackManifest, BybitRecoveryManifestMappingError> {
    let binding = BybitGatewayBinding::new(candidate.scope.binding.clone())
        .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    if candidate.account.raw_payloads.len() != 2 {
        return Err(BybitRecoveryManifestMappingError::Candidate(
            BybitError::Capability,
        ));
    }
    let account_raw = exact_raw_payload(
        &candidate.account.raw_payloads,
        BybitPrivateSource::AccountInfo,
    )?;
    let wallet_raw = exact_raw_payload(
        &candidate.account.raw_payloads,
        BybitPrivateSource::WalletBalance,
    )?;
    let account = complete_account_readback(&binding, account_raw.clone(), wallet_raw.clone())
        .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    if account != candidate.account {
        return Err(BybitRecoveryManifestMappingError::Candidate(
            BybitError::Projection,
        ));
    }
    let position_pages = candidate
        .positions
        .raw_pages
        .iter()
        .map(|raw| parse_position_page(&binding, raw))
        .collect::<Result<Vec<BybitPositionPage>, _>>()
        .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    let positions = complete_position_pages(&binding, &position_pages)
        .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    if positions != candidate.positions {
        return Err(BybitRecoveryManifestMappingError::Candidate(
            BybitError::Projection,
        ));
    }
    let order_families = validate_order_family_candidate(
        candidate.scope.clone(),
        validated_at_ms,
        [
            BybitOrderFamilyEvidence::Complete(Box::new(
                candidate.order_families.regular().clone(),
            )),
            BybitOrderFamilyEvidence::Complete(Box::new(
                candidate.order_families.conditional().clone(),
            )),
            BybitOrderFamilyEvidence::Unsupported(candidate.order_families.algo().clone()),
        ],
    )
    .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    let candidate = validate_capability_candidate(
        candidate.scope.clone(),
        validated_at_ms,
        candidate.api_key.clone(),
        account,
        positions,
        order_families,
        candidate.fills.clone(),
    )
    .map_err(BybitRecoveryManifestMappingError::Candidate)?;
    if scope.binding() != &candidate.scope.binding
        || private_stream.binding() != &candidate.scope.binding
        || private_stream.generation() != candidate.scope.generation
    {
        return Err(BybitRecoveryManifestMappingError::Scope);
    }

    let private_stream = serde_json::to_vec(private_stream)
        .map_err(|_| BybitRecoveryManifestMappingError::EvidenceEncoding)?;
    let attempt_id = candidate.scope.attempt_id;
    let generation = candidate.scope.generation;
    let regular = candidate.order_families.regular();
    let conditional = candidate.order_families.conditional();
    let receipts = vec![
        complete_receipt(
            &scope,
            PhysicalReadbackSurface::Account,
            attempt_id,
            generation,
            evidence_digest(
                b"venue-bybit-recovery-account-v1",
                &private_stream,
                &candidate.account.raw_payloads,
            )?,
            1,
        )?,
        complete_receipt(
            &scope,
            PhysicalReadbackSurface::Positions,
            attempt_id,
            generation,
            evidence_digest(
                b"venue-bybit-recovery-positions-v1",
                &private_stream,
                &candidate.positions.raw_pages,
            )?,
            record_count(candidate.positions.positions.len())?,
        )?,
        complete_order_family_receipt(
            &scope,
            PhysicalReadbackSurface::UmOrder,
            attempt_id,
            generation,
            &private_stream,
            regular,
        )?,
        complete_order_family_receipt(
            &scope,
            PhysicalReadbackSurface::UmConditional,
            attempt_id,
            generation,
            &private_stream,
            conditional,
        )?,
        PhysicalReadbackReceipt::verified_unsupported_order_family(
            &scope,
            PhysicalReadbackSurface::UmAlgo,
            attempt_id,
            generation,
            evidence_digest(
                b"venue-bybit-recovery-um-algo-unsupported-v1",
                &private_stream,
                &BybitUnsupportedFamilyCommitment {
                    binding: &candidate.scope.binding,
                    profile_version: candidate.order_families.algo().profile_version,
                    reason: candidate.order_families.algo().reason(),
                },
            )?,
            candidate.order_families.algo().profile_version,
        )
        .map_err(BybitRecoveryManifestMappingError::Manifest)?,
        complete_receipt(
            &scope,
            PhysicalReadbackSurface::FillsCursor,
            attempt_id,
            generation,
            evidence_digest(
                b"venue-bybit-recovery-fills-cursor-v1",
                &private_stream,
                &candidate.fills.raw_pages,
            )?,
            record_count(candidate.fills.fills.len())?,
        )?,
    ];
    PhysicalRecoveryReadbackManifest::verified(scope, receipts)
        .map_err(BybitRecoveryManifestMappingError::Manifest)
}

#[cfg(test)]
fn exact_raw_payload(
    payloads: &[BybitRawPrivatePayload],
    source: BybitPrivateSource,
) -> Result<&BybitRawPrivatePayload, BybitRecoveryManifestMappingError> {
    let mut matching = payloads.iter().filter(|payload| payload.source == source);
    let payload = matching
        .next()
        .ok_or(BybitRecoveryManifestMappingError::Candidate(
            BybitError::Capability,
        ))?;
    if matching.next().is_some() {
        return Err(BybitRecoveryManifestMappingError::Candidate(
            BybitError::Capability,
        ));
    }
    Ok(payload)
}

#[cfg(test)]
fn complete_order_family_receipt(
    scope: &PhysicalRecoveryScope,
    surface: PhysicalReadbackSurface,
    attempt_id: u64,
    generation: u64,
    private_stream: &[u8],
    family: &BybitCompleteOrderFamilyEvidence,
) -> Result<PhysicalReadbackReceipt, BybitRecoveryManifestMappingError> {
    complete_receipt(
        scope,
        surface,
        attempt_id,
        generation,
        evidence_digest(
            match surface {
                PhysicalReadbackSurface::UmOrder => b"venue-bybit-recovery-um-order-v1",
                PhysicalReadbackSurface::UmConditional => b"venue-bybit-recovery-um-conditional-v1",
                _ => return Err(BybitRecoveryManifestMappingError::Scope),
            },
            private_stream,
            &BybitCompleteFamilyCommitment {
                open_orders: &family.open_orders.raw_pages,
                order_history: &family.order_history.raw_pages,
            },
        )?,
        record_count(family.open_orders.orders.len())?,
    )
}

#[cfg(test)]
fn complete_receipt(
    scope: &PhysicalRecoveryScope,
    surface: PhysicalReadbackSurface,
    attempt_id: u64,
    generation: u64,
    evidence_sha256: [u8; 32],
    record_count: u64,
) -> Result<PhysicalReadbackReceipt, BybitRecoveryManifestMappingError> {
    PhysicalReadbackReceipt::verified_complete(
        scope,
        surface,
        attempt_id,
        generation,
        evidence_sha256,
        record_count,
    )
    .map_err(BybitRecoveryManifestMappingError::Manifest)
}

#[cfg(test)]
fn evidence_digest<T: Serialize>(
    domain: &[u8],
    private_stream: &[u8],
    evidence: &T,
) -> Result<[u8; 32], BybitRecoveryManifestMappingError> {
    let evidence = serde_json::to_vec(evidence)
        .map_err(|_| BybitRecoveryManifestMappingError::EvidenceEncoding)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(
        u64::try_from(private_stream.len())
            .map_err(|_| BybitRecoveryManifestMappingError::RecordCount)?
            .to_be_bytes(),
    );
    digest.update(private_stream);
    digest.update(
        u64::try_from(evidence.len())
            .map_err(|_| BybitRecoveryManifestMappingError::RecordCount)?
            .to_be_bytes(),
    );
    digest.update(evidence);
    Ok(digest.finalize().into())
}

#[cfg(test)]
fn record_count(value: usize) -> Result<u64, BybitRecoveryManifestMappingError> {
    u64::try_from(value).map_err(|_| BybitRecoveryManifestMappingError::RecordCount)
}

#[cfg(test)]
#[derive(Serialize)]
struct BybitCompleteFamilyCommitment<'a> {
    open_orders: &'a [BybitRawPrivatePayload],
    order_history: &'a [BybitRawPrivatePayload],
}

#[cfg(test)]
#[derive(Serialize)]
struct BybitUnsupportedFamilyCommitment<'a> {
    binding: &'a GatewayBinding,
    profile_version: u64,
    reason: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitRecoveryManifestMappingError {
    #[error("Bybit production recovery manifest requires opaque fresh-turn authority")]
    FreshTurnAuthorityUnavailable,
    #[cfg(test)]
    #[error("Bybit recovery candidate failed signed evidence replay: {0}")]
    Candidate(BybitError),
    #[cfg(test)]
    #[error("Bybit recovery candidate does not match the recovered/private-stream scope")]
    Scope,
    #[cfg(test)]
    #[error("Bybit recovery evidence cannot be encoded canonically")]
    EvidenceEncoding,
    #[cfg(test)]
    #[error("Bybit recovery evidence record count exceeds the manifest representation")]
    RecordCount,
    #[cfg(test)]
    #[error("Bybit recovery manifest is incomplete or stale: {0}")]
    Manifest(PhysicalRecoveryManifestError),
}

/// Exact shared delta that prevents this candidate from loading credentials or creating transport.
/// The adapter probe remains evidence, never account-node capability authority.
pub const BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA: &str = "GatewayRecoveryPermit lacks opaque Actor-applied Control and Canary receipts plus recovered Owner/WAL and installed writer-session authority; PhysicalGateway has no host-admitted capability separate from an adapter probe; DispatchPermit lacks the fresh same-generation BBO required by Bybit market and reduce-only IOC preparation";

/// Fixed-binary bridge candidate. Persisted paths are secret-free and inert; the synchronous
/// physical session is deliberately absent until the shared host can prove the complete authority
/// listed above before credential access.
pub struct BybitPhysicalGatewayCandidate {
    binding: BybitGatewayBinding,
    probe_path: PathBuf,
    instrument_path: PathBuf,
    session: Option<BybitSynchronousPhysicalSession>,
}

impl BybitPhysicalGatewayCandidate {
    #[must_use]
    pub fn new(binding: BybitGatewayBinding, artifacts_root: &Path) -> Self {
        let account_root = artifacts_root.join("account");
        Self {
            binding,
            probe_path: account_root.join(PROBE_FILE),
            instrument_path: account_root.join(INSTRUMENT_FILE),
            session: None,
        }
    }

    #[must_use]
    pub fn probe_path(&self) -> &Path {
        &self.probe_path
    }

    #[must_use]
    pub fn instrument_path(&self) -> &Path {
        &self.instrument_path
    }

    #[must_use]
    pub const fn has_loaded_session(&self) -> bool {
        self.session.is_some()
    }

    fn fail_before_credentials(&self) -> BybitPhysicalGatewayBridgeError {
        BybitPhysicalGatewayBridgeError::SharedAuthority(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA)
    }
}

impl PhysicalGateway for BybitPhysicalGatewayCandidate {
    type Error = BybitPhysicalGatewayBridgeError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.binding.gateway_binding().clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: CapabilityFlags::empty(),
        }
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != self.binding.gateway_binding() {
            return Err(BybitPhysicalGatewayBridgeError::Binding);
        }
        Err(self.fail_before_credentials())
    }

    fn signed_readback(
        &mut self,
        _request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        Err(self.fail_before_credentials())
    }

    fn verify_signed_readback(&self, _receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        Err(self.fail_before_credentials())
    }

    fn dispatch(&mut self, _permit: DispatchPermit) -> GatewayDispatchResult {
        GatewayDispatchResult::Rejected {
            reason_code: "bybit_shared_authority_missing".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitPhysicalGatewayBridgeError {
    #[error("Bybit physical gateway binding does not match the fixed node")]
    Binding,
    #[error("Bybit physical gateway remains pre-credential fail-closed: {0}")]
    SharedAuthority(&'static str),
}

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bybit)?;
    let command = launch.live_mvp_command()?;
    let adapter = BybitGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Bybit))?;
    AdapterIsolation {
        venue: VenueId::Bybit,
        mode: adapter.config().mode(),
        endpoints: &[
            adapter.config().rest_origin(),
            adapter.config().public_ws(),
            adapter.config().private_ws(),
        ],
        credential_environment: &["BYBIT_API_KEY", "BYBIT_API_SECRET"],
        credential_prefix: "BYBIT_",
        account_binding: "uta2_linear",
    }
    .validate(launch.binding())?;
    load_root_dotenv()?;
    let limits =
        BybitTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024).map_err(|error| {
            NodeError::LiveGateway {
                venue: VenueId::Bybit,
                message: error_chain(&error),
            }
        })?;
    let gateway = BybitAccountGateway::connect_from_environment(launch.binding().clone(), limits)
        .map_err(|error| NodeError::LiveGateway {
        venue: VenueId::Bybit,
        message: error_chain(&error),
    })?;
    run_live_mvp(&launch, command, gateway)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::NativeOrderFamily;
    use venue_gateway_api::{GatewayMode, MutationCapability};
    use venue_gateway_bybit::{
        BYBIT_LINEAR_ORDER_PROFILE_VERSION, BybitApiKeyEvidence, BybitCompleteOrderFamilyEvidence,
        BybitHistoryWindow, BybitOrderFamilyEvidence, BybitOrderFamilyScope, BybitPrivateSource,
        BybitUnsupportedOrderFamilyEvidence, complete_account_readback, complete_execution_pages,
        complete_open_order_pages, complete_order_history_pages, complete_position_pages,
        parse_execution_page, parse_open_order_page, parse_order_history_page, parse_position_page,
        prepare_private_request, validate_order_family_candidate,
    };
    use venue_runtime::account::{PhysicalReadbackCoverage, PhysicalRecoveryAuthorityRoots};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const ACCOUNT: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/account-info-uta2.json");
    const API_KEY: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/api-key-info.json");
    const WALLET: &[u8] = include_bytes!(
        "../../../../crates/venue-gateway-bybit/fixtures/wallet-balance-unified.json"
    );
    const POSITIONS: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/positions-linear.json");
    const ORDERS: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/open-orders-linear.json");
    const STOP_ORDERS: &[u8] = include_bytes!(
        "../../../../crates/venue-gateway-bybit/fixtures/open-stop-orders-linear.json"
    );
    const HISTORY: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/order-history-linear.json");
    const EXECUTIONS: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-bybit/fixtures/execution-trade-page.json");
    const EMPTY_PAGE: &[u8] = br#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[]},"time":2000}"#;

    fn candidate() -> Result<BybitPhysicalGatewayCandidate, Box<dyn std::error::Error>> {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)?;
        Ok(BybitPhysicalGatewayCandidate::new(
            binding,
            &std::env::temp_dir().join("venue-goal29-bybit-node"),
        ))
    }

    fn raw(
        binding: &BybitGatewayBinding,
        source: BybitPrivateSource,
        payload: &[u8],
    ) -> Result<BybitRawPrivatePayload, BybitError> {
        let history_window = matches!(
            source,
            BybitPrivateSource::OrderHistory(_) | BybitPrivateSource::Executions
        )
        .then(|| BybitHistoryWindow::new(1, 2_000))
        .transpose()?;
        let request =
            prepare_private_request(binding, 7, 11, 0, source, None, history_window, None)?;
        BybitRawPrivatePayload::from_response(binding, &request, 1_900, 2_000, payload.to_vec())
    }

    fn recovery_candidate()
    -> Result<(BybitGatewayBinding, BybitCapabilityCandidate), Box<dyn std::error::Error>> {
        let bridge = candidate()?;
        let binding = bridge.binding.clone();
        let api_key_raw = raw(&binding, BybitPrivateSource::ApiKeyInfo, API_KEY)?;
        let api_key = BybitApiKeyEvidence {
            binding: api_key_raw.binding.clone(),
            generation: api_key_raw.generation,
            attempt_id: api_key_raw.attempt_id,
            observed_at_ms: api_key_raw.received_at_ms,
            payload_sha256: api_key_raw.payload_sha256.clone(),
            read_only: false,
            contract_order: true,
            contract_position: true,
            derivatives_trade: true,
            withdraw: false,
            raw: api_key_raw,
        };
        let account = complete_account_readback(
            &binding,
            raw(&binding, BybitPrivateSource::AccountInfo, ACCOUNT)?,
            raw(&binding, BybitPrivateSource::WalletBalance, WALLET)?,
        )?;
        let positions = complete_position_pages(
            &binding,
            &[parse_position_page(
                &binding,
                &raw(&binding, BybitPrivateSource::Positions, POSITIONS)?,
            )?],
        )?;
        let regular_history = complete_order_history_pages(
            &binding,
            NativeOrderFamily::UmOrder,
            &[parse_order_history_page(
                &binding,
                &raw(
                    &binding,
                    BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
                    HISTORY,
                )?,
            )?],
        )?;
        let conditional_history = complete_order_history_pages(
            &binding,
            NativeOrderFamily::UmConditional,
            &[parse_order_history_page(
                &binding,
                &raw(
                    &binding,
                    BybitPrivateSource::OrderHistory(NativeOrderFamily::UmConditional),
                    EMPTY_PAGE,
                )?,
            )?],
        )?;
        let regular = complete_open_order_pages(
            &binding,
            NativeOrderFamily::UmOrder,
            &[parse_open_order_page(
                &binding,
                &raw(
                    &binding,
                    BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
                    ORDERS,
                )?,
            )?],
        )?;
        let conditional = complete_open_order_pages(
            &binding,
            NativeOrderFamily::UmConditional,
            &[parse_open_order_page(
                &binding,
                &raw(
                    &binding,
                    BybitPrivateSource::OpenOrders(NativeOrderFamily::UmConditional),
                    STOP_ORDERS,
                )?,
            )?],
        )?;
        let scope = BybitOrderFamilyScope {
            binding: binding.gateway_binding().clone(),
            profile_version: BYBIT_LINEAR_ORDER_PROFILE_VERSION,
            attempt_id: 11,
            generation: 7,
            observed_at_ms: 2_000,
            expires_at_ms: 3_000,
        };
        let families = validate_order_family_candidate(
            scope.clone(),
            2_500,
            [
                BybitOrderFamilyEvidence::Complete(Box::new(BybitCompleteOrderFamilyEvidence {
                    open_orders: regular,
                    order_history: regular_history.clone(),
                })),
                BybitOrderFamilyEvidence::Complete(Box::new(BybitCompleteOrderFamilyEvidence {
                    open_orders: conditional,
                    order_history: conditional_history,
                })),
                BybitOrderFamilyEvidence::Unsupported(BybitUnsupportedOrderFamilyEvidence::algo(
                    scope.binding.clone(),
                    BYBIT_LINEAR_ORDER_PROFILE_VERSION,
                )),
            ],
        )?;
        let fills = complete_execution_pages(
            &binding,
            &[parse_execution_page(
                &binding,
                &raw(&binding, BybitPrivateSource::Executions, EXECUTIONS)?,
                &regular_history.orders,
            )?],
            &regular_history.orders,
        )?;
        let candidate = validate_capability_candidate(
            scope, 2_500, api_key, account, positions, families, fills,
        )?;
        Ok((binding, candidate))
    }

    fn private_stream(
        binding: &GatewayBinding,
        generation: u64,
    ) -> Result<BybitPrivateStreamProbeEvidence, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "binding": binding,
            "connection_generation": generation,
            "private_generation": generation,
            "authenticated_at_ms": 1_950,
            "observed_at_ms": 2_100,
            "expires_at_ms": 3_000,
            "connection_id_sha256": "11".repeat(32),
        }))
    }

    fn recovery_scope(
        binding: GatewayBinding,
        recovered_generation: u64,
    ) -> Result<PhysicalRecoveryScope, PhysicalRecoveryManifestError> {
        PhysicalRecoveryScope::verified(
            binding,
            "config_1",
            1,
            1,
            recovered_generation,
            PhysicalRecoveryAuthorityRoots::verified([1; 32], [2; 32], [3; 32])?,
        )
    }

    #[test]
    fn live_candidate_is_inert_and_creates_no_capability() -> Result<(), Box<dyn std::error::Error>>
    {
        let candidate = candidate()?;
        assert_eq!(candidate.binding().mode, GatewayMode::Live);
        assert!(!candidate.has_loaded_session());
        assert_eq!(
            candidate.capability_snapshot().flags,
            CapabilityFlags::empty()
        );
        assert!(
            candidate
                .capability_snapshot()
                .authorize(candidate.binding(), 0, 1, MutationCapability::Cancel)
                .is_err()
        );
        assert_eq!(
            candidate.probe_path().file_name(),
            Some(PROBE_FILE.as_ref())
        );
        assert_eq!(
            candidate.instrument_path().file_name(),
            Some(INSTRUMENT_FILE.as_ref())
        );
        Ok(())
    }

    #[test]
    fn shared_delta_is_explicit_and_precredential() -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate()?;
        assert_eq!(
            candidate.fail_before_credentials(),
            BybitPhysicalGatewayBridgeError::SharedAuthority(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA)
        );
        for required in ["Control", "Canary", "Owner/WAL", "writer", "BBO"] {
            assert!(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA.contains(required));
        }
        assert!(!candidate.probe_path().exists());
        assert!(!candidate.instrument_path().exists());
        Ok(())
    }

    #[test]
    fn production_manifest_rejects_deserialized_generation_relabeling()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, old_candidate) = recovery_candidate()?;
        let old_stream = private_stream(binding.gateway_binding(), 7)?;
        let mut relabeled_stream = serde_json::to_value(old_stream)?;
        relabeled_stream["connection_generation"] = serde_json::json!(8);
        relabeled_stream["private_generation"] = serde_json::json!(8);
        let relabeled_stream: BybitPrivateStreamProbeEvidence =
            serde_json::from_value(relabeled_stream)?;

        assert_eq!(old_candidate.scope.generation, 7);
        assert_eq!(relabeled_stream.generation(), 8);
        assert_eq!(
            map_bybit_physical_recovery_manifest(recovery_scope(
                binding.gateway_binding().clone(),
                7,
            )?),
            Err(BybitRecoveryManifestMappingError::FreshTurnAuthorityUnavailable)
        );
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn complete_same_attempt_new_generation_maps_all_six_faces_without_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, candidate) = recovery_candidate()?;
        let stream = private_stream(binding.gateway_binding(), 7)?;
        let manifest = map_bybit_physical_recovery_fixture_manifest(
            recovery_scope(binding.gateway_binding().clone(), 6)?,
            &candidate,
            &stream,
            2_500,
        )?;

        assert_eq!(manifest.attempt_id(), 11);
        assert_eq!(manifest.private_generation(), 7);
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
                record_count: 2,
                ..
            }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmOrder),
            PhysicalReadbackCoverage::Complete { record_count, .. }
                if *record_count
                    == u64::try_from(candidate.order_families.regular().open_orders.orders.len())?
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmConditional),
            PhysicalReadbackCoverage::Complete { record_count, .. }
                if *record_count
                    == u64::try_from(
                        candidate
                            .order_families
                            .conditional()
                            .open_orders
                            .orders
                            .len()
                    )?
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::UmAlgo),
            PhysicalReadbackCoverage::Unsupported {
                profile_version: BYBIT_LINEAR_ORDER_PROFILE_VERSION,
                ..
            }
        ));
        assert!(matches!(
            manifest.coverage(PhysicalReadbackSurface::FillsCursor),
            PhysicalReadbackCoverage::Complete { record_count, .. }
                if *record_count == u64::try_from(candidate.fills.fills.len())?
        ));
        assert_eq!(capabilities(), CapabilityFlags::empty());
        assert_eq!(
            BybitPhysicalGatewayCandidate::new(binding, &std::env::temp_dir())
                .capability_snapshot()
                .flags,
            CapabilityFlags::empty()
        );
        Ok(())
    }

    #[test]
    fn missing_page_cross_attempt_and_stale_or_cross_generation_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let (binding, candidate) = recovery_candidate()?;
        let stream = private_stream(binding.gateway_binding(), 7)?;

        let mut missing_page = candidate.clone();
        missing_page.positions.raw_pages.clear();
        assert!(matches!(
            map_bybit_physical_recovery_fixture_manifest(
                recovery_scope(binding.gateway_binding().clone(), 6)?,
                &missing_page,
                &stream,
                2_500,
            ),
            Err(BybitRecoveryManifestMappingError::Candidate(_))
        ));

        let mut cross_attempt = candidate.clone();
        cross_attempt.fills.attempt_id = 12;
        assert!(matches!(
            map_bybit_physical_recovery_fixture_manifest(
                recovery_scope(binding.gateway_binding().clone(), 6)?,
                &cross_attempt,
                &stream,
                2_500,
            ),
            Err(BybitRecoveryManifestMappingError::Candidate(_))
        ));
        assert!(matches!(
            map_bybit_physical_recovery_fixture_manifest(
                recovery_scope(binding.gateway_binding().clone(), 7)?,
                &candidate,
                &stream,
                2_500,
            ),
            Err(BybitRecoveryManifestMappingError::Manifest(
                PhysicalRecoveryManifestError::StaleGeneration
            ))
        ));
        assert_eq!(
            map_bybit_physical_recovery_fixture_manifest(
                recovery_scope(binding.gateway_binding().clone(), 6)?,
                &candidate,
                &private_stream(binding.gateway_binding(), 8)?,
                2_500,
            ),
            Err(BybitRecoveryManifestMappingError::Scope)
        );
        let inert = BybitPhysicalGatewayCandidate::new(binding, root.path());
        assert!(!inert.probe_path().exists());
        assert!(!inert.instrument_path().exists());
        assert_eq!(root.path().read_dir()?.count(), 0);
        Ok(())
    }
}
