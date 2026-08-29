use std::process::ExitCode;

use sha2::{Digest, Sha256};
use venue_domain::domain::{NativeOrderFamily, PositionSide};
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};
use venue_gateway_okx::{
    OkxConfig, OkxPhysicalCandidate, OkxPositionMode, OkxPrivateReadbackCandidate,
    OkxPrivateSurface, OkxRawPrivatePage, capabilities,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_runtime, report_result,
};
use venue_runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryManifestError,
    PhysicalRecoveryReadbackManifest, PhysicalRecoveryScope,
};

const PROGRAM: &str = "venue-node-okx";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    assert_candidate_bridge::<OkxNodePhysicalCandidate>();
    assert_post_recovery_mapper(map_post_recovery_readback);
    let launch = NodeLaunch::from_environment(VenueId::Okx)?;
    launch.require_no_runtime_arguments()?;
    let adapter = OkxConfig::for_binding(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Okx))?;
    AdapterIsolation {
        venue: VenueId::Okx,
        mode: adapter.mode(),
        endpoints: &[
            adapter.rest_origin(),
            adapter.public_ws(),
            adapter.private_ws(),
        ],
        credential_environment: &["OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"],
        credential_prefix: "OKX_",
        account_binding: "linear_swap",
    }
    .validate(launch.binding())?;
    let _isolated_artifacts_root = launch.artifacts_root();
    reject_unintegrated_runtime(VenueId::Okx, launch.binding().mode, capabilities())
}

fn assert_candidate_bridge<G: PhysicalGateway>() {}

fn assert_post_recovery_mapper(
    _mapper: fn(
        PhysicalRecoveryScope,
        OkxPostRecoveryCollection<'_>,
    ) -> Result<PhysicalRecoveryReadbackManifest, OkxNodeBridgeError>,
) {
}

/// One complete OKX collection that was started only after the shared recovery scope was fixed.
/// Its fields intentionally have no production constructor in this binary: the existing durable
/// capability probe lacks the recovery-scope commitment and therefore cannot be relabelled as a
/// fresh collection. A future synchronous collector must construct this value after network I/O.
struct OkxPostRecoveryCollection<'a> {
    candidate: &'a OkxPrivateReadbackCandidate,
    private_generation: u64,
    recovery_scope_sha256: [u8; 32],
}

fn map_post_recovery_readback(
    recovery_scope: PhysicalRecoveryScope,
    collected: OkxPostRecoveryCollection<'_>,
) -> Result<PhysicalRecoveryReadbackManifest, OkxNodeBridgeError> {
    let candidate = collected.candidate;
    if candidate.scope().gateway_binding() != recovery_scope.binding()
        || collected.recovery_scope_sha256 != *recovery_scope.commitment_sha256()
    {
        return Err(OkxNodeBridgeError::Scope);
    }
    validate_position_mode(candidate)?;

    let attempt_id = candidate.scope().attempt_id();
    let account_pages = pages_for(candidate, |surface| {
        matches!(
            surface,
            OkxPrivateSurface::AccountConfig | OkxPrivateSurface::Balance
        )
    });
    if account_pages.len() != 2 {
        return Err(OkxNodeBridgeError::IncompleteSurface);
    }
    let position_pages = pages_for(candidate, |surface| surface == OkxPrivateSurface::Positions);
    if position_pages.len() != 1 {
        return Err(OkxNodeBridgeError::IncompleteSurface);
    }
    let fills_pages = pages_for(candidate, |surface| surface == OkxPrivateSurface::Fills);
    if fills_pages.is_empty() {
        return Err(OkxNodeBridgeError::IncompleteSurface);
    }

    let mut receipts = vec![
        complete_receipt(
            &recovery_scope,
            PhysicalReadbackSurface::Account,
            attempt_id,
            collected.private_generation,
            &account_pages,
            1,
        )?,
        complete_receipt(
            &recovery_scope,
            PhysicalReadbackSurface::Positions,
            attempt_id,
            collected.private_generation,
            &position_pages,
            usize_to_u64(candidate.positions.len())?,
        )?,
    ];
    for (family, surface) in [
        (NativeOrderFamily::UmOrder, PhysicalReadbackSurface::UmOrder),
        (
            NativeOrderFamily::UmConditional,
            PhysicalReadbackSurface::UmConditional,
        ),
        (NativeOrderFamily::UmAlgo, PhysicalReadbackSurface::UmAlgo),
    ] {
        let readback = candidate
            .order_family(family)
            .ok_or(OkxNodeBridgeError::IncompleteSurface)?;
        if readback.family != family
            || readback.raw_pages.is_empty()
            || readback.orders.iter().any(|order| {
                order.family != family
                    || order.trade_mode != candidate.scope().trade_mode()
                    || order.order.symbol != recovery_scope.binding().symbol
            })
        {
            return Err(OkxNodeBridgeError::IncompleteSurface);
        }
        receipts.push(complete_receipt(
            &recovery_scope,
            surface,
            attempt_id,
            collected.private_generation,
            &readback.raw_pages.iter().collect::<Vec<_>>(),
            usize_to_u64(readback.orders.len())?,
        )?);
    }
    receipts.push(complete_receipt(
        &recovery_scope,
        PhysicalReadbackSurface::FillsCursor,
        attempt_id,
        collected.private_generation,
        &fills_pages,
        usize_to_u64(candidate.fills.len())?,
    )?);

    PhysicalRecoveryReadbackManifest::verified(recovery_scope, receipts)
        .map_err(OkxNodeBridgeError::Manifest)
}

fn validate_position_mode(
    candidate: &OkxPrivateReadbackCandidate,
) -> Result<(), OkxNodeBridgeError> {
    if candidate.profile.position_mode() != candidate.scope().expected_position_mode()
        || candidate
            .positions
            .iter()
            .any(|position| position.position.symbol != candidate.scope().gateway_binding().symbol)
    {
        return Err(OkxNodeBridgeError::PositionMode);
    }
    let actual = candidate
        .positions
        .iter()
        .map(|position| position.position.side)
        .collect::<Vec<_>>();
    let expected = match candidate.profile.position_mode() {
        OkxPositionMode::Net => vec![PositionSide::Net],
        OkxPositionMode::LongShort => vec![PositionSide::Long, PositionSide::Short],
    };
    if actual != expected {
        return Err(OkxNodeBridgeError::PositionMode);
    }
    Ok(())
}

fn pages_for(
    candidate: &OkxPrivateReadbackCandidate,
    predicate: impl Fn(OkxPrivateSurface) -> bool,
) -> Vec<&OkxRawPrivatePage> {
    candidate
        .raw_pages
        .iter()
        .filter(|page| predicate(page.surface))
        .collect()
}

fn complete_receipt(
    recovery_scope: &PhysicalRecoveryScope,
    surface: PhysicalReadbackSurface,
    attempt_id: u64,
    private_generation: u64,
    pages: &[&OkxRawPrivatePage],
    record_count: u64,
) -> Result<PhysicalReadbackReceipt, OkxNodeBridgeError> {
    let evidence_sha256 = evidence_commitment(recovery_scope, pages)?;
    PhysicalReadbackReceipt::verified_complete(
        recovery_scope,
        surface,
        attempt_id,
        private_generation,
        evidence_sha256,
        record_count,
    )
    .map_err(OkxNodeBridgeError::Manifest)
}

fn evidence_commitment(
    recovery_scope: &PhysicalRecoveryScope,
    pages: &[&OkxRawPrivatePage],
) -> Result<[u8; 32], OkxNodeBridgeError> {
    if pages.is_empty() {
        return Err(OkxNodeBridgeError::IncompleteSurface);
    }
    let mut ordered = pages.to_vec();
    ordered.sort_by_key(|page| (page.surface, page.page_index));
    let mut digest = Sha256::new();
    digest.update(b"venue-okx-post-recovery-surface-v1");
    digest.update(recovery_scope.commitment_sha256());
    for page in ordered {
        page.validate()
            .map_err(|_| OkxNodeBridgeError::IncompleteSurface)?;
        if page.scope.gateway_binding() != recovery_scope.binding() {
            return Err(OkxNodeBridgeError::Scope);
        }
        let encoded =
            serde_json::to_vec(page).map_err(|_| OkxNodeBridgeError::EvidenceCommitment)?;
        digest.update(
            u64::try_from(encoded.len())
                .map_err(|_| OkxNodeBridgeError::EvidenceCommitment)?
                .to_be_bytes(),
        );
        digest.update(encoded);
    }
    Ok(digest.finalize().into())
}

fn usize_to_u64(value: usize) -> Result<u64, OkxNodeBridgeError> {
    u64::try_from(value).map_err(|_| OkxNodeBridgeError::EvidenceCommitment)
}

/// Compile-time bridge from the validated OKX physical candidate to the fixed node contract.
/// The shared host currently has no recovery-time constructor that can supply this value with an
/// Owner route, applied Control state and a post-connect full signed generation. Consequently the
/// binary never constructs it and remains fail-closed before credentials or network access.
struct OkxNodePhysicalCandidate {
    candidate: OkxPhysicalCandidate,
}

impl PhysicalGateway for OkxNodePhysicalCandidate {
    type Error = OkxNodeBridgeError;

    fn binding(&self) -> &GatewayBinding {
        self.candidate.binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.candidate.binding().clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: CapabilityFlags::empty(),
        }
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != self.candidate.binding()
            || self.candidate.private_generation() <= permit.private_generation_floor()
        {
            return Err(OkxNodeBridgeError::Scope);
        }
        // A durable probe predates recovery. It cannot be relabelled as the required fresh private
        // generation, and opening credentials/network here without the shared collector would
        // bypass the account-wide signed readback gate.
        Err(OkxNodeBridgeError::PostRecoveryCollectorUnavailable)
    }

    fn signed_readback(
        &mut self,
        _request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        Err(OkxNodeBridgeError::PostRecoveryCollectorUnavailable)
    }

    fn verify_signed_readback(&self, _receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        Err(OkxNodeBridgeError::PostRecoveryCollectorUnavailable)
    }

    fn dispatch(&mut self, _permit: DispatchPermit) -> GatewayDispatchResult {
        // `connect_after_recovery` cannot succeed until the shared post-recovery collector exists.
        // Preserve UNKNOWN/no-resubmit semantics if a future caller violates that sequencing.
        GatewayDispatchResult::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum OkxNodeBridgeError {
    #[error("OKX node physical candidate does not match the recovered binding or generation")]
    Scope,
    #[error("OKX readback does not contain exact Net or Long/Short position coverage")]
    PositionMode,
    #[error("OKX readback omits a required raw account, position, order-family, or fills surface")]
    IncompleteSurface,
    #[error("OKX readback evidence commitment could not be encoded")]
    EvidenceCommitment,
    #[error("OKX node lacks a scope-bound post-recovery full signed readback collector")]
    PostRecoveryCollectorUnavailable,
    #[error(transparent)]
    Manifest(PhysicalRecoveryManifestError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::GatewayMode;
    use venue_gateway_okx::{
        OkxAlgoOrderKind, OkxPrivateReadRequest, OkxPrivateReadScope, OkxTradeMode,
        build_account_config_request, build_algo_orders_request, build_balance_request,
        build_fills_request, build_positions_request, build_regular_orders_request,
        complete_private_readback, parse_instrument,
    };
    use venue_runtime::account::{PhysicalReadbackCoverage, PhysicalRecoveryAuthorityRoots};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
    const CONFIG_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const INSTRUMENT: &[u8] =
        include_bytes!("../../../../crates/venue-gateway-okx/fixtures/linear-swap-instrument.json");
    const HEDGE_PROFILE: &str =
        include_str!("../../../../crates/venue-gateway-okx/fixtures/account-config.json");
    const NET_PROFILE: &str = r#"{"code":"0","msg":"","data":[{"uid":"fixture-sub-account","mainUid":"fixture-main-account","acctLv":"3","posMode":"net_mode","perm":"read_only,trade"}]}"#;
    const EMPTY: &str = r#"{"code":"0","msg":"","data":[]}"#;
    const BALANCE: &str = r#"{"code":"0","msg":"","data":[{"uTime":"1899999999000","details":[{"ccy":"USDT","eq":"1000","availBal":"900","imr":"50","mmr":"10","uTime":"1899999998000"}]}]}"#;

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Test,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)
    }

    fn page(
        request: OkxPrivateReadRequest,
        payload: impl Into<Vec<u8>>,
    ) -> Result<OkxRawPrivatePage, Box<dyn std::error::Error>> {
        Ok(OkxRawPrivatePage::new(
            &request,
            1_900_000_000_000,
            payload.into(),
        )?)
    }

    fn candidate(
        mode: OkxPositionMode,
    ) -> Result<OkxPrivateReadbackCandidate, Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(binding()?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, 7)?;
        let scope = OkxPrivateReadScope::new(&config, &instrument, mode, OkxTradeMode::Cross, 11)?;
        let profile = match mode {
            OkxPositionMode::Net => NET_PROFILE,
            OkxPositionMode::LongShort => HEDGE_PROFILE,
        };
        let mut pages = vec![
            page(build_account_config_request(&scope)?, profile)?,
            page(build_balance_request(&scope)?, BALANCE)?,
            page(build_positions_request(&scope)?, EMPTY)?,
            page(build_regular_orders_request(&scope, 0, None)?, EMPTY)?,
            page(build_fills_request(&scope, 0, None)?, EMPTY)?,
        ];
        for kind in [
            OkxAlgoOrderKind::ConditionalOco,
            OkxAlgoOrderKind::Trigger,
            OkxAlgoOrderKind::MoveOrderStop,
            OkxAlgoOrderKind::Chase,
            OkxAlgoOrderKind::Iceberg,
            OkxAlgoOrderKind::Twap,
            OkxAlgoOrderKind::SmartIceberg,
        ] {
            pages.push(page(
                build_algo_orders_request(&scope, kind, 0, None)?,
                EMPTY,
            )?);
        }
        Ok(complete_private_readback(&scope, &instrument, pages)?)
    }

    fn recovery_scope(
        recovered_private_generation: u64,
    ) -> Result<PhysicalRecoveryScope, Box<dyn std::error::Error>> {
        let roots = PhysicalRecoveryAuthorityRoots::verified([1; 32], [2; 32], [3; 32])?;
        Ok(PhysicalRecoveryScope::verified(
            binding()?,
            CONFIG_DIGEST,
            3,
            recovered_private_generation,
            roots,
        )?)
    }

    fn record_count(coverage: &PhysicalReadbackCoverage) -> Option<u64> {
        match coverage {
            PhysicalReadbackCoverage::Complete { record_count, .. } => Some(*record_count),
            PhysicalReadbackCoverage::Unsupported { .. } => None,
        }
    }

    fn map_candidate(
        candidate: &OkxPrivateReadbackCandidate,
        scope: PhysicalRecoveryScope,
        private_generation: u64,
    ) -> Result<PhysicalRecoveryReadbackManifest, OkxNodeBridgeError> {
        let recovery_scope_sha256 = *scope.commitment_sha256();
        map_post_recovery_readback(
            scope,
            OkxPostRecoveryCollection {
                candidate,
                private_generation,
                recovery_scope_sha256,
            },
        )
    }

    #[test]
    fn net_and_hedge_collections_map_all_six_complete_surfaces()
    -> Result<(), Box<dyn std::error::Error>> {
        for (mode, expected_positions) in
            [(OkxPositionMode::Net, 1), (OkxPositionMode::LongShort, 2)]
        {
            let candidate = candidate(mode)?;
            let manifest = map_candidate(&candidate, recovery_scope(40)?, 41)?;
            assert_eq!(manifest.attempt_id(), 11);
            assert_eq!(manifest.private_generation(), 41);
            assert_eq!(
                manifest.scope().binding(),
                candidate.scope().gateway_binding()
            );
            assert_eq!(
                record_count(manifest.coverage(PhysicalReadbackSurface::Account)),
                Some(1)
            );
            assert_eq!(
                record_count(manifest.coverage(PhysicalReadbackSurface::Positions)),
                Some(expected_positions)
            );
            for surface in [
                PhysicalReadbackSurface::UmOrder,
                PhysicalReadbackSurface::UmConditional,
                PhysicalReadbackSurface::UmAlgo,
                PhysicalReadbackSurface::FillsCursor,
            ] {
                assert_eq!(record_count(manifest.coverage(surface)), Some(0));
                assert_ne!(manifest.coverage(surface).evidence_sha256(), &[0; 32]);
            }
        }
        Ok(())
    }

    #[test]
    fn stale_generation_and_missing_recovery_commitment_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate(OkxPositionMode::LongShort)?;
        assert_eq!(
            map_candidate(&candidate, recovery_scope(41)?, 41),
            Err(OkxNodeBridgeError::Manifest(
                PhysicalRecoveryManifestError::StaleGeneration
            ))
        );

        let scope = recovery_scope(40)?;
        assert_eq!(
            map_post_recovery_readback(
                scope,
                OkxPostRecoveryCollection {
                    candidate: &candidate,
                    private_generation: 41,
                    recovery_scope_sha256: [0; 32],
                },
            ),
            Err(OkxNodeBridgeError::Scope)
        );
        Ok(())
    }

    #[test]
    fn mapper_does_not_promote_adapter_capability_or_mutation() {
        assert!(capabilities().is_empty());
        assert_eq!(
            OkxNodeBridgeError::PostRecoveryCollectorUnavailable.to_string(),
            "OKX node lacks a scope-bound post-recovery full signed readback collector"
        );
    }
}
