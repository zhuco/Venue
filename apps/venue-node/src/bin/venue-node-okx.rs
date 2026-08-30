use std::process::ExitCode;

#[cfg(test)]
use sha2::{Digest, Sha256};
#[cfg(test)]
use venue_domain::domain::{NativeOrderFamily, PositionSide};
#[cfg(test)]
use venue_gateway_api::GatewayBinding;
use venue_gateway_api::VenueId;
use venue_gateway_okx::{OkxConfig, capabilities};
#[cfg(test)]
use venue_gateway_okx::{
    OkxPositionMode, OkxPrivateReadbackCandidate, OkxPrivateSurface, OkxRawPrivatePage,
};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_runtime, report_result,
};
#[cfg(test)]
use venue_runtime::account::{
    PhysicalReadbackReceipt, PhysicalReadbackSurface, PhysicalRecoveryManifestError,
    PhysicalRecoveryReadbackManifest, PhysicalRecoveryScope,
};

const PROGRAM: &str = "venue-node-okx";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
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

#[cfg(test)]
fn reject_missing_post_recovery_authority() -> OkxNodeBridgeError {
    OkxNodeBridgeError::PostRecoveryCollectorUnavailable
}

/// Test-only contract for a future collector. Production deliberately has no constructor or
/// mapper: the current durable probe does not carry post-recovery freshness or root authority.
#[cfg(test)]
fn map_test_only_recovery_readback(
    recovery_scope: PhysicalRecoveryScope,
    candidate: &OkxPrivateReadbackCandidate,
    private_generation: u64,
) -> Result<PhysicalRecoveryReadbackManifest, OkxNodeBridgeError> {
    if candidate.scope().gateway_binding() != recovery_scope.binding() {
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
            private_generation,
            candidate,
            &account_pages,
            1,
        )?,
        complete_receipt(
            &recovery_scope,
            PhysicalReadbackSurface::Positions,
            attempt_id,
            private_generation,
            candidate,
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
            private_generation,
            candidate,
            &readback.raw_pages.iter().collect::<Vec<_>>(),
            usize_to_u64(readback.orders.len())?,
        )?);
    }
    receipts.push(complete_receipt(
        &recovery_scope,
        PhysicalReadbackSurface::FillsCursor,
        attempt_id,
        private_generation,
        candidate,
        &fills_pages,
        usize_to_u64(candidate.fills.len())?,
    )?);

    PhysicalRecoveryReadbackManifest::verified(recovery_scope, receipts)
        .map_err(OkxNodeBridgeError::Manifest)
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn complete_receipt(
    recovery_scope: &PhysicalRecoveryScope,
    surface: PhysicalReadbackSurface,
    attempt_id: u64,
    private_generation: u64,
    candidate: &OkxPrivateReadbackCandidate,
    pages: &[&OkxRawPrivatePage],
    record_count: u64,
) -> Result<PhysicalReadbackReceipt, OkxNodeBridgeError> {
    let evidence_sha256 = evidence_commitment(recovery_scope, candidate, pages)?;
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

#[cfg(test)]
fn evidence_commitment(
    recovery_scope: &PhysicalRecoveryScope,
    candidate: &OkxPrivateReadbackCandidate,
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
        if page.scope != *candidate.scope()
            || page.scope.gateway_binding() != recovery_scope.binding()
        {
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

#[cfg(test)]
fn usize_to_u64(value: usize) -> Result<u64, OkxNodeBridgeError> {
    u64::try_from(value).map_err(|_| OkxNodeBridgeError::EvidenceCommitment)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum OkxNodeBridgeError {
    #[error("OKX node physical candidate does not match the recovered binding or generation")]
    Scope,
    #[cfg(test)]
    #[error("OKX readback does not contain exact Net or Long/Short position coverage")]
    PositionMode,
    #[cfg(test)]
    #[error("OKX readback omits a required raw account, position, order-family, or fills surface")]
    IncompleteSurface,
    #[cfg(test)]
    #[error("OKX readback evidence commitment could not be encoded")]
    EvidenceCommitment,
    #[error("OKX node lacks a scope-bound post-recovery full signed readback collector")]
    PostRecoveryCollectorUnavailable,
    #[cfg(test)]
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

    fn alternate_scope(
        mode: OkxPositionMode,
        trade_mode: OkxTradeMode,
        attempt_id: u64,
        instrument_generation: u64,
    ) -> Result<OkxPrivateReadScope, Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(binding()?)?;
        let instrument = parse_instrument(INSTRUMENT, &config, instrument_generation)?;
        Ok(OkxPrivateReadScope::new(
            &config,
            &instrument,
            mode,
            trade_mode,
            attempt_id,
        )?)
    }

    fn recovery_scope(
        recovered_private_generation: u64,
    ) -> Result<PhysicalRecoveryScope, Box<dyn std::error::Error>> {
        let roots = PhysicalRecoveryAuthorityRoots::verified([1; 32], [2; 32], [3; 32])?;
        Ok(PhysicalRecoveryScope::verified(
            binding()?,
            CONFIG_DIGEST,
            3,
            1,
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
        map_test_only_recovery_readback(scope, candidate, private_generation)
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
    fn stale_generation_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate(OkxPositionMode::LongShort)?;
        assert_eq!(
            map_candidate(&candidate, recovery_scope(41)?, 41),
            Err(OkxNodeBridgeError::Manifest(
                PhysicalRecoveryManifestError::StaleGeneration
            ))
        );
        Ok(())
    }

    #[test]
    fn cross_attempt_mode_and_instrument_generation_pages_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        for alternate in [
            alternate_scope(OkxPositionMode::LongShort, OkxTradeMode::Cross, 12, 7)?,
            alternate_scope(OkxPositionMode::Net, OkxTradeMode::Cross, 11, 7)?,
            alternate_scope(OkxPositionMode::LongShort, OkxTradeMode::Isolated, 11, 7)?,
        ] {
            let replacement = page(build_fills_request(&alternate, 0, None)?, EMPTY)?;
            replacement.validate()?;
            let mut mixed = candidate(OkxPositionMode::LongShort)?;
            let target = mixed
                .raw_pages
                .iter_mut()
                .find(|raw| raw.surface == OkxPrivateSurface::Fills)
                .ok_or("missing fills page")?;
            *target = replacement;
            assert_eq!(
                map_candidate(&mixed, recovery_scope(40)?, 41),
                Err(OkxNodeBridgeError::Scope)
            );
        }

        let alternate = alternate_scope(OkxPositionMode::LongShort, OkxTradeMode::Cross, 11, 8)?;
        let replacement = page(build_regular_orders_request(&alternate, 0, None)?, EMPTY)?;
        replacement.validate()?;
        let mut mixed = candidate(OkxPositionMode::LongShort)?;
        let regular = mixed
            .order_families
            .get_mut(&NativeOrderFamily::UmOrder)
            .ok_or("missing regular family")?;
        let [target] = regular.raw_pages.as_mut_slice() else {
            return Err("unexpected regular page count".into());
        };
        *target = replacement;
        assert_eq!(
            map_candidate(&mixed, recovery_scope(40)?, 41),
            Err(OkxNodeBridgeError::Scope)
        );
        Ok(())
    }

    #[test]
    fn old_probe_candidate_cannot_be_relabelled_by_production_bridge() {
        assert!(capabilities().is_empty());
        assert_eq!(
            reject_missing_post_recovery_authority(),
            OkxNodeBridgeError::PostRecoveryCollectorUnavailable
        );
        assert_eq!(
            reject_missing_post_recovery_authority().to_string(),
            "OKX node lacks a scope-bound post-recovery full signed readback collector"
        );
    }
}
