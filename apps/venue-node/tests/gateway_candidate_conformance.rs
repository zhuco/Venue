use std::{ffi::OsString, path::Path};

use venue_gateway_api::{GatewayMode, VenueId};
use venue_node::NodeLaunch;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000041";

struct CandidateContract {
    venue: VenueId,
    binary_source: &'static str,
    symbol: &'static str,
    bridge_markers: &'static [&'static str],
}

const CONTRACTS: [CandidateContract; 6] = [
    CandidateContract {
        venue: VenueId::Binance,
        binary_source: include_str!("../src/bin/venue-node-binance.rs"),
        symbol: "BTC/USDT",
        bridge_markers: &[
            "test_and_live_remain_exact_and_candidate_capability_is_empty",
            "readback_converts_net_and_hedge_with_exact_three_family_coverage",
            "shared_commands_translate_only_place_cancel_and_reduce_once",
            "acknowledgement_requires_exact_signed_readback_or_stays_unknown",
        ],
    },
    CandidateContract {
        venue: VenueId::Bitget,
        binary_source: include_str!("../src/bin/venue-node-bitget.rs"),
        symbol: "BTC/USDT",
        bridge_markers: &[
            "fixed_wrapper_binds_demo_and_live_but_grants_no_capability",
            "FamilyReadbackCoverage::complete(NativeOrderFamily::UmOrder)",
            "FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmConditional)",
            "FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmAlgo)",
            "ack_is_exposed_only_after_exact_readback_and_failure_becomes_unknown",
            "self.dispatches += 1",
        ],
    },
    CandidateContract {
        venue: VenueId::Bybit,
        binary_source: include_str!("../src/bin/venue-node-bybit.rs"),
        symbol: "BTC/USDT",
        bridge_markers: &[
            "test_and_live_candidates_are_inert_and_create_no_capability",
            "shared_delta_is_explicit_and_precredential",
            "Err(self.fail_before_credentials())",
            "GatewayDispatchResult::Rejected",
        ],
    },
    CandidateContract {
        venue: VenueId::Gate,
        binary_source: include_str!("../src/bin/venue-node-gate.rs"),
        symbol: "DOGE/USDT",
        bridge_markers: &[
            "candidate_binds_exact_test_live_origins_without_connecting",
            "candidate_requires_hedge_legs_regular_profile_and_exact_fill_cursor",
            "post_only_exact_cancel_and_reduce_once_cross_the_candidate_once",
            "ack_unknown_never_becomes_ack_or_retries_inside_the_wrapper",
        ],
    },
    CandidateContract {
        venue: VenueId::Hyperliquid,
        binary_source: include_str!("../src/bin/venue-node-hyperliquid.rs"),
        symbol: "BTC/USDC",
        bridge_markers: &[
            "fixed_candidate_bridge_is_physical_but_never_auto_authorizes",
            "Err(HyperliquidBridgeError::SharedIntegration)",
            "GatewayDispatchResult::Unknown",
        ],
    },
    CandidateContract {
        venue: VenueId::Okx,
        binary_source: include_str!("../src/bin/venue-node-okx.rs"),
        symbol: "BTC/USDT",
        bridge_markers: &[
            "assert_candidate_bridge::<OkxNodePhysicalCandidate>()",
            "Err(OkxNodeBridgeError::FreshReadbackUnavailable)",
            "GatewayDispatchResult::Unknown",
            "reject_unintegrated_runtime",
        ],
    },
];

fn arguments(mode: &str, symbol: &str) -> Vec<OsString> {
    vec![
        "venue-node-contract".into(),
        "--mode".into(),
        mode.into(),
        "--trading-account-id".into(),
        ACCOUNT.into(),
        "--symbol".into(),
        symbol.into(),
        "--artifacts-base".into(),
        std::env::temp_dir()
            .join("venue-goal41-gateway-contract")
            .into_os_string(),
    ]
}

#[test]
fn all_six_bindings_keep_test_and_live_roots_disjoint() -> Result<(), Box<dyn std::error::Error>> {
    for contract in &CONTRACTS {
        let test = NodeLaunch::try_parse_from(contract.venue, arguments("TEST", contract.symbol))?;
        let live = NodeLaunch::try_parse_from(contract.venue, arguments("LIVE", contract.symbol))?;

        assert_eq!(test.binding().venue, contract.venue);
        assert_eq!(test.binding().mode, GatewayMode::Test);
        assert_eq!(live.binding().mode, GatewayMode::Live);
        assert_ne!(test.artifacts_root(), live.artifacts_root());
        assert!(test.artifacts_root().ends_with(Path::new(ACCOUNT)));
        assert!(live.artifacts_root().ends_with(Path::new(ACCOUNT)));

        for invalid in ["test", "live", "SHADOW", " LIVE "] {
            assert!(
                NodeLaunch::try_parse_from(contract.venue, arguments(invalid, contract.symbol),)
                    .is_err()
            );
        }
    }
    Ok(())
}

#[test]
fn every_feature_is_bound_to_an_explicit_candidate_contract() {
    for contract in &CONTRACTS {
        assert!(contract.binary_source.contains("PhysicalGateway for"));
        assert!(contract.binary_source.contains("fn capability_snapshot"));
        for marker in contract.bridge_markers {
            assert!(
                contract.binary_source.contains(marker),
                "{} candidate contract marker is missing: {marker}",
                contract.venue
            );
        }
    }
}

#[test]
fn shared_host_regressions_cover_family_completeness_and_unknown_no_resubmit() {
    let shared_host_tests = include_str!("../src/safe_host_tests.rs");
    for regression in [
        "wrong_binding_or_mode_fails_before_artifact_creation",
        "submitted_crash_becomes_unknown_and_uses_readback_without_dispatch",
        "ack_then_disconnect_stays_unknown_until_signed_readback_and_never_retries",
        "incomplete_family_readback_fails_closed",
        "unsupported_signed_order_family_cannot_be_opened_by_capability_flags",
        "capability_binding_mismatch_is_rejected_before_wal",
    ] {
        assert!(
            shared_host_tests.contains(regression),
            "shared gateway regression is missing: {regression}"
        );
    }
}

#[test]
fn verifier_pins_the_baseline_and_all_six_feature_runs() {
    let verifier = include_str!("../../../scripts/verify_gateway_candidate_contract.ps1");
    assert!(verifier.contains("af54c157400ff819c0027c06cd96c6fcf6e101c8"));
    for contract in &CONTRACTS {
        assert!(
            verifier.contains(contract.venue.as_str()),
            "verifier omits {}",
            contract.venue
        );
    }
    assert!(verifier.contains("--no-default-features"));
    assert!(verifier.contains("gateway_candidate_conformance"));
}
