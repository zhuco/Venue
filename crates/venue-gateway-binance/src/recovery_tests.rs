use std::collections::BTreeSet;
use std::error::Error;

use bytes::Bytes;
use venue_domain::domain::{NativeOrderFamily, OrderOwner, OrderPurpose, Symbol};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use super::*;
use crate::{
    BinanceAccountBinding, BinancePrivateReadScope, build_account_config_request,
    build_account_request, build_algo_orders_request, build_fills_request,
    build_position_mode_request, build_positions_request, build_regular_orders_request,
    parse_instrument_rules,
};

const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";
const EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
const ACCOUNT: &[u8] = include_bytes!("../fixtures/portfolio-account.json");
const ACCOUNT_CONFIG: &[u8] = include_bytes!("../fixtures/account-config.json");
const POSITION_MODE: &[u8] = include_bytes!("../fixtures/position-mode-hedge.json");
const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-hedge-long-only.json");
const REGULAR: &[u8] = include_bytes!("../fixtures/open-orders.json");
const ALGO: &[u8] = include_bytes!("../fixtures/open-algo-orders.json");
const FILLS: &[u8] = include_bytes!("../fixtures/user-trades-page.json");

type TestResult = Result<(), Box<dyn Error>>;

fn config_and_rules() -> Result<(BinanceConfig, BinanceInstrumentRules), Box<dyn Error>> {
    config_and_rules_for(GatewayMode::Test, ACCOUNT_ID)
}

fn config_and_rules_for(
    mode: GatewayMode,
    account_id: &str,
) -> Result<(BinanceConfig, BinanceInstrumentRules), Box<dyn Error>> {
    let binding = GatewayBinding::new(VenueId::Binance, mode, account_id, "BTC/USDT".parse()?)?;
    let config = BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
    let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol, 7)?;
    Ok((config, rules))
}

fn authority_roots(seed: u8) -> Result<BinanceRecoveryAuthorityRoots, Box<dyn Error>> {
    Ok(BinanceRecoveryAuthorityRoots::verified(
        [seed; 32],
        [seed.saturating_add(1); 32],
        [seed.saturating_add(2); 32],
    )?)
}

fn collection_scope_with(
    config: &BinanceConfig,
    config_digest: &str,
    private_generation: u64,
    deadline_at_ms: u64,
    roots_seed: u8,
) -> Result<BinanceRecoveryCollectionScope, Box<dyn Error>> {
    collection_scope_with_connection(
        config,
        config_digest,
        private_generation,
        deadline_at_ms,
        roots_seed,
        9,
    )
}

fn collection_scope_with_connection(
    config: &BinanceConfig,
    config_digest: &str,
    private_generation: u64,
    deadline_at_ms: u64,
    roots_seed: u8,
    connection_generation: u64,
) -> Result<BinanceRecoveryCollectionScope, Box<dyn Error>> {
    Ok(BinanceRecoveryCollectionScope::verified(
        config,
        BinanceRecoveryScopeInput {
            config_digest: config_digest.to_owned(),
            config_epoch: 5,
            connection_generation,
            recovered_private_generation: 16,
            private_generation,
            attempt_id: 11,
            started_at_ms: 900,
            deadline_at_ms,
            authority_roots: authority_roots(roots_seed)?,
            symbol_universe: BTreeSet::from([config.gateway_binding().symbol.clone()]),
        },
    )?)
}

fn raw_page(
    request: crate::BinancePrivateReadRequest,
    payload: &[u8],
    offset: u64,
) -> Result<BinanceRawPrivatePage, Box<dyn Error>> {
    Ok(BinanceRawPrivatePage::new(
        &request,
        1_000 + offset,
        1_001 + offset,
        Bytes::copy_from_slice(payload),
    )?)
}

fn replay(
    config: &BinanceConfig,
    rules: &BinanceInstrumentRules,
    private_generation: u64,
    regular: &[u8],
    algo: &[u8],
) -> Result<BinanceRecoveryReplay, Box<dyn Error>> {
    let scope = BinancePrivateReadScope::new(config, rules, private_generation, 11, 900)?;
    let cursor = RecentFillsCursor {
        observed_through_ms: 1_000,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let pages = vec![
        raw_page(build_account_request(&scope)?, ACCOUNT, 0)?,
        raw_page(build_account_config_request(&scope)?, ACCOUNT_CONFIG, 1)?,
        raw_page(build_position_mode_request(&scope)?, POSITION_MODE, 2)?,
        raw_page(build_positions_request(&scope)?, POSITIONS, 3)?,
        raw_page(build_regular_orders_request(&scope)?, regular, 4)?,
        raw_page(build_algo_orders_request(&scope)?, algo, 5)?,
        raw_page(
            build_fills_request(&scope, 1, cursor, 1_000, 2_000)?,
            FILLS,
            6,
        )?,
    ];
    Ok(BinanceRecoveryReplay::new(
        9,
        config.clone(),
        rules.clone(),
        cursor,
        2_000,
        pages,
    ))
}

fn owner(symbol: Symbol, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: "grid_btc".to_owned(),
        run_id: "run_1".to_owned(),
        exchange: "binance".to_owned(),
        account: ACCOUNT_ID.to_owned(),
        symbol,
        purpose,
    }
}

fn exact_routes(symbol: &Symbol) -> Result<Vec<BinanceRecoveryOwnerRoute>, Box<dyn Error>> {
    Ok(vec![
        BinanceRecoveryOwnerRoute::verified(
            NativeOrderFamily::UmOrder,
            "101",
            "venue_regular_1",
            owner(symbol.clone(), OrderPurpose::Entry),
        )?,
        BinanceRecoveryOwnerRoute::verified(
            NativeOrderFamily::UmAlgo,
            "201",
            "venue_algo_1",
            owner(symbol.clone(), OrderPurpose::Protection),
        )?,
    ])
}

fn collected_candidate(
    routes: Vec<BinanceRecoveryOwnerRoute>,
) -> Result<BinanceFreshRecoveryCandidate, Box<dyn Error>> {
    let (config, rules) = config_and_rules()?;
    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    Ok(BinanceFreshRecoveryCollector::begin(scope, routes)?
        .finish(2_100, vec![replay(&config, &rules, 17, REGULAR, ALGO)?])?)
}

#[test]
fn fresh_candidate_commits_all_six_faces_and_exact_owners() -> TestResult {
    let (config, _) = config_and_rules()?;
    let candidate = collected_candidate(exact_routes(&config.gateway_binding().symbol)?)?;

    assert_eq!(candidate.faces().len(), 6);
    assert_eq!(
        candidate
            .faces()
            .iter()
            .map(BinanceRecoveryFaceCommitment::face)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(RECOVERY_FACES)
    );
    assert!(
        candidate
            .faces()
            .iter()
            .all(|face| face.evidence_sha256().iter().any(|byte| *byte != 0))
    );
    assert_eq!(candidate.projections().len(), 1);
    assert_eq!(candidate.projections()[0].order_custody().len(), 2);
    assert!(
        candidate.projections()[0]
            .order_custody()
            .iter()
            .all(|custody| matches!(custody, BinanceRecoveryOrderCustody::ExactOwner { .. }))
    );
    candidate.verify_fresh(candidate.scope(), 2_200)?;
    assert!(crate::capabilities().is_empty());
    Ok(())
}

#[test]
fn open_orders_without_exact_owner_are_structured_unknown() -> TestResult {
    let candidate = collected_candidate(Vec::new())?;
    assert!(
        candidate.projections()[0]
            .order_custody()
            .iter()
            .all(|custody| matches!(
                custody,
                BinanceRecoveryOrderCustody::Unknown {
                    reason: BinanceRecoveryUnknownReason::MissingOwnerRoute,
                    ..
                }
            ))
    );

    let (config, _) = config_and_rules()?;
    let conflicting = BinanceRecoveryOwnerRoute::verified(
        NativeOrderFamily::UmOrder,
        "999",
        "venue_regular_1",
        owner(config.gateway_binding().symbol.clone(), OrderPurpose::Entry),
    )?;
    let candidate = collected_candidate(vec![conflicting])?;
    assert!(matches!(
        candidate.projections()[0].order_custody()[0],
        BinanceRecoveryOrderCustody::Unknown {
            reason: BinanceRecoveryUnknownReason::ConflictingOwnerIdentity,
            ..
        }
    ));
    Ok(())
}

#[test]
fn raw_tampering_breaks_projection_commitment() -> TestResult {
    let (config, _) = config_and_rules()?;
    let mut candidate = collected_candidate(exact_routes(&config.gateway_binding().symbol)?)?;
    let account = candidate.replays[0]
        .raw_pages
        .iter_mut()
        .find(|page| page.surface == BinancePrivateSurface::Account)
        .ok_or("missing account page")?;
    let mut payload = account.payload.to_vec();
    payload.push(b' ');
    account.payload = Bytes::from(payload);

    assert_eq!(
        candidate.verify_fresh(candidate.scope(), 2_200),
        Err(BinanceRecoveryCollectorError::ProjectionCommitment)
    );
    Ok(())
}

#[test]
fn old_probe_cannot_be_relabelled_or_used_after_deadline() -> TestResult {
    let (config, _) = config_and_rules()?;
    let candidate = collected_candidate(exact_routes(&config.gateway_binding().symbol)?)?;
    let relabelled = collection_scope_with(&config, "config_2", 17, 2_500, 9)?;
    let connection_relabelled =
        collection_scope_with_connection(&config, "config_1", 17, 2_500, 1, 10)?;
    let (other_config, _) =
        config_and_rules_for(GatewayMode::Live, "00000000-0000-4000-8000-000000000099")?;
    let mode_account_relabelled = collection_scope_with(&other_config, "config_1", 17, 2_500, 1)?;

    for drifted in [
        &relabelled,
        &connection_relabelled,
        &mode_account_relabelled,
    ] {
        assert_eq!(
            candidate.verify_fresh(drifted, 2_200),
            Err(BinanceRecoveryCollectorError::Relabelled)
        );
    }
    assert_eq!(
        candidate.verify_fresh(candidate.scope(), 2_501),
        Err(BinanceRecoveryCollectorError::Expired)
    );
    Ok(())
}

#[test]
fn missing_order_family_fails_the_whole_attempt() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    for missing in [
        BinancePrivateSurface::RegularOrders,
        BinancePrivateSurface::AlgoOrders,
    ] {
        let mut incomplete = replay(&config, &rules, 17, REGULAR, ALGO)?;
        incomplete.raw_pages.retain(|page| page.surface != missing);
        assert_eq!(
            BinanceFreshRecoveryCollector::begin(scope.clone(), Vec::new())?
                .finish(2_100, vec![incomplete]),
            Err(BinanceRecoveryCollectorError::Replay)
        );
    }
    Ok(())
}

#[test]
fn cross_generation_pages_and_stale_generation_fail_closed() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    let mut mixed = replay(&config, &rules, 17, REGULAR, ALGO)?;
    let newer = replay(&config, &rules, 18, REGULAR, ALGO)?;
    mixed.raw_pages[4] = newer.raw_pages[4].clone();
    assert_eq!(
        BinanceFreshRecoveryCollector::begin(scope, Vec::new())?.finish(2_100, vec![mixed]),
        Err(BinanceRecoveryCollectorError::AttemptDrift)
    );

    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    let mut after_completion = replay(&config, &rules, 17, REGULAR, ALGO)?;
    after_completion.raw_pages[0].received_at_ms = 2_200;
    assert_eq!(
        BinanceFreshRecoveryCollector::begin(scope, Vec::new())?
            .finish(2_100, vec![after_completion]),
        Err(BinanceRecoveryCollectorError::AttemptDrift)
    );

    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    let mut wrong_connection = replay(&config, &rules, 17, REGULAR, ALGO)?;
    wrong_connection.connection_generation = 10;
    assert_eq!(
        BinanceFreshRecoveryCollector::begin(scope, Vec::new())?
            .finish(2_100, vec![wrong_connection]),
        Err(BinanceRecoveryCollectorError::Scope)
    );

    assert!(collection_scope_with(&config, "config_1", 16, 2_500, 1).is_err());
    assert!(collection_scope_with(&config, "config_1", 17, 31_001, 1).is_err());
    Ok(())
}

#[test]
fn owner_routes_must_match_account_symbol_and_both_native_id_namespaces() -> TestResult {
    let (config, _) = config_and_rules()?;
    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    let symbol = config.gateway_binding().symbol.clone();
    let first = BinanceRecoveryOwnerRoute::verified(
        NativeOrderFamily::UmOrder,
        "101",
        "venue_regular_1",
        owner(symbol.clone(), OrderPurpose::Entry),
    )?;
    let duplicate_client = BinanceRecoveryOwnerRoute::verified(
        NativeOrderFamily::UmOrder,
        "102",
        "venue_regular_1",
        owner(symbol.clone(), OrderPurpose::Entry),
    )?;
    assert_eq!(
        BinanceFreshRecoveryCollector::begin(scope.clone(), vec![first, duplicate_client]),
        Err(BinanceRecoveryCollectorError::OwnerRoute)
    );

    let mut wrong_account = owner(symbol, OrderPurpose::Entry);
    wrong_account.account = "00000000-0000-4000-8000-000000000099".to_owned();
    let wrong = BinanceRecoveryOwnerRoute::verified(
        NativeOrderFamily::UmOrder,
        "101",
        "venue_regular_1",
        wrong_account,
    )?;
    assert_eq!(
        BinanceFreshRecoveryCollector::begin(scope, vec![wrong]),
        Err(BinanceRecoveryCollectorError::OwnerRoute)
    );
    Ok(())
}
