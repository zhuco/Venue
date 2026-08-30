use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use venue_domain::domain::{NativeOrderFamily, OrderOwner, OrderPurpose, Symbol};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use super::*;
use crate::{
    BinanceAccountBinding, BinanceCredentials, BinanceHttpTransport, BinancePrivateReadScope,
    BinanceTransportLimits, build_account_config_request, build_account_request,
    build_algo_orders_request, build_fills_request, build_position_mode_request,
    build_positions_request, build_regular_orders_request, parse_instrument_rules,
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
const REGULAR_EXTRA_SYMBOL: &[u8] = br#"[{"symbol":"BTCUSDT","orderId":101,"clientOrderId":"venue_regular_1","status":"NEW","side":"BUY","positionSide":"LONG","type":"LIMIT","timeInForce":"GTX","origQty":"0.002","executedQty":"0","price":"50000","avgPrice":"0","reduceOnly":false},{"symbol":"ETHUSDT","orderId":102,"clientOrderId":"foreign_regular","status":"NEW","side":"BUY","positionSide":"LONG","type":"LIMIT","timeInForce":"GTX","origQty":"0.002","executedQty":"0","price":"3000","avgPrice":"0","reduceOnly":false}]"#;
const ALGO_EXTRA_SYMBOL: &[u8] = br#"[{"symbol":"BTCUSDT","algoId":201,"clientAlgoId":"venue_algo_1","algoStatus":"NEW","orderType":"STOP_MARKET","side":"SELL","positionSide":"LONG","quantity":"0.010","triggerPrice":"49000","workingType":"MARK_PRICE","closePosition":false,"reduceOnly":true},{"symbol":"ETHUSDT","algoId":202,"clientAlgoId":"foreign_algo","algoStatus":"NEW","orderType":"STOP_MARKET","side":"SELL","positionSide":"LONG","quantity":"0.010","triggerPrice":"2900","workingType":"MARK_PRICE","closePosition":false,"reduceOnly":true}]"#;

type TestResult = Result<(), Box<dyn Error>>;

struct FixedRuntimeScopeProbe([u8; 32]);

impl BinanceRuntimeRecoveryScopeProbe for FixedRuntimeScopeProbe {
    fn current_runtime_scope_sha256(&self) -> [u8; 32] {
        self.0
    }
}

struct DriftAfterFirstScopeProbe {
    calls: AtomicUsize,
}

impl BinanceRuntimeRecoveryScopeProbe for DriftAfterFirstScopeProbe {
    fn current_runtime_scope_sha256(&self) -> [u8; 32] {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            [4; 32]
        } else {
            [9; 32]
        }
    }
}

fn config_and_rules() -> Result<(BinanceConfig, BinanceInstrumentRules), Box<dyn Error>> {
    config_and_rules_for(GatewayMode::Live, ACCOUNT_ID)
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

fn runtime_commitments(seed: u8) -> Result<BinanceRuntimeRecoveryCommitments, Box<dyn Error>> {
    Ok(BinanceRuntimeRecoveryCommitments::verified(
        [seed; 32],
        [seed.saturating_add(1); 32],
        [seed.saturating_add(2); 32],
        [seed.saturating_add(3); 32],
    )?)
}

fn collection_scope_with(
    config: &BinanceConfig,
    config_digest: &str,
    private_generation: u64,
    deadline_at_ms: u64,
    roots_seed: u8,
) -> Result<BinanceRecoveryCollectionScope, Box<dyn Error>> {
    Ok(BinanceRecoveryCollectionScope::verified_inner(
        config,
        BinanceRecoveryScopeInput {
            config_digest: config_digest.to_owned(),
            config_epoch: 5,
            recovered_private_generation: 16,
            private_generation,
            attempt_id: 11,
            started_at_ms: 900,
            deadline_at_ms,
            maximum_total_bytes: 8 * 1024 * 1024,
            maximum_total_pages: 1_000,
            runtime_commitments: runtime_commitments(roots_seed)?,
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

async fn fake_signed_reads(payloads: Vec<&'static [u8]>) -> Result<String, Box<dyn Error>> {
    Ok(fake_signed_reads_counted(payloads).await?.0)
}

async fn fake_signed_reads_counted(
    payloads: Vec<&'static [u8]>,
) -> Result<(String, Arc<AtomicUsize>), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let count = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&count);
    tokio::spawn(async move {
        for payload in payloads {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            let mut request = vec![0_u8; 16 * 1024];
            let _ = stream.read(&mut request).await;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            let _ = stream.write_all(header.as_bytes()).await;
            let _ = stream.write_all(payload).await;
        }
    });
    Ok((format!("http://{address}"), count))
}

fn production_scope(
    config: &BinanceConfig,
    started_at_ms: u64,
    maximum_total_pages: u32,
    symbol_universe: BTreeSet<Symbol>,
) -> Result<BinanceRecoveryCollectionScope, BinanceRecoveryCollectorError> {
    BinanceRecoveryCollectionScope::verified(
        config,
        BinanceRecoveryScopeInput {
            config_digest: "config_authenticated".to_owned(),
            config_epoch: 5,
            recovered_private_generation: 16,
            private_generation: 17,
            attempt_id: 11,
            started_at_ms,
            deadline_at_ms: started_at_ms + 5_000,
            maximum_total_bytes: 8 * 1024 * 1024,
            maximum_total_pages,
            runtime_commitments: runtime_commitments(1)
                .map_err(|_| BinanceRecoveryCollectorError::RuntimeCommitment)?,
            symbol_universe,
        },
    )
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
fn runtime_bundle_rejects_unknown_regular_and_algo_custody() -> TestResult {
    let (config, _) = config_and_rules()?;
    let owned = collected_candidate(exact_routes(&config.gateway_binding().symbol)?)?;
    let owned_scope = owned.scope().clone();
    let bundle = owned.into_runtime_bundle(&owned_scope, 2_200)?;
    assert_eq!(bundle.scope().runtime_commitments().owner(), &[1; 32]);
    assert_eq!(bundle.scope().runtime_commitments().wal(), &[2; 32]);
    assert_eq!(bundle.scope().runtime_commitments().unknown(), &[3; 32]);
    assert_eq!(
        bundle.scope().runtime_commitments().runtime_scope_sha256(),
        &[4; 32]
    );
    bundle.verify_fresh(&owned_scope, 2_200)?;
    let drifted_commitments = collection_scope_with(&config, "config_1", 17, 2_500, 9)?;
    assert_eq!(
        bundle.verify_fresh(&drifted_commitments, 2_200),
        Err(BinanceRecoveryCollectorError::Relabelled)
    );

    let unmanaged = collected_candidate(Vec::new())?;
    let unmanaged_scope = unmanaged.scope().clone();
    assert_eq!(
        unmanaged.into_runtime_bundle(&unmanaged_scope, 2_200),
        Err(BinanceRecoveryCollectorError::UnmanagedOrder)
    );

    let symbol = config.gateway_binding().symbol.clone();
    for routes in [
        vec![BinanceRecoveryOwnerRoute::verified(
            NativeOrderFamily::UmOrder,
            "101",
            "venue_regular_1",
            owner(symbol.clone(), OrderPurpose::Entry),
        )?],
        vec![BinanceRecoveryOwnerRoute::verified(
            NativeOrderFamily::UmAlgo,
            "201",
            "venue_algo_1",
            owner(symbol.clone(), OrderPurpose::Protection),
        )?],
    ] {
        let partial = collected_candidate(routes)?;
        let partial_scope = partial.scope().clone();
        assert_eq!(
            partial.into_runtime_bundle(&partial_scope, 2_200),
            Err(BinanceRecoveryCollectorError::UnmanagedOrder)
        );
    }
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
    let (other_config, _) =
        config_and_rules_for(GatewayMode::Live, "00000000-0000-4000-8000-000000000099")?;
    let account_relabelled = collection_scope_with(&other_config, "config_1", 17, 2_500, 1)?;

    for drifted in [&relabelled, &account_relabelled] {
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
fn extra_symbol_in_any_order_family_invalidates_the_whole_attempt() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let scope = collection_scope_with(&config, "config_1", 17, 2_500, 1)?;
    for (regular, algo) in [(REGULAR_EXTRA_SYMBOL, ALGO), (REGULAR, ALGO_EXTRA_SYMBOL)] {
        assert_eq!(
            BinanceFreshRecoveryCollector::begin(scope.clone(), Vec::new())?
                .finish(2_100, vec![replay(&config, &rules, 17, regular, algo)?],),
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

#[tokio::test]
async fn real_signed_http_establishes_private_sealed_read_only_session() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = BinanceRecoveryCollectionScope::verified(
        &config,
        BinanceRecoveryScopeInput {
            config_digest: "config_authenticated".to_owned(),
            config_epoch: 5,
            recovered_private_generation: 16,
            private_generation: 17,
            attempt_id: 11,
            started_at_ms,
            deadline_at_ms: started_at_ms + 5_000,
            maximum_total_bytes: 8 * 1024 * 1024,
            maximum_total_pages: 100,
            runtime_commitments: runtime_commitments(1)?,
            symbol_universe: BTreeSet::from([config.gateway_binding().symbol.clone()]),
        },
    )?;
    let endpoint = fake_signed_reads(vec![
        ACCOUNT,
        ACCOUNT_CONFIG,
        POSITION_MODE,
        POSITIONS,
        REGULAR,
        ALGO,
        b"[]",
    ])
    .await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config.clone(),
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    let candidate = BinanceFreshRecoveryCollector::collect_authenticated_fixture(
        scope,
        &credentials,
        vec![(&transport, rules, cursor, started_at_ms)],
        Vec::new(),
    )
    .await?;

    assert_eq!(candidate.faces().len(), 6);
    assert!(
        candidate.projections()[0]
            .order_custody()
            .iter()
            .all(|custody| matches!(custody, BinanceRecoveryOrderCustody::Unknown { .. }))
    );
    assert!(
        candidate
            .request_universe_sha256()
            .iter()
            .any(|byte| *byte != 0)
    );
    assert!(crate::capabilities().is_empty());
    Ok(())
}

#[tokio::test]
async fn runtime_scope_drift_after_authenticated_await_fails_before_next_get() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = production_scope(
        &config,
        started_at_ms,
        100,
        BTreeSet::from([config.gateway_binding().symbol.clone()]),
    )?;
    let (endpoint, reads) = fake_signed_reads_counted(vec![ACCOUNT]).await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config,
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let source = BinanceRecoverySymbolSource::verified_inner(
        &transport,
        rules,
        cursor,
        started_at_ms,
        true,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    let probe = DriftAfterFirstScopeProbe {
        calls: AtomicUsize::new(0),
    };

    assert!(matches!(
        BinanceFreshRecoveryCollector::collect_runtime_bundle_authenticated(
            scope,
            &credentials,
            vec![source],
            Vec::new(),
            &probe,
        )
        .await,
        Err(BinanceRecoveryCollectorError::RuntimeScopeDrift)
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn authenticated_runtime_bundle_carries_complete_owned_live_evidence() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = production_scope(
        &config,
        started_at_ms,
        100,
        BTreeSet::from([config.gateway_binding().symbol.clone()]),
    )?;
    let endpoint = fake_signed_reads(vec![
        ACCOUNT,
        ACCOUNT_CONFIG,
        POSITION_MODE,
        POSITIONS,
        REGULAR,
        ALGO,
        b"[]",
    ])
    .await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config.clone(),
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let source = BinanceRecoverySymbolSource::verified_inner(
        &transport,
        rules,
        cursor,
        started_at_ms,
        true,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    let bundle = BinanceFreshRecoveryCollector::collect_runtime_bundle_authenticated(
        scope,
        &credentials,
        vec![source],
        exact_routes(&config.gateway_binding().symbol)?,
        &FixedRuntimeScopeProbe([4; 32]),
    )
    .await?;

    assert_eq!(bundle.position_mode(), BinancePositionMode::Hedge);
    assert_eq!(
        bundle.execution_profile_version(),
        BINANCE_EXECUTION_PROFILE_VERSION
    );
    assert_eq!(bundle.attempt_id(), 11);
    assert_eq!(bundle.private_generation(), 17);
    assert_eq!(bundle.scope().symbol_universe().len(), 1);
    assert!(
        bundle.projections()[0]
            .order_custody()
            .iter()
            .all(|custody| matches!(custody, BinanceRecoveryOrderCustody::ExactOwner { .. }))
    );
    assert!(crate::capabilities().is_empty());
    Ok(())
}

#[tokio::test]
async fn production_source_rejects_non_fixed_endpoint_before_authentication() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let endpoint = fake_signed_reads(Vec::new()).await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config,
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
    )?;
    let cursor = RecentFillsCursor {
        observed_through_ms: 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    assert!(matches!(
        BinanceRecoverySymbolSource::verified(&transport, rules, cursor, 2),
        Err(BinanceRecoveryCollectorError::TransportEndpoint)
    ));
    Ok(())
}

#[test]
fn recovery_scope_requires_nonzero_runtime_commitments() -> TestResult {
    assert_eq!(
        BinanceRuntimeRecoveryCommitments::verified([1; 32], [2; 32], [3; 32], [0; 32]),
        Err(BinanceRecoveryCollectorError::RuntimeCommitment)
    );
    Ok(())
}

#[tokio::test]
async fn account_success_status_must_parse_before_session_is_sealed() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = production_scope(
        &config,
        started_at_ms,
        100,
        BTreeSet::from([config.gateway_binding().symbol.clone()]),
    )?;
    let endpoint = fake_signed_reads(vec![b"{}"]).await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config,
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    assert!(matches!(
        BinanceFreshRecoveryCollector::collect_authenticated_fixture(
            scope,
            &credentials,
            vec![(&transport, rules, cursor, started_at_ms)],
            Vec::new(),
        )
        .await,
        Err(BinanceRecoveryCollectorError::Authentication)
    ));
    Ok(())
}

#[tokio::test]
async fn global_page_budget_is_checked_before_the_next_http_get() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = production_scope(
        &config,
        started_at_ms,
        1,
        BTreeSet::from([config.gateway_binding().symbol.clone()]),
    )?;
    let (endpoint, count) = fake_signed_reads_counted(vec![ACCOUNT]).await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config,
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    assert!(matches!(
        BinanceFreshRecoveryCollector::collect_authenticated_fixture(
            scope,
            &credentials,
            vec![(&transport, rules, cursor, started_at_ms)],
            Vec::new(),
        )
        .await,
        Err(BinanceRecoveryCollectorError::PageLimit)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn exhausted_page_budget_prevents_the_next_symbol_authentication_get() -> TestResult {
    let (btc_config, btc_rules) = config_and_rules()?;
    let eth_symbol: Symbol = "ETH/USDT".parse()?;
    let eth_binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        ACCOUNT_ID,
        eth_symbol.clone(),
    )?;
    let eth_config =
        BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &eth_binding)?;
    let eth_exchange_info = EXCHANGE_INFO
        .replace("BTCUSDT", "ETHUSDT")
        .replace("\"BTC\"", "\"ETH\"");
    let eth_rules = parse_instrument_rules(&eth_exchange_info, eth_symbol.clone(), 7)?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = production_scope(
        &btc_config,
        started_at_ms,
        7,
        BTreeSet::from([btc_config.gateway_binding().symbol.clone(), eth_symbol]),
    )?;
    let (btc_endpoint, btc_count) = fake_signed_reads_counted(vec![
        ACCOUNT,
        ACCOUNT_CONFIG,
        POSITION_MODE,
        POSITIONS,
        REGULAR,
        ALGO,
        b"[]",
    ])
    .await?;
    let btc_transport = BinanceHttpTransport::with_endpoint(
        btc_config,
        7,
        17,
        btc_endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let (eth_endpoint, eth_count) = fake_signed_reads_counted(Vec::new()).await?;
    let eth_transport = BinanceHttpTransport::with_endpoint(
        eth_config,
        7,
        17,
        eth_endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024 * 1024)?,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;

    assert!(matches!(
        BinanceFreshRecoveryCollector::collect_authenticated_fixture(
            scope,
            &credentials,
            vec![
                (&btc_transport, btc_rules, cursor, started_at_ms),
                (&eth_transport, eth_rules, cursor, started_at_ms),
            ],
            Vec::new(),
        )
        .await,
        Err(BinanceRecoveryCollectorError::PageLimit)
    ));
    assert_eq!(btc_count.load(Ordering::SeqCst), 7);
    assert_eq!(eth_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn complete_multi_symbol_universe_is_required_before_any_http_get() -> TestResult {
    let (config, rules) = config_and_rules()?;
    let started_at_ms = unix_ms()?;
    let cursor = RecentFillsCursor {
        observed_through_ms: started_at_ms - 1,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let eth: Symbol = "ETH/USDT".parse()?;
    let scope = production_scope(
        &config,
        started_at_ms,
        100,
        BTreeSet::from([config.gateway_binding().symbol.clone(), eth]),
    )?;
    let (endpoint, count) = fake_signed_reads_counted(Vec::new()).await?;
    let transport = BinanceHttpTransport::with_endpoint(
        config,
        7,
        17,
        endpoint,
        BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
    )?;
    let credentials = BinanceCredentials::from_values("key", "secret")?;
    assert!(matches!(
        BinanceFreshRecoveryCollector::collect_authenticated_fixture(
            scope,
            &credentials,
            vec![(&transport, rules, cursor, started_at_ms)],
            Vec::new(),
        )
        .await,
        Err(BinanceRecoveryCollectorError::RequestUniverse)
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    Ok(())
}
