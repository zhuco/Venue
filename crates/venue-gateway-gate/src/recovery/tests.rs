use std::{
    sync::{Mutex, OnceLock},
    time::Duration,
};

use rust_decimal::Decimal;
use venue_domain::domain::{
    Amount, Instrument, MarketKind, NativeOrderFamily, OrderPurpose, Price,
};
use venue_gateway_api::{GatewayBinding, GatewayMode};

use crate::{
    GateRuntimeOrderProfile, GateRuntimePositionMode, GateRuntimeRecoveryRegistration,
    GateRuntimeRecoveryScopeInput, GateRuntimeStructuredUnknown,
};

use super::*;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000055";
const ACCOUNT_PAYLOAD: &str = r#"{"position_mode":"dual","total":"10","available":"9"}"#;
static AUTHENTICATED_SESSION_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn authenticated_session_test_lock() -> Result<std::sync::MutexGuard<'static, ()>, std::io::Error> {
    AUTHENTICATED_SESSION_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| std::io::Error::other("Gate authenticated-session test lock poisoned"))
}

fn roots(seed: u8) -> Result<GateRecoveryAuthorityRoots, GateFreshRecoveryError> {
    GateRecoveryAuthorityRoots::verified(
        [seed; 32],
        [seed.saturating_add(1); 32],
        [seed.saturating_add(2); 32],
    )
}

fn symbol_scope(
    mode: GatewayMode,
    symbol: &str,
    native_symbol: &str,
    generation: u64,
    fills_cursor: Option<&str>,
) -> Result<GateRecoverySymbolScope, Box<dyn std::error::Error>> {
    let symbol: Symbol = symbol.parse()?;
    let binding = GateGatewayBinding::new(GatewayBinding::new(
        VenueId::Gate,
        mode,
        ACCOUNT,
        symbol.clone(),
    )?)?;
    let rules = GateContractRules {
        native_symbol: native_symbol.to_owned(),
        instrument: Instrument {
            settlement_asset: Some("USDT".parse()?),
            minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
            symbol,
            market: MarketKind::LinearPerpetual,
            generation,
            price_tick: Price::new(Decimal::new(1, 5))?,
            quantity_step: Decimal::new(1, 1),
        },
        quanto_multiplier: Decimal::new(1, 1),
        minimum_contracts: Decimal::ONE,
        decimal_contracts: false,
    };
    Ok(GateRecoverySymbolScope::verified(
        binding,
        rules,
        GateFillsCursor::new(fills_cursor.map(str::to_owned))?,
    )?)
}

fn start(
    mode: GatewayMode,
    root_seed: u8,
    connection_generation: u64,
    recovered_private_generation: u64,
    attempt_id: u64,
) -> Result<GateRecoveryCollectionStart, GateFreshRecoveryError> {
    Ok(GateRecoveryCollectionStart {
        mode,
        trading_account_id: ACCOUNT.to_owned(),
        config_digest: "gate_config_55".to_owned(),
        config_epoch: 55,
        connection_generation,
        recovered_private_generation,
        attempt_id,
        started_at_ms: 1_000,
        deadline_at_ms: 2_000,
        authority_roots: roots(root_seed)?,
    })
}

fn owner_route(
    symbol: &str,
    client_id: &str,
    venue_order_id: &str,
) -> Result<GateRecoveryOwnerRoute, Box<dyn std::error::Error>> {
    GateRecoveryOwnerRoute::verified(
        CommandId::new(client_id)?,
        venue_order_id,
        OrderOwner {
            strategy_instance_id: format!("grid_{}", symbol.replace('/', "_")),
            run_id: "run_55".to_owned(),
            exchange: "gate".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: symbol.parse()?,
            purpose: OrderPurpose::Entry,
        },
    )
    .map_err(Into::into)
}

fn runtime_scope(
    connection_generation: u64,
    root_seed: u8,
    position_mode: GateRuntimePositionMode,
    order_profile: GateRuntimeOrderProfile,
    owner_routes: Vec<GateRecoveryOwnerRoute>,
) -> Result<GateRuntimeRecoveryScope, Box<dyn std::error::Error>> {
    Ok(GateRuntimeRecoveryScope::verified(
        GateRuntimeRecoveryScopeInput {
            mode: GatewayMode::Live,
            trading_account_id: ACCOUNT.to_owned(),
            config_digest: "gate_config_55".to_owned(),
            config_epoch: 55,
            connection_generation,
            recovered_private_generation: 9,
            position_mode,
            order_profile,
            recovery_session_sha256: [31; 32],
            authority_roots: roots(root_seed)?,
            registrations: vec![GateRuntimeRecoveryRegistration::verified(
                "DOGE/USDT".parse()?,
                "hedged_grid",
                "grid_DOGE_USDT",
                "gate_config_55",
                55,
            )?],
            owner_routes,
            structured_unknowns: vec![GateRuntimeStructuredUnknown::verified(
                CommandId::new("command_pending_55")?,
                CommandId::new("hgo_e7_pending_l1")?,
                NativeOrderFamily::UmOrder,
                "DOGE/USDT".parse()?,
            )?],
        },
    )?)
}

struct FixedRuntimeScope([u8; 32]);

impl GateRuntimeRecoveryRevalidator for FixedRuntimeScope {
    fn current_scope_sha256(&self) -> Option<[u8; 32]> {
        Some(self.0)
    }
}

fn positions_payload(native_symbol: &str) -> String {
    format!(
        r#"[{{"user":55,"contract":"{native_symbol}","mode":"dual_long","size":"0","entry_price":"0","mark_price":"0"}},{{"user":55,"contract":"{native_symbol}","mode":"dual_short","size":"2","entry_price":"0.1","mark_price":"0.11"}}]"#
    )
}

fn empty_payloads(native_symbol: &str) -> [String; 4] {
    [
        ACCOUNT_PAYLOAD.to_owned(),
        positions_payload(native_symbol),
        "[]".to_owned(),
        "[]".to_owned(),
    ]
}

fn raw_response(
    collector: &GateFreshRecoveryCollector,
    symbol_scope: &GateRecoverySymbolScope,
    source: GatePrivateReadSource,
    cursor: GateFillsCursor,
    payload: String,
) -> Result<GateFreshRecoveryRawResponse, Box<dyn std::error::Error>> {
    let prepared = collector.prepare_read(
        &symbol_scope.binding().gateway_binding().symbol,
        source,
        cursor,
    )?;
    let requested_at_ms = collector.scope.started_at_ms.saturating_add(1);
    let received_at_ms = requested_at_ms.saturating_add(1);
    let response = GateFreshRecoveryRawResponse::from_response(
        &prepared,
        symbol_scope,
        requested_at_ms,
        received_at_ms,
        payload,
    )?;
    if let Some(session) = &collector.authenticated_session {
        session.reserve_get(&prepared.request, response.raw.payload.len())?;
        let next_cursor = match prepared.request.source {
            GatePrivateReadSource::RegularOrders | GatePrivateReadSource::Fills => response
                .next_page_cursor()?
                .and_then(|cursor| cursor.last_native_id().map(str::to_owned)),
            GatePrivateReadSource::Account | GatePrivateReadSource::DualPositions => None,
        };
        session.settle_get(&prepared.request, response.raw.payload.len(), next_cursor)?;
    }
    Ok(response)
}

fn complete_for_symbol(
    collector: &GateFreshRecoveryCollector,
    symbol_scope: &GateRecoverySymbolScope,
    payloads: [String; 4],
) -> Result<Vec<GateFreshRecoveryRawResponse>, Box<dyn std::error::Error>> {
    let cursors = [
        GateFillsCursor::default(),
        GateFillsCursor::default(),
        GateFillsCursor::default(),
        symbol_scope.fills_cursor().clone(),
    ];
    let sources = [
        GatePrivateReadSource::Account,
        GatePrivateReadSource::DualPositions,
        GatePrivateReadSource::RegularOrders,
        GatePrivateReadSource::Fills,
    ];
    sources
        .into_iter()
        .zip(cursors)
        .zip(payloads)
        .map(|((source, cursor), payload)| {
            raw_response(collector, symbol_scope, source, cursor, payload)
        })
        .collect()
}

#[test]
fn production_collector_rejects_caller_reported_session_generation_and_universe()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    assert!(matches!(
        GateFreshRecoveryCollector::start(
            start(GatewayMode::Live, 1, 4, 9, 55)?,
            [symbol],
            std::iter::empty::<GateRecoveryOwnerRoute>(),
        ),
        Err(GateFreshRecoveryError::AuthenticatedSessionRequired)
    ));
    Ok(())
}

#[test]
fn authenticated_start_derives_attempt_universe_and_unbound_roots_from_session()
-> Result<(), Box<dyn std::error::Error>> {
    let _session_lock = authenticated_session_test_lock()?;
    let symbol = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let limits = crate::GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
    let credentials = GateCredentials::from_values("key", "secret")?;
    let lease = crate::GateAuthenticatedRecoverySessionLease::issue(
        symbol.binding(),
        symbol.binding().config().rest_origin().to_owned(),
        symbol.binding().config().usdt_futures_ws().to_owned(),
        7,
        limits,
        &credentials,
    )?;
    let session = lease.begin([symbol], unix_ms()?.saturating_add(2_000), 64 * 1024, 8)?;
    let expected_attempt = session.attempt_id();
    let expected_universe = *session.request_universe_sha256();
    let collector = GateFreshRecoveryCollector::start_authenticated(session)?;
    assert_eq!(collector.scope().attempt_id(), expected_attempt);
    assert_eq!(
        collector.scope().request_universe_sha256(),
        &expected_universe
    );
    assert_eq!(
        collector.scope().authority_roots(),
        &GateRecoveryAuthorityRoots::unbound()
    );
    Ok(())
}

#[test]
fn authenticated_session_applies_one_global_byte_budget_across_all_faces()
-> Result<(), Box<dyn std::error::Error>> {
    let _session_lock = authenticated_session_test_lock()?;
    let symbol = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let limits = crate::GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
    let credentials = GateCredentials::from_values("key", "secret")?;
    let lease = crate::GateAuthenticatedRecoverySessionLease::issue(
        symbol.binding(),
        symbol.binding().config().rest_origin().to_owned(),
        symbol.binding().config().usdt_futures_ws().to_owned(),
        7,
        limits,
        &credentials,
    )?;
    let session = lease.begin(
        [symbol.clone()],
        unix_ms()?.saturating_add(2_000),
        16 * 1024,
        8,
    )?;
    let account = prepare_private_read(
        symbol.binding(),
        symbol.rules(),
        7,
        session.attempt_id(),
        GatePrivateReadSource::Account,
        GateFillsCursor::default(),
    )?;
    session.reserve_get(&account, 10 * 1024)?;
    session.settle_get(&account, 10 * 1024, None)?;
    let positions = prepare_private_read(
        symbol.binding(),
        symbol.rules(),
        7,
        session.attempt_id(),
        GatePrivateReadSource::DualPositions,
        GateFillsCursor::default(),
    )?;
    assert_eq!(
        session.reserve_get(&positions, 10 * 1024),
        Err(GateFreshRecoveryError::Budget)
    );
    Ok(())
}

#[test]
fn authenticated_runtime_session_emits_complete_hedge_regular_only_bundle()
-> Result<(), Box<dyn std::error::Error>> {
    let _session_lock = authenticated_session_test_lock()?;
    let symbol = symbol_scope(
        GatewayMode::Live,
        "DOGE/USDT",
        "DOGE_USDT",
        7,
        Some("227262265"),
    )?;
    let limits = crate::GateTransportLimits::new(Duration::from_secs(2), 16 * 1024)?;
    let credentials = GateCredentials::from_values("key", "secret")?;
    let lease = crate::GateAuthenticatedRecoverySessionLease::issue(
        symbol.binding(),
        symbol.binding().config().rest_origin().to_owned(),
        symbol.binding().config().usdt_futures_ws().to_owned(),
        7,
        limits,
        &credentials,
    )?;
    let route = owner_route("DOGE/USDT", "hgo_e7_long_open_l1", "9001")?;
    let runtime_scope = runtime_scope(
        44,
        41,
        GateRuntimePositionMode::Hedge,
        GateRuntimeOrderProfile::stage7_regular_only(),
        vec![route],
    )?;
    let expected_runtime_scope = *runtime_scope.commitment_sha256();
    let session = lease.begin_runtime(
        runtime_scope.clone(),
        [symbol.clone()],
        unix_ms()?.saturating_add(2_000),
        64 * 1024,
        8,
    )?;
    assert_eq!(session.private_generation(), 10);
    let collector = GateFreshRecoveryCollector::start_authenticated(session)?;
    assert_eq!(
        collector.scope().runtime_scope_sha256(),
        Some(&expected_runtime_scope)
    );
    assert_eq!(
        collector.scope().authority_roots(),
        runtime_scope.authority_roots()
    );
    let responses = complete_for_symbol(
        &collector,
        &symbol,
        [
            ACCOUNT_PAYLOAD.to_owned(),
            positions_payload("DOGE_USDT"),
            include_str!("../../tests/fixtures/regular_orders.json").to_owned(),
            include_str!("../../tests/fixtures/fills.json").to_owned(),
        ],
    )?;
    let validated_at_ms = collector.scope().started_at_ms().saturating_add(10);
    let bundle = collector.finish_runtime(
        validated_at_ms,
        responses,
        &FixedRuntimeScope(expected_runtime_scope),
    )?;

    assert_eq!(bundle.runtime_scope(), &runtime_scope);
    assert_eq!(bundle.runtime_scope().registrations().len(), 1);
    assert_eq!(bundle.runtime_scope().structured_unknowns().len(), 1);
    assert_eq!(bundle.candidate().owned_open_orders().len(), 1);
    assert_eq!(bundle.candidate().unknown_open_orders().len(), 1);
    assert_ne!(bundle.commitment_sha256(), &[0; 32]);
    assert_eq!(
        bundle
            .candidate()
            .surface(GateRecoverySurface::ConditionalOrders)
            .ok_or("conditional surface")?
            .coverage(),
        &GateRecoveryCoverage::Unsupported {
            profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
        }
    );
    Ok(())
}

#[test]
fn runtime_scope_rejects_profile_owner_root_and_structured_unknown_drift()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(
        runtime_scope(
            44,
            1,
            GateRuntimePositionMode::Net,
            GateRuntimeOrderProfile::stage7_regular_only(),
            vec![],
        )
        .is_err()
    );
    assert!(
        runtime_scope(
            44,
            1,
            GateRuntimePositionMode::Hedge,
            GateRuntimeOrderProfile {
                profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
                regular_supported: true,
                conditional_supported: true,
                algo_supported: false,
            },
            vec![],
        )
        .is_err()
    );
    let wrong_owner = GateRecoveryOwnerRoute::verified(
        CommandId::new("hgo_e7_long_open_l1")?,
        "9001",
        OrderOwner {
            strategy_instance_id: "other_instance".to_owned(),
            run_id: "run_55".to_owned(),
            exchange: "gate".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
    )?;
    assert!(
        runtime_scope(
            44,
            1,
            GateRuntimePositionMode::Hedge,
            GateRuntimeOrderProfile::stage7_regular_only(),
            vec![wrong_owner],
        )
        .is_err()
    );
    assert_eq!(
        GateRuntimeStructuredUnknown::verified(
            CommandId::new("command_algo_unknown")?,
            CommandId::new("algo_native_unknown")?,
            NativeOrderFamily::UmAlgo,
            "DOGE/USDT".parse()?,
        ),
        Err(GateFreshRecoveryError::RuntimeUnknown)
    );

    let mismatched_registration =
        GateRuntimeRecoveryScope::verified(GateRuntimeRecoveryScopeInput {
            mode: GatewayMode::Live,
            trading_account_id: ACCOUNT.to_owned(),
            config_digest: "gate_config_55".to_owned(),
            config_epoch: 55,
            connection_generation: 44,
            recovered_private_generation: 9,
            position_mode: GateRuntimePositionMode::Hedge,
            order_profile: GateRuntimeOrderProfile::stage7_regular_only(),
            recovery_session_sha256: [31; 32],
            authority_roots: roots(1)?,
            registrations: vec![
                GateRuntimeRecoveryRegistration::verified(
                    "BTC/USDT".parse()?,
                    "hedged_grid",
                    "grid_BTC_USDT",
                    "gate_config_55",
                    55,
                )?,
                GateRuntimeRecoveryRegistration::verified(
                    "DOGE/USDT".parse()?,
                    "hedged_grid",
                    "grid_DOGE_USDT",
                    "stale_config",
                    54,
                )?,
            ],
            owner_routes: vec![],
            structured_unknowns: vec![],
        });
    assert_eq!(
        mismatched_registration,
        Err(GateFreshRecoveryError::RuntimeUniverse)
    );

    let valid = runtime_scope(
        44,
        1,
        GateRuntimePositionMode::Hedge,
        GateRuntimeOrderProfile::stage7_regular_only(),
        vec![],
    )?;
    let drifted = runtime_scope(
        44,
        9,
        GateRuntimePositionMode::Hedge,
        GateRuntimeOrderProfile::stage7_regular_only(),
        vec![],
    )?;
    assert!(matches!(
        GateRuntimeRecoveryAwaitGuard::new(
            &valid,
            &FixedRuntimeScope(*drifted.commitment_sha256()),
        ),
        Err(GateFreshRecoveryError::RuntimeScopeDrift)
    ));
    Ok(())
}

#[test]
fn collection_start_binds_exact_live_endpoints() -> Result<(), Box<dyn std::error::Error>> {
    let mode = GatewayMode::Live;
    let rest = "https://api.gateio.ws/api/v4";
    let websocket = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    let symbol = symbol_scope(mode, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let collector =
        GateFreshRecoveryCollector::start_fixture(start(mode, 1, 4, 9, 55)?, [symbol.clone()], [])?;
    assert_eq!(collector.scope().mode(), mode);
    assert_eq!(collector.scope().rest_origin(), rest);
    assert_eq!(collector.scope().private_ws_endpoint(), websocket);
    assert_eq!(collector.scope().private_generation(), 10);
    let prepared = collector.prepare_read(
        &"DOGE/USDT".parse()?,
        GatePrivateReadSource::Account,
        GateFillsCursor::default(),
    )?;
    assert!(prepared.rest_url().starts_with(rest));
    assert_eq!(prepared.symbol(), &"DOGE/USDT".parse()?);
    Ok(())
}

#[test]
fn complete_attempt_emits_six_raw_commitments_and_structured_unknown_owner()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = symbol_scope(
        GatewayMode::Live,
        "DOGE/USDT",
        "DOGE_USDT",
        7,
        Some("227262265"),
    )?;
    let route = owner_route("DOGE/USDT", "hgo_e7_long_open_l1", "9001")?;
    let collector = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [symbol.clone()],
        [route],
    )?;
    let responses = complete_for_symbol(
        &collector,
        &symbol,
        [
            ACCOUNT_PAYLOAD.to_owned(),
            positions_payload("DOGE_USDT"),
            include_str!("../../tests/fixtures/regular_orders.json").to_owned(),
            include_str!("../../tests/fixtures/fills.json").to_owned(),
        ],
    )?;
    for response in responses.iter().filter(|response| {
        matches!(
            response.raw.source,
            GatePrivateReadSource::RegularOrders | GatePrivateReadSource::Fills
        )
    }) {
        assert_eq!(response.next_page_cursor()?, None);
    }
    let candidate = collector.finish(1_300, responses)?;

    assert_eq!(candidate.scope().symbol_universe(), &["DOGE/USDT".parse()?]);
    assert_eq!(candidate.symbol_readbacks().len(), 1);
    assert_eq!(candidate.owned_open_orders().len(), 1);
    assert_eq!(candidate.owned_open_orders()[0].venue_order_id, "9001");
    assert_eq!(candidate.unknown_open_orders().len(), 1);
    assert_eq!(
        candidate.unknown_open_orders()[0].reason,
        GateUnknownOpenOrderReason::OwnerRouteMissing
    );
    assert_eq!(
        candidate.unknown_open_orders()[0]
            .client_order_id
            .as_deref(),
        Some("hgo_e7_short_open_l1")
    );
    for surface in [
        GateRecoverySurface::Account,
        GateRecoverySurface::Positions,
        GateRecoverySurface::RegularOrders,
        GateRecoverySurface::ConditionalOrders,
        GateRecoverySurface::AlgoOrders,
        GateRecoverySurface::FillsCursor,
    ] {
        assert_ne!(
            candidate
                .surface(surface)
                .ok_or("missing surface")?
                .raw_commitment_sha256(),
            &[0; 32]
        );
    }
    assert!(matches!(
        candidate
            .surface(GateRecoverySurface::RegularOrders)
            .ok_or("missing regular")?
            .coverage(),
        GateRecoveryCoverage::Complete { record_count: 2 }
    ));
    for surface in [
        GateRecoverySurface::ConditionalOrders,
        GateRecoverySurface::AlgoOrders,
    ] {
        assert!(matches!(
            candidate
                .surface(surface)
                .ok_or("missing unsupported")?
                .coverage(),
            GateRecoveryCoverage::Unsupported {
                profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION
            }
        ));
    }
    Ok(())
}

#[test]
fn missing_face_and_missing_symbol_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let doge = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let btc = symbol_scope(GatewayMode::Live, "BTC/USDT", "BTC_USDT", 8, None)?;

    let collector = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [doge.clone()],
        [],
    )?;
    let mut missing_face = complete_for_symbol(&collector, &doge, empty_payloads("DOGE_USDT"))?;
    missing_face.retain(|response| response.raw.source != GatePrivateReadSource::Fills);
    assert!(matches!(
        collector.finish(1_300, missing_face),
        Err(GateFreshRecoveryError::PrivateRead(
            GatePrivateReadError::MissingSurface
        ))
    ));

    let collector = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [doge.clone(), btc],
        [],
    )?;
    let only_doge = complete_for_symbol(&collector, &doge, empty_payloads("DOGE_USDT"))?;
    assert!(matches!(
        collector.finish(1_300, only_doge),
        Err(GateFreshRecoveryError::SymbolUniverse)
    ));
    Ok(())
}

#[test]
fn account_raw_fork_across_symbol_universe_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let doge = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let btc = symbol_scope(GatewayMode::Live, "BTC/USDT", "BTC_USDT", 8, None)?;
    let collector = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [doge.clone(), btc.clone()],
        [],
    )?;
    let mut responses = complete_for_symbol(&collector, &doge, empty_payloads("DOGE_USDT"))?;
    let mut btc_payloads = empty_payloads("BTC_USDT");
    btc_payloads[0] = format!("{ACCOUNT_PAYLOAD} ");
    responses.extend(complete_for_symbol(&collector, &btc, btc_payloads)?);
    assert!(matches!(
        collector.finish(1_300, responses),
        Err(GateFreshRecoveryError::RawDivergence)
    ));
    Ok(())
}

#[test]
fn wrong_native_owner_identity_stays_structured_unknown() -> Result<(), Box<dyn std::error::Error>>
{
    let symbol = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let route = owner_route("DOGE/USDT", "hgo_e7_long_open_l1", "9999")?;
    let collector = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [symbol.clone()],
        [route],
    )?;
    let responses = complete_for_symbol(
        &collector,
        &symbol,
        [
            ACCOUNT_PAYLOAD.to_owned(),
            positions_payload("DOGE_USDT"),
            include_str!("../../tests/fixtures/regular_orders.json").to_owned(),
            "[]".to_owned(),
        ],
    )?;
    let candidate = collector.finish(1_300, responses)?;
    assert!(candidate.owned_open_orders().is_empty());
    assert_eq!(candidate.unknown_open_orders().len(), 2);
    assert_eq!(
        candidate.unknown_open_orders()[0].reason,
        GateUnknownOpenOrderReason::NativeIdentityMismatch
    );
    Ok(())
}

#[test]
fn expired_or_cross_generation_root_scope_cannot_relabel_old_raw()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = symbol_scope(GatewayMode::Live, "DOGE/USDT", "DOGE_USDT", 7, None)?;
    let expired = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [symbol.clone()],
        [],
    )?;
    let expired_responses = complete_for_symbol(&expired, &symbol, empty_payloads("DOGE_USDT"))?;
    assert!(matches!(
        expired.finish(2_000, expired_responses),
        Err(GateFreshRecoveryError::Deadline)
    ));

    let old = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 1, 4, 9, 55)?,
        [symbol.clone()],
        [],
    )?;
    let old_responses = complete_for_symbol(&old, &symbol, empty_payloads("DOGE_USDT"))?;
    let old_scope = *old.scope().commitment_sha256();
    let new = GateFreshRecoveryCollector::start_fixture(
        start(GatewayMode::Live, 9, 5, 10, 56)?,
        [symbol],
        [],
    )?;
    assert_ne!(old_scope, *new.scope().commitment_sha256());
    assert_eq!(new.scope().private_generation(), 11);
    assert!(matches!(
        new.finish(1_300, old_responses),
        Err(GateFreshRecoveryError::ScopeDrift)
    ));
    Ok(())
}
