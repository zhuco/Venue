use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, NativeOrderFamily, PositionSide};
use venue_gateway_api::{CapabilityFlags, GatewayBinding, GatewayMode, VenueId};

use crate::*;

const ACCOUNT: &[u8] = include_bytes!("../fixtures/account-info-uta2.json");
const API_KEY: &[u8] = include_bytes!("../fixtures/api-key-info.json");
const WALLET: &[u8] = include_bytes!("../fixtures/wallet-balance-unified.json");
const POSITIONS: &[u8] = include_bytes!("../fixtures/positions-linear.json");
const ORDERS: &[u8] = include_bytes!("../fixtures/open-orders-linear.json");
const STOP_ORDERS: &[u8] = include_bytes!("../fixtures/open-stop-orders-linear.json");
const HISTORY: &[u8] = include_bytes!("../fixtures/order-history-linear.json");
const EXECUTIONS: &[u8] = include_bytes!("../fixtures/execution-trade-page.json");
const EMPTY_PAGE: &[u8] = br#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","nextPageCursor":"","list":[]},"time":2000}"#;

type TestError = Box<dyn std::error::Error>;

fn binding(mode: GatewayMode) -> Result<BybitGatewayBinding, TestError> {
    Ok(BybitGatewayBinding::new(GatewayBinding::new(
        VenueId::Bybit,
        mode,
        "00000000-0000-4000-8000-000000000001",
        "BTC/USDT".parse()?,
    )?)?)
}

fn raw_page(
    binding: &BybitGatewayBinding,
    source: BybitPrivateSource,
    page_index: u32,
    cursor: Option<&str>,
    payload: &[u8],
) -> Result<BybitRawPrivatePayload, BybitError> {
    raw_page_with_generation(binding, source, 7, page_index, cursor, payload)
}

fn raw_page_with_generation(
    binding: &BybitGatewayBinding,
    source: BybitPrivateSource,
    generation: u64,
    page_index: u32,
    cursor: Option<&str>,
    payload: &[u8],
) -> Result<BybitRawPrivatePayload, BybitError> {
    let history_window = matches!(
        source,
        BybitPrivateSource::OrderHistory(_) | BybitPrivateSource::Executions
    )
    .then(|| BybitHistoryWindow::new(1, 2_000))
    .transpose()?;
    let request = prepare_private_request(
        binding,
        generation,
        11,
        page_index,
        source,
        cursor,
        history_window,
        None,
    )?;
    BybitRawPrivatePayload::from_response(binding, &request, 1_900, 2_000, payload.to_vec())
}

fn raw(
    binding: &BybitGatewayBinding,
    source: BybitPrivateSource,
    payload: &[u8],
) -> Result<BybitRawPrivatePayload, BybitError> {
    raw_page(binding, source, 0, None, payload)
}

#[test]
fn signed_queries_are_exact_bounded_and_identity_specific() -> Result<(), TestError> {
    let binding = binding(GatewayMode::Live)?;
    let window = BybitHistoryWindow::new(1_000, 2_000)?;
    let lookup = BybitOrderLookup::by_client_order_id("client-1")?;
    let request = prepare_private_request(
        &binding,
        7,
        11,
        0,
        BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
        None,
        Some(window.clone()),
        Some(lookup.clone()),
    )?;
    assert_eq!(request.path, endpoints::ORDER_HISTORY);
    assert_eq!(
        request.query,
        "category=linear&symbol=BTCUSDT&orderFilter=Order&startTime=1000&endTime=2000&limit=50&orderLinkId=client-1"
    );
    let execution = prepare_private_request(
        &binding,
        7,
        11,
        0,
        BybitPrivateSource::Executions,
        None,
        Some(window),
        Some(lookup),
    )?;
    assert!(execution.query.contains("execType=Trade&limit=100"));
    let credentials = BybitCredentials::from_values("test", "secret")?;
    assert!(
        sign_private_request(&credentials, &binding, &request, 1_670_000_000_000)?
            .get("X-BAPI-SIGN")
            .is_some()
    );
    assert_eq!(
        prepare_private_request(
            &binding,
            7,
            11,
            0,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmAlgo),
            None,
            None,
            None,
        ),
        Err(BybitError::OrderFamily)
    );
    assert_eq!(
        BybitHistoryWindow::new(1, BYBIT_HISTORY_WINDOW_MAX_MS + 2),
        Err(BybitError::Clock)
    );
    Ok(())
}

#[test]
fn account_positions_orders_and_fills_replay_exact_raw_evidence() -> Result<(), TestError> {
    let binding = binding(GatewayMode::Live)?;
    let account = complete_account_readback(
        &binding,
        raw(&binding, BybitPrivateSource::AccountInfo, ACCOUNT)?,
        raw(&binding, BybitPrivateSource::WalletBalance, WALLET)?,
    )?;
    assert_eq!(account.identity.mode, BybitAccountMode::Uta2);
    assert_eq!(
        account.wallet.total_available_balance_usd,
        Decimal::new(900, 0)
    );

    let position_page = parse_position_page(
        &binding,
        &raw(&binding, BybitPrivateSource::Positions, POSITIONS)?,
    )?;
    let positions = complete_position_pages(&binding, &[position_page])?;
    assert!(positions.hedge_mode);
    assert_eq!(positions.positions[0].position.side, PositionSide::Long);

    let regular_page = parse_open_order_page(
        &binding,
        &raw(
            &binding,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            ORDERS,
        )?,
    )?;
    let regular = complete_open_order_pages(&binding, NativeOrderFamily::UmOrder, &[regular_page])?;
    assert_eq!(regular.orders[0].order.order_id, "20");

    let conditional_page = parse_open_order_page(
        &binding,
        &raw(
            &binding,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmConditional),
            STOP_ORDERS,
        )?,
    )?;
    let conditional = complete_open_order_pages(
        &binding,
        NativeOrderFamily::UmConditional,
        &[conditional_page],
    )?;
    assert_eq!(
        conditional.orders[0].stop_order_type.as_deref(),
        Some("Stop")
    );

    let history_page = parse_order_history_page(
        &binding,
        &raw(
            &binding,
            BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
            HISTORY,
        )?,
    )?;
    let history =
        complete_order_history_pages(&binding, NativeOrderFamily::UmOrder, &[history_page])?;
    let execution_page = parse_execution_page(
        &binding,
        &raw(&binding, BybitPrivateSource::Executions, EXECUTIONS)?,
        &history.orders,
    )?;
    let fills = complete_execution_pages(&binding, &[execution_page], &history.orders)?;
    assert_eq!(fills.fills[0].fill.fill_id, "c");
    assert_eq!(
        fills.fills[0].fill.execution_sequence,
        FieldState::Known(103)
    );
    Ok(())
}

#[test]
fn pagination_requires_the_exact_cursor_chain_and_unique_native_ids() -> Result<(), TestError> {
    let binding = binding(GatewayMode::Live)?;
    let first_payload = String::from_utf8(ORDERS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    let first = parse_open_order_page(
        &binding,
        &raw_page(
            &binding,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            0,
            None,
            first_payload.as_bytes(),
        )?,
    )?;
    assert_eq!(
        complete_open_order_pages(
            &binding,
            NativeOrderFamily::UmOrder,
            std::slice::from_ref(&first),
        ),
        Err(BybitError::Pagination)
    );
    let terminal = parse_open_order_page(
        &binding,
        &raw_page(
            &binding,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            1,
            Some("next"),
            EMPTY_PAGE,
        )?,
    )?;
    assert_eq!(
        complete_open_order_pages(
            &binding,
            NativeOrderFamily::UmOrder,
            &[first.clone(), terminal]
        )?
        .orders
        .len(),
        2
    );
    let duplicate = parse_open_order_page(
        &binding,
        &raw_page(
            &binding,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            1,
            Some("next"),
            ORDERS,
        )?,
    )?;
    assert_eq!(
        complete_open_order_pages(&binding, NativeOrderFamily::UmOrder, &[first, duplicate]),
        Err(BybitError::Pagination)
    );
    Ok(())
}

#[test]
fn windows_cursors_generations_symbols_accounts_and_hedge_legs_fail_closed() -> Result<(), TestError>
{
    let live = binding(GatewayMode::Live)?;
    assert_eq!(
        BybitHistoryWindow::new(1_000, 1_000),
        Err(BybitError::Clock)
    );
    assert_eq!(
        prepare_private_request(
            &live,
            7,
            11,
            1,
            BybitPrivateSource::Positions,
            Some("bad&cursor"),
            None,
            None,
        ),
        Err(BybitError::Pagination)
    );

    let first_payload = String::from_utf8(ORDERS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    let first = parse_open_order_page(
        &live,
        &raw_page_with_generation(
            &live,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            7,
            0,
            None,
            first_payload.as_bytes(),
        )?,
    )?;
    let wrong_generation = parse_open_order_page(
        &live,
        &raw_page_with_generation(
            &live,
            BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
            8,
            1,
            Some("next"),
            EMPTY_PAGE,
        )?,
    )?;
    assert_eq!(
        complete_open_order_pages(
            &live,
            NativeOrderFamily::UmOrder,
            &[first, wrong_generation],
        ),
        Err(BybitError::Pagination)
    );

    let wrong_symbol = String::from_utf8(POSITIONS.to_vec())?.replace("BTCUSDT", "ETHUSDT");
    let wrong_symbol = raw(
        &live,
        BybitPrivateSource::Positions,
        wrong_symbol.as_bytes(),
    )?;
    assert_eq!(
        parse_position_page(&live, &wrong_symbol),
        Err(BybitError::Binding)
    );

    let other_account = BybitGatewayBinding::new(GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000002",
        "BTC/USDT".parse()?,
    )?)?;
    let positions = raw(&live, BybitPrivateSource::Positions, POSITIONS)?;
    assert_eq!(
        parse_position_page(&other_account, &positions),
        Err(BybitError::Binding)
    );

    let mut incomplete: serde_json::Value = serde_json::from_slice(POSITIONS)?;
    incomplete["result"]["list"]
        .as_array_mut()
        .ok_or("missing list")?
        .truncate(1);
    let incomplete = raw(
        &live,
        BybitPrivateSource::Positions,
        &serde_json::to_vec(&incomplete)?,
    )?;
    assert_eq!(
        parse_position_page(&live, &incomplete),
        Err(BybitError::Payload)
    );
    Ok(())
}

#[test]
fn all_family_and_capability_candidates_remain_non_authoritative() -> Result<(), TestError> {
    let binding = binding(GatewayMode::Live)?;
    let credentials = BybitCredentials::from_values("test", "secret")?;
    let api_key = parse_api_key_evidence(
        &binding,
        &credentials,
        &raw(&binding, BybitPrivateSource::ApiKeyInfo, API_KEY)?,
    )?;
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
    assert_eq!(
        families.algo(),
        &BybitUnsupportedOrderFamilyEvidence::algo(
            scope.binding.clone(),
            BYBIT_LINEAR_ORDER_PROFILE_VERSION,
        )
    );
    let mut tampered_fills = fills.clone();
    tampered_fills.fills[0].fill.order_id = "unbound-order".to_owned();
    assert_eq!(
        validate_capability_candidate(
            scope.clone(),
            2_500,
            api_key.clone(),
            account.clone(),
            positions.clone(),
            families.clone(),
            tampered_fills,
        ),
        Err(BybitError::Projection)
    );
    let candidate =
        validate_capability_candidate(scope, 2_500, api_key, account, positions, families, fills)?;
    assert!(candidate.candidate_flags.contains(CapabilityFlags::TRADE));
    assert!(
        !candidate
            .candidate_flags
            .contains(CapabilityFlags::PRIVATE_STREAM)
    );
    assert!(
        !candidate
            .candidate_flags
            .contains(CapabilityFlags::WITHDRAW)
    );
    assert_eq!(capabilities(), CapabilityFlags::empty());
    Ok(())
}

#[test]
fn cross_binding_family_relabelling_and_payload_tampering_fail_closed() -> Result<(), TestError> {
    let live = binding(GatewayMode::Live)?;
    let test = binding(GatewayMode::Test)?;
    let test_raw = raw(&test, BybitPrivateSource::Positions, POSITIONS)?;
    assert_eq!(
        parse_position_page(&live, &test_raw),
        Err(BybitError::Binding)
    );
    let mut tampered = raw(
        &live,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
        ORDERS,
    )?;
    tampered.payload[0] ^= 1;
    assert_eq!(
        parse_open_order_page(&live, &tampered),
        Err(BybitError::Binding)
    );
    let conditional = raw(
        &live,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmConditional),
        STOP_ORDERS,
    )?;
    let mut relabelled = conditional;
    relabelled.source = BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder);
    assert_eq!(
        parse_open_order_page(&live, &relabelled),
        Err(BybitError::Binding)
    );
    Ok(())
}
