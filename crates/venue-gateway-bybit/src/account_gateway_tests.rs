use super::*;
use rust_decimal::Decimal;
use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose, Position};
use venue_gateway_api::{GatewayMode, VenueId};

const INSTRUMENT: &str = include_str!("../fixtures/instruments-linear.json");
const BBO: &str = include_str!("../fixtures/orderbook-linear-bbo.json");
const EXECUTIONS: &[u8] = include_bytes!("../fixtures/execution-trade-page.json");

fn limit_facts() -> Result<
    (
        BybitGatewayBinding,
        BybitLinearInstrumentRules,
        crate::BybitRestBbo,
    ),
    Box<dyn std::error::Error>,
> {
    let binding = BybitGatewayBinding::new(GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        "BTC/USDT".parse()?,
    )?)?;
    let rules = parse_linear_instrument(
        &binding,
        crate::BybitRawPublicPayload::new(
            &binding,
            crate::BybitPublicSource::LinearInstrument,
            7,
            10_000,
            INSTRUMENT.to_owned(),
        )?,
    )?;
    let bbo = parse_rest_bbo(
        &binding,
        crate::BybitRawPublicPayload::new(
            &binding,
            crate::BybitPublicSource::RestOrderBook,
            7,
            10_000,
            BBO.to_owned(),
        )?,
    )?;
    Ok((binding, rules, bbo))
}

fn limit_intent(
    quote_delta: Decimal,
) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
    Ok(AccountLimitNormalizationIntent {
        command_id: CommandId::new("bybit_limit")?,
        client_order_id: CommandId::new("bybit_limit_client")?,
        owner: OrderOwner {
            strategy_instance_id: "grid1".to_owned(),
            run_id: "run1".to_owned(),
            exchange: "bybit".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quote_delta,
        reduce_only: false,
    })
}

fn account_wide_execution(
    binding: &BybitGatewayBinding,
    page_index: u32,
    request_cursor: Option<&str>,
    payload: Vec<u8>,
) -> crate::BybitRawPrivatePayload {
    crate::BybitRawPrivatePayload {
        parser_schema_version: crate::BYBIT_PRIVATE_PARSER_SCHEMA_VERSION,
        binding: binding.gateway_binding().clone(),
        source: BybitPrivateSource::AccountWideExecutions,
        native_symbol: "BTCUSDT".to_owned(),
        generation: 7,
        attempt_id: 1,
        page_index,
        request_cursor: request_cursor.map(str::to_owned),
        history_window: Some(BybitHistoryWindow {
            start_ms: 1,
            end_ms: 2_100,
        }),
        lookup: None,
        request_path: "/v5/execution/list".to_owned(),
        request_query: "category=linear".to_owned(),
        request_timestamp_ms: 2_000,
        received_at_ms: 3_000,
        payload_sha256: "fixture".to_owned(),
        payload,
    }
}

fn reduce(quantity: Decimal) -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
    Ok(MarketReduceCommand {
        command_id: CommandId::new("bybit_reduce")?,
        client_order_id: CommandId::new("bybit_reduce_client")?,
        owner: OrderOwner {
            strategy_instance_id: "grid1".to_owned(),
            run_id: "run1".to_owned(),
            exchange: "bybit".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        position_side: PositionSide::Long,
        side: venue_domain::domain::OrderSide::Sell,
        quantity,
        risk_episode_id: CommandId::new("bybit_episode")?,
        position_generation: 3,
    })
}

fn recovery_limit_command(
    time_in_force: LimitTimeInForce,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new("bybit_recovery_limit")?,
        client_order_id: CommandId::new("bybit_recovery_client")?,
        owner: OrderOwner {
            strategy_instance_id: "grid1".to_owned(),
            run_id: "run1".to_owned(),
            exchange: "bybit".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::new(60_000, 0))?,
        time_in_force,
        reduce_only: false,
    }))
}

#[test]
fn production_profile_is_bounded() {
    assert_eq!(EXACT_READBACK_MAX_PAGES, 32);
    assert_eq!(
        HISTORY_WINDOW_MS,
        std::time::Duration::from_secs(7 * 24 * 60 * 60).as_millis() as u64
    );
}

#[test]
fn signed_snapshot_keeps_absent_or_unsupported_policy_unknown() {
    assert_eq!(
        signed_limit_time_in_force(
            &serde_json::json!({"orderType":"Limit","timeInForce":"PostOnly"}),
            NativeOrderFamily::UmOrder,
        ),
        Some(LimitTimeInForce::PostOnly)
    );
    assert_eq!(
        signed_limit_time_in_force(
            &serde_json::json!({"orderType":"Limit","timeInForce":"GTC"}),
            NativeOrderFamily::UmOrder,
        ),
        Some(LimitTimeInForce::Gtc)
    );
    assert_eq!(
        signed_limit_time_in_force(
            &serde_json::json!({"orderType":"Limit"}),
            NativeOrderFamily::UmOrder,
        ),
        None
    );
}

#[test]
fn recovery_limit_policy_mismatch_or_absence_stays_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let command = recovery_limit_command(LimitTimeInForce::PostOnly)?;
    assert!(recovery_limit_time_in_force_matches(
        &command,
        Some(LimitTimeInForce::PostOnly)
    ));
    assert!(!recovery_limit_time_in_force_matches(
        &command,
        Some(LimitTimeInForce::Gtc)
    ));
    assert!(!recovery_limit_time_in_force_matches(&command, None));
    Ok(())
}

#[test]
fn market_reduce_never_crosses_or_uses_a_wrong_signed_hedge_leg()
-> Result<(), Box<dyn std::error::Error>> {
    let position = Position {
        symbol: "BTC/USDT".parse()?,
        side: PositionSide::Long,
        quantity: Decimal::ONE,
        entry_price: None,
        mark_price: None,
    };
    assert!(validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &position).is_ok());
    assert!(
        validate_market_reduce_against_position(&reduce(Decimal::new(1001, 3))?, &position)
            .is_err()
    );
    let mut wrong_leg = position;
    wrong_leg.side = PositionSide::Short;
    assert!(validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &wrong_leg).is_err());
    Ok(())
}

#[test]
fn limit_normalization_uses_same_side_bbo_and_floors_price_and_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, bbo) = limit_facts()?;
    let intent = limit_intent(Decimal::new(100, 0))?;
    let ExecutionCommand::PlaceLimit(command) =
        normalize_limit_from_bbo(&binding, &rules, &intent, &bbo, 10_010)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(command.limit_price.value(), Decimal::new(654_854, 1));
    assert_eq!(command.quantity, Decimal::new(1, 3));
    assert_eq!(command.command_id, intent.command_id);
    assert_eq!(command.client_order_id, intent.client_order_id);
    assert_eq!(command.owner, intent.owner);
    Ok(())
}

#[test]
fn limit_normalization_rejects_minimum_symbol_direction_and_stale_bbo()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, bbo) = limit_facts()?;
    assert!(
        normalize_limit_from_bbo(
            &binding,
            &rules,
            &limit_intent(Decimal::new(4, 0))?,
            &bbo,
            10_010,
        )
        .is_err()
    );
    let mut wrong_symbol = limit_intent(Decimal::new(100, 0))?;
    wrong_symbol.owner.symbol = "ETH/USDT".parse()?;
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_symbol, &bbo, 10_010).is_err());
    let mut wrong_account = limit_intent(Decimal::new(100, 0))?;
    wrong_account.owner.account = "00000000-0000-4000-8000-000000000002".to_owned();
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_account, &bbo, 10_010).is_err());
    let mut wrong_leg = limit_intent(Decimal::new(100, 0))?;
    wrong_leg.position_side = PositionSide::Short;
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_leg, &bbo, 10_010).is_err());
    assert!(
        normalize_limit_from_bbo(
            &binding,
            &rules,
            &limit_intent(Decimal::new(100, 0))?,
            &bbo,
            11_001,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn snapshot_fills_deduplicates_closed_account_wide_pages_and_keeps_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, _, _) = limit_facts()?;
    let first = String::from_utf8(EXECUTIONS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    let (fills, cursor) = snapshot_fills(
        &[
            account_wide_execution(&binding, 0, None, first.into_bytes()),
            account_wide_execution(&binding, 1, Some("next"), EXECUTIONS.to_vec()),
        ],
        None,
    )?;
    assert_eq!(fills.len(), 3);
    assert_eq!(fills[0].execution_sequence, FieldState::Known(103));
    assert_eq!(fills[0].exchange_time_ms, Some(2_000));
    assert_eq!(cursor, "bybit-exec:2000:c");
    Ok(())
}

#[test]
fn snapshot_fills_rejects_unclosed_or_unknown_account_symbols()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, _, _) = limit_facts()?;
    let unclosed = String::from_utf8(EXECUTIONS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    assert!(
        snapshot_fills(
            &[account_wide_execution(
                &binding,
                0,
                None,
                unclosed.into_bytes(),
            )],
            None
        )
        .is_err()
    );
    let wrong_symbol = String::from_utf8(EXECUTIONS.to_vec())?.replace("BTCUSDT", "BTCUSDC");
    assert!(
        snapshot_fills(
            &[account_wide_execution(
                &binding,
                0,
                None,
                wrong_symbol.into_bytes(),
            )],
            None
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn fills_cursor_restarts_with_overlap_and_rejects_missing_window()
-> Result<(), Box<dyn std::error::Error>> {
    let resumed = fills_history_window(1_000_000, Some("bybit-exec:990000:exec-9"))?;
    assert_eq!(resumed.start_ms, 930_000);
    assert!(fills_history_window(HISTORY_WINDOW_MS + 2, Some("bybit-exec:1:exec-1")).is_err());
    assert!(fills_history_window(1_000_000, Some("okx-bill:99")).is_err());
    Ok(())
}

#[test]
fn priced_limit_keeps_selected_price_policy_and_cap() -> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, _) = limit_facts()?;
    let priced = AccountPricedLimitIntent {
        intent: limit_intent(Decimal::new(100, 0))?,
        limit_price: Price::new(Decimal::new(654_854, 1))?,
        time_in_force: LimitTimeInForce::Gtc,
        maximum_quantity: Some(Decimal::new(1, 3)),
    };
    let ExecutionCommand::PlaceLimit(command) = normalize_priced_limit(&binding, &rules, &priced)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(command.limit_price, priced.limit_price);
    assert_eq!(command.time_in_force, LimitTimeInForce::Gtc);
    assert!(command.quantity <= Decimal::new(1, 3));
    Ok(())
}

#[test]
fn priced_limit_rejects_an_off_tick_user_price() -> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, bbo) = limit_facts()?;
    let priced = AccountPricedLimitIntent {
        intent: limit_intent(Decimal::new(100, 0))?,
        limit_price: bbo.snapshot.bids[0].price,
        time_in_force: LimitTimeInForce::Gtc,
        maximum_quantity: None,
    };
    assert!(normalize_priced_limit(&binding, &rules, &priced).is_err());
    Ok(())
}

#[test]
fn signed_snapshot_uses_native_creation_time_only() {
    assert!(matches!(
        optional_order_created_at_ms(
            &serde_json::json!({"createdTime":"1800","updatedTime":"1900"})
        ),
        Ok(Some(1800))
    ));
    assert!(matches!(
        optional_order_created_at_ms(&serde_json::json!({"updatedTime":"1900"})),
        Ok(None)
    ));
}

#[test]
fn signed_partial_order_keeps_original_quantity_and_requires_native_balance()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, _, _) = limit_facts()?;
    let payload = serde_json::json!({
        "retCode": 0,
        "result": {"list": [{
            "symbol": "BTCUSDT",
            "side": "Buy",
            "positionIdx": 1,
            "orderLinkId": "partial-client",
            "orderId": "partial-order",
            "qty": "5",
            "leavesQty": "3",
            "cumExecQty": "2",
            "price": "60000",
            "orderType": "Limit",
            "timeInForce": "GTC",
            "reduceOnly": false,
            "orderStatus": "PartiallyFilled"
        }]}
    });
    let orders = snapshot_orders(
        &[account_wide_execution(
            &binding,
            0,
            None,
            serde_json::to_vec(&payload)?,
        )],
        &[],
    )?;
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].quantity, Decimal::new(5, 0));
    assert_eq!(orders[0].filled_quantity, Some(Decimal::new(2, 0)));

    let mut invalid = payload.clone();
    invalid["result"]["list"][0]["leavesQty"] = serde_json::json!("4");
    assert!(
        snapshot_orders(
            &[account_wide_execution(
                &binding,
                0,
                None,
                serde_json::to_vec(&invalid)?,
            )],
            &[],
        )
        .is_err()
    );

    invalid["result"]["list"][0]["leavesQty"] = serde_json::json!("-1");
    assert!(
        snapshot_orders(
            &[account_wide_execution(
                &binding,
                0,
                None,
                serde_json::to_vec(&invalid)?,
            )],
            &[],
        )
        .is_err()
    );
    Ok(())
}
