use super::*;
use venue_domain::domain::{OrderSide, Price};
use venue_gateway_binance::{
    BinanceAccountBinding, BinanceConfig, GatewayBinding, GatewayMode, VenueId,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn fixture(
    cancel: bool,
    patch: serde_json::Value,
) -> Result<
    (
        ExecutionRequest,
        venue_gateway_binance::BinanceInstrumentRules,
        BinanceMutationAck,
    ),
    Box<dyn std::error::Error>,
> {
    let symbol = "BTC/USDT".parse()?;
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        symbol,
    )?;
    let config = BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
    let rules = venue_gateway_binance::parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        binding.symbol.clone(),
        7,
    )?;
    let fence = BinanceGridDispatchFence::new(&config, rules.clone(), 17, 1, 1000)?;
    let prepared = if cancel {
        fence.prepare_cancel(&BinanceCancelIntent {
            client_order_id: "venue_place_1".into(),
        })?
    } else {
        fence.prepare_place_limit(&BinancePlaceIntent {
            client_order_id: "venue_place_1".into(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 3),
            limit_price: Price::new(Decimal::from(50000))?,
            time_in_force: BinanceTimeInForce::PostOnly,
            reduce_only: false,
        })?
    };
    let mut value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../crates/venue-gateway-binance/fixtures/exact-order-readback.json"
    ))?;
    for (key, item) in patch.as_object().ok_or("invalid patch")? {
        value[key] = item.clone();
    }
    let ack = venue_gateway_binance::parse_mutation_ack(
        &prepared,
        fence.scope(),
        &serde_json::to_vec(&value)?,
        2200,
    )?;
    let request = ExecutionRequest {
        command_id: "command".into(),
        client_order_id: "venue_place_1".into(),
        credential_id: "credential".into(),
        trading_account_id: "account".into(),
        symbol: binding.symbol,
        known_native_order_id: None,
        reconciled_close_reservations: vec![],
        order_kind: if cancel {
            ExecutionOrderKind::CancelExact {
                native_order_id: Some("401".into()),
                target_client_order_id: Some("venue_place_1".into()),
            }
        } else {
            ExecutionOrderKind::LimitPostOnly {
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::new(2, 3),
                price: Decimal::from(50000),
                reducing: false,
            }
        },
    };
    Ok((request, rules, ack))
}

#[test]
fn full_result_confirms_place_and_cancel_without_query() -> TestResult {
    for (cancel, patch, expected) in [
        (false, serde_json::json!({}), ExecutionReadback::Accepted),
        (
            true,
            serde_json::json!({"status":"CANCELED"}),
            ExecutionReadback::Reconciled,
        ),
    ] {
        let (request, rules, ack) = fixture(cancel, patch)?;
        assert_eq!(grid_result_outcome(&request, &rules, &ack).state, expected);
    }
    Ok(())
}

#[test]
fn incomplete_or_conflicting_result_requires_reconciliation() -> TestResult {
    for patch in [
        serde_json::json!({"executedQty":null}),
        serde_json::json!({"status":null}),
        serde_json::json!({"positionSide":"SHORT"}),
        serde_json::json!({"origQty":"0.003"}),
        serde_json::json!({"price":"50001"}),
        serde_json::json!({"timeInForce":"GTC"}),
        serde_json::json!({"status":"FILLED","executedQty":"0.002"}),
    ] {
        let (request, rules, ack) = fixture(false, patch)?;
        assert_eq!(
            grid_result_outcome(&request, &rules, &ack).state,
            ExecutionReadback::Unknown
        );
    }
    let (request, rules, ack) = fixture(
        true,
        serde_json::json!({"status":"FILLED","executedQty":"0.002"}),
    )?;
    assert_eq!(
        grid_result_outcome(&request, &rules, &ack).state,
        ExecutionReadback::Unknown
    );
    Ok(())
}

#[test]
fn cancel_result_must_match_exact_target() -> TestResult {
    let (mut request, rules, ack) = fixture(true, serde_json::json!({"status":"CANCELED"}))?;
    request.order_kind = ExecutionOrderKind::CancelExact {
        native_order_id: Some("402".into()),
        target_client_order_id: Some("venue_place_1".into()),
    };
    assert_eq!(
        grid_result_outcome(&request, &rules, &ack).state,
        ExecutionReadback::Unknown
    );
    Ok(())
}
