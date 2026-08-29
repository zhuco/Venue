use super::*;

#[test]
fn parses_usdt_futures_rules_and_signature() -> Result<(), BitgetError> {
    let rules = parse_contract_rules(
        &json!({"symbol":"DOGEUSDT","category":"USDT-FUTURES","baseCoin":"DOGE","quoteCoin":"USDT","type":"perpetual","status":"online","minOrderQty":"1","minOrderAmount":"5","priceMultiplier":"0.00001","quantityMultiplier":"1"}),
        "DOGE/USDT".parse().map_err(|_| BitgetError::Symbol)?,
        1,
    )?;
    assert_eq!(rules.instrument.quantity_step, Decimal::ONE);
    assert_eq!(rules.minimum_notional, Decimal::new(5, 0));
    assert_eq!(
        bitget_signature(
            "secret",
            "123",
            "POST",
            "/api/v3/trade/place-order",
            "",
            br#"{"symbol":"DOGEUSDT"}"#
        )?
        .len(),
        44
    );
    Ok(())
}

#[test]
fn uta_risk_uses_usdt_equity_not_asset_balance_and_preserves_leg_pnl()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let (account, legs) = parse_risk_snapshots(
        &json!({
            "accountEquity":"21", "usdtEquity":"20",
            "assets":[{"coin":"USDT","balance":"12","equity":"19"}]
        }),
        &[json!({
            "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
            "posSide":"long", "total":"600", "markPrice":"0.1",
            "unrealisedPnl":"1.1"
        })],
        &symbol,
        "uta_usdt_futures_hedge",
        8,
        1_000,
    )?;
    assert_eq!(account.account_equity, Decimal::new(20, 0));
    assert_eq!(legs[0].notional, Decimal::new(60, 0));
    assert_eq!(legs[0].unrealized_pnl, Decimal::new(11, 1));
    assert_eq!(legs[0].position_side, PositionSide::Long);
    Ok(())
}

#[test]
fn uta_risk_rejects_missing_equity_and_non_hedge_positions()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    assert!(
        parse_risk_snapshots(
            &json!({"assets":[{"coin":"USDT","balance":"20"}]}),
            &[],
            &symbol,
            "uta_usdt_futures_hedge",
            8,
            1_000,
        )
        .is_err()
    );
    assert!(matches!(
        parse_risk_snapshots(
            &json!({"usdtEquity":"20"}),
            &[json!({
                "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"one_way_mode",
                "posSide":"long", "total":"600", "markPrice":"0.1",
                "unrealisedPnl":"1.1"
            })],
            &symbol,
            "uta_usdt_futures_hedge",
            8,
            1_000,
        ),
        Err(BitgetError::PositionMode)
    ));
    Ok(())
}

#[test]
fn uta_market_reduce_is_market_close_on_exact_hedge_side() -> Result<(), Box<dyn std::error::Error>>
{
    let rules = parse_contract_rules(
        &json!({"symbol":"DOGEUSDT","category":"USDT-FUTURES","baseCoin":"DOGE","quoteCoin":"USDT","type":"perpetual","status":"online","minOrderQty":"1","minOrderAmount":"5","priceMultiplier":"0.00001","quantityMultiplier":"1"}),
        "DOGE/USDT".parse()?,
        1,
    )?;
    let cases = [
        (PositionSide::Long, OrderSide::Sell, "sell", "long"),
        (PositionSide::Short, OrderSide::Buy, "buy", "short"),
    ];
    for (position_side, side, expected_side, expected_position_side) in cases {
        let command = MarketReduceCommand {
            command_id: crate::domain::CommandId::new("risk_reduce_1")?,
            client_order_id: crate::domain::CommandId::new("risk_reduce_client_1")?,
            owner: crate::domain::OrderOwner {
                strategy_instance_id: "hedged_grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "bitget".to_owned(),
                account: "uta_usdt_futures_hedge".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: crate::domain::OrderPurpose::ExposureTakeProfit,
            },
            position_side,
            side,
            quantity: Decimal::new(180, 0),
            risk_episode_id: crate::domain::CommandId::new("risk_episode_1")?,
            position_generation: 9,
        };
        let body = market_reduce_body(&command, &rules)?;
        assert_eq!(
            body.get("orderType").and_then(Value::as_str),
            Some("market")
        );
        assert_eq!(
            body.get("side").and_then(Value::as_str),
            Some(expected_side)
        );
        assert_eq!(
            body.get("posSide").and_then(Value::as_str),
            Some(expected_position_side)
        );
        assert!(body.get("tradeSide").is_none());
        assert!(body.get("reduceOnly").is_none());
    }
    Ok(())
}

#[test]
fn uta_stream_and_signed_fill_share_exact_price_and_maker_parser()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    for (trade_scope, expected_maker) in [("maker", true), ("taker", false)] {
        let row = json!({
            "execId":"10", "orderId":"1", "clientOid":"hgo_e1_long_close_l1",
            "category":"USDT-FUTURES", "symbol":"DOGEUSDT", "side":"sell",
            "holdSide":"long", "execQty":"50", "execPrice":"0.10001",
            "execPnl":"0", "tradeScope":trade_scope, "execTime":"1", "updatedTime":"2",
            "feeDetail":[{"feeCoin":"USDT","fee":"0.01"}]
        });
        let signed = parse_fill(&row, &symbol)?;
        let stream = parse_private_fill_message(
            &json!({"arg":{"topic":"fill"},"data":[row]}).to_string(),
            &symbol,
        )?;
        assert_eq!(stream[0].fill, signed);
        assert_eq!(stream[0].fill.execution_sequence, FieldState::Known(10));
        assert_eq!(stream[0].fill.maker, FieldState::Known(expected_maker));
        assert_eq!(stream[0].fill.price.value(), Decimal::new(10001, 5));
        assert_eq!(stream[0].fill.exchange_time_ms, Some(1));
    }
    Ok(())
}

#[test]
fn uta_fill_with_unknown_role_remains_unknown() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let fill = parse_fill(
        &json!({
            "execId":"f1", "orderId":"1", "category":"USDT-FUTURES",
            "symbol":"DOGEUSDT", "side":"sell", "holdSide":"long",
            "execQty":"50", "execPrice":"0.1", "execPnl":"0",
            "tradeScope":"future_role", "updatedTime":"1"
        }),
        &symbol,
    )?;
    assert!(matches!(
        fill.maker,
        FieldState::Unavailable {
            reason: crate::domain::UnknownReason::Ambiguous
        }
    ));
    Ok(())
}

#[test]
fn parses_hedge_close_order_and_fill_identity() -> Result<(), BitgetError> {
    let symbol: Symbol = "DOGE/USDT".parse().map_err(|_| BitgetError::Symbol)?;
    let order = parse_order(
        &json!({"orderId":"1","clientOid":"hgo_e1_long_close_l1","category":"USDT-FUTURES","symbol":"DOGEUSDT","orderStatus":"live","side":"sell","posSide":"long","tradeSide":"close","qty":"50","cumExecQty":"0","price":"0.1","avgPrice":"0"}),
        &symbol,
    )?;
    assert!(order.reduce_only);
    let fill = parse_fill(
        &json!({"execId":"f1","orderId":"1","clientOid":"hgo_e1_long_close_l1","category":"USDT-FUTURES","symbol":"DOGEUSDT","side":"sell","holdSide":"long","execQty":"50","execPrice":"0.1","execPnl":"0","tradeScope":"maker","updatedTime":"1","feeDetail":[{"feeCoin":"USDT","fee":"0.01"}]}),
        &symbol,
    )?;
    assert_eq!(fill.fill_id, "f1");
    Ok(())
}

#[test]
fn hedge_mode_readback_derives_close_from_side_and_position_side() -> Result<(), BitgetError> {
    let symbol: Symbol = "DOGE/USDT".parse().map_err(|_| BitgetError::Symbol)?;
    let order = parse_order(
        &json!({"orderId":"1","clientOid":"hgo_e1_long_close_l1","category":"USDT-FUTURES","symbol":"DOGEUSDT","orderStatus":"new","side":"sell","posSide":"long","holdMode":"hedge_mode","reduceOnly":"NO","qty":"50","cumExecQty":"0","price":"0.1","avgPrice":"0"}),
        &symbol,
    )?;
    assert!(order.reduce_only);
    Ok(())
}

#[test]
fn parses_filled_directional_hedge_close_order_for_terminal_settlement() -> Result<(), BitgetError>
{
    let symbol: Symbol = "DOGE/USDT".parse().map_err(|_| BitgetError::Symbol)?;
    let order = parse_order(
        &json!({
            "orderId":"1476780296421732352",
            "clientOid":"ord-etp-l-0000000000000015",
            "category":"USDT-FUTURES",
            "symbol":"DOGEUSDT",
            "orderType":"market",
            "side":"sell",
            "price":"",
            "qty":"972",
            "cumExecQty":"972",
            "avgPrice":"0.08728",
            "orderStatus":"filled",
            "posSide":"long",
            "holdMode":"hedge_mode",
            "tradeSide":"close_long",
            "reduceOnly":"NO",
            "delegateType":"market"
        }),
        &symbol,
    )?;
    assert_eq!(order.state, OrderState::Filled);
    assert_eq!(order.filled_quantity, Decimal::new(972, 0));
    assert!(order.reduce_only);

    let mismatched = parse_order(
        &json!({
            "orderId":"1", "category":"USDT-FUTURES", "symbol":"DOGEUSDT",
            "orderStatus":"filled", "side":"sell", "posSide":"long",
            "holdMode":"hedge_mode", "tradeSide":"close_short",
            "qty":"1", "cumExecQty":"1", "price":"", "avgPrice":"0.1"
        }),
        &symbol,
    );
    assert!(matches!(mismatched, Err(BitgetError::Payload)));
    Ok(())
}

#[test]
fn regular_open_order_readback_rejects_non_normal_delegate_families() -> Result<(), BitgetError> {
    let symbol: Symbol = "DOGE/USDT".parse().map_err(|_| BitgetError::Symbol)?;
    let mut order = json!({
        "orderId":"1", "clientOid":"hgo_e1_long_open_l1",
        "category":"USDT-FUTURES", "symbol":"DOGEUSDT", "orderStatus":"live",
        "side":"buy", "posSide":"long", "holdMode":"hedge_mode",
        "tradeSide":null, "reduceOnly":"NO", "qty":"50", "cumExecQty":"0",
        "price":"0.1", "avgPrice":"0", "delegateType":"normal"
    });
    assert!(parse_regular_open_order(&order, &symbol).is_ok());
    order["delegateType"] = json!("market");
    assert!(matches!(
        parse_regular_open_order(&order, &symbol),
        Err(BitgetError::Payload)
    ));
    Ok(())
}

#[test]
fn preserves_business_rejection_code_for_unknown_recovery() {
    assert!(matches!(
        bitget_data(&json!({"code":"43001","msg":"not found"})),
        Err(BitgetError::RejectedCode { code, message })
            if code == "43001" && message == "not found"
    ));
}

#[test]
fn preserves_sanitized_http_rejection_details() {
    assert!(matches!(
        http_rejection(
            StatusCode::BAD_REQUEST,
            r#"{"code":"40725","msg":"post only\nwould cross"}"#,
        ),
        BitgetError::RejectedHttp {
            status: 400,
            code,
            message,
        } if code == "40725" && message == "post only would cross"
    ));
}

#[test]
fn private_readback_accepts_only_one_complete_observation_tuple() -> Result<(), BitgetError> {
    let complete =
        complete_private_readback_tuple((Ok(1_u8), Ok(2_u8), Ok(3_u8), Ok(4_u8), Ok(5_u8)))?;
    assert_eq!(complete, (1, 2, 3, 4, 5));

    let incomplete = complete_private_readback_tuple((
        Ok(1_u8),
        Err::<u8, _>(BitgetError::Http),
        Ok(3_u8),
        Ok(4_u8),
        Ok(5_u8),
    ));
    assert!(matches!(incomplete, Err(BitgetError::Http)));
    Ok(())
}

#[test]
fn post_only_verification_requires_bitget_post_only_time_in_force() {
    assert!(is_post_only_order(&json!({"timeInForce":"post_only"})));
    assert!(!is_post_only_order(&json!({"timeInForce":"normal"})));
    assert!(!is_post_only_order(&json!({})));
}

#[test]
fn private_stream_keeps_only_the_bound_symbol_for_position_order_and_fill()
-> Result<(), BitgetError> {
    let mut position = json!({
        "arg":{"topic":"position"},
        "data":[{"symbol":"DOGEUSDT","size":"1"},{"symbol":"SOLUSDT","size":"2"}]
    });
    assert!(filter_private_event_for_symbol(&mut position, "DOGEUSDT")?);
    assert_eq!(position["data"].as_array().map(Vec::len), Some(1));

    let mut foreign_order = json!({
        "arg":{"topic":"order"},
        "data":[{"symbol":"SOLUSDT"}]
    });
    assert!(!filter_private_event_for_symbol(
        &mut foreign_order,
        "DOGEUSDT"
    )?);
    assert_eq!(foreign_order["data"].as_array().map(Vec::len), Some(0));
    Ok(())
}

#[test]
fn fill_history_pagination_uses_the_server_cursor_without_query_injection()
-> Result<(), BitgetError> {
    assert_eq!(
        fill_history_query(None, Some("next cursor&x=1"))?,
        "category=USDT-FUTURES&limit=100&cursor=next%20cursor%26x%3D1"
    );
    let bounded = fill_history_query(Some(1), None)?;
    assert!(bounded.starts_with("category=USDT-FUTURES&limit=100&startTime="));
    assert!(!bounded.contains("symbol="));
    assert_eq!(
        fill_history_cursor(&json!({"list": [], "cursor": "1001"}))?,
        Some("1001".to_owned())
    );
    assert_eq!(fill_history_cursor(&json!({"list": []}))?, None);
    assert!(matches!(
        fill_history_cursor(&json!({"list": [], "cursor": ""})),
        Err(BitgetError::Payload)
    ));
    assert_eq!(
        open_orders_query("SOLUSDT", Some("next cursor&x=1")),
        "category=USDT-FUTURES&symbol=SOLUSDT&limit=100&cursor=next%20cursor%26x%3D1"
    );
    Ok(())
}
