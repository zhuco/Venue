use rust_decimal::Decimal;
use serde_json::json;

use crate::{
    domain::{CommandId, NativeOrderFamily, OrderOwner, OrderPurpose},
    exchange::grid::{GridOrderFamilyReadback, GridOrderFamilySnapshot, gate_grid_readback},
};

use super::*;

#[test]
fn public_stream_skips_heartbeat_control_before_depth_event()
-> Result<(), Box<dyn std::error::Error>> {
    use std::{net::TcpListener, thread};

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let depth = r#"{"time":1787674921,"channel":"futures.order_book_update","event":"update","result":{"t":1787674921717,"U":1,"u":2,"s":"DOGE_USDT","a":[],"b":[],"l":"20"}}"#;
    let server_depth = depth.to_owned();
    let server = thread::spawn(move || {
        let Ok((tcp, _)) = listener.accept() else {
            return false;
        };
        let Ok(mut socket) = tungstenite::accept(tcp) else {
            return false;
        };
        if socket
            .send(Message::Text(
                r#"{"channel":"futures.pong","event":"update","error":null}"#
                    .to_owned()
                    .into(),
            ))
            .is_err()
        {
            return false;
        }
        socket.send(Message::Text(server_depth.into())).is_ok()
    });

    let tcp = TcpStream::connect(address)?;
    let request = format!("ws://{address}/").into_client_request()?;
    let (socket, _) = tungstenite::client(request, MaybeTlsStream::Plain(tcp))?;
    let mut stream = GatePublicStream {
        socket,
        last_heartbeat_at: Instant::now(),
    };
    stream.set_read_timeout(Duration::from_secs(1))?;

    assert_eq!(stream.next_raw_event()?, Some(depth.to_owned()));
    assert!(matches!(server.join(), Ok(true)));
    Ok(())
}

#[test]
fn exact_order_not_found_is_authoritative_absence() {
    assert_eq!(
        exact_order_result(Err(GateError::Rejected {
            label: "ORDER_NOT_FOUND".to_owned(),
        })),
        Err(GateError::OrderAbsent)
    );
    assert_eq!(
        exact_order_result(Err(GateError::Rejected {
            label: "INVALID_ARGUMENT".to_owned(),
        })),
        Err(GateError::Rejected {
            label: "INVALID_ARGUMENT".to_owned(),
        })
    );
}

fn rules() -> Result<GateContractRules, GateError> {
    parse_contract_rules(
        &json!({
            "name": "DOGE_USDT",
            "quanto_multiplier": "10",
            "order_size_min": "1",
            "order_price_round": "0.00001",
            "enable_decimal": false,
            "in_delisting": false,
            "status": "trading"
        }),
        "DOGE/USDT".parse().map_err(|_| GateError::Symbol)?,
        1,
    )
}

#[test]
fn parses_integer_contract_rules_without_leaking_native_symbol() -> Result<(), GateError> {
    let rules = rules()?;
    assert_eq!(rules.instrument.symbol.to_string(), "DOGE/USDT");
    assert_eq!(rules.instrument.quantity_step, Decimal::new(10, 0));
    assert_eq!(rules.minimum_quantity(), Decimal::new(10, 0));
    assert_eq!(rules.native_contracts(Decimal::new(10, 0))?, Decimal::ONE);
    assert!(matches!(
        rules.native_contracts(Decimal::ONE),
        Err(GateError::Quantity)
    ));
    Ok(())
}

#[test]
fn gate_grid_readback_marks_every_non_regular_family_explicitly_unsupported()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = "DOGE/USDT".parse().map_err(|_| GateError::Symbol)?;
    let order = parse_order(
        &json!({
            "id": "gate-order-1",
            "contract": "DOGE_USDT",
            "size": "1",
            "left": "1",
            "price": "0.1",
            "status": "open",
            "is_reduce_only": false,
            "text": "t-hgo_e1_long_open_l1"
        }),
        &symbol,
        &rules()?,
    )?;
    let evidence = GridOrderFamilyReadback::regular_only_adapter_profile(
        vec![order.clone()],
        vec!["[{\"id\":\"gate-order-1\"}]".to_owned()],
    )?;

    assert!(evidence.covers_all_families());
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmOrder),
        Some(GridOrderFamilySnapshot::Complete {
            orders,
            signed_payloads,
        }) if orders == &vec![order] && signed_payloads.len() == 1
    ));
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmConditional),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmAlgo),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    Ok(())
}

#[test]
fn gate_grid_order_family_evidence_rejects_a_missing_signed_regular_page() {
    assert!(matches!(
        GridOrderFamilyReadback::regular_only_adapter_profile(Vec::new(), Vec::new()),
        Err(crate::exchange::grid::GridVenueError::PrivateReadbackIncomplete)
    ));
}

#[test]
fn gate_grid_readback_attaches_complete_family_evidence_to_the_production_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol = "DOGE/USDT".parse().map_err(|_| GateError::Symbol)?;
    let order = parse_order(
        &json!({
            "id": "gate-order-1",
            "contract": "DOGE_USDT",
            "size": "1",
            "left": "1",
            "price": "0.1",
            "status": "open",
            "is_reduce_only": false,
            "text": "t-hgo_e1_long_open_l1"
        }),
        &symbol,
        &rules()?,
    )?;
    let signed_regular_orders = "[{\"id\":\"gate-order-1\"}]".to_owned();
    let readback = GatePrivateReadback {
        user_id: "gate-user".to_owned(),
        raw_payloads: vec![
            "{\"total\":\"1\"}".to_owned(),
            signed_regular_orders.clone(),
        ],
        signed_regular_order_payloads: vec![signed_regular_orders],
        balance: parse_account_balance(&json!({
            "total": "1",
            "available": "1",
            "position_initial_margin": "0",
            "order_initial_margin": "0",
            "maintenance_margin": "0"
        }))?,
        dual_position_mode: true,
        positions: Vec::new(),
        orders: vec![order.clone()],
        fills: Vec::new(),
    };

    let snapshot = gate_grid_readback(readback)?;
    let evidence = snapshot
        .order_family_readback
        .ok_or("Gate must attach order-family evidence")?;
    assert!(evidence.covers_all_families());
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmOrder),
        Some(GridOrderFamilySnapshot::Complete { orders, .. }) if orders == &vec![order]
    ));
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmConditional),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    assert!(matches!(
        evidence.snapshot(NativeOrderFamily::UmAlgo),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    Ok(())
}

#[test]
fn parses_current_object_order_book_levels() -> Result<(), GateError> {
    let (bid, ask) = parse_best_bid_ask(&json!({
        "bids": [{"p":"0.09251","s":1}],
        "asks": [{"p":"0.09252","s":1}]
    }))?;
    assert_eq!(bid.value(), Decimal::new(9251, 5));
    assert_eq!(ask.value(), Decimal::new(9252, 5));
    Ok(())
}

#[test]
fn post_only_verification_requires_gate_poc_time_in_force() {
    assert!(is_post_only_order(&json!({"tif":"poc"})));
    assert!(!is_post_only_order(&json!({"tif":"gtc"})));
    assert!(!is_post_only_order(&json!({})));
}

#[test]
fn account_readback_requires_exact_dual_position_mode() {
    assert_eq!(
        parse_dual_position_mode(&json!({"position_mode":"dual"})),
        Ok(true)
    );
    assert!(matches!(
        parse_dual_position_mode(&json!({"position_mode":"dual_plus"})),
        Err(GateError::PositionMode)
    ));
}

#[test]
fn negative_dual_total_uses_the_signed_available_balance() -> Result<(), GateError> {
    let balance = parse_account_balance(&json!({
        "total": "-0.002",
        "available": "15.1",
        "position_initial_margin": "0",
        "order_initial_margin": null,
        "maintenance_margin": "0"
    }))?;
    assert_eq!(balance.wallet_balance, Decimal::new(151, 1));
    assert_eq!(balance.available_balance, Decimal::new(151, 1));
    Ok(())
}

#[test]
fn gate_classic_and_evolved_cross_equity_use_distinct_official_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let positions = vec![json!({
        "contract":"DOGE_USDT", "mode":"dual_long", "size":"7",
        "mark_price":"0.1", "value":"7", "unrealised_pnl":"1.2"
    })];
    let (mode, classic, legs) = parse_risk_snapshots(
        &json!({
            "position_mode":"dual", "total":"20", "unrealised_pnl":"2"
        }),
        &positions,
        &symbol,
        &rules,
        "usdt_futures_dual",
        5,
        1_000,
    )?;
    assert_eq!(mode, GateRiskAccountMode::Classic);
    assert_eq!(classic.account_equity, Decimal::new(22, 0));
    assert_eq!(legs[0].quantity, Decimal::new(70, 0));
    assert_eq!(legs[0].notional, Decimal::new(7, 0));

    let (mode, evolved, _) = parse_risk_snapshots(
        &json!({
            "position_mode":"dual", "total":"20", "unrealised_pnl":"2",
            "cross_margin_balance":"23.5"
        }),
        &positions,
        &symbol,
        &rules,
        "usdt_futures_dual",
        6,
        2_000,
    )?;
    assert_eq!(mode, GateRiskAccountMode::EvolvedClassicCross);
    assert_eq!(evolved.account_equity, Decimal::new(235, 1));
    Ok(())
}

#[test]
fn gate_single_currency_uses_signed_unified_usdt_margin_balance()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let positions = vec![json!({
        "contract":"DOGE_USDT", "mode":"dual_short", "size":"-7",
        "mark_price":"0.1", "value":"7", "unrealised_pnl":"1.2"
    })];
    let (mode, account, legs) = gate_risk::parse_risk_snapshots_with_unified(
        &json!({
            "position_mode":"dual", "margin_mode":3,
            "total":"0", "unrealised_pnl":"-1"
        }),
        &json!({"mode":"single_currency"}),
        &json!({
            "mode":"single_currency", "locked":false,
            "balances":{"USDT":{"margin_balance":"22.5"}}
        }),
        &positions,
        &symbol,
        &rules,
        "usdt_futures",
        7,
        3_000,
    )?;
    assert_eq!(mode, GateRiskAccountMode::UnifiedSingleCurrency);
    assert_eq!(account.account_equity, Decimal::new(225, 1));
    assert_eq!(legs[0].position_side, PositionSide::Short);
    Ok(())
}

#[test]
fn gate_unified_mode_mismatch_locked_account_and_other_margin_modes_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let futures = json!({"position_mode":"dual","margin_mode":3});
    let mode = json!({"mode":"single_currency"});
    let locked = json!({
        "mode":"single_currency", "locked":true,
        "balances":{"USDT":{"margin_balance":"22.5"}}
    });
    assert!(matches!(
        gate_risk::parse_risk_snapshots_with_unified(
            &futures,
            &mode,
            &locked,
            &[],
            &symbol,
            &rules,
            "usdt_futures",
            7,
            3_000,
        ),
        Err(GateError::RiskAccountMode)
    ));
    assert!(matches!(
        gate_risk::parse_risk_snapshots_with_unified(
            &futures,
            &json!({"mode":"portfolio"}),
            &json!({
                "mode":"single_currency", "locked":false,
                "balances":{"USDT":{"margin_balance":"22.5"}}
            }),
            &[],
            &symbol,
            &rules,
            "usdt_futures",
            7,
            3_000,
        ),
        Err(GateError::RiskAccountMode)
    ));
    for margin_mode in [1, 2, 4] {
        assert!(matches!(
            parse_risk_snapshots(
                &json!({
                    "position_mode":"dual", "margin_mode":margin_mode,
                    "total":"20", "unrealised_pnl":"2",
                    "cross_margin_balance":"22"
                }),
                &[],
                &symbol,
                &rules,
                "usdt_futures",
                7,
                3_000,
            ),
            Err(GateError::RiskAccountMode)
        ));
    }
    Ok(())
}

#[test]
fn gate_risk_readback_window_is_bounded() {
    assert!(gate_risk::validate_risk_readback_window(1_000, 4_000).is_ok());
    assert!(matches!(
        gate_risk::validate_risk_readback_window(1_000, 4_001),
        Err(GateError::RiskSnapshot)
    ));
    assert!(matches!(
        gate_risk::validate_risk_readback_window(2_000, 1_999),
        Err(GateError::RiskSnapshot)
    ));
}

#[test]
fn gate_unknown_risk_account_mode_and_non_dual_leg_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    assert!(matches!(
        parse_risk_snapshots(
            &json!({"position_mode":"dual", "available":"20"}),
            &[],
            &symbol,
            &rules,
            "usdt_futures_dual",
            5,
            1_000,
        ),
        Err(GateError::RiskAccountMode)
    ));
    assert!(matches!(
        parse_risk_snapshots(
            &json!({
                "position_mode":"dual", "total":"20", "unrealised_pnl":"2"
            }),
            &[json!({
                "contract":"DOGE_USDT", "mode":"single", "size":"7",
                "mark_price":"0.1", "value":"7", "unrealised_pnl":"1.2"
            })],
            &symbol,
            &rules,
            "usdt_futures_dual",
            5,
            1_000,
        ),
        Err(GateError::PositionMode)
    ));
    Ok(())
}

#[test]
fn gate_market_reduce_is_ioc_reduce_only_and_exact_hedge_side()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (PositionSide::Long, OrderSide::Sell, "-3"),
        (PositionSide::Short, OrderSide::Buy, "3"),
    ];
    for (position_side, side, expected_size) in cases {
        let command = MarketReduceCommand {
            command_id: CommandId::new("risk_reduce_1")?,
            client_order_id: CommandId::new("risk_reduce_client_1")?,
            owner: OrderOwner {
                strategy_instance_id: "hedged_grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "gate".to_owned(),
                account: "usdt_futures_dual".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::ExposureTakeProfit,
            },
            position_side,
            side,
            quantity: Decimal::new(30, 0),
            risk_episode_id: CommandId::new("risk_episode_1")?,
            position_generation: 9,
        };
        let body = market_reduce_body(&command, &rules()?)?;
        assert_eq!(
            body.get("size").and_then(Value::as_str),
            Some(expected_size)
        );
        assert_eq!(body.get("reduce_only").and_then(Value::as_bool), Some(true));
        assert_eq!(body.get("tif").and_then(Value::as_str), Some("ioc"));
        assert_eq!(body.get("price").and_then(Value::as_str), Some("0"));
    }
    Ok(())
}

#[test]
fn gate_stream_and_signed_fill_share_exact_price_and_maker_parser()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = rules()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    for (role, expected_maker) in [("maker", true), ("taker", false)] {
        let row = json!({
            "id":"10", "order_id":"1", "contract":"DOGE_USDT", "size":"-5",
            "price":"0.10001", "fee":"0.01", "pnl":"0", "role":role,
            "create_time_ms":"1", "text":"t-hgo_e1_long_close_l1"
        });
        let signed = parse_fill(&row, &symbol, &rules)?;
        let stream_event = parse_private_event(
            &json!({
                "channel":"futures.usertrades", "event":"update", "error":null,
                "result":row
            })
            .to_string(),
        )?
        .ok_or("missing event")?;
        let GatePrivateEvent::Fill { value, .. } = stream_event else {
            return Err("wrong event".into());
        };
        let stream = parse_fill(&value, &symbol, &rules)?;
        assert_eq!(stream, signed);
        assert_eq!(stream.execution_sequence, FieldState::Known(10));
        assert_eq!(stream.maker, FieldState::Known(expected_maker));
        assert_eq!(stream.price.value(), Decimal::new(10001, 5));
    }
    Ok(())
}

#[test]
fn gate_fill_with_unknown_role_remains_unknown() -> Result<(), Box<dyn std::error::Error>> {
    let fill = parse_fill(
        &json!({
            "id":"f1", "order_id":"1", "contract":"DOGE_USDT", "size":"-5",
            "price":"0.1", "role":"future_role",
            "text":"t-hgo_e1_long_close_l1"
        }),
        &"DOGE/USDT".parse()?,
        &rules()?,
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
fn gate_canonical_exposure_fill_ids_prove_the_hedge_side() -> Result<(), Box<dyn std::error::Error>>
{
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let rules = rules()?;
    for (client_id, signed_size, expected_side) in [
        ("t-ord-etp-l-0000000000000001", "-41", PositionSide::Long),
        ("t-ord-etp-s-abcdef0123456789", "41", PositionSide::Short),
    ] {
        let row = json!({
            "id":"227262266", "order_id":"56013526190595884", "contract":"DOGE_USDT",
            "size":signed_size, "price":"0.08548", "role":"taker", "text":client_id
        });
        let fill = parse_fill(&row, &symbol, &rules)?;
        assert_eq!(fill.position_side, FieldState::Known(expected_side));
        assert_eq!(fill.quantity, Decimal::new(410, 0));
        assert_eq!(
            parse_fill_client_order_id(&row)?,
            FieldState::Known(client_id.trim_start_matches("t-").to_owned())
        );
    }
    Ok(())
}

#[test]
fn gate_noncanonical_exposure_fill_ids_remain_ambiguous() -> Result<(), Box<dyn std::error::Error>>
{
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let rules = rules()?;
    for client_id in [
        "t-ord-etp-l-000000000000000",
        "t-ord-etp-s-ABCDEF0123456789",
        "t-ord-etp-long-0000000000000001",
        "t-ord-etp-l-0000000000000001_short_",
    ] {
        let fill = parse_fill(
            &json!({
                "id":"227262266", "order_id":"56013526190595884", "contract":"DOGE_USDT",
                "size":"-41", "price":"0.08548", "role":"taker", "text":client_id
            }),
            &symbol,
            &rules,
        )?;
        assert!(matches!(
            fill.position_side,
            FieldState::Unavailable {
                reason: crate::domain::UnknownReason::Ambiguous
            }
        ));
    }
    Ok(())
}

#[test]
fn parses_post_only_order_with_exact_normalized_identity() -> Result<(), GateError> {
    let rules = rules()?;
    let order = parse_order(
        &json!({
            "id": "123",
            "contract": "DOGE_USDT",
            "size": "1",
            "left": "1",
            "is_reduce_only": false,
            "status": "open",
            "price": "0.1",
            "fill_price": "0",
            "text": "t-hgo_e1_long_open_l1"
        }),
        &"DOGE/USDT".parse().map_err(|_| GateError::Symbol)?,
        &rules,
    )?;
    assert_eq!(order.order_id, "123");
    assert_eq!(order.quantity, Decimal::new(10, 0));
    assert_eq!(
        order.client_order_id,
        FieldState::Known("hgo_e1_long_open_l1".to_owned())
    );
    assert_eq!(order.position_side, FieldState::Known(PositionSide::Long));
    Ok(())
}

#[test]
fn preserves_gate_signature_body_and_query_contract() -> Result<(), GateError> {
    let signature = gate_signature(
        "secret",
        "POST",
        "/futures/usdt/orders",
        "",
        br#"{""contract"":""DOGE_USDT"}"#,
        "123",
    )?;
    assert_eq!(signature.len(), 128);
    assert_ne!(
        signature,
        gate_signature(
            "secret",
            "POST",
            "/futures/usdt/orders",
            "x=1",
            br#"{""contract"":""DOGE_USDT"}"#,
            "123"
        )?
    );
    Ok(())
}

#[test]
fn gate_client_identity_is_bounded_and_safe() {
    assert_eq!(
        native_client_order_id("hgo_e1_long_open_l1"),
        Ok("t-hgo_e1_long_open_l1".to_owned())
    );
    assert!(matches!(
        native_client_order_id(&"x".repeat(29)),
        Err(GateError::ClientOrderId)
    ));
}

#[test]
fn gate_error_label_preserves_only_the_documented_machine_label() {
    assert_eq!(
        gate_error_label(
            r#"{"label":"POC_FILL_IMMEDIATELY","message":"order would take"}"#,
            StatusCode::BAD_REQUEST,
        ),
        "POC_FILL_IMMEDIATELY"
    );
    assert_eq!(
        gate_error_label(
            r#"{"label":"unsafe label","message":"ignored"}"#,
            StatusCode::BAD_REQUEST,
        ),
        "HTTP_400"
    );
    assert_eq!(
        gate_error_label("not-json", StatusCode::UNAUTHORIZED),
        "HTTP_401"
    );
}

#[test]
fn private_balance_subscription_uses_only_the_required_user_id() {
    let channels = private_subscription_channels("1001", "DOGE_USDT");
    assert_eq!(channels[3], ("futures.balances", json!(["1001"])));
}

#[test]
fn futures_heartbeat_is_public_and_timestamp_bound() {
    let payload = gate_futures_ping(123).unwrap();
    let value: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        value,
        json!({
            "time": 123,
            "channel": "futures.ping",
        })
    );
    assert!(value.get("auth").is_none());
}

#[test]
fn private_page_cursor_is_only_advanced_by_a_nonempty_final_venue_id() {
    let page = json!([
        {"id": "11"},
        {"id": 12},
    ]);
    let ids = page
        .as_array()
        .into_iter()
        .flatten()
        .map(|value| identifier(value.get("id")))
        .collect::<Result<Vec<_>, _>>();
    assert_eq!(ids, Ok(vec!["11".to_owned(), "12".to_owned()]));
}

#[test]
fn timestamp_parser_accepts_gate_fractional_second_fields() -> Result<(), GateError> {
    assert_eq!(
        timestamp_ms(Some(&json!("1787512420.195")))?,
        1_787_512_420_195
    );
    assert_eq!(
        timestamp_ms(Some(&json!(1787512420195_u64)))?,
        1_787_512_420_195
    );
    assert!(matches!(
        timestamp_ms(Some(&json!("0"))),
        Err(GateError::Clock)
    ));
    Ok(())
}

#[test]
fn numeric_zero_price_is_unavailable_instead_of_a_parse_failure() -> Result<(), GateError> {
    assert_eq!(optional_price(Some(&json!(0)))?, None);
    assert!(matches!(
        optional_price_state(Some(&json!(0)))?,
        FieldState::Unavailable { .. }
    ));
    Ok(())
}

#[test]
fn gate_reduce_only_side_maps_to_the_existing_hedge_leg() -> Result<(), GateError> {
    let rules = rules()?;
    let command = OrderCommand {
        command_id: CommandId::new("grid_reduce_command").map_err(|_| GateError::Command)?,
        client_order_id: CommandId::new("hgo_e1_long_close_l1").map_err(|_| GateError::Command)?,
        owner: OrderOwner {
            strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse().map_err(|_| GateError::Symbol)?,
            purpose: OrderPurpose::Reduce,
        },
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(10, 0),
        limit_price: Price::new(Decimal::new(1, 1)).map_err(|_| GateError::Payload)?,
        reduce_only: true,
    };
    assert_eq!(
        signed_contracts(rules.native_contracts(command.quantity)?, command.side)?,
        -Decimal::ONE
    );
    Ok(())
}

#[test]
fn private_stream_events_reject_remote_errors_and_keep_channels_exact() -> Result<(), GateError> {
    let event = parse_private_event(
        r#"{"channel":"futures.usertrades","event":"update","error":null,"result":{"id":"1"}}"#,
    )?;
    assert_eq!(
            event,
            Some(GatePrivateEvent::Fill {
                value: json!({"id":"1"}),
                raw_payload: r#"{"channel":"futures.usertrades","event":"update","error":null,"result":{"id":"1"}}"#.to_owned(),
            })
        );
    assert!(matches!(
        parse_private_event(
            r#"{"channel":"futures.usertrades","event":"update","error":{"code":4},"result":null}"#
        ),
        Err(GateError::WebSocket)
    ));
    Ok(())
}
