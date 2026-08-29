use std::collections::VecDeque;

use super::*;

fn user_trade_page(first_id: u64, count: u16, first_time_ms: u64) -> Result<String, PrivateError> {
    let values = (0..u64::from(count))
        .map(|offset| {
            serde_json::json!({
                "id": first_id + offset,
                "time": first_time_ms + offset,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&values).map_err(|_| PrivateError::FillPage)
}

#[test]
fn user_trades_query_keeps_from_id_separate_from_time_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    assert_eq!(
        user_trades_parameters(&symbol, Some(41), 100, 200),
        vec![
            ("symbol", "DOGEUSDT".to_owned()),
            ("fromId", "41".to_owned()),
            ("limit", "1000".to_owned()),
        ]
    );
    assert_eq!(
        user_trades_parameters(&symbol, None, 100, 200),
        vec![
            ("symbol", "DOGEUSDT".to_owned()),
            ("startTime", "100".to_owned()),
            ("endTime", "200".to_owned()),
            ("limit", "1000".to_owned()),
        ]
    );
    Ok(())
}

#[test]
fn user_trades_paginator_continues_full_pages_and_commits_short_page_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let mut pages = VecDeque::from(vec![
        user_trade_page(1, USER_TRADES_PAGE_LIMIT, 101)?,
        "[]".to_owned(),
    ]);
    let mut requests = Vec::new();
    let result = paginate_recent_fills(
        RecentFillsCursor {
            observed_through_ms: 100,
            last_trade_id: None,
            last_event_time_ms: None,
        },
        2_000,
        |request| {
            requests.push(request);
            pages.pop_front().ok_or(PrivateError::FillPage)
        },
    )?;

    assert_eq!(result.pages, 2);
    assert_eq!(requests[0].from_id, None);
    assert_eq!(requests[1].from_id, Some(1_001));
    assert_eq!(requests[0].limit, USER_TRADES_PAGE_LIMIT);
    assert_eq!(result.cursor.last_trade_id, Some(1_000));
    assert_eq!(result.cursor.last_event_time_ms, Some(1_100));
    assert_eq!(result.cursor.observed_through_ms, 2_000);
    assert_eq!(
        serde_json::from_str::<Vec<Value>>(&result.payload)?.len(),
        1_000
    );
    Ok(())
}

#[test]
fn user_trades_paginator_resumes_from_persisted_native_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let mut requests = Vec::new();
    let result = paginate_recent_fills(
        RecentFillsCursor {
            observed_through_ms: 120,
            last_trade_id: Some(5),
            last_event_time_ms: Some(120),
        },
        200,
        |request| {
            requests.push(request);
            user_trade_page(6, 1, 130)
        },
    )?;

    assert_eq!(requests[0].from_id, Some(6));
    assert_eq!(requests[0].start_time_ms, 120);
    assert_eq!(requests[0].end_time_ms, 200);
    assert_eq!(result.cursor.last_trade_id, Some(6));
    assert_eq!(result.cursor.observed_through_ms, 200);
    Ok(())
}

#[test]
fn user_trades_paginator_rejects_repeated_or_backward_page_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let repeated = paginate_recent_fills(
        RecentFillsCursor {
            observed_through_ms: 100,
            last_trade_id: Some(5),
            last_event_time_ms: Some(120),
        },
        200,
        |_| user_trade_page(5, 1, 130),
    );
    assert!(matches!(repeated, Err(PrivateError::FillPage)));

    let malformed = paginate_recent_fills(
        RecentFillsCursor {
            observed_through_ms: 100,
            last_trade_id: None,
            last_event_time_ms: None,
        },
        200,
        |_| Ok(r#"[{"id":1}]"#.to_owned()),
    );
    assert!(matches!(malformed, Err(PrivateError::FillPage)));

    let time_gap = paginate_recent_fills(
        RecentFillsCursor {
            observed_through_ms: 100,
            last_trade_id: None,
            last_event_time_ms: None,
        },
        200,
        |_| user_trade_page(1, 1, 99),
    );
    assert!(matches!(time_gap, Err(PrivateError::FillPage)));
    Ok(())
}

#[test]
fn http_connect_proxy_parser_accepts_only_plain_authorities()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        parse_http_connect_proxy("http://proxy.example:8080/")?,
        HttpConnectProxy {
            host: "proxy.example".to_owned(),
            port: 8080,
        }
    );
    assert_eq!(
        parse_http_connect_proxy("http://127.0.0.1")?,
        HttpConnectProxy {
            host: "127.0.0.1".to_owned(),
            port: 80,
        }
    );
    for invalid in [
        "https://proxy.example:443",
        "socks5://proxy.example:1080",
        "http://user:password@proxy.example:8080",
        "http://proxy.example:8080/path",
        "http://proxy.example:0",
    ] {
        assert!(parse_http_connect_proxy(invalid).is_err());
    }
    Ok(())
}

#[test]
fn connect_request_and_response_are_bounded_and_secret_free()
-> Result<(), Box<dyn std::error::Error>> {
    let request_bytes = proxy_connect_request();
    let request = std::str::from_utf8(&request_bytes)?;
    assert_eq!(
        request,
        "CONNECT fstream.binance.com:443 HTTP/1.1\r\nHost: fstream.binance.com:443\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    assert!(!request.contains("listenKey"));
    assert!(
        parse_proxy_connect_response(
            b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: test\r\n\r\n"
        )
        .is_ok()
    );
    assert!(
        parse_proxy_connect_response(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
            .is_err()
    );
    assert!(parse_proxy_connect_response(b"HTTP/1.1 200 OK\r\n\r\nextra").is_err());
    assert!(parse_proxy_connect_response(&vec![b'x'; PROXY_RESPONSE_LIMIT + 1]).is_err());
    Ok(())
}

#[test]
fn portfolio_stream_expiry_is_identity_checked_and_redacted_before_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let listen_key = PrivateListenKey("account-stream-secret".to_owned());
    assert_eq!(
        private_stream_url(&listen_key),
        "wss://fstream.binance.com/pm/ws/account-stream-secret"
    );

    let payload = sanitize_private_stream_payload_for_transport(
        &listen_key,
        r#"{"e":"listenKeyExpired","E":123,"listenKey":"account-stream-secret"}"#.to_owned(),
    )?;
    assert!(payload.contains("listenKeyExpired"));
    assert!(payload.contains("[redacted]"));
    assert!(!payload.contains("account-stream-secret"));

    assert!(matches!(
        sanitize_private_stream_payload_for_transport(
            &listen_key,
            r#"{"e":"listenKeyExpired","E":123,"listenKey":"different"}"#.to_owned(),
        ),
        Err(PrivateError::ListenKey)
    ));

    let ordinary = r#"{"e":"ORDER_TRADE_UPDATE","E":123}"#.to_owned();
    assert_eq!(
        sanitize_private_stream_payload_for_transport(&listen_key, ordinary.clone())?,
        ordinary
    );
    Ok(())
}

#[test]
fn depth_stream_uses_native_symbol_only_inside_adapter() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "btc/usdt".parse()?;

    assert_eq!(
        depth_stream_url(&symbol),
        "wss://fstream.binance.com/public/ws/btcusdt@depth@100ms"
    );
    Ok(())
}

#[test]
fn every_public_stream_uses_the_documented_symbol_scope() -> Result<(), Box<dyn std::error::Error>>
{
    let symbol: Symbol = "DOGE/USDT".parse()?;
    assert_eq!(
        public_stream_url(&symbol, PublicStream::AggTrade),
        "wss://fstream.binance.com/market/ws/dogeusdt@aggTrade"
    );
    assert_eq!(
        public_stream_url(&symbol, PublicStream::BookTicker),
        "wss://fstream.binance.com/public/ws/dogeusdt@bookTicker"
    );
    assert_eq!(
        public_stream_url(&symbol, PublicStream::Kline1m),
        "wss://fstream.binance.com/market/ws/dogeusdt@kline_1m"
    );
    assert_eq!(
        public_stream_url(&symbol, PublicStream::MarkFunding),
        "wss://fstream.binance.com/market/ws/dogeusdt@markPrice@1s"
    );
    Ok(())
}

#[test]
fn open_one_minute_klines_are_valid_but_not_yet_admissible()
-> Result<(), Box<dyn std::error::Error>> {
    let open = r#"{"e":"kline","E":119000,"s":"DOGEUSDT","k":{"t":60000,"T":119999,"s":"DOGEUSDT","i":"1m","x":false}}"#;
    let closed = r#"{"e":"kline","E":119999,"s":"DOGEUSDT","st":1,"k":{"t":60000,"T":119999,"s":"DOGEUSDT","i":"1m","x":true}}"#;

    assert!(!kline_payload_is_closed(open, "DOGEUSDT")?);
    assert!(kline_payload_is_closed(closed, "DOGEUSDT")?);
    assert!(kline_payload_is_closed(open, "BTCUSDT").is_err());
    Ok(())
}

#[test]
fn instrument_rules_require_live_tradeability_evidence() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let payload = r#"{"symbols":[{"symbol":"DOGEUSDT","baseAsset":"DOGE","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING","filters":[{"filterType":"PRICE_FILTER","tickSize":"0.00001"},{"filterType":"LOT_SIZE","minQty":"1","stepSize":"1"},{"filterType":"MIN_NOTIONAL","notional":"5"}]}]}"#;
    let instrument = parse_instrument(payload, symbol.clone(), 1)?;
    let rules = parse_contract_rules(payload, symbol.clone(), 1)?;

    assert_eq!(instrument.minimum_notional.value, Decimal::new(5, 0));
    assert_eq!(instrument.quantity_step, Decimal::ONE);
    assert_eq!(rules.instrument, instrument);
    assert_eq!(rules.minimum_quantity, Decimal::ONE);
    assert!(parse_instrument(&payload.replace("TRADING", "BREAK"), symbol.clone(), 1).is_err());
    assert!(
        parse_contract_rules(
            &payload.replace("\"minQty\":\"1\"", "\"minQty\":\"1.5\""),
            symbol,
            1,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn grid_identity_and_post_only_proof_match_binance_wire_contract() {
    assert!(client_order_id_is_valid("hgo_e1_long_open_l1"));
    assert!(client_order_id_is_valid("a.b/c:d_e-f"));
    assert!(!client_order_id_is_valid(""));
    assert!(!client_order_id_is_valid(&"x".repeat(37)));
    assert!(!client_order_id_is_valid("contains space"));
    assert!(is_post_only_order_response(
        r#"{"clientOrderId":"hgo_e1_long_open_l1","timeInForce":"GTX"}"#
    ));
    assert!(!is_post_only_order_response(
        r#"{"clientOrderId":"hgo_e1_long_open_l1","timeInForce":"GTC"}"#
    ));
    assert!(!is_post_only_order_response("not-json"));
}

#[test]
fn public_streams_preserve_unknowns_without_defaulting() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let trade = RawMarketRecord::new(RawSource::WebSocketTrade, symbol.clone(), 1, 1, r#"{"e":"aggTrade","E":1,"s":"DOGEUSDT","a":2,"p":"0.1","q":"50","nq":"5","f":3,"l":4,"T":1,"st":1}"#.to_owned())?;
    let ticker = RawMarketRecord::new(RawSource::WebSocketTicker, symbol.clone(), 1, 2, r#"{"e":"bookTicker","u":2,"E":2,"T":2,"s":"DOGEUSDT","b":"0.1","B":"50","a":"0.2","A":"25","st":1}"#.to_owned())?;
    let bar = RawMarketRecord::new(RawSource::WebSocketKline, symbol.clone(), 1, 119_999, r#"{"e":"kline","E":119999,"s":"DOGEUSDT","st":1,"k":{"t":60000,"T":119999,"s":"DOGEUSDT","i":"1m","o":"0.1","h":"0.2","l":"0.09","c":"0.15","x":true}}"#.to_owned())?;
    let mark = RawMarketRecord::new(RawSource::WebSocketMarkFunding, symbol, 1, 3, r#"{"e":"markPriceUpdate","E":3,"s":"DOGEUSDT","p":"0.1","i":"0.1","r":"0.0001","T":4,"st":1}"#.to_owned())?;

    assert!(matches!(
        normalize(&trade, "DOGEUSDT")?,
        MarketEvent::Trade(PublicTrade {
            aggressor: FieldState::Missing,
            ..
        })
    ));
    assert!(matches!(
        normalize(&ticker, "DOGEUSDT")?,
        MarketEvent::Ticker(_)
    ));
    assert!(matches!(
        normalize(&bar, "DOGEUSDT")?,
        MarketEvent::Bar(PublicBar {
            sequence: 2,
            close,
            ..
        }) if close.value() == Decimal::new(15, 2)
    ));
    assert!(matches!(
        normalize(&mark, "DOGEUSDT")?,
        MarketEvent::MarkFunding(MarkFunding {
            estimated_settle_price: FieldState::Missing,
            predicted_funding_rate: FieldState::Missing,
            ..
        })
    ));
    Ok(())
}

#[test]
fn public_http_faults_are_classified_for_session_backoff() {
    assert!(matches!(
        public_http_fault(reqwest::StatusCode::TOO_MANY_REQUESTS),
        Some(PublicError::RateLimited)
    ));
    assert!(matches!(
        public_http_fault(reqwest::StatusCode::BAD_GATEWAY),
        Some(PublicError::ServerFailure(502))
    ));
    assert!(public_http_fault(reqwest::StatusCode::BAD_REQUEST).is_none());
}

#[test]
fn signed_query_is_stable_and_percent_encodes_each_component()
-> Result<(), Box<dyn std::error::Error>> {
    let query = signed_query(
        b"secret",
        &[
            ("symbol", "DOGEUSDT".to_owned()),
            ("recvWindow", "5000".to_owned()),
            ("timestamp", "1".to_owned()),
        ],
    )?;

    assert_eq!(
        query,
        "symbol=DOGEUSDT&recvWindow=5000&timestamp=1&signature=1d3b55d13fc959584fc9e292ecb650a271a4f7d55f53e048ce44cfeb7f0c81d0"
    );
    assert_eq!(encode_component("client order/1"), "client%20order%2F1");
    Ok(())
}

#[test]
fn market_inventory_order_uses_papi_um_market_parameters() -> Result<(), Box<dyn std::error::Error>>
{
    let command = MarketOrderCommand {
        command_id: crate::domain::CommandId::new("market_1")?,
        client_order_id: crate::domain::CommandId::new("venue_market_1")?,
        owner: crate::domain::OrderOwner {
            strategy_instance_id: "hedged_grid_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".parse()?,
            purpose: crate::domain::OrderPurpose::Entry,
        },
        position_side: crate::domain::PositionSide::Short,
        side: crate::domain::OrderSide::Sell,
        quantity: Decimal::new(15, 2),
        reduce_only: false,
    };

    assert_eq!(
        market_order_parameters(&command)?,
        vec![
            ("symbol", "SOLUSDC".to_owned()),
            ("side", "SELL".to_owned()),
            ("type", "MARKET".to_owned()),
            ("quantity", "0.15".to_owned()),
            ("positionSide", "SHORT".to_owned()),
            ("newOrderRespType", "RESULT".to_owned()),
            ("newClientOrderId", "venue_market_1".to_owned()),
        ]
    );
    Ok(())
}

#[test]
fn exposure_take_profit_market_targets_exact_hedge_leg_without_reduce_only_wire_flag()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (
            crate::domain::PositionSide::Long,
            crate::domain::OrderSide::Sell,
            "SELL",
            "LONG",
        ),
        (
            crate::domain::PositionSide::Short,
            crate::domain::OrderSide::Buy,
            "BUY",
            "SHORT",
        ),
    ];
    for (position_side, side, expected_side, expected_position_side) in cases {
        let command = MarketReduceCommand {
            command_id: crate::domain::CommandId::new("risk_reduce_1")?,
            client_order_id: crate::domain::CommandId::new("risk_reduce_client_1")?,
            owner: crate::domain::OrderOwner {
                strategy_instance_id: "hedged_grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "portfolio_margin_um".to_owned(),
                symbol: "SOL/USDC".parse()?,
                purpose: crate::domain::OrderPurpose::ExposureTakeProfit,
            },
            position_side,
            side,
            quantity: Decimal::new(12, 2),
            risk_episode_id: crate::domain::CommandId::new("risk_episode_1")?,
            position_generation: 9,
        };
        let parameters = market_reduce_parameters(&command)?;
        assert!(parameters.contains(&("side", expected_side.to_owned())));
        assert!(parameters.contains(&("positionSide", expected_position_side.to_owned())));
        assert!(parameters.contains(&("type", "MARKET".to_owned())));
        assert!(!parameters.iter().any(|(key, _)| *key == "reduceOnly"));
        assert!(!parameters.iter().any(|(key, _)| *key == "timeInForce"));
    }
    Ok(())
}

#[test]
fn stop_market_protection_uses_only_close_all_hedge_parameters()
-> Result<(), Box<dyn std::error::Error>> {
    let command = StopMarketCloseAllCommand {
        command_id: crate::domain::CommandId::new("protect_1")?,
        client_strategy_id: crate::domain::CommandId::new("venue_protect_1")?,
        owner: crate::domain::OrderOwner {
            strategy_instance_id: "scalping_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: crate::domain::OrderPurpose::Protection,
        },
        side: crate::domain::OrderSide::Sell,
        position_side: crate::domain::PositionSide::Long,
        stop_price: Price::new(Decimal::new(9, 2))?,
        position_generation: 1,
    };
    let parameters = stop_market_close_all_parameters(&command)?;
    assert!(parameters.contains(&("strategyType", "STOP_MARKET".to_owned())));
    assert!(parameters.contains(&("newClientStrategyId", "venue_protect_1".to_owned())));
    assert!(parameters.contains(&("closePosition", "true".to_owned())));
    assert!(parameters.contains(&("workingType", "MARK_PRICE".to_owned())));
    assert!(
        !parameters
            .iter()
            .any(|(key, _)| *key == "quantity" || *key == "reduceOnly")
    );
    Ok(())
}
