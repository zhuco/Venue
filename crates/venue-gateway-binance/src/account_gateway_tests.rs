use bytes::Bytes;

use super::account_gateway_limit::normalize_priced_limit;
use super::account_gateway_symbol_dispatch::uses_refreshed_anchor_private;
use super::*;
use crate::BinanceRawPrivateFrame;

const EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
const MIXED_EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_mixed_quotes.json");
const USDT_ASSET_INDEX: &str = include_str!("../tests/fixtures/asset-index-usdt.json");
const USDC_ASSET_INDEX: &str = include_str!("../tests/fixtures/asset-index-usdc.json");
const LIMIT_BOOK: &[u8] = include_bytes!("../tests/fixtures/limit-book-ticker.json");

#[test]
fn signed_snapshot_collection_retries_only_the_read_and_stays_bounded() {
    let mut calls = 0_u8;
    let mut delays = Vec::new();
    let recovered = retry_signed_snapshot_collection(
        || {
            calls = calls.saturating_add(1);
            if calls < 3 { Err("transient") } else { Ok(7) }
        },
        |delay| delays.push(delay),
    );
    assert_eq!(recovered, Ok(7));
    assert_eq!(calls, 3);
    assert_eq!(
        delays,
        vec![Duration::from_millis(250), Duration::from_millis(500)]
    );

    let mut failed_calls = 0_u8;
    let failed: Result<(), &str> = retry_signed_snapshot_collection(
        || {
            failed_calls = failed_calls.saturating_add(1);
            Err("persistent")
        },
        |_| {},
    );
    assert_eq!(failed, Err("persistent"));
    assert_eq!(failed_calls, 3);
}

#[test]
fn dispatch_reuses_only_the_fresh_anchor_private_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, _, binding) = limit_fixture()?;
    assert!(uses_refreshed_anchor_private(&binding, &binding.symbol));
    assert!(!uses_refreshed_anchor_private(
        &binding,
        &"ETH/USDC".parse()?
    ));
    Ok(())
}

#[test]
fn signed_snapshot_balance_accepts_the_portfolio_account_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let balances = snapshot_balances(include_bytes!("../fixtures/portfolio-account.json"))?;
    assert_eq!(balances.len(), 1);
    assert_eq!(balances[0].asset.as_str(), "USD");
    assert_eq!(balances[0].equity, Decimal::new(1000, 0));
    assert_eq!(balances[0].available_margin, Some(Decimal::new(900, 0)));
    Ok(())
}

#[test]
fn signed_snapshot_balance_rejects_an_asset_array_from_another_account_surface() {
    assert!(snapshot_balances(br#"{"assets":[]}"#).is_err());
}

fn limit_fixture() -> Result<
    (
        AccountLimitNormalizationIntent,
        BinanceInstrumentRules,
        GatewayBinding,
    ),
    Box<dyn std::error::Error>,
> {
    use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose};
    use venue_gateway_api::{GatewayMode, VenueId};
    let symbol = "BTC/USDT".parse()?;
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        symbol,
    )?;
    let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), 7)?;
    let intent = AccountLimitNormalizationIntent {
        command_id: CommandId::new("limit-fixture-command")?,
        client_order_id: CommandId::new("limit-fixture-client")?,
        owner: OrderOwner {
            strategy_instance_id: "copy-test".to_owned(),
            run_id: "run-test".to_owned(),
            exchange: "binance".to_owned(),
            account: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quote_delta: Decimal::new(10, 0),
        reduce_only: false,
    };
    Ok((intent, rules, binding))
}

#[test]
fn limit_normalization_preserves_identity_and_never_rounds_up_quote()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut intent, rules, binding) = limit_fixture()?;
    let ExecutionCommand::PlaceLimit(buy) =
        normalize_fresh_limit(&intent, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(buy.command_id, intent.command_id);
    assert_eq!(buy.client_order_id, intent.client_order_id);
    assert_eq!(buy.owner, intent.owner);
    assert_eq!(buy.limit_price.value(), Decimal::new(5000, 0));
    assert_eq!(buy.quantity, Decimal::new(2, 3));
    intent.side = OrderSide::Sell;
    intent.position_side = PositionSide::Short;
    let ExecutionCommand::PlaceLimit(sell) =
        normalize_fresh_limit(&intent, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(sell.limit_price.value(), Decimal::new(50001, 1));
    assert_eq!(sell.quantity, Decimal::new(1, 3));
    assert!(sell.quantity * sell.limit_price.value() <= intent.quote_delta);
    Ok(())
}

#[test]
fn priced_limit_uses_explicit_gtc_price_and_never_fetches_bbo()
-> Result<(), Box<dyn std::error::Error>> {
    let (intent, rules, binding) = limit_fixture()?;
    let priced = AccountPricedLimitIntent {
        intent,
        limit_price: Price::new(Decimal::new(5000, 0))?,
        time_in_force: LimitTimeInForce::Gtc,
        maximum_quantity: Some(Decimal::new(15, 4)),
    };
    let ExecutionCommand::PlaceLimit(command) = normalize_priced_limit(&priced, &rules, &binding)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(command.limit_price, priced.limit_price);
    assert_eq!(command.time_in_force, LimitTimeInForce::Gtc);
    assert_eq!(command.quantity, Decimal::new(1, 3));
    assert!(command.quantity * command.limit_price.value() <= priced.intent.quote_delta);

    let mut unaligned = priced.clone();
    unaligned.limit_price = Price::new(Decimal::new(50001, 2))?;
    assert_eq!(
        normalize_priced_limit(&unaligned, &rules, &binding),
        Err(AccountHostValidationError::Command)
    );
    Ok(())
}

#[test]
fn one_catalogue_binds_two_canonical_symbols_without_cross_route()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, _, binding) = limit_fixture()?;
    let btc: Symbol = "BTC/USDT".parse()?;
    let sol: Symbol = "SOL/USDC".parse()?;
    let catalogue = parse_rules_catalog(
        MIXED_EXCHANGE_INFO,
        &binding,
        &BTreeSet::from([btc.clone(), sol.clone()]),
        17,
    )?;
    assert_eq!(catalogue.len(), 2);
    assert_eq!(catalogue[&btc].instrument.symbol, btc);
    assert_eq!(catalogue[&sol].instrument.symbol, sol);
    assert_ne!(
        catalogue[&"BTC/USDT".parse()?].native_symbol,
        catalogue[&"SOL/USDC".parse()?].native_symbol
    );
    assert!(
        parse_rules_catalog(
            MIXED_EXCHANGE_INFO,
            &binding,
            &BTreeSet::from(["SOL/USDC".parse()?]),
            17,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn limit_normalization_rejects_stale_future_wrong_symbol_empty_and_crossed_book()
-> Result<(), Box<dyn std::error::Error>> {
    let (intent, rules, binding) = limit_fixture()?;
    for now in [1_720_000_000_099, 1_720_000_003_101] {
        assert!(normalize_fresh_limit(&intent, &rules, &binding, LIMIT_BOOK, now).is_err());
    }
    for (field, value) in [
        ("symbol", Value::from("ETHUSDT")),
        ("time", Value::Null),
        ("bidQty", Value::from("0")),
        ("askPrice", Value::from("5000.00")),
        ("bidPrice", Value::from("5000.01")),
        ("askQty", Value::from("NaN")),
    ] {
        let mut book: Value = serde_json::from_slice(LIMIT_BOOK)?;
        book[field] = value;
        assert!(
            normalize_fresh_limit(
                &intent,
                &rules,
                &binding,
                &serde_json::to_vec(&book)?,
                1_720_000_000_200
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn limit_normalization_rejects_wrong_owner_direction_and_quantity_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let (intent, rules, binding) = limit_fixture()?;
    let mut altered = intent.clone();
    altered.owner.account = "other-account".to_owned();
    assert!(
        normalize_fresh_limit(&altered, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200).is_err()
    );
    altered = intent.clone();
    altered.side = OrderSide::Sell;
    assert!(
        normalize_fresh_limit(&altered, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200).is_err()
    );
    altered = intent.clone();
    altered.quote_delta = Decimal::new(4, 0);
    assert!(
        normalize_fresh_limit(&altered, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200).is_err()
    );
    altered = intent;
    altered.quote_delta = Decimal::new(10_000_000, 0);
    assert!(
        normalize_fresh_limit(&altered, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200).is_err()
    );
    Ok(())
}

#[test]
fn private_stream_fill_keeps_socket_generation_and_admits_against_current_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, rules, binding) = limit_fixture()?;
    let frame = BinanceRawPrivateFrame {
            binding: binding.clone(),
            instrument_generation: rules.instrument.generation,
            private_generation: 9,
            received_at_ms: 1_720_000_000_100,
            payload: Bytes::from_static(
                br#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"BTCUSDT","c":"grid-e1-long-open-l1","x":"TRADE","X":"FILLED","S":"BUY","ps":"LONG","t":7,"i":11,"q":"0.002","l":"0.002","z":"0.002","L":"50000","m":true}}"#,
            ),
        };
    let event = normalize_private_stream_event(
        frame.clone(),
        &binding,
        rules.instrument.generation,
        9,
        10,
    )?
    .ok_or("expected fill")?;
    let BinancePrivateAccountEvent::Fill(event) = event else {
        return Err("expected normalized fill event".into());
    };
    assert_eq!(event.stream_private_generation, 9);
    assert_eq!(event.private_generation, 10);
    assert_eq!(event.fill.order_id, "11");
    assert_eq!(event.native_order_id(), "11");
    assert_eq!(
        event.complete_order_progress(),
        Some((Decimal::new(2, 3), Decimal::new(2, 3), OrderState::Filled))
    );
    assert!(
        matches!(event.client_order_id, FieldState::Known(ref id) if id == "grid-e1-long-open-l1")
    );
    assert!(
        normalize_private_stream_event(
            frame.clone(),
            &binding,
            rules.instrument.generation,
            10,
            10
        )
        .is_err()
    );
    assert!(
        normalize_private_stream_event(frame, &binding, rules.instrument.generation, 9, 8).is_err()
    );
    Ok(())
}

#[test]
fn private_stream_admits_each_enabled_kol_symbol_without_retaining_the_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, rules, binding) = limit_fixture()?;
    let eth: Symbol = "ETH/USDT".parse()?;
    let symbols = BTreeSet::from([binding.symbol.clone(), eth.clone()]);
    let event = normalize_private_stream_event_for_symbols(
        BinanceRawPrivateFrame {
            binding: binding.clone(),
            instrument_generation: rules.instrument.generation,
            private_generation: 9,
            received_at_ms: 1_720_000_000_100,
            payload: Bytes::from_static(
                br#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"ETHUSDT","c":"kol-eth-1","x":"TRADE","X":"PARTIALLY_FILLED","S":"SELL","ps":"SHORT","t":8,"i":12,"q":"0.05","l":"0.02","z":"0.02","L":"3000","m":false}}"#,
            ),
        },
        &binding,
        &symbols,
        rules.instrument.generation,
        9,
        10,
    )?
    .ok_or("expected fill")?;
    let BinancePrivateAccountEvent::Fill(event) = event else {
        return Err("expected normalized fill event".into());
    };
    assert_eq!(event.fill.symbol, eth);
    assert_eq!(event.fill.fill_id, "8");
    Ok(())
}

#[test]
fn private_stream_terminal_order_and_expiry_request_signed_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, rules, binding) = limit_fixture()?;
    let terminal = BinanceRawPrivateFrame {
        binding: binding.clone(),
        instrument_generation: rules.instrument.generation,
        private_generation: 9,
        received_at_ms: 1_720_000_000_100,
        payload: Bytes::from_static(
            br#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"BTCUSDT","c":"grid-e1-long-open-l1","x":"CANCELED","S":"BUY","ps":"LONG","i":11}}"#,
        ),
    };
    assert!(matches!(
        normalize_private_stream_event(terminal, &binding, rules.instrument.generation, 9, 10)?,
        Some(BinancePrivateAccountEvent::ReconcileRequired {
            stream_private_generation: 9,
            private_generation: 10,
            ..
        })
    ));
    for payload in [
        br#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"BTCUSDT","c":"grid-e1-long-open-l1","x":"AMENDMENT","X":"NEW","S":"BUY","ps":"LONG","i":11}}"#.as_slice(),
        br#"{"e":"ORDER_TRADE_UPDATE","E":1000,"o":{"s":"BTCUSDT","c":"grid-e1-long-open-l1","x":"CALCULATED","X":"FILLED","S":"SELL","ps":"LONG","i":11}}"#.as_slice(),
        br#"{"e":"ACCOUNT_CONFIG_UPDATE","E":1000,"T":999,"ac":{"s":"BTCUSDT","l":10}}"#.as_slice(),
    ] {
        assert!(matches!(
            normalize_private_stream_event(
                BinanceRawPrivateFrame {
                    binding: binding.clone(),
                    instrument_generation: rules.instrument.generation,
                    private_generation: 9,
                    received_at_ms: 1_720_000_000_100,
                    payload: Bytes::copy_from_slice(payload),
                },
                &binding,
                rules.instrument.generation,
                9,
                10,
            )?,
            Some(BinancePrivateAccountEvent::ReconcileRequired { .. })
        ));
    }
    let expired = BinanceRawPrivateFrame {
        binding: binding.clone(),
        instrument_generation: rules.instrument.generation,
        private_generation: 9,
        received_at_ms: 1_720_000_000_101,
        payload: Bytes::from_static(
            br#"{"e":"listenKeyExpired","E":1001,"listenKey":"[redacted]"}"#,
        ),
    };
    assert!(
        normalize_private_stream_event(expired, &binding, rules.instrument.generation, 9, 10)
            .is_err()
    );
    Ok(())
}

#[test]
fn private_stream_reconnect_backoff_is_bounded_exponential_and_staggered() {
    let first = private_stream_reconnect_delay(10, 1, 1);
    let second = private_stream_reconnect_delay(10, 1, 2);
    assert!(first >= Duration::from_secs(1));
    assert!(second >= Duration::from_secs(2));
    assert!(second > first);
    assert_ne!(first, private_stream_reconnect_delay(11, 1, 1));
    assert!(private_stream_reconnect_delay(10, 1, u32::MAX) <= PRIVATE_STREAM_MAX_RECONNECT_DELAY);
}

#[test]
fn one_private_stream_outage_reports_once_until_a_valid_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let now = Instant::now();
    let mut state = PrivateStreamReconnectState::default();
    assert!(state.record_failure(now, 10, 1));
    assert!(state.waiting(now));
    let retry_at = state.retry_deadline().ok_or("retry deadline")?;
    assert!(!state.waiting(retry_at));
    assert!(!state.record_failure(retry_at, 10, 2));
    state.record_connected();
    assert!(state.record_failure(retry_at, 10, 3));
    state.record_valid_frame();
    assert!(!state.waiting(retry_at));
    assert!(state.record_failure(retry_at, 10, 4));
    Ok(())
}

#[test]
fn public_stream_normalizes_only_bound_supported_market_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let (_, rules, binding) = limit_fixture()?;
    let frame = BinanceRawPublicFrame {
            binding: binding.clone(),
            instrument_generation: rules.instrument.generation,
            received_at_ms: 1_100,
            payload: Bytes::from_static(
                br#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1000,"T":999,"s":"BTCUSDT","a":7,"f":10,"l":11,"p":"100","q":"2","m":false}}"#,
            ),
        };
    let event =
        normalize_public_stream_event(frame.clone(), &binding, rules.instrument.generation)?
            .ok_or("expected public trade")?;
    assert!(
        matches!(event, BinancePublicMarketEvent::Trade(value) if value.last_trade_id == Some(11))
    );
    assert!(
        normalize_public_stream_event(
            BinanceRawPublicFrame {
                instrument_generation: rules.instrument.generation + 1,
                ..frame
            },
            &binding,
            rules.instrument.generation,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn private_snapshot_generation_advances_only_as_a_new_collection_candidate() {
    // Callers install this candidate only after the complete signed collection returns Ok;
    // rules and connection generation intentionally have no such per-collection counter.
    assert!(matches!(next_private_generation(41), Ok(42)));
    assert!(next_private_generation(u64::MAX).is_err());
}

#[test]
fn full_account_risk_rejects_unpriced_opening_algo_orders() {
    assert!(
        account_entry_order_notionals(
            EXCHANGE_INFO,
            br#"[]"#,
            br#"[{"reduceOnly":false,"closePosition":false}]"#,
            7,
        )
        .is_err()
    );
}

#[test]
fn full_account_risk_reserves_remaining_regular_order_notional()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(account_rules(EXCHANGE_INFO, "BTCUSDT", 7).is_ok());
    let values = account_entry_order_notionals(
            EXCHANGE_INFO,
            br#"[{"symbol":"BTCUSDT","reduceOnly":false,"origQty":"0.002","executedQty":"0","price":"50000"}]"#,
            br#"[]"#,
            7,
        );
    assert_eq!(
        values,
        Ok(vec![AccountRiskAmount {
            asset: "USDT".parse()?,
            value: Decimal::new(100, 0),
        }])
    );
    Ok(())
}

#[test]
fn full_account_risk_uses_each_symbol_quote_and_a_non_parity_usdc_rate()
-> Result<(), Box<dyn std::error::Error>> {
    let positions = account_position_notionals(
        MIXED_EXCHANGE_INFO,
        br#"[
                {"symbol":"BTCUSDT","positionAmt":"0.001","markPrice":"50000","notional":"50"},
                {"symbol":"SOLUSDC","positionAmt":"1","markPrice":"100","notional":"100"}
            ]"#,
        7,
    )?;
    let orders = account_entry_order_notionals(
            MIXED_EXCHANGE_INFO,
            br#"[
                {"symbol":"BTCUSDT","reduceOnly":false,"origQty":"0.001","executedQty":"0","price":"50000"},
                {"symbol":"SOLUSDC","reduceOnly":false,"origQty":"0.1","executedQty":"0","price":"100"}
            ]"#,
            br#"[]"#,
            7,
        )?;
    assert_eq!(positions[0].asset, "USDT".parse()?);
    assert_eq!(positions[1].asset, "USDC".parse()?);
    assert_eq!(orders[0].asset, "USDT".parse()?);
    assert_eq!(orders[1].asset, "USDC".parse()?);

    let usdt: Asset = "USDT".parse()?;
    let usdc: Asset = "USDC".parse()?;
    let mut usd_per_asset = BTreeMap::new();
    usd_per_asset.insert(
        usdt.clone(),
        crate::portfolio::parse_usd_conversion_evidence(
            USDT_ASSET_INDEX,
            usdt.clone(),
            7,
            1_720_000_000_050,
            60_000,
        )?,
    );
    usd_per_asset.insert(
        usdc.clone(),
        crate::portfolio::parse_usd_conversion_evidence(
            USDC_ASSET_INDEX,
            usdc.clone(),
            7,
            1_720_000_000_050,
            60_000,
        )?,
    );
    let quote_assets = positions
        .iter()
        .chain(orders.iter())
        .map(|amount| amount.asset.clone())
        .collect::<BTreeSet<_>>();
    let rates = quote_to_usdt_rates(&quote_assets, &usd_per_asset, 7)?;
    assert_eq!(rates.len(), 1);
    assert_eq!(rates[0].asset, usdc);
    assert_eq!(
        rates[0].usdt_per_asset,
        Decimal::new(1_003, 3)
            .checked_div(Decimal::new(998, 3))
            .ok_or("decimal division")?
    );
    assert_ne!(rates[0].usdt_per_asset, Decimal::ONE);
    Ok(())
}

#[test]
fn full_account_risk_rejects_missing_or_stale_quote_conversion_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let usdt: Asset = "USDT".parse()?;
    let usdc: Asset = "USDC".parse()?;
    assert!(
        crate::portfolio::parse_usd_conversion_evidence(
            USDC_ASSET_INDEX,
            usdc.clone(),
            7,
            1_720_000_060_001,
            60_000,
        )
        .is_err()
    );
    let quote_assets = BTreeSet::from([usdt.clone(), usdc]);
    let mut only_usdt = BTreeMap::new();
    only_usdt.insert(
        usdt.clone(),
        crate::portfolio::parse_usd_conversion_evidence(
            USDT_ASSET_INDEX,
            usdt,
            7,
            1_720_000_000_050,
            60_000,
        )?,
    );
    assert!(quote_to_usdt_rates(&quote_assets, &only_usdt, 7).is_err());
    Ok(())
}

#[test]
fn signed_snapshot_rejects_ambiguous_open_order_ceiling() {
    let row = serde_json::Map::new();
    let rows = vec![row; ACCOUNT_WIDE_OPEN_ORDER_ROW_LIMIT];
    assert!(!account_wide_order_rows_are_complete(&rows, &[]));
    assert!(!account_wide_order_rows_are_complete(&[], &rows));
}

#[test]
fn signed_snapshot_policy_preserves_missing_and_unrepresented_values() {
    let mut row = serde_json::Map::new();
    assert_eq!(snapshot_limit_time_in_force(&row, "timeInForce"), Ok(None));

    row.insert("timeInForce".to_owned(), Value::String("IOC".to_owned()));
    assert_eq!(snapshot_limit_time_in_force(&row, "timeInForce"), Ok(None));

    row.insert("timeInForce".to_owned(), Value::String("GTX".to_owned()));
    assert_eq!(
        snapshot_limit_time_in_force(&row, "timeInForce"),
        Ok(Some(LimitTimeInForce::PostOnly))
    );

    row.insert("timeInForce".to_owned(), Value::Bool(true));
    assert!(snapshot_limit_time_in_force(&row, "timeInForce").is_err());
}

#[test]
fn signed_snapshot_partial_regular_order_keeps_original_quantity_and_filled_amount()
-> Result<(), Box<dyn std::error::Error>> {
    let regular = json_rows_snapshot(
        br#"[
            {"symbol":"BTCUSDT","orderId":"501","clientOrderId":"partial-regular-1","status":"PARTIALLY_FILLED","side":"BUY","positionSide":"LONG","timeInForce":"GTX","origQty":"0.002","executedQty":"0.0005","price":"50000","reduceOnly":false}
        ]"#,
    )?;
    let facts = snapshot_order_facts(EXCHANGE_INFO, &regular, &[], 7)?;
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].quantity, Decimal::new(2, 3));
    assert_eq!(facts[0].filled_quantity, Some(Decimal::new(5, 4)));
    assert_eq!(facts[0].state, Some(OrderState::PartiallyFilled));
    Ok(())
}

#[test]
fn signed_snapshot_uses_order_creation_time_not_update_time() {
    let mut row = serde_json::Map::new();
    row.insert("time".to_owned(), Value::from(123_u64));
    row.insert("updateTime".to_owned(), Value::from(999_u64));
    assert_eq!(snapshot_created_at_ms(&row, "time"), Ok(Some(123)));

    row.remove("time");
    assert_eq!(snapshot_created_at_ms(&row, "time"), Ok(None));
}

#[test]
fn unknown_limit_readback_requires_the_original_policy() -> Result<(), Box<dyn std::error::Error>> {
    let (intent, rules, binding) = limit_fixture()?;
    let ExecutionCommand::PlaceLimit(mut place) =
        normalize_fresh_limit(&intent, &rules, &binding, LIMIT_BOOK, 1_720_000_000_200)?
    else {
        return Err("limit required".into());
    };
    place.time_in_force = LimitTimeInForce::Gtc;
    let command = ExecutionCommand::PlaceLimit(place);
    let matching = r#"{"symbol":"BTCUSDT","clientOrderId":"limit-fixture-client","orderId":"7","status":"NEW","side":"BUY","positionSide":"LONG","timeInForce":"GTC","origQty":"0.002","executedQty":"0","price":"5000","reduceOnly":false}"#;
    assert!(matches!(
        snapshot_exact_regular_result(matching.as_bytes(), &command, "limit-fixture-client"),
        SignedUnknownResult::Accepted { .. }
    ));
    let wrong = matching.replace("GTC", "GTX");
    assert!(matches!(
        snapshot_exact_regular_result(wrong.as_bytes(), &command, "limit-fixture-client"),
        SignedUnknownResult::Unknown
    ));
    let missing = br#"{"clientOrderId":"limit-fixture-client","orderId":"7","status":"NEW"}"#;
    assert!(matches!(
        snapshot_exact_regular_result(missing, &command, "limit-fixture-client"),
        SignedUnknownResult::Unknown
    ));
    let wrong_quantity = matching.replace("\"origQty\":\"0.002\"", "\"origQty\":\"0.003\"");
    assert!(matches!(
        snapshot_exact_regular_result(wrong_quantity.as_bytes(), &command, "limit-fixture-client"),
        SignedUnknownResult::Unknown
    ));
    Ok(())
}

#[test]
fn signed_snapshot_rejects_unrepresentable_close_all_algo() -> Result<(), Box<dyn std::error::Error>>
{
    let regular = json_rows_snapshot(br#"[]"#)?;
    let algo = json_rows_snapshot(
            br#"[{"symbol":"BTCUSDT","clientAlgoId":"algo-1","algoId":"1","closePosition":true,"quantity":"0","side":"SELL","positionSide":"LONG","triggerPrice":"50000","reduceOnly":true}]"#,
        )?;
    assert!(snapshot_order_facts(EXCHANGE_INFO, &regular, &algo, 7).is_err());
    Ok(())
}

#[test]
fn signed_snapshot_fills_reject_duplicate_or_out_of_window_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let mut cursor = RecentFillsCursor {
        observed_through_ms: 10,
        last_trade_id: Some(7),
        last_event_time_ms: Some(11),
    };
    let duplicate = json_rows_snapshot(br#"[{"id":"7","time":"12"}]"#)?;
    assert!(advance_snapshot_fill_cursor(&mut cursor, &duplicate, 10).is_err());
    let outside = json_rows_snapshot(br#"[{"id":"8","time":"9"}]"#)?;
    assert!(advance_snapshot_fill_cursor(&mut cursor, &outside, 10).is_err());
    Ok(())
}

#[test]
fn signed_snapshot_fills_cursor_keeps_symbol_watermarks_and_rejects_sha_legacy() {
    assert!(matches!(
        parse_snapshot_fills_cursor(Some(
            "binance-fills-v1|BTCUSDT,100,7,100;DOGEUSDT,100,,"
        )),
        Ok(cursor) if cursor.by_native_symbol.len() == 2
    ));
    assert!(
        parse_snapshot_fills_cursor(Some(
            "4983f3d75db0d72aeb1e68c57d9f171d981edc3ef31b8ca16c4d5f1caa26dce5",
        ))
        .is_err()
    );
}
