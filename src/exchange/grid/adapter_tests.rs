use crate::domain::{Amount, Asset, MarketKind};
use serde_json::json;

use super::*;

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT")?),
        generation: 7,
        price_tick: Price::new(Decimal::new(1, 5))?,
        quantity_step: Decimal::new(10, 0),
        minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
    })
}

#[test]
fn gate_rule_recheck_requires_every_startup_execution_field()
-> Result<(), Box<dyn std::error::Error>> {
    let startup = GateContractRules {
        native_symbol: "DOGE_USDT".to_owned(),
        instrument: instrument()?,
        quanto_multiplier: Decimal::new(10, 0),
        minimum_contracts: Decimal::ONE,
        decimal_contracts: false,
    };
    assert!(verify_gate_instrument_rules(&startup, &startup).is_ok());

    let mut changed_tick = startup.clone();
    changed_tick.instrument.price_tick = Price::new(Decimal::new(2, 5))?;
    assert!(matches!(
        verify_gate_instrument_rules(&startup, &changed_tick),
        Err(GridVenueError::InstrumentRulesDrift)
    ));

    let mut changed_minimum = startup.clone();
    changed_minimum.minimum_contracts = Decimal::new(2, 0);
    assert!(matches!(
        verify_gate_instrument_rules(&startup, &changed_minimum),
        Err(GridVenueError::InstrumentRulesDrift)
    ));

    let mut changed_contract_mode = startup.clone();
    changed_contract_mode.decimal_contracts = true;
    assert!(matches!(
        verify_gate_instrument_rules(&startup, &changed_contract_mode),
        Err(GridVenueError::InstrumentRulesDrift)
    ));
    Ok(())
}

#[test]
fn binance_rule_recheck_and_public_bridge_require_exact_durable_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let startup = BinanceContractRules {
        instrument: instrument()?,
        minimum_quantity: Decimal::new(10, 0),
    };
    assert!(verify_binance_instrument_rules(&startup, &startup).is_ok());
    let mut changed = startup.clone();
    changed.minimum_quantity = Decimal::new(20, 0);
    assert!(matches!(
        verify_binance_instrument_rules(&startup, &changed),
        Err(GridVenueError::InstrumentRulesDrift)
    ));

    let mut market = BinancePublicMarket::new("DOGE/USDT".parse()?);
    market.seed_generation(9)?;
    market.accept(GridPublicPayload::new(
        9,
        GridPublicPayloadSource::RestSnapshot,
        1_000,
        r#"{"lastUpdateId":10,"bids":[["0.10000","10"]],"asks":[["0.10100","20"]]}"#.to_owned(),
    )?)?;
    assert!(matches!(
        market.best_bid_ask(1_000),
        Err(GridVenueError::PublicNotReady)
    ));
    market.accept(GridPublicPayload::new(
        9,
        GridPublicPayloadSource::WebSocketDepth,
        1_001,
        r#"{"e":"depthUpdate","E":1001,"T":1001,"s":"DOGEUSDT","U":11,"u":11,"pu":10,"st":1,"b":[["0.10000","11"]],"a":[]}"#
            .to_owned(),
    )?)?;
    let (bid, ask) = market.best_bid_ask(1_002)?;
    assert_eq!(bid.value(), Decimal::new(10000, 5));
    assert_eq!(ask.value(), Decimal::new(10100, 5));
    assert!(matches!(
        market.accept(GridPublicPayload::new(
            8,
            GridPublicPayloadSource::WebSocketDepth,
            1_003,
            "{}".to_owned(),
        )?),
        Err(GridVenueError::PublicPayload)
    ));
    Ok(())
}

#[test]
fn binance_private_stream_trade_is_normalized_without_losing_raw_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let raw = r#"{"e":"ORDER_TRADE_UPDATE","E":1000,"T":999,"o":{"s":"SOLUSDC","c":"hgo_e1_long_open_l1","x":"TRADE","S":"BUY","ps":"LONG","t":7,"i":11,"l":"0.06","L":"97.19","m":true}}"#.to_owned();
    let GridPrivateEvent::Fill {
        fill,
        client_order_id,
        raw_payload,
    } = binance_private_event(raw.clone(), &"SOL/USDC".parse()?)?
    else {
        return Err("expected normalized Binance fill".into());
    };
    assert_eq!(raw_payload, raw);
    assert_eq!(fill.fill_id, "7");
    assert_eq!(fill.order_id, "11");
    assert_eq!(fill.maker, FieldState::Known(true));
    assert_eq!(
        client_order_id,
        FieldState::Known("hgo_e1_long_open_l1".to_owned())
    );
    Ok(())
}

#[test]
fn binance_private_non_trade_reconciles_and_expiry_is_detected()
-> Result<(), Box<dyn std::error::Error>> {
    let raw = r#"{"e":"ACCOUNT_UPDATE","E":1000}"#.to_owned();
    assert_eq!(
        binance_private_event(raw.clone(), &"SOL/USDC".parse()?)?,
        GridPrivateEvent::Reconcile { raw_payload: raw }
    );
    assert!(binance_private_stream_expired(
        r#"{"e":"listenKeyExpired","listenKey":"[redacted]"}"#
    ));
    Ok(())
}

#[test]
fn binance_canary_capability_is_bound_to_pm_account_symbol_and_api_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binance_capability_binding(&"SOL/USDC".parse()?, "a".repeat(64));
    assert_eq!(binding.exchange, "binance");
    assert_eq!(binding.account_binding, "portfolio_margin_um");
    assert_eq!(binding.symbol, "SOL/USDC");
    assert_eq!(binding.api_key_sha256, "a".repeat(64));
    binding.validate()?;
    Ok(())
}

#[test]
fn binance_mutation_response_returns_only_the_exact_native_order_id() {
    assert_eq!(
        binance_accepted_order_id(
            r#"{"orderId":13677753681,"clientOrderId":"hgo_e961_long_close_l1"}"#,
            "hgo_e961_long_close_l1",
        )
        .unwrap(),
        "13677753681"
    );
    assert!(
        binance_accepted_order_id(
            r#"{"orderId":"13677753681","clientOrderId":"other"}"#,
            "hgo_e961_long_close_l1",
        )
        .is_err()
    );
}

#[test]
fn binance_current_profile_requires_independent_signed_algo_page()
-> Result<(), Box<dyn std::error::Error>> {
    let profile = GridOrderFamilyReadback::regular_and_algo_adapter_profile(
        Vec::new(),
        vec!["[]".to_owned()],
        Vec::new(),
        vec!["[]".to_owned()],
    )?;
    assert!(matches!(
        profile.snapshot(NativeOrderFamily::UmConditional),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    assert!(profile.open_orders_are_empty()?);
    assert!(matches!(
        GridOrderFamilyReadback::regular_and_algo_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
            Vec::new(),
            Vec::new(),
        ),
        Err(GridVenueError::PrivateReadbackIncomplete)
    ));
    Ok(())
}

#[test]
fn bitget_rule_recheck_requires_normalized_rules_and_minimum_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let mut normalized = instrument()?;
    normalized.minimum_notional = Amount::new(Asset::new("USDT")?, Decimal::new(5, 0));
    let startup = BitgetContractRules {
        native_symbol: "DOGEUSDT".to_owned(),
        instrument: normalized,
        minimum_quantity: Decimal::new(10, 0),
        minimum_notional: Decimal::new(5, 0),
    };
    assert!(verify_bitget_instrument_rules(&startup, &startup).is_ok());

    let mut changed_step = startup.clone();
    changed_step.instrument.quantity_step = Decimal::new(20, 0);
    assert!(matches!(
        verify_bitget_instrument_rules(&startup, &changed_step),
        Err(GridVenueError::InstrumentRulesDrift)
    ));

    let mut changed_minimum = startup.clone();
    changed_minimum.minimum_quantity = Decimal::new(20, 0);
    assert!(matches!(
        verify_bitget_instrument_rules(&startup, &changed_minimum),
        Err(GridVenueError::InstrumentRulesDrift)
    ));
    Ok(())
}

#[test]
fn bitget_grid_readback_binds_the_signed_regular_page_to_the_stage7_profile()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let order = crate::exchange::bitget::parse_order(
        &json!({
            "orderId":"1", "clientOid":"hgo_e1_long_open_l1",
            "category":"USDT-FUTURES", "symbol":"DOGEUSDT", "orderStatus":"live",
            "side":"buy", "posSide":"long", "holdMode":"hedge_mode", "reduceOnly":"NO",
            "qty":"10", "cumExecQty":"0",
            "price":"0.1", "avgPrice":"0"
        }),
        &symbol,
    )?;
    let signed_regular_page = "[{\"orderId\":\"1\"}]".to_owned();
    let readback = bitget_grid_readback(crate::exchange::bitget::BitgetPrivateReadback {
        raw_payloads: vec!["{\"account\":true}".to_owned(), signed_regular_page.clone()],
        signed_regular_order_payloads: vec![signed_regular_page],
        balance: AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::ONE,
            available_balance: Decimal::ONE,
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: vec![order.clone()],
        fills: Vec::new(),
    })?;
    readback.validate_order_family_readback()?;
    assert!(matches!(
        readback
            .order_family_readback
            .as_ref()
            .and_then(|families| families.snapshot(NativeOrderFamily::UmOrder)),
        Some(GridOrderFamilySnapshot::Complete { orders, signed_payloads })
            if orders == &vec![order] && signed_payloads.len() == 1
    ));
    assert!(matches!(
        readback
            .order_family_readback
            .as_ref()
            .and_then(|families| families.snapshot(NativeOrderFamily::UmConditional)),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    assert!(matches!(
        readback
            .order_family_readback
            .as_ref()
            .and_then(|families| families.snapshot(NativeOrderFamily::UmAlgo)),
        Some(GridOrderFamilySnapshot::ExplicitlyUnsupported)
    ));
    Ok(())
}

#[test]
fn bitget_reconnect_keeps_snapshot_and_following_trade_in_one_transport_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let mut market = BitgetPublicMarket::new(symbol);
    market.seed_generation(445)?;
    market.accept(GridPublicPayload::new(
        445,
        GridPublicPayloadSource::WebSocketDepth,
        1_000,
        r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"snapshot","ts":"1001","data":[{"a":[["0.101","20"]],"b":[["0.100","10"]],"pseq":"0","seq":"100","maxdepth":"50","ts":"1000"}]}"#.to_owned(),
    )?)?;

    market.reset();
    assert_eq!(market.generation(), 446);
    market.accept(GridPublicPayload::new(
        446,
        GridPublicPayloadSource::WebSocketDepth,
        2_000,
        r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"snapshot","ts":"2001","data":[{"a":[["0.101","20"]],"b":[["0.100","10"]],"pseq":"0","seq":"200","maxdepth":"50","ts":"2000"}]}"#.to_owned(),
    )?)?;
    market.accept(GridPublicPayload::new(
        446,
        GridPublicPayloadSource::WebSocketTrade,
        2_001,
        r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"DOGEUSDT"},"data":[{"i":"1475707793211920384","p":"0.09079","v":"2714","S":"buy","T":"1787562075903","L":"1475707793211920385","isRPI":"no"}],"ts":1787562075904}"#.to_owned(),
    )?)?;
    assert_eq!(market.generation(), 446);
    Ok(())
}
