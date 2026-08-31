use crate::domain::{Amount, Asset, MarketKind};

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
fn gate_queued_depth_uses_exchange_time_not_fresh_local_drain_time()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = GateContractRules {
        native_symbol: "DOGE_USDT".to_owned(),
        instrument: instrument()?,
        quanto_multiplier: Decimal::new(10, 0),
        minimum_contracts: Decimal::ONE,
        maximum_contracts: None,
        decimal_contracts: false,
    };
    let mut market = GatePublicMarket::new(&rules)?;
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::WebSocketDepth,
        100_000,
        r#"{"time_ms":1000,"channel":"futures.order_book_update","event":"update","result":{"t":1001,"s":"DOGE_USDT","U":101,"u":101,"b":[{"p":"0.100","s":"2"}],"a":[{"p":"0.101","s":"3"}]}}"#.to_owned(),
    )?)?;
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::RestSnapshot,
        100_000,
        r#"{"id":100,"current":1.000,"update":1.000,"bids":[{"p":"0.100","s":"2"}],"asks":[{"p":"0.101","s":"3"}]}"#.to_owned(),
    )?)?;
    assert!(matches!(
        market.best_bid_ask(100_000),
        Err(GridVenueError::PublicNotReady)
    ));
    Ok(())
}

#[test]
fn binance_queued_depth_uses_exchange_time_not_fresh_local_drain_time()
-> Result<(), Box<dyn std::error::Error>> {
    let mut market = BinancePublicMarket::new("DOGE/USDT".parse()?);
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::RestSnapshot,
        100_000,
        r#"{"lastUpdateId":10,"bids":[["0.10000","10"]],"asks":[["0.10100","20"]]}"#.to_owned(),
    )?)?;
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::WebSocketDepth,
        100_000,
        r#"{"e":"depthUpdate","E":1001,"T":1001,"s":"DOGEUSDT","U":11,"u":11,"pu":10,"st":1,"b":[["0.10000","11"]],"a":[]}"#
            .to_owned(),
    )?)?;
    assert!(matches!(
        market.best_bid_ask(100_000),
        Err(GridVenueError::PublicNotReady)
    ));
    Ok(())
}

#[test]
fn bitget_queued_depth_uses_exchange_time_not_fresh_local_drain_time()
-> Result<(), Box<dyn std::error::Error>> {
    let mut market = BitgetPublicMarket::new("DOGE/USDT".parse()?);
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::WebSocketDepth,
        100_000,
        r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"snapshot","ts":"1001","data":[{"a":[["0.101","20"]],"b":[["0.100","10"]],"pseq":"0","seq":"100","maxdepth":"50","ts":"1000"}]}"#.to_owned(),
    )?)?;
    market.accept(GridPublicPayload::new(
        1,
        GridPublicPayloadSource::WebSocketDepth,
        100_000,
        r#"{"arg":{"instType":"usdt-futures","topic":"books","symbol":"DOGEUSDT"},"action":"update","ts":"1002","data":[{"a":[],"b":[["0.100","11"]],"pseq":"100","seq":"101","maxdepth":"50","ts":"1002"}]}"#.to_owned(),
    )?)?;
    assert!(matches!(
        market.best_bid_ask(100_000),
        Err(GridVenueError::PublicNotReady)
    ));
    Ok(())
}
