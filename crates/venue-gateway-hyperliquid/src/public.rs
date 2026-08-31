//! Pure Hyperliquid public trade and candle normalization.

use rust_decimal::Decimal;
use serde::Deserialize;
use venue_domain::domain::{
    AggressorSide, FieldState, Price, PublicTrade, PublicTradeId, PublicTradeOrdering,
};
use venue_gateway_api::{GatewayBinding, VenueId};

use crate::HyperliquidError;

pub const HYPERLIQUID_PUBLIC_MAX_FACTS_PER_FRAME: usize = 1_024;
const ONE_MINUTE_MS: u64 = 60_000;

/// Parses one bounded `trades` message. Hyperliquid documents `tid` as only a 50-bit hash; the
/// opaque `(time, coin, tid)` tuple is therefore retained without inventing a numeric sequence.
pub fn parse_public_trades(
    payload: &str,
    binding: &GatewayBinding,
    native_coin: &str,
    generation: u64,
    received_at_ms: u64,
) -> Result<Vec<PublicTrade>, HyperliquidError> {
    if binding.venue != VenueId::Hyperliquid
        || binding.symbol.quote() != "USDC"
        || generation == 0
        || received_at_ms == 0
        || native_coin.is_empty()
    {
        return Err(HyperliquidError::Payload);
    }
    let envelope: TradesEnvelope =
        serde_json::from_str(payload).map_err(|_| HyperliquidError::Payload)?;
    if envelope.channel != "trades"
        || envelope.data.is_empty()
        || envelope.data.len() > HYPERLIQUID_PUBLIC_MAX_FACTS_PER_FRAME
    {
        return Err(HyperliquidError::Payload);
    }
    envelope
        .data
        .into_iter()
        .map(|row| {
            if row.coin != native_coin || row.time == 0 || row.tid == 0 {
                return Err(HyperliquidError::Binding);
            }
            let price = price(&row.px)?;
            let quantity = positive_decimal(&row.sz)?;
            let quote_quantity = quantity
                .checked_mul(price.value())
                .ok_or(HyperliquidError::Payload)?;
            let aggressor = match row.side.as_str() {
                "B" => AggressorSide::Buy,
                "A" => AggressorSide::Sell,
                _ => return Err(HyperliquidError::Payload),
            };
            let aggregate_trade_id =
                PublicTradeId::Opaque(format!("{}:{}:{}", row.time, row.coin, row.tid));
            if !aggregate_trade_id.is_valid() {
                return Err(HyperliquidError::Payload);
            }
            let fact = PublicTrade {
                symbol: binding.symbol.clone(),
                generation,
                received_at_ms,
                exchange_time_ms: row.time,
                transaction_time_ms: row.time,
                aggregate_trade_id,
                first_trade_id: None,
                last_trade_id: None,
                ordering: PublicTradeOrdering::Unsequenced,
                price,
                quantity,
                quote_quantity,
                aggressor: FieldState::Known(aggressor),
            };
            fact.is_valid()
                .then_some(fact)
                .ok_or(HyperliquidError::Payload)
        })
        .collect()
}

/// Parses a 1m candle update but does not claim it is closed. A public websocket update alone is
/// never publishable: the receiver needs an authoritative candleSnapshot for this exact bucket.
pub(crate) fn parse_1m_candle(
    payload: &str,
    binding: &GatewayBinding,
    native_coin: &str,
    generation: u64,
    received_at_ms: u64,
) -> Result<FormingCandle, HyperliquidError> {
    if binding.venue != VenueId::Hyperliquid
        || binding.symbol.quote() != "USDC"
        || generation == 0
        || received_at_ms == 0
        || native_coin.is_empty()
    {
        return Err(HyperliquidError::Payload);
    }
    let envelope: CandleEnvelope =
        serde_json::from_str(payload).map_err(|_| HyperliquidError::Payload)?;
    let row = envelope.data;
    if envelope.channel != "candle"
        || row.symbol != native_coin
        || row.interval != "1m"
        || row.open_time == 0
        || row.close_time < row.open_time
    {
        return Err(HyperliquidError::Binding);
    }
    let expected_close = row
        .open_time
        .checked_add(ONE_MINUTE_MS - 1)
        .ok_or(HyperliquidError::Payload)?;
    if row.open_time % ONE_MINUTE_MS != 0 || row.close_time != expected_close {
        return Err(HyperliquidError::Payload);
    }
    let _base_volume = non_negative_decimal(&row.volume)?;
    let _trade_count = row.trade_count;
    let open = price(&row.open)?;
    let high = price(&row.high)?;
    let low = price(&row.low)?;
    let close = price(&row.close)?;
    if high < open.max(close) || low > open.min(close) || high < low {
        return Err(HyperliquidError::Payload);
    }
    Ok(FormingCandle)
}

/// Adapter-private proof that an incoming 1m wire update has the expected scope and shape.
/// It deliberately is not a `PublicBar`: the wire protocol does not certify closure.
pub(crate) struct FormingCandle;

fn price(value: &str) -> Result<Price, HyperliquidError> {
    Price::new(positive_decimal(value)?).map_err(|_| HyperliquidError::Payload)
}
fn positive_decimal(value: &str) -> Result<Decimal, HyperliquidError> {
    value
        .parse::<Decimal>()
        .ok()
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(HyperliquidError::Payload)
}
fn non_negative_decimal(value: &str) -> Result<Decimal, HyperliquidError> {
    value
        .parse::<Decimal>()
        .ok()
        .filter(|value| !value.is_sign_negative())
        .ok_or(HyperliquidError::Payload)
}

#[derive(Deserialize)]
struct TradesEnvelope {
    channel: String,
    data: Vec<TradeRow>,
}
#[derive(Deserialize)]
struct TradeRow {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    tid: u64,
}
#[derive(Deserialize)]
struct CandleEnvelope {
    channel: String,
    data: CandleRow,
}
#[derive(Deserialize)]
struct CandleRow {
    #[serde(rename = "t")]
    open_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    trade_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};
    const TRADES: &str = include_str!("../fixtures/public-trades-ws.json");
    const CANDLE: &str = include_str!("../fixtures/public-candle-1m-ws.json");
    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)
    }
    #[test]
    fn trades_keep_tuple_identity_and_quote_quantity() -> Result<(), Box<dyn std::error::Error>> {
        let facts = parse_public_trades(TRADES, &binding()?, "BTC", 7, 2_000)?;
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].aggregate_trade_id.to_string(), "1000:BTC:7");
        assert_eq!(facts[0].ordering, PublicTradeOrdering::Unsequenced);
        assert_eq!(facts[0].quote_quantity, Decimal::new(2_002, 1));
        Ok(())
    }
    #[test]
    fn trades_reject_more_than_1024_facts() -> Result<(), Box<dyn std::error::Error>> {
        let row = serde_json::json!({"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1});
        let payload = serde_json::json!({"channel":"trades","data":vec![row; HYPERLIQUID_PUBLIC_MAX_FACTS_PER_FRAME + 1]}).to_string();
        assert!(parse_public_trades(&payload, &binding()?, "BTC", 7, 2_000).is_err());
        Ok(())
    }
    #[test]
    fn trade_rejects_opaque_identity_over_256_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let coin = "X".repeat(253);
        let payload = serde_json::json!({"channel":"trades","data":[{"coin":coin,"side":"B","px":"1","sz":"1","time":1,"tid":1}]}).to_string();
        assert!(parse_public_trades(&payload, &binding()?, &coin, 7, 2_000).is_err());
        Ok(())
    }
    #[test]
    fn candle_is_formed_but_not_a_public_bar() -> Result<(), Box<dyn std::error::Error>> {
        let _candle = parse_1m_candle(CANDLE, &binding()?, "BTC", 7, 2_000)?;
        Ok(())
    }
}
