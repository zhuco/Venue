//! Secret-free public display data. It conveys no account or execution permission.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::{FieldState, Price, PublicBar, Symbol, UnknownReason};

pub mod clock;

pub type Result<T> = std::result::Result<T, String>;

pub fn received_ms() -> Result<u64> {
    clock::received_ms()
}

#[derive(Clone, Debug)]
pub struct Instrument {
    pub symbol: Symbol,
    pub price_scale: u32,
    pub quantity_scale: u32,
    pub contract_size: Decimal,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub symbol: Symbol,
    pub last: Decimal,
    pub change_percent: Decimal,
    pub quote_volume: Option<Decimal>,
    pub time_ms: u64,
}

pub fn trade(
    symbol: &Symbol,
    id: String,
    time: u64,
    now: u64,
    generation: u64,
    price: Decimal,
    quantity: Decimal,
    side: venue_domain::AggressorSide,
) -> Result<venue_domain::PublicTrade> {
    if quantity <= Decimal::ZERO || time == 0 || generation == 0 || id.is_empty() || time > now {
        return Err("invalid public trade".into());
    }
    Ok(venue_domain::PublicTrade {
        symbol: symbol.clone(),
        generation,
        received_at_ms: now,
        exchange_time_ms: time,
        transaction_time_ms: time,
        aggregate_trade_id: id.into(),
        first_trade_id: None,
        last_trade_id: None,
        ordering: venue_domain::PublicTradeOrdering::Unsequenced,
        price: Price::new(price).map_err(|_| "invalid public trade price")?,
        quantity,
        quote_quantity: price.checked_mul(quantity).ok_or("public trade overflow")?,
        aggressor: FieldState::Known(side),
    })
}

#[derive(Clone, Debug)]
pub struct Book {
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub time_ms: u64,
}

pub fn number(value: &Value) -> Result<Decimal> {
    let result: Decimal = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
        .parse()
        .map_err(|_| "invalid public decimal".to_owned())?;
    // Display arithmetic is bounded separately from account balances and risk calculations.
    if result.abs() > Decimal::from(1_000_000_000_000_000_u64) {
        return Err("public decimal exceeds display bounds".into());
    }
    Ok(result)
}
pub fn stamp(value: &Value) -> Result<u64> {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
        .parse()
        .map_err(|_| "invalid public timestamp".to_owned())
}
pub fn product(left: Decimal, right: Decimal) -> Result<Decimal> {
    left.checked_mul(right)
        .ok_or_else(|| "public decimal overflow".into())
}
pub fn seconds_ms(value: &Value) -> Result<u64> {
    use rust_decimal::prelude::ToPrimitive;
    product(number(value)?, Decimal::from(1000))?
        .to_u64()
        .filter(|v| *v > 0)
        .ok_or_else(|| "invalid public seconds".into())
}
pub fn array(value: &Value) -> Result<&Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| "invalid public array".to_owned())
}
pub fn string(value: &Value) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| "invalid public string".to_owned())
}
pub fn symbol(base: &str, quote: &str) -> Result<Symbol> {
    format!("{base}/{quote}")
        .parse()
        .map_err(|_| "invalid public symbol".to_owned())
}
pub fn levels(rows: &Value, multiplier: Decimal) -> Result<Vec<(Decimal, Decimal)>> {
    array(rows)?
        .iter()
        .take(20)
        .map(|row| {
            let price = number(&row[0])?;
            let size = number(&row[1])?
                .checked_mul(multiplier)
                .ok_or("public size overflow")?;
            if price <= Decimal::ZERO || size < Decimal::ZERO {
                return Err("invalid public level".into());
            }
            Ok((price, size))
        })
        .collect()
}
pub async fn json(http: &reqwest::Client, request: reqwest::RequestBuilder) -> Result<Value> {
    let _ = http;
    let mut response = request
        .send()
        .await
        .map_err(|e| format!("public endpoint unreachable: {}", e.without_url()))?
        .error_for_status()
        .map_err(|e| format!("public HTTP {}", e.status().map_or(0, |s| s.as_u16())))?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| {
        if e.is_timeout() {
            "public body read timed out".to_owned()
        } else {
            format!("public body unavailable: {}", e.without_url())
        }
    })? {
        if body.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
            return Err("public response too large".into());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| "invalid public JSON".into())
}
pub fn bar(
    symbol: &Symbol,
    generation: u64,
    now: u64,
    interval: u64,
    open_ms: u64,
    ohlc: [Decimal; 4],
    volume: Decimal,
    quote_volume: Option<Decimal>,
) -> Result<PublicBar> {
    if interval == 0 {
        return Err("invalid candle interval".into());
    }
    let price = |v| Price::new(v).map_err(|_| "invalid candle price".to_owned());
    let result = PublicBar {
        symbol: symbol.clone(),
        generation,
        received_at_ms: now,
        sequence: (open_ms / interval)
            .checked_add(1)
            .ok_or("candle sequence overflow")?,
        open_time_ms: open_ms,
        close_time_ms: open_ms
            .checked_add(interval)
            .and_then(|v| v.checked_sub(1))
            .ok_or("candle time overflow")?,
        interval_ms: interval,
        open: price(ohlc[0])?,
        high: price(ohlc[1])?,
        low: price(ohlc[2])?,
        close: price(ohlc[3])?,
        base_volume: FieldState::Known(volume),
        quote_volume: quote_volume
            .map(FieldState::Known)
            .unwrap_or(FieldState::Unavailable {
                reason: UnknownReason::SourceOmitted,
            }),
        trade_count: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        taker_buy_base_volume: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
        taker_buy_quote_volume: FieldState::Unavailable {
            reason: UnknownReason::SourceOmitted,
        },
    };
    if !result.is_valid() {
        return Err("invalid public candle".into());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fractional_seconds_and_contract_size_are_explicit() -> Result<()> {
        assert_eq!(seconds_ms(&json!(1788691680.054))?, 1788691680054);
        assert_eq!(
            product(number(&json!(8))?, number(&json!("0.0001"))?)?,
            number(&json!("0.0008"))?
        );
        assert!(product(Decimal::MAX, Decimal::from(2)).is_err());
        assert!(seconds_ms(&json!(-1)).is_err());
        Ok(())
    }

    #[test]
    fn invalid_interval_and_empty_trade_identity_fail_closed() -> Result<()> {
        let symbol = symbol("BTC", "USDT")?;
        assert!(bar(&symbol, 1, 100, 0, 1, [Decimal::ONE; 4], Decimal::ONE, None).is_err());
        assert!(
            trade(
                &symbol,
                String::new(),
                1,
                2,
                1,
                Decimal::ONE,
                Decimal::ONE,
                venue_domain::AggressorSide::Buy
            )
            .is_err()
        );
        Ok(())
    }
}
