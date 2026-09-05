use venue_domain::domain::{Price, Symbol};

use crate::{BinanceAccountGatewayError, native_symbol};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceMarkPrice {
    pub symbol: Symbol,
    pub price: Price,
    pub observed_at_ms: u64,
}

/// A public mark is independent of whether an account already owns the selected position.
pub fn parse_mark_price(
    payload: &[u8],
    symbol: &Symbol,
    received_at_ms: u64,
) -> Result<BinanceMarkPrice, BinanceAccountGatewayError> {
    let invalid = || BinanceAccountGatewayError::Readback;
    let value: serde_json::Value = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if value.get("symbol").and_then(serde_json::Value::as_str)
        != Some(native_symbol(symbol).as_str())
    {
        return Err(invalid());
    }
    let price = value
        .get("markPrice")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse().ok())
        .and_then(|value| Price::new(value).ok())
        .ok_or_else(invalid)?;
    let observed_at_ms = value
        .get("time")
        .and_then(serde_json::Value::as_u64)
        .filter(|time| *time > 0 && *time <= received_at_ms && received_at_ms - *time <= 5_000)
        .ok_or_else(invalid)?;
    Ok(BinanceMarkPrice {
        symbol: symbol.clone(),
        price,
        observed_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marks_require_exact_symbol_positive_price_and_fresh_native_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = "BTC/USDT".parse()?;
        let value = br#"{"symbol":"BTCUSDT","markPrice":"50000","time":1000}"#;
        assert_eq!(
            parse_mark_price(value, &symbol, 1_001)?.observed_at_ms,
            1_000
        );
        assert!(parse_mark_price(value, &"ETH/USDT".parse()?, 1_001).is_err());
        assert!(parse_mark_price(value, &symbol, 999).is_err());
        assert!(parse_mark_price(value, &symbol, 6_001).is_err());
        assert!(
            parse_mark_price(
                br#"{"symbol":"BTCUSDT","markPrice":"0","time":1000}"#,
                &symbol,
                1_001
            )
            .is_err()
        );
        assert!(
            parse_mark_price(
                br#"{"symbol":"BTCUSDT","markPrice":"50000"}"#,
                &symbol,
                1_001
            )
            .is_err()
        );
        Ok(())
    }
}
