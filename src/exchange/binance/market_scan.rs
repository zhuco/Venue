use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde_json::Value;

use crate::{domain::Symbol, market::MarketRankSample};

use super::BinanceError;

/// Normalizes Binance's complete exchange-info and 24-hour ticker payloads for the phase-8
/// scanner. Only currently trading, USDT-settled linear perpetual contracts are admitted.
pub fn parse_usdt_perpetual_market_rank_samples(
    exchange_info_payload: &str,
    ticker_payload: &str,
    observed_at_ms: u64,
    source_generation: u64,
) -> Result<Vec<MarketRankSample>, BinanceError> {
    if observed_at_ms == 0 || source_generation == 0 {
        return Err(BinanceError::Payload);
    }
    let exchange_info: Value =
        serde_json::from_str(exchange_info_payload).map_err(|_| BinanceError::Payload)?;
    let tickers: Value = serde_json::from_str(ticker_payload).map_err(|_| BinanceError::Payload)?;
    let symbols = exchange_info
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or(BinanceError::Payload)?;
    let tickers = tickers.as_array().ok_or(BinanceError::Payload)?;

    let mut eligible = BTreeMap::new();
    for item in symbols {
        if item.get("status").and_then(Value::as_str) != Some("TRADING")
            || item.get("contractType").and_then(Value::as_str) != Some("PERPETUAL")
            || item.get("quoteAsset").and_then(Value::as_str) != Some("USDT")
            || item.get("marginAsset").and_then(Value::as_str) != Some("USDT")
        {
            continue;
        }
        let native = item
            .get("symbol")
            .and_then(Value::as_str)
            .ok_or(BinanceError::Payload)?;
        let base = item
            .get("baseAsset")
            .and_then(Value::as_str)
            .ok_or(BinanceError::Payload)?;
        // Binance may list a small number of venue-native assets whose spelling cannot cross the
        // canonical Symbol boundary. They are outside the normalized universe, not a reason to
        // discard every valid contract in the same complete response.
        let Ok(symbol) = Symbol::new(base, "USDT") else {
            continue;
        };
        if eligible.insert(native.to_owned(), symbol).is_some() {
            return Err(BinanceError::Payload);
        }
    }

    let mut samples = Vec::with_capacity(eligible.len());
    for ticker in tickers {
        let Some(native) = ticker.get("symbol").and_then(Value::as_str) else {
            return Err(BinanceError::Payload);
        };
        let Some(symbol) = eligible.remove(native) else {
            continue;
        };
        let change_percent = decimal_field(ticker, "priceChangePercent")?;
        let quote_volume = decimal_field(ticker, "quoteVolume")?;
        samples.push(MarketRankSample {
            symbol,
            observed_at_ms,
            source_generation,
            change_24h_bps: change_percent * Decimal::new(100, 0),
            quote_volume,
        });
    }
    if !eligible.is_empty() || samples.is_empty() {
        return Err(BinanceError::Payload);
    }
    samples.sort_by(|left, right| left.symbol.cmp(&right.symbol));
    Ok(samples)
}

fn decimal_field(value: &Value, field: &str) -> Result<Decimal, BinanceError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BinanceError::Payload)?
        .parse()
        .map_err(|_| BinanceError::Payload)
}

#[cfg(test)]
mod tests {
    use super::parse_usdt_perpetual_market_rank_samples;

    #[test]
    fn keeps_only_complete_trading_usdt_perpetuals() -> Result<(), Box<dyn std::error::Error>> {
        let exchange_info = r#"{"symbols":[
          {"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING"},
          {"symbol":"ETHUSDT_260925","baseAsset":"ETH","quoteAsset":"USDT","marginAsset":"USDT","contractType":"CURRENT_QUARTER","status":"TRADING"},
          {"symbol":"SOLUSDT","baseAsset":"SOL","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"SETTLING"}
        ]}"#;
        let tickers = r#"[
          {"symbol":"BTCUSDT","priceChangePercent":"-2.50","quoteVolume":"123.4"},
          {"symbol":"ETHUSDT_260925","priceChangePercent":"3","quoteVolume":"99"},
          {"symbol":"SOLUSDT","priceChangePercent":"4","quoteVolume":"88"}
        ]"#;
        let samples = parse_usdt_perpetual_market_rank_samples(exchange_info, tickers, 10, 7)?;
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].symbol.to_string(), "BTC/USDT");
        assert_eq!(samples[0].change_24h_bps.to_string(), "-250.00");
        Ok(())
    }

    #[test]
    fn rejects_missing_ticker_for_an_eligible_contract() {
        let exchange_info = r#"{"symbols":[{"symbol":"BTCUSDT","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","contractType":"PERPETUAL","status":"TRADING"}]}"#;
        assert!(parse_usdt_perpetual_market_rank_samples(exchange_info, "[]", 10, 7).is_err());
    }
}
