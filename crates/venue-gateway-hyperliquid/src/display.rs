//! Native Hyperliquid USDC perpetual display (the default DEX).
use rust_decimal::Decimal;
use serde_json::{Value, json};
use venue_domain::PublicBar;
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/hyperliquid/info";
pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let value = info(http, json!({"type":"metaAndAssetCtxs"})).await?;
    let universe = array(&value[0]["universe"])?;
    let contexts = array(&value[1])?;
    if universe.len() != contexts.len() {
        return Err("Hyperliquid catalogue mismatch".into());
    }
    let mut result = Vec::new();
    for (asset, ctx) in universe.iter().zip(contexts) {
        let Some(i) = instruments
            .iter()
            .find(|i| asset["name"] == i.symbol.base())
        else {
            continue;
        };
        let parsed = (|| -> Result<Quote> {
            let last = number(&ctx["markPx"])?;
            let change = last
                .checked_div(number(&ctx["prevDayPx"])?)
                .ok_or("invalid prior price")?;
            Ok(Quote {
                symbol: i.symbol.clone(),
                last,
                change_percent: product(change - Decimal::ONE, Decimal::from(100))?,
                quote_volume: Some(number(&ctx["dayNtlVlm"])?),
                time_ms: 0,
                derivatives: Some(derivatives(ctx)),
            })
        })();
        if let Ok(q) = parsed {
            result.push(q);
        }
    }
    Ok(result)
}

fn derivatives(context: &Value) -> DerivativeQuote {
    DerivativeQuote {
        use_local_observation_time: true,
        funding_rate: number(&context["funding"]).ok(),
        funding_time_ms: None,
        next_funding_time_ms: None,
        mark_price: number(&context["markPx"]).ok().filter(|value| *value > Decimal::ZERO),
        // Hyperliquid's oraclePx is not silently relabeled as an exchange index price.
        index_price: None,
        open_interest_base: number(&context["openInterest"]).ok().filter(|value| *value >= Decimal::ZERO),
        open_interest_native_quantity: None,
        open_interest_native_unit: None,
        open_interest_time_ms: None,
    }
}

#[cfg(test)]
mod derivative_tests {
    use super::*;
    #[test]
    fn asset_context_keeps_base_oi_and_local_observation_clock() {
        let row = serde_json::json!({"funding":"-0.000025", "openInterest":"1285463316.0",
            "markPx":"0.097504", "oraclePx":"0.097425"});
        let value = derivatives(&row);
        assert_eq!(value.funding_rate, Some(Decimal::new(-25, 6)));
        assert_eq!(value.open_interest_base, Some(Decimal::from(1_285_463_316_u64)));
        assert!(value.index_price.is_none());
        assert!(value.use_local_observation_time);
    }
}
pub async fn trades(
    http: &reqwest::Client,
    instrument: &Instrument,
    generation: u64,
    _request_started_ms: u64,
) -> Result<Vec<venue_domain::PublicTrade>> {
    let value = info(
        http,
        json!({"type":"recentTrades","coin":instrument.symbol.base()}),
    )
    .await?;
    let now = received_ms()?;
    let mut result = Vec::new();
    for row in array(&value)? {
        if row["coin"] != instrument.symbol.base() {
            return Err("trade coin mismatch".into());
        }
        let side = match string(&row["side"])? {
            "B" => venue_domain::AggressorSide::Buy,
            "A" => venue_domain::AggressorSide::Sell,
            _ => return Err("invalid trade side".into()),
        };
        let time = stamp(&row["time"])?;
        result.push(trade(
            &instrument.symbol,
            format!("{time}:{}", row["tid"]),
            time,
            now,
            generation,
            number(&row["px"])?,
            number(&row["sz"])?,
            side,
        )?);
    }
    result.sort_by_key(|t| t.transaction_time_ms);
    Ok(result)
}
async fn info(http: &reqwest::Client, body: Value) -> Result<Value> {
    venue_gateway_api::display::json(http, http.post(ORIGIN).json(&body)).await
}
pub async fn catalog(http: &reqwest::Client) -> Result<Vec<Instrument>> {
    let value = info(http, json!({"type":"meta"})).await?;
    let mut result = Vec::new();
    for row in array(&value["universe"])? {
        if row["isDelisted"] == true {
            continue;
        }
        let Ok(symbol) = symbol(string(&row["name"])?, "USDC") else {
            continue;
        };
        let quantity_scale = stamp(&row["szDecimals"])? as u32;
        result.push(Instrument {
            symbol,
            native_symbol: string(&row["name"])?.to_owned(),
            price_tick: None,
            price_scale: 6_u32.saturating_sub(quantity_scale),
            quantity_scale,
            contract_size: Decimal::ONE,
        });
    }
    Ok(result)
}
pub async fn candles(
    http: &reqwest::Client,
    instrument: &Instrument,
    ms: u64,
    generation: u64,
    now: u64,
    before: Option<u64>,
    limit: usize,
) -> Result<Vec<PublicBar>> {
    let interval = match ms {
        60_000 => "1m",
        300_000 => "5m",
        900_000 => "15m",
        3_600_000 => "1h",
        14_400_000 => "4h",
        86_400_000 => "1d",
        _ => return Err("unsupported interval".into()),
    };
    let end = before.map(|b| b.saturating_sub(1)).unwrap_or(now);
    let value = info(http,json!({"type":"candleSnapshot","req":{"coin":instrument.symbol.base(),"interval":interval,"startTime":end.saturating_sub(ms.saturating_mul(limit.clamp(1, 200) as u64)),"endTime":end}})).await?;
    let mut result = Vec::new();
    for row in array(&value)? {
        if row["s"] != instrument.symbol.base() || row["i"] != interval {
            return Err("Hyperliquid candle scope mismatch".into());
        }
        let open = stamp(&row["t"])?;
        if before.is_some_and(|b| open >= b) {
            continue;
        }
        result.push(bar(
            &instrument.symbol,
            generation,
            now,
            ms,
            open,
            [
                number(&row["o"])?,
                number(&row["h"])?,
                number(&row["l"])?,
                number(&row["c"])?,
            ],
            number(&row["v"])?,
            None,
        )?);
    }
    result.sort_by_key(|b| b.open_time_ms);
    Ok(result)
}
pub async fn book(http: &reqwest::Client, instrument: &Instrument) -> Result<Book> {
    let value = info(
        http,
        json!({"type":"l2Book","coin":instrument.symbol.base()}),
    )
    .await?;
    if value["coin"] != instrument.symbol.base() {
        return Err("Hyperliquid book scope mismatch".into());
    }
    let side = |index: usize| -> Result<_> {
        array(&value["levels"][index])?
            .iter()
            .take(20)
            .map(|r| Ok((number(&r["px"])?, number(&r["sz"])?)))
            .collect()
    };
    Ok(Book {
        bids: side(0)?,
        asks: side(1)?,
        time_ms: stamp(&value["time"])?,
    })
}
