// Public perpetual market display via the fixed Venue HTTPS relay.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::{FieldState, MarketTimeSource, OpenInterestSample, OpenInterestUnit, PublicBar};
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/okx/api/v5";
async fn get(http: &reqwest::Client, path: &str) -> Result<Value> {
    let value = json(http, http.get(format!("{ORIGIN}/{path}"))).await?;
    if value["code"] != "0" {
        return Err("okx public request rejected".into());
    }
    Ok(value)
}
pub async fn catalog(http: &reqwest::Client) -> Result<Vec<Instrument>> {
    let value = get(http, "public/instruments?instType=SWAP").await?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
        if row["state"] != "live" || row["ctType"] != "linear" {
            continue;
        }
        let native = string(&row["instId"])?;
        let parts = native.split('-').collect::<Vec<_>>();
        if parts.len() != 3 || !matches!(parts[1], "USDT" | "USDC") {
            continue;
        }
        let size = number(&row["ctVal"])?;
        if row["ctValCcy"] != parts[0] || size <= Decimal::ZERO {
            continue;
        }
        let Ok(symbol) = symbol(parts[0], parts[1]) else {
            continue;
        };
        result.push(Instrument {
            symbol,
            native_symbol: native.to_owned(),
            price_tick: Some(number(&row["tickSz"])?),
            price_scale: number(&row["tickSz"])?.normalize().scale(),
            quantity_scale: product(number(&row["lotSz"])?, size)?.normalize().scale(),
            contract_size: size,
        });
    }

    if result.is_empty() {
        return Err("empty public catalogue".into());
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
    let native = format!(
        "{}-{}-SWAP",
        instrument.symbol.base(),
        instrument.symbol.quote()
    );
    let interval = match ms {
        60_000 => "1m",
        300_000 => "5m",
        900_000 => "15m",
        3_600_000 => "1H",
        14_400_000 => "4H",
        86_400_000 => "1Dutc",
        _ => return Err("unsupported interval".into()),
    };
    let cursor = before
        .map(|before| format!("&after={before}"))
        .unwrap_or_default();
    let endpoint = if before.is_some() {
        "history-candles"
    } else {
        "candles"
    };
    let value = get(
        http,
        &format!("market/{endpoint}?instId={native}&bar={interval}&limit={}{cursor}", limit.clamp(1, 100)),
    )
    .await?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
        let open = stamp(&row[0])?;
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
                number(&row[1])?,
                number(&row[2])?,
                number(&row[3])?,
                number(&row[4])?,
            ],
            number(&row[6])?,
            Some(number(&row[7])?),
        )?);
    }
    result.sort_by_key(|b| b.open_time_ms);
    Ok(result)
}
pub async fn book(http: &reqwest::Client, instrument: &Instrument) -> Result<Book> {
    let native = format!(
        "{}-{}-SWAP",
        instrument.symbol.base(),
        instrument.symbol.quote()
    );
    let value = get(http, &format!("market/books?instId={native}&sz=20")).await?;
    let data = &value["data"][0];
    Ok(Book {
        bids: levels(&data["bids"], instrument.contract_size)?,
        asks: levels(&data["asks"], instrument.contract_size)?,
        time_ms: stamp(&data["ts"])?,
    })
}

pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let value = get(http, "market/tickers?instType=SWAP").await?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
        let Some(i) = instruments
            .iter()
            .find(|i| row["instId"] == format!("{}-{}-SWAP", i.symbol.base(), i.symbol.quote()))
        else {
            continue;
        };
        let parsed = (|| -> Result<Quote> {
            let last = number(&row["last"])?;
            if last <= Decimal::ZERO {
                return Err("inactive ticker".into());
            }
            Ok(Quote {
                symbol: i.symbol.clone(),
                last,
                change_percent: product(
                    last.checked_div(number(&row["open24h"])?)
                        .ok_or("invalid open")?
                        .checked_sub(Decimal::ONE)
                        .ok_or("change overflow")?,
                    Decimal::from(100),
                )?,
                // OKX reports base volume here, not actual quote turnover.
                quote_volume: None,
                time_ms: stamp(&row["ts"])?,
                derivatives: None,
            })
        })();
        if let Ok(quote) = parsed {
            result.push(quote);
        }
    }
    Ok(result)
}

/// Read only the selected native contract. Funding and OI have independent source timestamps.
pub async fn current_derivatives(http: &reqwest::Client, instrument: &Instrument) -> Result<DerivativeQuote> {
    let native = &instrument.native_symbol;
    let funding_path = format!("public/funding-rate?instId={native}");
    let interest_path = format!("public/open-interest?instType=SWAP&instId={native}");
    let (funding, interest) = tokio::join!(
        get(http, &funding_path),
        get(http, &interest_path),
    );
    let funding = funding.and_then(|value| parse_funding(&value, native)).ok();
    let interest = interest.and_then(|value| parse_interest(&value, native)).ok();
    if funding.is_none() && interest.is_none() { return Err("OKX public derivatives unavailable".into()); }
    Ok(DerivativeQuote {
        use_local_observation_time: false,
        funding_rate: funding.map(|(rate, _, _)| rate),
        funding_time_ms: funding.map(|(_, time, _)| time),
        next_funding_time_ms: funding.and_then(|(_, _, next)| next),
        mark_price: None,
        index_price: None,
        open_interest_base: interest.map(|(base, _)| base),
        open_interest_native_quantity: None,
        open_interest_native_unit: None,
        open_interest_time_ms: interest.map(|(_, time)| time),
    })
}

fn parse_funding(value: &Value, native: &str) -> Result<(Decimal, u64, Option<u64>)> {
    let rows = array(&value["data"])?;
    let row = rows.first().ok_or("missing OKX funding")?;
    if row["instId"] != native || row["instType"] != "SWAP" { return Err("OKX funding scope mismatch".into()); }
    let time = stamp(&row["ts"])?;
    if time == 0 { return Err("missing OKX funding time".into()); }
    let next = stamp(&row["fundingTime"]).ok().filter(|value| *value > 0);
    Ok((number(&row["fundingRate"])?, time, next))
}

fn parse_interest(value: &Value, native: &str) -> Result<(Decimal, u64)> {
    let rows = array(&value["data"])?;
    let row = rows.first().ok_or("missing OKX OI")?;
    if row["instId"] != native || row["instType"] != "SWAP" { return Err("OKX OI scope mismatch".into()); }
    let time = stamp(&row["ts"])?;
    let base = number(&row["oiCcy"])?;
    if time == 0 || base < Decimal::ZERO { return Err("invalid OKX OI".into()); }
    Ok((base, time))
}

/// Exact native contract OI, in completed five-minute samples.
pub async fn open_interest_history(http: &reqwest::Client, instrument: &Instrument,
    generation: u64, now: u64, since: Option<u64>) -> Result<Vec<OpenInterestSample>> {
    let mut samples = Vec::new();
    let mut end = None::<u64>;
    for _ in 0..3 {
        let cursor = end.map(|time| format!("&end={time}")).unwrap_or_default();
        let value = get(http, &format!("rubik/stat/contracts/open-interest-history?instId={}&period=5m&limit=100{cursor}",
            instrument.native_symbol)).await?;
        let page = parse_interest_history(&value, instrument, generation, now)?;
        if page.is_empty() { break; }
        let oldest = page.first().map(|sample| sample.exchange_time_ms);
        if oldest == end { break; }
        end = oldest;
        samples.extend(page);
        if end.is_some_and(|time| since.is_some_and(|cached| time <= cached)) { break; }
        if end.is_some_and(|time| time <= now.saturating_sub(86_700_000)) { break; }
    }
    samples.sort_by_key(|sample| sample.exchange_time_ms);
    samples.dedup_by_key(|sample| sample.exchange_time_ms);
    samples.retain(|sample| sample.exchange_time_ms.saturating_add(300_000) <= now);
    Ok(samples)
}

fn parse_interest_history(value: &Value, instrument: &Instrument,
    generation: u64, now: u64) -> Result<Vec<OpenInterestSample>> {
    let mut samples = Vec::new();
    for row in array(&value["data"])? {
        let fields = array(row)?;
        if fields.len() < 4 { return Err("invalid OKX OI history row".into()); }
        let time = stamp(&fields[0])?;
        let contracts = number(&fields[1])?;
        let base = number(&fields[2])?;
        let usd = number(&fields[3])?;
        if time == 0 || time > now || contracts < Decimal::ZERO || base < Decimal::ZERO || usd < Decimal::ZERO {
            return Err("invalid OKX OI history value".into());
        }
        let sample = OpenInterestSample { symbol: instrument.symbol.clone(), generation,
            received_at_ms: now, exchange_time_ms: time, time_source: MarketTimeSource::Exchange,
            sampling_interval_ms: Some(300_000), native_quantity: contracts,
            native_unit: OpenInterestUnit::Contracts { base_per_contract: instrument.contract_size },
            base_quantity: FieldState::Known(base), quote_notional: FieldState::Known(usd),
            quote_asset: Some("USD".to_owned()) };
        if !sample.is_valid() { return Err("invalid OKX OI history sample".into()); }
        samples.push(sample);
    }
    samples.sort_by_key(|sample| sample.exchange_time_ms);
    Ok(samples)
}

#[cfg(test)]
mod interest_history_tests {
    use super::*;
    #[test]
    fn exact_contract_samples_preserve_units_and_completion() -> Result<()> {
        let instrument = Instrument { symbol: symbol("DOGE", "USDT")?,
            native_symbol: "DOGE-USDT-SWAP".into(), price_tick: None, price_scale: 5,
            quantity_scale: 0, contract_size: Decimal::from(1000) };
        let payload = serde_json::json!({"data": [
            ["600000", "1.5", "1500", "145"],
            ["900000", "2", "2000", "190"]]});
        let samples = parse_interest_history(&payload, &instrument, 5, 1_000_000)?;
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].native_quantity, Decimal::new(15, 1));
        assert_eq!(samples[0].base_quantity, FieldState::Known(Decimal::from(1500)));
        assert_eq!(samples[0].quote_asset.as_deref(), Some("USD"));
        assert!(samples[0].is_valid());
        let public_response = serde_json::json!({"data": [["1790535900000",
            "1094913.80999999969", "1094913809.99999969", "106721249.0606999697843"]]});
        let exact = parse_interest_history(&public_response, &instrument, 5, 1_790_536_200_000)?;
        assert_eq!(exact.len(), 1);
        assert!(exact[0].is_valid());
        Ok(())
    }
}

#[cfg(test)]
mod derivative_tests {
    use super::*;
    #[test]
    fn exact_contract_and_native_timestamps() -> Result<()> {
        let funding = serde_json::json!({"data":[{"instId":"DOGE-USDT-SWAP","instType":"SWAP",
            "fundingRate":"-0.0001","fundingTime":"900000","nextFundingTime":"1200000","ts":"600000"}]});
        let interest = serde_json::json!({"data":[{"instId":"DOGE-USDT-SWAP","instType":"SWAP",
            "oi":"10","oiCcy":"10000","oiUsd":"1000","ts":"600100"}]});
        assert_eq!(parse_funding(&funding, "DOGE-USDT-SWAP")?, (Decimal::new(-1, 4), 600_000, Some(900_000)));
        assert_eq!(parse_interest(&interest, "DOGE-USDT-SWAP")?, (Decimal::from(10_000), 600_100));
        assert!(parse_interest(&interest, "DOGE-USDC-SWAP").is_err());
        Ok(())
    }
}
pub async fn trades(
    http: &reqwest::Client,
    instrument: &Instrument,
    generation: u64,
    _request_started_ms: u64,
) -> Result<Vec<venue_domain::PublicTrade>> {
    let value = get(
        http,
        &format!(
            "market/trades?instId={}-{}-SWAP&limit=100",
            instrument.symbol.base(),
            instrument.symbol.quote()
        ),
    )
    .await?;
    let now = received_ms()?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
        let side = match string(&row["side"])? {
            "Buy" | "buy" => venue_domain::AggressorSide::Buy,
            "Sell" | "sell" => venue_domain::AggressorSide::Sell,
            _ => return Err("invalid aggressor".into()),
        };
        result.push(trade(
            &instrument.symbol,
            string(&row["tradeId"])?.into(),
            stamp(&row["ts"])?,
            now,
            generation,
            number(&row["px"])?,
            product(number(&row["sz"])?, instrument.contract_size)?,
            side,
        )?);
    }
    result.sort_by_key(|t| t.transaction_time_ms);
    Ok(result)
}
