//! Gate USDT perpetual display. Contract quantities are converted using the public multiplier.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::{
    FieldState, MarketTimeSource, OpenInterestSample, OpenInterestUnit, PublicBar, UnknownReason,
};
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/gate/api/v4/futures/usdt";
pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let value = get(http, "tickers").await?;
    let mut result = Vec::new();
    for row in array(&value)? {
        let Some(i) = instruments
            .iter()
            .find(|i| row["contract"] == i.native_symbol)
        else {
            continue;
        };
        let parsed = (|| -> Result<Quote> {
            Ok(Quote {
                symbol: i.symbol.clone(),
                last: number(&row["last"])?,
                change_percent: number(&row["change_percentage"])?,
                quote_volume: Some(number(&row["volume_24h_quote"])?),
                time_ms: 0,
                derivatives: Some(derivatives(row, i)),
            })
        })();
        if let Ok(q) = parsed {
            result.push(q);
        }
    }
    Ok(result)
}

fn derivatives(row: &Value, instrument: &Instrument) -> DerivativeQuote {
    let interest = number(&row["total_size"])
        .ok()
        .filter(|contracts| *contracts >= Decimal::ZERO);
    let base = interest.and_then(|contracts| product(contracts, instrument.contract_size).ok());
    DerivativeQuote {
        use_local_observation_time: true,
        funding_rate: number(&row["funding_rate"]).ok(),
        funding_time_ms: None,
        next_funding_time_ms: None,
        mark_price: number(&row["mark_price"])
            .ok()
            .filter(|value| *value > Decimal::ZERO),
        index_price: number(&row["index_price"])
            .ok()
            .filter(|value| *value > Decimal::ZERO),
        open_interest_base: base,
        open_interest_native_quantity: interest,
        open_interest_native_unit: Some(venue_domain::OpenInterestUnit::Contracts {
            base_per_contract: instrument.contract_size,
        }),
        open_interest_time_ms: None,
    }
}

pub async fn open_interest_history(
    http: &reqwest::Client,
    instrument: &Instrument,
    generation: u64,
    now: u64,
) -> Result<Vec<OpenInterestSample>> {
    let payload = get(
        http,
        &format!(
            "contract_stats?contract={}&interval=5m&limit=300",
            instrument.native_symbol
        ),
    )
    .await?;
    parse_interest_history(&payload, instrument, generation, now)
}

fn parse_interest_history(
    payload: &Value,
    instrument: &Instrument,
    generation: u64,
    now: u64,
) -> Result<Vec<OpenInterestSample>> {
    let mut result = Vec::new();
    for row in array(payload)? {
        let time = stamp(&row["time"])?
            .checked_mul(1000)
            .ok_or("Gate OI time overflow")?;
        if time == 0 || time.saturating_add(300_000) > now {
            continue;
        }
        let contracts = number(&row["open_interest"])?;
        let base = product(contracts, instrument.contract_size)?;
        if contracts < Decimal::ZERO {
            return Err("negative Gate OI".into());
        }
        let notional = number(&row["open_interest_usd"])
            .ok()
            .filter(|value| *value >= Decimal::ZERO);
        let sample = OpenInterestSample {
            symbol: instrument.symbol.clone(),
            generation,
            received_at_ms: now,
            exchange_time_ms: time,
            time_source: MarketTimeSource::Exchange,
            sampling_interval_ms: Some(300_000),
            native_quantity: contracts,
            native_unit: OpenInterestUnit::Contracts {
                base_per_contract: instrument.contract_size,
            },
            base_quantity: FieldState::Known(base),
            quote_notional: notional.map_or(
                FieldState::Unavailable {
                    reason: UnknownReason::SourceOmitted,
                },
                FieldState::Known,
            ),
            quote_asset: notional.map(|_| "USD".to_owned()),
        };
        if !sample.is_valid() {
            return Err("invalid Gate OI sample".into());
        }
        result.push(sample);
    }
    result.sort_by_key(|sample| sample.exchange_time_ms);
    result.dedup_by_key(|sample| sample.exchange_time_ms);
    Ok(result)
}

#[cfg(test)]
mod derivative_tests {
    use super::*;
    #[test]
    fn ticker_contracts_convert_with_catalog_multiplier() -> Result<()> {
        let instrument = Instrument {
            symbol: symbol("DOGE", "USDT")?,
            native_symbol: "DOGE_USDT".into(),
            price_tick: None,
            price_scale: 5,
            quantity_scale: 0,
            contract_size: Decimal::from(10),
        };
        let row = serde_json::json!({"contract":"DOGE_USDT", "funding_rate":"-0.0001",
            "total_size":"237702564", "mark_price":"0.0974", "index_price":"0.097427"});
        let value = derivatives(&row, &instrument);
        assert_eq!(value.funding_rate, Some(Decimal::new(-1, 4)));
        assert_eq!(
            value.open_interest_base,
            Some(Decimal::from(2_377_025_640_u64))
        );
        assert!(value.use_local_observation_time);
        Ok(())
    }
    #[test]
    fn contract_stats_keep_five_minute_samples_and_multiplier() -> Result<()> {
        let instrument = Instrument {
            symbol: symbol("DOGE", "USDT")?,
            native_symbol: "DOGE_USDT".into(),
            price_tick: None,
            price_scale: 5,
            quantity_scale: 0,
            contract_size: Decimal::from(10),
        };
        let payload = serde_json::json!([
            {"time":600,"open_interest":"123","open_interest_usd":"120"},
            {"time":900,"open_interest":"124","open_interest_usd":"121"}
        ]);
        let result = parse_interest_history(&payload, &instrument, 1, 1_000_000)?;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].base_quantity,
            FieldState::Known(Decimal::from(1230))
        );
        assert_eq!(result[0].native_quantity, Decimal::from(123));
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
            "trades?contract={}_USDT&limit=100",
            instrument.symbol.base()
        ),
    )
    .await?;
    let now = received_ms()?;
    let mut result = Vec::new();
    for row in array(&value)? {
        let size = number(&row["size"])?;
        let side = if size > Decimal::ZERO {
            venue_domain::AggressorSide::Buy
        } else {
            venue_domain::AggressorSide::Sell
        };
        result.push(trade(
            &instrument.symbol,
            row["id"].to_string(),
            seconds_ms(&row["create_time"])?,
            now,
            generation,
            number(&row["price"])?,
            product(size.abs(), instrument.contract_size)?,
            side,
        )?);
    }
    result.sort_by_key(|t| t.transaction_time_ms);
    Ok(result)
}
async fn get(http: &reqwest::Client, path: &str) -> Result<Value> {
    let mut request = http.get(format!("{ORIGIN}/{path}"));
    if path == "contracts" {
        // The complete public contract catalogue exceeds 1 MiB; it is fetched
        // once at startup and remains cancelable on source switch.
        request = request.timeout(std::time::Duration::from_secs(30));
    }
    json(http, request).await
}
pub async fn catalog(http: &reqwest::Client) -> Result<Vec<Instrument>> {
    let value = get(http, "contracts").await?;
    let mut result = Vec::new();
    for row in array(&value)? {
        if row["in_delisting"] == true {
            continue;
        }
        let name = string(&row["name"])?;
        let Some(base) = name.strip_suffix("_USDT") else {
            continue;
        };
        let size = number(&row["quanto_multiplier"])?;
        if size <= Decimal::ZERO {
            return Err("invalid Gate contract size".into());
        }
        let Ok(symbol) = symbol(base, "USDT") else {
            continue;
        };
        result.push(Instrument {
            symbol,
            native_symbol: name.to_owned(),
            price_tick: Some(number(&row["order_price_round"])?),
            price_scale: number(&row["order_price_round"])?.normalize().scale(),
            quantity_scale: size.normalize().scale(),
            contract_size: size,
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
    let limit = limit.clamp(1, 200);
    let interval = match ms {
        60_000 => "1m",
        300_000 => "5m",
        900_000 => "15m",
        3_600_000 => "1h",
        14_400_000 => "4h",
        86_400_000 => "1d",
        _ => return Err("unsupported interval".into()),
    };
    let cursor = before
        .map(|b| {
            format!(
                "&to={}&from={}",
                b.saturating_sub(1) / 1000,
                b.saturating_sub(ms.saturating_mul(limit as u64)) / 1000
            )
        })
        .unwrap_or_else(|| format!("&limit={limit}"));
    let value = get(
        http,
        &format!(
            "candlesticks?contract={}_USDT&interval={interval}{cursor}",
            instrument.symbol.base()
        ),
    )
    .await?;
    let mut result = Vec::new();
    for row in array(&value)? {
        let open = stamp(&row["t"])?
            .checked_mul(1000)
            .ok_or("Gate time overflow")?;
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
            product(number(&row["v"])?, instrument.contract_size)?,
            Some(number(&row["sum"])?),
        )?);
    }
    result.sort_by_key(|b| b.open_time_ms);
    Ok(result)
}
pub async fn book(http: &reqwest::Client, instrument: &Instrument) -> Result<Book> {
    let data = get(
        http,
        &format!(
            "order_book?contract={}_USDT&limit=20&with_id=true",
            instrument.symbol.base()
        ),
    )
    .await?;
    let side = |key| -> Result<_> {
        array(&data[key])?
            .iter()
            .map(|r| {
                Ok((
                    number(&r["p"])?,
                    product(number(&r["s"])?, instrument.contract_size)?,
                ))
            })
            .collect()
    };
    let time_ms = seconds_ms(&data["current"])?;
    Ok(Book {
        bids: side("bids")?,
        asks: side("asks")?,
        time_ms,
    })
}
