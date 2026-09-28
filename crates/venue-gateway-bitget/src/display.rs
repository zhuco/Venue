// Public perpetual market display via the fixed Venue HTTPS relay.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::PublicBar;
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/bitget/api/v3/market";
async fn get(http: &reqwest::Client, path: &str) -> Result<Value> {
    let value = json(http, http.get(format!("{ORIGIN}/{path}"))).await?;
    if value["code"] != "00000" {
        return Err("bitget public request rejected".into());
    }
    Ok(value)
}
pub async fn catalog(http: &reqwest::Client) -> Result<Vec<Instrument>> {
    let mut result = Vec::new();
    for category in ["USDT-FUTURES", "USDC-FUTURES"] {
        let value = get(http, &format!("instruments?category={category}")).await?;
        for row in array(&value["data"])? {
            if let Some(instrument) = parse_instrument(row, category)? {
                result.push(instrument);
            }
        }
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
    let native = &instrument.native_symbol;
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
        .map(|before| format!("&endTime={}", before.saturating_sub(1)))
        .unwrap_or_default();
    let value = get(
        http,
        &format!(
            "candles?category={}-FUTURES&symbol={native}&interval={interval}&limit={}{cursor}",
            instrument.symbol.quote(),
            limit.clamp(1, 100)
        ),
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
            number(&row[5])?,
            Some(number(&row[6])?),
        )?);
    }
    result.sort_by_key(|b| b.open_time_ms);
    Ok(result)
}
pub async fn book(http: &reqwest::Client, instrument: &Instrument) -> Result<Book> {
    let native = &instrument.native_symbol;
    let value = get(
        http,
        &format!(
            "orderbook?category={}-FUTURES&symbol={native}&limit=20",
            instrument.symbol.quote()
        ),
    )
    .await?;
    let data = &value["data"];
    Ok(Book {
        bids: levels(&data["b"], instrument.contract_size)?,
        asks: levels(&data["a"], instrument.contract_size)?,
        time_ms: stamp(&data["ts"])?,
    })
}

pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let mut result = Vec::new();
    for category in ["USDT-FUTURES", "USDC-FUTURES"] {
        if !instruments
            .iter()
            .any(|i| format!("{}-FUTURES", i.symbol.quote()) == category)
        {
            continue;
        }
        let value = get(http, &format!("tickers?category={category}")).await?;
        for row in array(&value["data"])? {
            let Some(i) = instruments.iter().find(|i| {
                row["symbol"] == i.native_symbol
                    && format!("{}-FUTURES", i.symbol.quote()) == category
            }) else {
                continue;
            };
            let parsed = (|| -> Result<Quote> {
                let last = number(&row["lastPrice"])?;
                if last <= Decimal::ZERO {
                    return Err("inactive ticker".into());
                }
                Ok(Quote {
                    symbol: i.symbol.clone(),
                    last,
                    change_percent: number(&row["price24hPcnt"])? * Decimal::from(100),
                    quote_volume: Some(number(&row["turnover24h"])?),
                    time_ms: stamp(&row["ts"])?,
                    derivatives: Some(derivatives(row)),
                })
            })();
            if let Ok(quote) = parsed {
                result.push(quote);
            }
        }
    }
    Ok(result)
}

fn parse_instrument(row: &Value, category: &str) -> Result<Option<Instrument>> {
    if row["status"] != "online" || row["type"] != "perpetual" || row["category"] != category {
        return Ok(None);
    }
    let base = string(&row["baseCoin"])?;
    let quote = string(&row["quoteCoin"])?;
    if !matches!(quote, "USDT" | "USDC") || format!("{quote}-FUTURES") != category {
        return Ok(None);
    }
    let Ok(symbol) = symbol(base, quote) else {
        return Ok(None);
    };
    let native_symbol = string(&row["symbol"])?;
    if native_symbol.is_empty() || !native_symbol.starts_with(base) {
        return Ok(None);
    }
    Ok(Some(Instrument {
        symbol,
        native_symbol: native_symbol.to_owned(),
        price_tick: Some(number(&row["priceMultiplier"])?),
        price_scale: stamp(&row["pricePrecision"])? as u32,
        quantity_scale: stamp(&row["quantityPrecision"])? as u32,
        contract_size: Decimal::ONE,
    }))
}

fn derivatives(row: &Value) -> DerivativeQuote {
    DerivativeQuote {
        use_local_observation_time: false,
        funding_rate: number(&row["fundingRate"]).ok(),
        funding_time_ms: None,
        next_funding_time_ms: row["nextFundingTime"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|time| *time > 0),
        mark_price: number(&row["markPrice"])
            .ok()
            .filter(|price| *price > Decimal::ZERO),
        index_price: number(&row["indexPrice"])
            .ok()
            .filter(|price| *price > Decimal::ZERO),
        // UTA linear perpetual quantities are base coin; the v2 public OI readback
        // matches this exact v3 ticker field for the same native symbol.
        open_interest_base: number(&row["openInterest"])
            .ok()
            .filter(|quantity| *quantity >= Decimal::ZERO),
        open_interest_native_quantity: None,
        open_interest_native_unit: None,
        open_interest_time_ms: None,
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
            "fills?category={}-FUTURES&symbol={}&limit=100",
            instrument.symbol.quote(),
            instrument.native_symbol
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
            string(&row["execId"])?.into(),
            stamp(&row["ts"])?,
            now,
            generation,
            number(&row["price"])?,
            number(&row["size"])? * instrument.contract_size,
            side,
        )?);
    }
    result.sort_by_key(|t| t.transaction_time_ms);
    Ok(result)
}

#[cfg(test)]
mod derivative_tests {
    use super::*;

    #[test]
    fn uta_ticker_preserves_funding_sign_and_base_open_interest() {
        let row = serde_json::json!({"symbol":"DOGEUSDT", "fundingRate":"-0.0001",
            "markPrice":"0.09772", "indexPrice":"0.09773",
            "openInterest":"1238599464", "nextFundingTime":"1790524800000"});
        let value = derivatives(&row);
        assert_eq!(value.funding_rate, Some(Decimal::new(-1, 4)));
        assert_eq!(
            value.open_interest_base,
            Some(Decimal::from(1_238_599_464_u64))
        );
        assert_eq!(value.next_funding_time_ms, Some(1_790_524_800_000));
        assert_eq!(
            derivatives(&serde_json::json!({"openInterest":"-1"})).open_interest_base,
            None
        );
    }

    #[test]
    fn usdc_perpetual_uses_catalog_native_symbol() -> Result<()> {
        let row = serde_json::json!({"symbol":"DOGEPERP", "category":"USDC-FUTURES",
            "baseCoin":"DOGE", "quoteCoin":"USDC", "status":"online", "type":"perpetual",
            "priceMultiplier":"0.00001", "pricePrecision":"5", "quantityPrecision":"0"});
        let instrument = parse_instrument(&row, "USDC-FUTURES")?.ok_or("missing instrument")?;
        assert_eq!(instrument.symbol.to_string(), "DOGE/USDC");
        assert_eq!(instrument.native_symbol, "DOGEPERP");
        assert!(parse_instrument(&row, "USDT-FUTURES")?.is_none());
        Ok(())
    }
}
