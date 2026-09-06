//! Gate USDT perpetual display. Contract quantities are converted using the public multiplier.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::PublicBar;
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/gate/api/v4/futures/usdt";
pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let value = get(http, "tickers").await?;
    let mut result = Vec::new();
    for row in array(&value)? {
        let Some(i) = instruments
            .iter()
            .find(|i| row["contract"] == format!("{}_USDT", i.symbol.base()))
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
            })
        })();
        if let Ok(q) = parsed {
            result.push(q);
        }
    }
    Ok(result)
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
    let cursor = before
        .map(|b| {
            format!(
                "&to={}&from={}",
                b.saturating_sub(1) / 1000,
                b.saturating_sub(ms * 200) / 1000
            )
        })
        .unwrap_or_else(|| "&limit=200".into());
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
