pub mod stream;
// Public perpetual market display via the fixed Venue HTTPS relay.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::PublicBar;
use venue_gateway_api::display::*;
const ORIGIN: &str = "https://clawdbotweb.site/quotes/bybit/v5/market";
pub async fn synchronize_display_clock(http: &reqwest::Client) -> Result<()> {
    let started = std::time::Instant::now();
    let value = get(http, "time").await?;
    let nanos = stamp(&value["result"]["timeNano"])?;
    let millis = nanos / 1_000_000;
    if millis.abs_diff(stamp(&value["time"])?) > 1_000 {
        return Err("inconsistent public server time".into());
    }
    clock::synchronize(millis, started)
}
async fn get(http: &reqwest::Client, path: &str) -> Result<Value> {
    let value = json(http, http.get(format!("{ORIGIN}/{path}"))).await?;
    if value["retCode"] != 0 {
        return Err("bybit public request rejected".into());
    }
    Ok(value)
}
pub async fn catalog(http: &reqwest::Client) -> Result<Vec<Instrument>> {
    let value = get(http, "instruments-info?category=linear&limit=1000").await?;
    let mut result = Vec::new();
    for row in array(&value["result"]["list"])? {
        if row["status"] != "Trading" || row["contractType"] != "LinearPerpetual" {
            continue;
        }
        let base = string(&row["baseCoin"])?;
        let quote = string(&row["quoteCoin"])?;
        if !matches!(quote, "USDT" | "USDC") || row["symbol"] != format!("{base}{quote}") {
            continue;
        }
        let Ok(symbol) = symbol(base, quote) else {
            continue;
        };
        result.push(Instrument {
            symbol,
            price_scale: number(&row["priceFilter"]["tickSize"])?.normalize().scale(),
            quantity_scale: number(&row["lotSizeFilter"]["qtyStep"])?
                .normalize()
                .scale(),
            contract_size: Decimal::ONE,
        });
    }
    if value["result"]["nextPageCursor"]
        .as_str()
        .is_some_and(|s| !s.is_empty())
    {
        return Err("Bybit catalogue pagination required".into());
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
) -> Result<Vec<PublicBar>> {
    let native = format!("{}{}", instrument.symbol.base(), instrument.symbol.quote());
    let interval = interval(ms)?;
    let cursor = before
        .map(|before| format!("&end={}", before.saturating_sub(1)))
        .unwrap_or_default();
    let value = get(
        http,
        &format!("kline?category=linear&symbol={native}&interval={interval}&limit=200{cursor}"),
    )
    .await?;
    let mut result = Vec::new();
    for row in array(&value["result"]["list"])? {
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
    let native = format!("{}{}", instrument.symbol.base(), instrument.symbol.quote());
    let value = get(
        http,
        &format!("orderbook?category=linear&symbol={native}&limit=25"),
    )
    .await?;
    let data = &value["result"];
    Ok(Book {
        bids: levels(&data["b"], instrument.contract_size)?,
        asks: levels(&data["a"], instrument.contract_size)?,
        time_ms: stamp(&data["ts"])?,
    })
}

pub async fn quotes(http: &reqwest::Client, instruments: &[Instrument]) -> Result<Vec<Quote>> {
    let value = get(http, "tickers?category=linear").await?;
    let mut result = Vec::new();
    for row in array(&value["result"]["list"])? {
        let Some(i) = instruments
            .iter()
            .find(|i| row["symbol"] == format!("{}{}", i.symbol.base(), i.symbol.quote()))
        else {
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
                time_ms: stamp(&value["time"])?,
            })
        })();
        if let Ok(quote) = parsed {
            result.push(quote);
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
            "recent-trade?category=linear&symbol={}{}&limit=100",
            instrument.symbol.base(),
            instrument.symbol.quote()
        ),
    )
    .await?;
    let now = received_ms()?;
    let mut result = Vec::new();
    for row in array(&value["result"]["list"])? {
        let side = match string(&row["side"])? {
            "Buy" | "buy" => venue_domain::AggressorSide::Buy,
            "Sell" | "sell" => venue_domain::AggressorSide::Sell,
            _ => return Err("invalid aggressor".into()),
        };
        result.push(trade(
            &instrument.symbol,
            string(&row["execId"])?.into(),
            stamp(&row["time"])?,
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

fn interval(ms: u64) -> Result<&'static str> {
    match ms {
        60_000 => Ok("1"),
        300_000 => Ok("5"),
        900_000 => Ok("15"),
        3_600_000 => Ok("60"),
        14_400_000 => Ok("240"),
        86_400_000 => Ok("D"),
        _ => return Err("unsupported interval".into()),
    }
}
