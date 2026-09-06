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
    let value = get(http, "instruments?category=USDT-FUTURES").await?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
        if row["status"] != "online" || row["type"] != "perpetual" {
            continue;
        }
        let base = string(&row["baseCoin"])?;
        let quote = string(&row["quoteCoin"])?;
        if !matches!(quote, "USDT" | "USDC") {
            continue;
        }
        let Ok(symbol) = symbol(base, quote) else {
            continue;
        };
        result.push(Instrument {
            symbol,
            price_scale: stamp(&row["pricePrecision"])? as u32,
            quantity_scale: stamp(&row["quantityPrecision"])? as u32,
            contract_size: Decimal::ONE,
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
) -> Result<Vec<PublicBar>> {
    let native = format!("{}{}", instrument.symbol.base(), instrument.symbol.quote());
    let interval = match ms {
        60_000 => "1m",
        300_000 => "5m",
        900_000 => "15m",
        3_600_000 => "1H",
        14_400_000 => "4H",
        86_400_000 => "1D",
        _ => return Err("unsupported interval".into()),
    };
    let cursor = before
        .map(|before| format!("&endTime={}", before.saturating_sub(1)))
        .unwrap_or_default();
    let value = get(
        http,
        &format!(
            "candles?category={}-FUTURES&symbol={native}&interval={interval}&limit=100{cursor}",
            instrument.symbol.quote()
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
    let native = format!("{}{}", instrument.symbol.base(), instrument.symbol.quote());
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
    let value = get(http, "tickers?category=USDT-FUTURES").await?;
    let mut result = Vec::new();
    for row in array(&value["data"])? {
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
                time_ms: stamp(&row["ts"])?,
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
            "fills?category={}-FUTURES&symbol={}{}&limit=100",
            instrument.symbol.quote(),
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
