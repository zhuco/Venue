// Public perpetual market display via the fixed Venue HTTPS relay.
use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::PublicBar;
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
        &format!("market/{endpoint}?instId={native}&bar={interval}&limit=100{cursor}"),
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
