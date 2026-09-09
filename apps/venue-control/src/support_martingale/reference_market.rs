//! Credential-free Binance USD-M reference market reads for support-martingale.
//!
//! This boundary owns HTTP and adapter parsing only. It never accepts credentials and returns
//! normalized facts to the strategy/runtime; execution venue prices remain outside this module.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{StreamExt, stream};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use venue_domain::{PublicBar, PublicTicker, Symbol};
use venue_gateway_api::PublicMarketBinding;
use venue_gateway_binance::{
    BinanceKlineInterval, parse_public_market_rest_bbo, parse_public_market_rest_klines,
};

const USD_M_ORIGIN: &str = "https://fapi.binance.com";
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const BAR_FRESHNESS_GRACE_MS: u64 = 60_000;
const CLOCK_SKEW_GRACE_MS: u64 = 2_000;

#[derive(Clone, Debug)]
pub struct BinanceReferenceClient {
    client: Client,
    origin: String,
    maximum_age_ms: u64,
    bar_cache: Arc<Mutex<BTreeMap<(Symbol, String), Vec<PublicBar>>>>,
}

impl BinanceReferenceClient {
    pub fn new(
        request_timeout: Duration,
        maximum_age_ms: u64,
    ) -> Result<Self, ReferenceMarketError> {
        if request_timeout.is_zero() || maximum_age_ms == 0 {
            return Err(ReferenceMarketError::InvalidLimits);
        }
        let client = Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|_| ReferenceMarketError::HttpClient)?;
        Ok(Self {
            client,
            origin: USD_M_ORIGIN.to_owned(),
            maximum_age_ms,
            bar_cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    #[must_use]
    pub const fn maximum_age_ms(&self) -> u64 {
        self.maximum_age_ms
    }

    /// Fetches all requested symbols and a separate BTC 4h environment input. The returned map is
    /// complete: one missing, stale, malformed, or discontinuous symbol fails the whole snapshot.
    pub async fn fetch_snapshot(
        &self,
        symbols: &[Symbol],
        now_ms: u64,
        generation: u64,
    ) -> Result<ReferenceSnapshot, ReferenceMarketError> {
        if symbols.is_empty() || now_ms == 0 || generation == 0 {
            return Err(ReferenceMarketError::InvalidRequest);
        }
        let btc = Symbol::new("BTC", "USDT").map_err(|_| ReferenceMarketError::InvalidRequest)?;
        let mut unique = BTreeMap::new();
        for symbol in symbols {
            unique.insert(symbol.clone(), ());
        }
        let mut market = BTreeMap::new();
        let mut requests = stream::iter(unique.keys().cloned().map(|symbol| async move {
            self.fetch_symbol(&symbol, now_ms, generation)
                .await
                .map(|reference| (symbol, reference))
        }))
        .buffer_unordered(4);
        while let Some(result) = requests.next().await {
            let (symbol, reference) = result?;
            market.insert(symbol, reference);
        }
        let btc_environment = self
            .fetch_bars(&btc, BinanceKlineInterval::FourHours, now_ms, generation)
            .await?;
        let completed_at = wall_clock_ms()?;
        validate_contiguous(
            &btc_environment,
            BinanceKlineInterval::FourHours,
            &btc,
            now_ms,
        )?;
        for (symbol, reference) in &market {
            validate_contiguous(
                &reference.fifteen_minutes,
                BinanceKlineInterval::FifteenMinutes,
                symbol,
                now_ms,
            )?;
            validate_contiguous(
                &reference.one_hour,
                BinanceKlineInterval::OneHour,
                symbol,
                now_ms,
            )?;
            validate_contiguous(
                &reference.four_hour,
                BinanceKlineInterval::FourHours,
                symbol,
                now_ms,
            )?;
            validate_ticker(&reference.ticker, symbol, self.maximum_age_ms)?;
            if completed_at.saturating_sub(reference.ticker.exchange_time_ms) > self.maximum_age_ms
            {
                return Err(ReferenceMarketError::StaleTicker {
                    symbol: symbol.clone(),
                });
            }
        }
        Ok(ReferenceSnapshot {
            fetched_at_ms: completed_at,
            btc_environment,
            symbols: market,
        })
    }

    async fn fetch_symbol(
        &self,
        symbol: &Symbol,
        now_ms: u64,
        generation: u64,
    ) -> Result<SymbolReference, ReferenceMarketError> {
        let fifteen = self
            .fetch_bars(
                symbol,
                BinanceKlineInterval::FifteenMinutes,
                now_ms,
                generation,
            )
            .await?;
        let one_hour = self
            .fetch_bars(symbol, BinanceKlineInterval::OneHour, now_ms, generation)
            .await?;
        let four_hour = self
            .fetch_bars(symbol, BinanceKlineInterval::FourHours, now_ms, generation)
            .await?;
        validate_contiguous(
            &fifteen,
            BinanceKlineInterval::FifteenMinutes,
            symbol,
            now_ms,
        )?;
        validate_contiguous(&one_hour, BinanceKlineInterval::OneHour, symbol, now_ms)?;
        validate_contiguous(&four_hour, BinanceKlineInterval::FourHours, symbol, now_ms)?;
        let ticker = self.fetch_ticker(symbol, now_ms, generation).await?;
        validate_ticker(&ticker, symbol, self.maximum_age_ms)?;
        Ok(SymbolReference {
            fifteen_minutes: fifteen,
            one_hour,
            four_hour,
            ticker,
        })
    }

    async fn fetch_bars(
        &self,
        symbol: &Symbol,
        interval: BinanceKlineInterval,
        now_ms: u64,
        generation: u64,
    ) -> Result<Vec<PublicBar>, ReferenceMarketError> {
        let key = (symbol.clone(), interval.as_str().to_owned());
        let closed_cycle = (now_ms / interval.milliseconds()).saturating_sub(1);
        if let Some(mut cached) = self.bar_cache.lock().await.get(&key).cloned()
            && cached
                .last()
                .is_some_and(|bar| bar.close_time_ms / interval.milliseconds() == closed_cycle)
        {
            for bar in &mut cached {
                bar.generation = generation;
            }
            return Ok(cached);
        }
        let native = format!("{}{}", symbol.base(), symbol.quote());
        let url = format!(
            "{}/fapi/v1/klines?symbol={}&interval={}&limit=100",
            self.origin,
            native,
            interval.as_str()
        );
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| ReferenceMarketError::Http)?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ReferenceMarketError::Http)?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err(ReferenceMarketError::BodyTooLarge);
        }
        if !status.is_success() {
            return Err(ReferenceMarketError::HttpStatus(status.as_u16()));
        }
        let binding = PublicMarketBinding::binance_usds_m(symbol.clone())
            .map_err(|_| ReferenceMarketError::InvalidRequest)?;
        let payload = std::str::from_utf8(&bytes).map_err(|_| ReferenceMarketError::Payload)?;
        let bars = parse_public_market_rest_klines(
            payload,
            &binding,
            generation,
            wall_clock_ms()?,
            interval,
        )
        .map_err(|_| ReferenceMarketError::Parse)?
        .into_iter()
        .filter(|bar| bar.close_time_ms < now_ms)
        .collect::<Vec<_>>();
        self.bar_cache.lock().await.insert(key, bars.clone());
        Ok(bars)
    }

    async fn fetch_ticker(
        &self,
        symbol: &Symbol,
        _now_ms: u64,
        generation: u64,
    ) -> Result<PublicTicker, ReferenceMarketError> {
        let native = format!("{}{}", symbol.base(), symbol.quote());
        let url = format!(
            "{}/fapi/v1/ticker/bookTicker?symbol={}",
            self.origin, native
        );
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| ReferenceMarketError::Http)?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ReferenceMarketError::Http)?;
        if bytes.len() > MAX_BODY_BYTES {
            return Err(ReferenceMarketError::BodyTooLarge);
        }
        if !status.is_success() {
            return Err(ReferenceMarketError::HttpStatus(status.as_u16()));
        }
        let binding = PublicMarketBinding::binance_usds_m(symbol.clone())
            .map_err(|_| ReferenceMarketError::InvalidRequest)?;
        let payload = std::str::from_utf8(&bytes).map_err(|_| ReferenceMarketError::Payload)?;
        parse_public_market_rest_bbo(payload, &binding, generation, wall_clock_ms()?)
            .map_err(|_| ReferenceMarketError::Parse)
    }
}

fn validate_ticker(
    ticker: &PublicTicker,
    symbol: &Symbol,
    maximum_age_ms: u64,
) -> Result<(), ReferenceMarketError> {
    if ticker.exchange_time_ms > ticker.received_at_ms.saturating_add(CLOCK_SKEW_GRACE_MS)
        || ticker
            .received_at_ms
            .saturating_sub(ticker.exchange_time_ms)
            > maximum_age_ms
    {
        return Err(ReferenceMarketError::StaleTicker {
            symbol: symbol.clone(),
        });
    }
    Ok(())
}

fn wall_clock_ms() -> Result<u64, ReferenceMarketError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| u64::try_from(value.as_millis()).ok())
        .filter(|value| *value > 0)
        .ok_or(ReferenceMarketError::InvalidRequest)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceSnapshot {
    pub fetched_at_ms: u64,
    pub btc_environment: Vec<PublicBar>,
    pub symbols: BTreeMap<Symbol, SymbolReference>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SymbolReference {
    pub fifteen_minutes: Vec<PublicBar>,
    pub one_hour: Vec<PublicBar>,
    pub four_hour: Vec<PublicBar>,
    pub ticker: PublicTicker,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReferenceMarketError {
    #[error("invalid public market client limits")]
    InvalidLimits,
    #[error("invalid public market request")]
    InvalidRequest,
    #[error("unable to build public HTTP client")]
    HttpClient,
    #[error("public market HTTP request failed")]
    Http,
    #[error("public market returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("public market response is too large")]
    BodyTooLarge,
    #[error("public market response is not UTF-8")]
    Payload,
    #[error("public market response failed Binance normalization")]
    Parse,
    #[error("public market ticker is stale for {symbol}")]
    StaleTicker { symbol: Symbol },
    #[error("public market bars are missing, future, or discontinuous for {symbol}")]
    DiscontinuousBars { symbol: Symbol },
}

fn validate_contiguous(
    bars: &[PublicBar],
    interval: BinanceKlineInterval,
    symbol: &Symbol,
    now_ms: u64,
) -> Result<(), ReferenceMarketError> {
    if bars.is_empty() {
        return Err(ReferenceMarketError::DiscontinuousBars {
            symbol: symbol.clone(),
        });
    }
    for window in bars.windows(2) {
        if window[1].open_time_ms
            != window[0]
                .open_time_ms
                .saturating_add(interval.milliseconds())
        {
            return Err(ReferenceMarketError::DiscontinuousBars {
                symbol: symbol.clone(),
            });
        }
    }
    if bars
        .iter()
        .any(|bar| bar.close_time_ms >= now_ms || bar.interval_ms != interval.milliseconds())
    {
        return Err(ReferenceMarketError::DiscontinuousBars {
            symbol: symbol.clone(),
        });
    }
    if bars.last().is_none_or(|bar| {
        now_ms.saturating_sub(bar.close_time_ms)
            > interval
                .milliseconds()
                .saturating_add(BAR_FRESHNESS_GRACE_MS)
    }) {
        return Err(ReferenceMarketError::DiscontinuousBars {
            symbol: symbol.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use super::*;
    use rust_decimal::Decimal;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use venue_domain::Price;

    fn bar(open: u64, interval: u64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let price = Price::new(Decimal::from(100))?;
        Ok(PublicBar {
            symbol,
            generation: 1,
            received_at_ms: 1_000_000,
            sequence: open / interval + 1,
            open_time_ms: open,
            close_time_ms: open + interval - 1,
            interval_ms: interval,
            open: price,
            high: price,
            low: price,
            close: price,
            base_volume: venue_domain::FieldState::Known(Decimal::ONE),
            quote_volume: venue_domain::FieldState::Known(Decimal::from(100)),
            trade_count: venue_domain::FieldState::Known(1),
            taker_buy_base_volume: venue_domain::FieldState::Known(Decimal::ONE),
            taker_buy_quote_volume: venue_domain::FieldState::Known(Decimal::from(100)),
        })
    }

    #[test]
    fn continuity_rejects_a_gap() -> Result<(), Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let bars = vec![bar(0, 900_000)?, bar(1_800_000, 900_000)?];
        assert!(matches!(
            validate_contiguous(
                &bars,
                BinanceKlineInterval::FifteenMinutes,
                &symbol,
                3_000_000
            ),
            Err(ReferenceMarketError::DiscontinuousBars { .. })
        ));
        Ok(())
    }

    #[test]
    fn continuity_accepts_closed_contiguous_bars() -> Result<(), Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let bars = vec![bar(0, 900_000)?, bar(900_000, 900_000)?];
        assert!(
            validate_contiguous(
                &bars,
                BinanceKlineInterval::FifteenMinutes,
                &symbol,
                2_500_000
            )
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn fixture_parser_keeps_only_closed_kline_and_normalizes_book_ticker()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let binding = PublicMarketBinding::binance_usds_m(symbol)?;
        let klines = r#"[[900000,"100","101","99","100","2",1799999,"200",3,"1","100","0"],[1800000,"100","102","99","101","2",2699999,"201",3,"1","101","0"]]"#;
        let bars = parse_public_market_rest_klines(
            klines,
            &binding,
            1,
            3_000_000,
            BinanceKlineInterval::FifteenMinutes,
        )?;
        assert_eq!(bars.len(), 2);
        let ticker = parse_public_market_rest_bbo(
            r#"{"lastUpdateId":7,"symbol":"BTCUSDT","bidPrice":"100","bidQty":"2","askPrice":"101","askQty":"3","time":1900000}"#,
            &binding,
            1,
            2_000_000,
        )?;
        assert_eq!(ticker.bid_price.value(), Decimal::from(100));
        assert_eq!(ticker.ask_price.value(), Decimal::from(101));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires Binance USD-M public REST"]
    async fn mainnet_snapshot_is_complete_for_the_live_candidate_symbols()
    -> Result<(), Box<dyn std::error::Error>> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let symbols = vec![Symbol::new("SOL", "USDT")?, Symbol::new("DOGE", "USDT")?];
        let snapshot = BinanceReferenceClient::new(Duration::from_secs(8), 5_000)?
            .fetch_snapshot(&symbols, now_ms, 1)
            .await?;
        assert_eq!(snapshot.symbols.len(), 2);
        assert!(snapshot.symbols.values().all(|market| {
            market.fifteen_minutes.len() >= 60
                && market.one_hour.len() >= 60
                && market.four_hour.len() >= 60
        }));
        assert!(snapshot.btc_environment.len() >= 60);
        Ok(())
    }

    fn fixture_body(path: &str, now_ms: u64) -> String {
        let symbol = path
            .split("symbol=")
            .nth(1)
            .and_then(|value| value.split('&').next())
            .unwrap_or("SOLUSDT");
        if path.contains("bookTicker") {
            return format!(
                r#"{{"lastUpdateId":7,"symbol":"{symbol}","bidPrice":"100","bidQty":"2","askPrice":"101","askQty":"3","time":{}}}"#,
                now_ms.saturating_sub(100)
            );
        }
        let interval = if path.contains("interval=15m") {
            900_000
        } else if path.contains("interval=1h") {
            3_600_000
        } else {
            14_400_000
        };
        let latest_open = now_ms / interval * interval - interval;
        let previous_open = latest_open - interval;
        format!(
            r#"[[{previous_open},"100","101","99","100","2",{},"200",3,"1","100","0"],[{latest_open},"100","102","99","101","2",{},"201",3,"1","101","0"]]"#,
            previous_open + interval - 1,
            latest_open + interval - 1,
        )
    }

    async fn serve_fixture(
        listener: TcpListener,
        now_ms: Arc<AtomicU64>,
        bar_requests: Arc<AtomicUsize>,
        ticker_requests: Arc<AtomicUsize>,
    ) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let now_ms = Arc::clone(&now_ms);
            let bar_requests = Arc::clone(&bar_requests);
            let ticker_requests = Arc::clone(&ticker_requests);
            tokio::spawn(async move {
                let _ = respond_fixture(
                    stream,
                    now_ms.load(Ordering::Relaxed),
                    &bar_requests,
                    &ticker_requests,
                )
                .await;
            });
        }
    }

    async fn respond_fixture(
        mut stream: TcpStream,
        now_ms: u64,
        bar_requests: &AtomicUsize,
        ticker_requests: &AtomicUsize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut request = [0_u8; 4096];
        let size = stream.read(&mut request).await?;
        let line = std::str::from_utf8(&request[..size])?
            .lines()
            .next()
            .unwrap_or("");
        let path = line.split_whitespace().nth(1).unwrap_or("/");
        if path.contains("bookTicker") {
            ticker_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            bar_requests.fetch_add(1, Ordering::Relaxed);
        }
        let body = fixture_body(path, now_ms);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_caches_closed_bars_but_refreshes_bbo()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let now_ms = Arc::new(AtomicU64::new(wall_clock_ms()?));
        let bar_requests = Arc::new(AtomicUsize::new(0));
        let ticker_requests = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(serve_fixture(
            listener,
            Arc::clone(&now_ms),
            Arc::clone(&bar_requests),
            Arc::clone(&ticker_requests),
        ));
        let client = BinanceReferenceClient {
            client: Client::builder().build()?,
            origin,
            maximum_age_ms: 5_000,
            bar_cache: Arc::new(Mutex::new(BTreeMap::new())),
        };
        let symbol = Symbol::new("SOL", "USDT")?;
        let first = client
            .fetch_snapshot(&[symbol.clone()], now_ms.load(Ordering::Relaxed), 1)
            .await?;
        let second = client
            .fetch_snapshot(&[symbol], now_ms.load(Ordering::Relaxed), 2)
            .await?;
        server.abort();
        assert_eq!(first.symbols.len(), 1);
        assert_eq!(second.symbols.len(), 1);
        assert_eq!(bar_requests.load(Ordering::Relaxed), 4);
        assert_eq!(ticker_requests.load(Ordering::Relaxed), 2);
        Ok(())
    }
}
