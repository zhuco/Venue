use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use venue_domain::OpenInterestSample;
use venue_gateway_api::PublicMarketBinding;
use venue_gateway_binance::{
    parse_public_open_interest_current, parse_public_open_interest_history,
};

const CURRENT_URL: &str = "https://clawdbotweb.site/fapi/v1/openInterest";
const HISTORY_URL: &str = "https://clawdbotweb.site/futures/data/openInterestHist";
// Subscription replacement must not bypass an exchange-wide rate-limit cooldown.
static SOURCE_DUE_MS: AtomicU64 = AtomicU64::new(0);

pub(super) struct Poller(tokio::task::JoinHandle<()>);

impl Drop for Poller {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
struct PollState {
    current_due_ms: u64,
    history_due_ms: u64,
    last_history_bucket: Option<u64>,
    current_failures: u8,
}

pub(super) fn start(
    generation: u64,
    selections: &[MarketSelection],
    http: reqwest::Client,
    events: MarketSender,
) -> Poller {
    let scopes = group_selections(selections);
    Poller(tokio::spawn(async move {
        let mut states = scopes
            .keys()
            .cloned()
            .map(|binding| (binding, PollState::default()))
            .collect::<BTreeMap<_, _>>();
        loop {
            for (binding, destinations) in &scopes {
                let Some(state) = states.get_mut(binding) else {
                    continue;
                };
                let now = now_ms();
                if now < SOURCE_DUE_MS.load(Ordering::Relaxed) {
                    break;
                }
                if now >= state.current_due_ms {
                    match fetch_current(&http, binding, generation).await {
                        Ok(sample) => {
                            state.current_failures = 0;
                            state.current_due_ms = now_ms().saturating_add(15_000);
                            if !publish(
                                destinations,
                                generation,
                                sample.exchange_time_ms,
                                sample.received_at_ms,
                                MarketPayload::OpenInterestCurrent(sample),
                                &events,
                            ) {
                                return;
                            }
                        }
                        Err(error) => {
                            state.current_failures =
                                state.current_failures.saturating_add(1).min(5);
                            let backoff = 15_u64.saturating_mul(1 << (state.current_failures - 1));
                            state.current_due_ms = error.retry_at_ms(now_ms(), backoff);
                            if error.rate_limited {
                                SOURCE_DUE_MS.fetch_max(state.current_due_ms, Ordering::Relaxed);
                            }
                            if !publish(
                                destinations,
                                generation,
                                now,
                                now,
                                MarketPayload::OpenInterestUnavailable(error.detail),
                                &events,
                            ) {
                                return;
                            }
                        }
                    }
                }
                if now_ms() < SOURCE_DUE_MS.load(Ordering::Relaxed) {
                    continue;
                }
                let bucket = now / 300_000;
                if state.last_history_bucket != Some(bucket) && now >= state.history_due_ms {
                    let cache = public_cache::PublicHistoryCache::local();
                    let cached = cache
                        .as_ref()
                        .and_then(|cache| cache.interest(binding, generation));
                    let result = if cached.as_ref().is_some_and(|samples| {
                        public_cache::PublicHistoryCache::interest_fresh(samples, now)
                    }) {
                        Ok(cached.unwrap_or_default())
                    } else {
                        fetch_history(&http, binding, generation)
                            .await
                            .map(|fresh| {
                                cache
                                    .as_ref()
                                    .and_then(|cache| {
                                        cache.remember_interest(binding, generation, fresh.clone())
                                    })
                                    .unwrap_or(fresh)
                            })
                    };
                    match result {
                        Ok(samples) => {
                            state.last_history_bucket = Some(bucket);
                            state.history_due_ms = bucket
                                .saturating_add(1)
                                .saturating_mul(300_000)
                                .saturating_add(5_000);
                            let received = samples
                                .last()
                                .map_or(now_ms(), |sample| sample.received_at_ms);
                            let event_time = samples
                                .last()
                                .map_or(received, |sample| sample.exchange_time_ms);
                            if !publish(
                                destinations,
                                generation,
                                event_time,
                                received,
                                MarketPayload::OpenInterestHistory(samples),
                                &events,
                            ) {
                                return;
                            }
                        }
                        Err(error) => {
                            state.history_due_ms = error.retry_at_ms(now_ms(), 60);
                            if error.rate_limited {
                                SOURCE_DUE_MS.fetch_max(state.history_due_ms, Ordering::Relaxed);
                            }
                            if !publish(
                                destinations,
                                generation,
                                now,
                                now,
                                MarketPayload::OpenInterestHistoryUnavailable(error.detail),
                                &events,
                            ) {
                                return;
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }))
}

fn group_selections(
    selections: &[MarketSelection],
) -> BTreeMap<PublicMarketBinding, Vec<MarketSelection>> {
    let mut scopes = BTreeMap::<PublicMarketBinding, Vec<MarketSelection>>::new();
    for selection in selections {
        scopes
            .entry(selection.binding.clone())
            .or_default()
            .push(selection.clone());
    }
    scopes
}

fn publish(
    selections: &[MarketSelection],
    generation: u64,
    event_time_ms: u64,
    received_ms: u64,
    payload: MarketPayload,
    events: &MarketSender,
) -> bool {
    for selection in selections {
        let envelope = MarketEnvelope {
            generation,
            selection: selection.clone(),
            event_time_ms: event_time_ms.min(received_ms),
            received_ms,
            payload: payload.clone(),
        };
        if events
            .send_timeout(
                LocalMarketClientEvent::Market(Box::new(envelope)),
                COMMAND_SEND_TIMEOUT,
            )
            .is_err()
        {
            return false;
        }
    }
    true
}

struct FetchError {
    detail: String,
    retry_seconds: u64,
    relay_fallback: bool,
    rate_limited: bool,
}

impl FetchError {
    fn retry_at_ms(&self, now: u64, backoff_seconds: u64) -> u64 {
        // The local exponential backoff is bounded; an upstream cooldown must not be shortened.
        now.saturating_add(
            self.retry_seconds
                .max(backoff_seconds)
                .saturating_mul(1_000),
        )
    }
}

async fn fetch_current(
    http: &reqwest::Client,
    binding: &PublicMarketBinding,
    generation: u64,
) -> Result<OpenInterestSample, FetchError> {
    let url = current_url(binding)?;
    let payload = fetch_body(http, url).await?;
    parse_public_open_interest_current(&payload, binding, generation, now_ms()).map_err(|error| {
        FetchError {
            detail: format!("OI current parse: {error}"),
            retry_seconds: 30,
            relay_fallback: false,
            rate_limited: false,
        }
    })
}

async fn fetch_history(
    http: &reqwest::Client,
    binding: &PublicMarketBinding,
    generation: u64,
) -> Result<Vec<OpenInterestSample>, FetchError> {
    let url = history_url(binding)?;
    let payload = fetch_body(http, url).await?;
    let received = now_ms();
    let samples = parse_public_open_interest_history(&payload, binding, generation, received)
        .map_err(|error| FetchError {
            detail: format!("OI history parse: {error}"),
            retry_seconds: 60,
            relay_fallback: false,
            rate_limited: false,
        })?;
    let completed = samples
        .into_iter()
        .filter(|sample| sample.exchange_time_ms <= received)
        .collect::<Vec<_>>();
    if completed.is_empty() {
        return Err(FetchError {
            detail: "OI history has no completed samples".into(),
            retry_seconds: 60,
            relay_fallback: false,
            rate_limited: false,
        });
    }
    Ok(completed)
}

fn current_url(binding: &PublicMarketBinding) -> Result<reqwest::Url, FetchError> {
    let mut url = reqwest::Url::parse(CURRENT_URL).map_err(|_| invalid_url())?;
    url.query_pairs_mut()
        .append_pair("symbol", &native_symbol(&binding.symbol));
    Ok(url)
}

fn history_url(binding: &PublicMarketBinding) -> Result<reqwest::Url, FetchError> {
    let mut url = reqwest::Url::parse(HISTORY_URL).map_err(|_| invalid_url())?;
    url.query_pairs_mut()
        .append_pair("symbol", &native_symbol(&binding.symbol))
        .append_pair("period", "5m")
        .append_pair("limit", "300");
    Ok(url)
}

fn invalid_url() -> FetchError {
    FetchError {
        detail: "invalid OI endpoint".into(),
        retry_seconds: 300,
        relay_fallback: false,
        rate_limited: false,
    }
}

async fn fetch_body(http: &reqwest::Client, url: reqwest::Url) -> Result<String, FetchError> {
    let mut direct = url.clone();
    direct
        .set_host(Some("fapi.binance.com"))
        .map_err(|_| invalid_url())?;
    match tokio::time::timeout(Duration::from_secs(2), fetch_once(http, direct)).await {
        Ok(Ok(body)) => Ok(body),
        Ok(Err(error)) if !error.relay_fallback => Err(error),
        _ => fetch_once(http, url).await,
    }
}

async fn fetch_once(http: &reqwest::Client, url: reqwest::Url) -> Result<String, FetchError> {
    let response = http.get(url).send().await.map_err(|error| FetchError {
        detail: format!("OI endpoint unreachable: {}", error.without_url()),
        retry_seconds: 30,
        relay_fallback: true,
        rate_limited: false,
    })?;
    if !response.status().is_success() {
        let retry_seconds = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(30)
            .max(15);
        return Err(FetchError {
            detail: format!("OI HTTP {}", response.status().as_u16()),
            retry_seconds,
            rate_limited: matches!(response.status().as_u16(), 418 | 429),
            relay_fallback: response.status().as_u16() == 403
                || response.status().as_u16() == 451
                || response.status().is_server_error(),
        });
    }
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await.map_err(|error| FetchError {
        detail: format!("OI body failed: {}", error.without_url()),
        retry_seconds: 30,
        relay_fallback: true,
        rate_limited: false,
    })? {
        if body.len().saturating_add(chunk.len()) > HTTP_BODY_LIMIT {
            return Err(FetchError {
                detail: "OI response exceeded 1 MiB".into(),
                retry_seconds: 60,
                relay_fallback: false,
                rate_limited: false,
            });
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|_| FetchError {
        detail: "OI response was not UTF-8".into(),
        retry_seconds: 60,
        relay_fallback: false,
        rate_limited: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oi_http_429_honors_retry_after_without_falling_back_to_another_source()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await?;
            socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 3600\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
            Ok::<(), std::io::Error>(())
        });
        let url = reqwest::Url::parse(&format!("http://{address}/open-interest"))?;
        let client = reqwest::Client::builder().no_proxy().build()?;
        let error = fetch_once(&client, url)
            .await
            .err()
            .ok_or_else(|| std::io::Error::other("429 response was accepted"))?;
        assert_eq!(error.detail, "OI HTTP 429");
        assert_eq!(error.retry_seconds, 3600);
        assert_eq!(error.retry_at_ms(10_000, 240), 3_610_000);
        assert_eq!(error.retry_at_ms(u64::MAX, 240), u64::MAX);
        assert!(error.rate_limited);
        assert!(!error.relay_fallback);
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        Ok(())
    }

    #[test]
    fn one_binding_across_intervals_uses_one_oi_source_and_exact_native_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let minute = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute)?;
        let hourly = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneHour)?;
        let other = MarketSelection::binance_usd_m("DOGE/USDT", ChartInterval::OneMinute)?;
        let scopes = group_selections(&[minute.clone(), hourly, other]);
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[&minute.binding].len(), 2);
        let current = current_url(&minute.binding).map_err(|error| error.detail)?;
        assert_eq!(current.query(), Some("symbol=DOGEUSDC"));
        let history = history_url(&minute.binding).map_err(|error| error.detail)?;
        assert_eq!(history.query(), Some("symbol=DOGEUSDC&period=5m&limit=300"));
        Ok(())
    }
}
