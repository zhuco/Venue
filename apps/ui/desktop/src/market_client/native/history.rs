use super::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Default)]
pub(super) struct InitialHistoryCache {
    entries: VecDeque<(MarketSelection, Vec<PublicBar>)>,
    shared_minutes: VecDeque<(venue_gateway_api::PublicMarketBinding, Vec<PublicBar>)>,
    preview_generation: u64,
    previewed: BTreeSet<MarketSelection>,
}

impl InitialHistoryCache {
    pub(super) fn preview_once(&mut self, selection: &MarketSelection, generation: u64) -> bool {
        if self.preview_generation != generation {
            self.preview_generation = generation;
            self.previewed.clear();
        }
        self.previewed.insert(selection.clone())
    }

    pub(super) fn get(&self, selection: &MarketSelection) -> Option<&[PublicBar]> {
        self.entries
            .iter()
            .find(|(key, _)| key == selection)
            .map(|(_, bars)| bars.as_slice())
    }

    pub(super) fn remember(&mut self, selection: &MarketSelection, bars: &[PublicBar]) {
        if bars.is_empty() {
            return;
        }
        self.entries.retain(|(key, _)| key != selection);
        self.entries.push_back((selection.clone(), bars.to_vec()));
        while self.entries.len() > MAX_SUBSCRIPTIONS {
            self.entries.pop_front();
        }
    }

    pub(super) fn shared_minutes(
        &self,
        binding: &venue_gateway_api::PublicMarketBinding,
    ) -> Option<&[PublicBar]> {
        self.shared_minutes
            .iter()
            .find(|(key, _)| key == binding)
            .map(|(_, bars)| bars.as_slice())
    }

    pub(super) fn remember_shared_minutes(
        &mut self,
        binding: &venue_gateway_api::PublicMarketBinding,
        bars: &[PublicBar],
    ) {
        if bars.is_empty() {
            return;
        }
        self.shared_minutes.retain(|(key, _)| key != binding);
        self.shared_minutes
            .push_back((binding.clone(), bars.to_vec()));
        while self.shared_minutes.len() > MAX_SUBSCRIPTIONS {
            self.shared_minutes.pop_front();
        }
    }
}

pub(super) fn merge_shared_minutes(
    cached: Option<Vec<PublicBar>>,
    fresh: Vec<PublicBar>,
    generation: u64,
) -> Vec<PublicBar> {
    let cached = if let (Some(previous), Some(next)) =
        (cached.as_ref().and_then(|bars| bars.last()), fresh.first())
    {
        if previous.open_time_ms.saturating_add(previous.interval_ms) < next.open_time_ms {
            None
        } else {
            cached
        }
    } else {
        cached
    };
    let mut by_open = BTreeMap::new();
    for mut bar in cached.into_iter().flatten().chain(fresh) {
        bar.generation = generation;
        by_open.insert(bar.open_time_ms, bar);
    }
    by_open
        .into_values()
        .rev()
        .take(1_500)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(super) fn missing_limit(
    cached: Option<&[PublicBar]>,
    interval: ChartInterval,
    now_ms: u64,
) -> usize {
    let Some(last) = cached.and_then(|bars| bars.last()) else {
        return INITIAL_VISIBLE_HISTORY_LIMIT;
    };
    let duration = interval.duration_ms();
    let current_open = now_ms - now_ms % duration;
    let next_open = last.open_time_ms.saturating_add(duration);
    if next_open >= current_open {
        return 0;
    }
    let missing = (current_open - next_open) / duration;
    usize::try_from(missing.saturating_add(2))
        .unwrap_or(INITIAL_VISIBLE_HISTORY_LIMIT)
        .min(INITIAL_VISIBLE_HISTORY_LIMIT)
}

pub(super) fn merge_latest(
    cached: Option<Vec<PublicBar>>,
    fresh: Vec<PublicBar>,
    generation: u64,
) -> Vec<PublicBar> {
    // A long offline gap cannot be drawn as if old and new candles were adjacent.
    let cached = if let (Some(previous), Some(next)) =
        (cached.as_ref().and_then(|bars| bars.last()), fresh.first())
    {
        if previous.open_time_ms.saturating_add(previous.interval_ms) < next.open_time_ms {
            None
        } else {
            cached
        }
    } else {
        cached
    };
    let mut by_open = BTreeMap::new();
    for mut bar in cached.into_iter().flatten().chain(fresh) {
        bar.generation = generation;
        by_open.insert(bar.open_time_ms, bar);
    }
    by_open
        .into_values()
        .rev()
        .take(DEFAULT_HISTORY_LIMIT)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

pub(super) fn batch<F: std::future::Future>(
    requests: impl futures_util::Stream<Item = F>,
) -> impl futures_util::Stream<Item = F::Output> {
    requests.buffer_unordered(2)
}

pub(super) fn url(
    selection: &MarketSelection,
    limit: usize,
    before: Option<u64>,
) -> Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(&rest_klines_url(selection, limit)?)
        .map_err(|_| "invalid history URL".to_owned())?;
    if let Some(before) = before {
        let end = before
            .checked_sub(1)
            .ok_or_else(|| "invalid history cursor".to_owned())?;
        url.query_pairs_mut()
            .append_pair("endTime", &end.to_string());
    }
    Ok(url)
}

pub(super) fn start(
    http: reqwest::Client,
    requests: Receiver<crate::market::HistoryRequest>,
    events: MarketSender,
) {
    tokio::spawn(async move {
        loop {
            let request = match requests.try_recv() {
                Ok(request) => request,
                Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let result = if request.generation == 0 || request.selection.validate().is_err() {
                Err("invalid history scope".to_owned())
            } else {
                fetch_history(
                    &http,
                    &request.selection,
                    request.generation,
                    500,
                    Some(request.before),
                )
                .await
                .map(|(bars, _, _, _)| bars)
            };
            // History can be retried, but a lost completion must not leave the UI permanently busy.
            if events
                .send_timeout(
                    LocalMarketClientEvent::History { request, result },
                    COMMAND_SEND_TIMEOUT,
                )
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

pub(super) fn start_shared(
    http: reqwest::Client,
    requests: Receiver<crate::market::SharedHistoryRequest>,
    events: MarketSender,
    source_demands: tokio::sync::watch::Receiver<SourceDemands>,
) {
    tokio::spawn(async move {
        loop {
            let request = match requests.try_recv() {
                Ok(request) => request,
                Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let selection = MarketSelection {
                binding: request.binding.clone(),
                interval: request.interval,
            };
            let valid = request.generation != 0
                && selection.validate().is_ok()
                && matches!(
                    request.interval,
                    ChartInterval::OneMinute | ChartInterval::OneDay
                );
            let demanded =
                source_is_requested(&source_demands.borrow(), &request.binding, request.interval);
            let cached = (valid && demanded)
                .then(|| {
                    public_cache::PublicHistoryCache::local()
                        .and_then(|cache| {
                            cache.page(&selection, request.before, request.generation)
                        })
                        .filter(|bars| {
                            crate::market::history_page_covers_gap(
                                bars,
                                request.before,
                                request.gap_after,
                            )
                        })
                })
                .flatten();
            let cache_hit = cached.is_some();
            let result = if !valid {
                Err("invalid shared history scope".to_owned())
            } else if !demanded {
                Err("shared source has no consumer".to_owned())
            } else if let Some(bars) = cached {
                Ok(bars)
            } else {
                tokio::select! {
                    result = fetch_history_inner(&http, &selection, request.generation, 500,
                        Some(request.before), request.gap_after) =>
                        result.map(|(bars, _, _, _)| bars),
                    _ = wait_until_source_disabled(source_demands.clone(), request.binding.clone(),
                        request.interval) => Err("shared source has no consumer".to_owned()),
                }
            };
            if events
                .send_timeout(
                    LocalMarketClientEvent::SharedHistory { request, result },
                    COMMAND_SEND_TIMEOUT,
                )
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(if cache_hit { 10 } else { 200 })).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::{FieldState, Price};

    fn bar(open_time_ms: u64, close: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let price = Price::new(Decimal::from(close))?;
        Ok(PublicBar {
            symbol: "BTC/USDT".parse()?,
            generation: 1,
            received_at_ms: open_time_ms + 60_000,
            sequence: open_time_ms / 60_000,
            open_time_ms,
            close_time_ms: open_time_ms + 59_999,
            interval_ms: 60_000,
            open: price,
            high: price,
            low: price,
            close: price,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(Decimal::from(close)),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        })
    }

    #[test]
    fn returning_to_current_chart_makes_no_rest_request_and_gap_fetch_is_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let cached = vec![bar(60_000, 10)?, bar(120_000, 11)?];
        assert_eq!(
            missing_limit(Some(&cached), ChartInterval::OneMinute, 180_500),
            0
        );
        assert_eq!(
            missing_limit(Some(&cached), ChartInterval::OneMinute, 300_500),
            4
        );
        assert_eq!(
            missing_limit(None, ChartInterval::OneMinute, 300_500),
            INITIAL_VISIBLE_HISTORY_LIMIT
        );
        assert_eq!(
            missing_limit(Some(&cached), ChartInterval::OneMinute, 90_000_000),
            INITIAL_VISIBLE_HISTORY_LIMIT
        );
        let merged = merge_latest(Some(cached), vec![bar(120_000, 12)?, bar(180_000, 13)?], 4);
        assert_eq!(
            merged
                .iter()
                .map(|bar| bar.open_time_ms)
                .collect::<Vec<_>>(),
            vec![60_000, 120_000, 180_000]
        );
        assert_eq!(merged[1].close.value(), Decimal::from(12));
        assert!(merged.iter().all(|bar| bar.generation == 4));
        Ok(())
    }
    #[tokio::test]
    async fn batch_delivers_ready_chart_before_slow_chart_with_exactly_two_in_flight() {
        use futures_util::FutureExt;
        let started = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (first_tx, first_rx) = tokio::sync::oneshot::channel::<usize>();
        let (second_tx, second_rx) = tokio::sync::oneshot::channel::<usize>();
        let (third_tx, third_rx) = tokio::sync::oneshot::channel::<usize>();
        let mut results = batch(futures_util::stream::iter(
            [first_rx, second_rx, third_rx].into_iter().map(|rx| {
                let started = started.clone();
                async move {
                    started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    rx.await.unwrap()
                }
            }),
        ));
        assert!(results.next().now_or_never().is_none());
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 2);
        second_tx.send(2).unwrap();
        assert_eq!(results.next().await, Some(2));
        assert!(results.next().now_or_never().is_none());
        assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), 3);
        third_tx.send(3).unwrap();
        assert_eq!(results.next().await, Some(3));
        first_tx.send(1).unwrap();
        assert_eq!(results.next().await, Some(1));
        assert_eq!(results.next().await, None);
    }
    #[test]
    fn backward_page_is_public_scoped_and_excludes_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let selection = MarketSelection::binance_usd_m("BTC/USDC", ChartInterval::OneMinute)?;
        let page = url(&selection, 500, Some(180_000))?;
        assert_eq!(page.scheme(), "https");
        assert_eq!(page.host_str(), Some("clawdbotweb.site"));
        assert_eq!(page.path(), "/fapi/v1/klines");
        let query = page
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(query.get("endTime").map(|s| s.as_ref()), Some("179999"));
        assert_eq!(query.get("symbol").map(|s| s.as_ref()), Some("BTCUSDC"));
        assert_eq!(query.get("interval").map(|s| s.as_ref()), Some("1m"));
        assert!(!page.as_str().contains("signature"));
        assert!(url(&selection, 500, Some(0)).is_err());
        Ok(())
    }
}
