use super::*;

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
                .map(|(bars, _, _)| bars)
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

#[cfg(test)]
mod tests {
    use super::*;
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
