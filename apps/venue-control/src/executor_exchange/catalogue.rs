use super::*;
use venue_gateway_binance::BinanceInstrumentRules;

const RULES_TTL: std::time::Duration = std::time::Duration::from_secs(60);
const FAILURE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone, Default)]
pub(super) struct SharedCatalogue {
    state: Arc<tokio::sync::Mutex<CatalogueState>>,
}

#[derive(Default)]
struct CatalogueState {
    current: Option<Catalogue>,
    retry_after: Option<Instant>,
}

struct Catalogue {
    payload: Arc<str>,
    received: Instant,
    rules: BTreeMap<(Symbol, u64), Result<BinanceInstrumentRules, BinanceExecutionError>>,
}

impl SharedCatalogue {
    pub(super) async fn rules(
        &self,
        transport: &BinanceHttpTransport,
        symbol: &Symbol,
    ) -> Result<BinanceInstrumentRules, BinanceExecutionError> {
        self.view(transport, symbol).await.map(|(rules, _)| rules)
    }

    pub(super) async fn view(
        &self,
        transport: &BinanceHttpTransport,
        symbol: &Symbol,
    ) -> Result<(BinanceInstrumentRules, Arc<str>), BinanceExecutionError> {
        self.rules_with(symbol, transport.instrument_generation(), || async {
            let response = transport
                .fetch_usd_m_exchange_info()
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            String::from_utf8(response.payload.to_vec())
                .map_err(|_| BinanceExecutionError::Unavailable)
        })
        .await
    }

    async fn rules_with<F, Fut>(
        &self,
        symbol: &Symbol,
        generation: u64,
        load: F,
    ) -> Result<(BinanceInstrumentRules, Arc<str>), BinanceExecutionError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<String, BinanceExecutionError>>,
    {
        // One refresh for an entire fan-out. Only public catalogue data is shared: account
        // snapshots, credentials and mutation clients are never kept behind this lock.
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if state
            .current
            .as_ref()
            .is_none_or(|cache| now.duration_since(cache.received) >= RULES_TTL)
        {
            if state.retry_after.is_some_and(|retry| now < retry) {
                return Err(BinanceExecutionError::Unavailable);
            }
            match load().await {
                Ok(payload) => {
                    state.current = Some(Catalogue {
                        payload: payload.into(),
                        received: Instant::now(),
                        rules: BTreeMap::new(),
                    });
                    state.retry_after = None;
                }
                Err(error) => {
                    state.retry_after = Some(Instant::now() + FAILURE_BACKOFF);
                    return Err(error);
                }
            }
        }
        let cache = state
            .current
            .as_mut()
            .ok_or(BinanceExecutionError::Unavailable)?;
        let rules = cache
            .rules
            .entry((symbol.clone(), generation))
            .or_insert_with(|| {
                parse_instrument_rules(&cache.payload, symbol.clone(), generation)
                    .map_err(|_| BinanceExecutionError::Invalid)
            })
            .clone()?;
        Ok((rules, cache.payload.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    const CATALOGUE: &str = include_str!(
        "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
    );

    #[tokio::test]
    async fn two_hundred_followers_share_one_catalogue_and_one_normalized_rule()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = SharedCatalogue::default();
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..200 {
            let cache = cache.clone();
            let loads = loads.clone();
            tasks.spawn(async move {
                cache
                    .rules_with(
                        &"BTC/USDT"
                            .parse()
                            .map_err(|_| BinanceExecutionError::Invalid)?,
                        1,
                        || async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Ok(CATALOGUE.into())
                        },
                    )
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result??.0.native_symbol, "BTCUSDT");
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        let state = cache.state.lock().await;
        assert_eq!(state.current.as_ref().ok_or("cache absent")?.rules.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn expired_catalogue_is_not_used_after_refresh_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = SharedCatalogue::default();
        let symbol = "BTC/USDT".parse()?;
        cache
            .rules_with(&symbol, 1, || async { Ok(CATALOGUE.into()) })
            .await?;
        cache
            .state
            .lock()
            .await
            .current
            .as_mut()
            .ok_or("cache absent")?
            .received = Instant::now() - RULES_TTL;
        assert!(
            cache
                .rules_with(&symbol, 1, || async {
                    Err(BinanceExecutionError::Unavailable)
                })
                .await
                .is_err()
        );
        let called = AtomicBool::new(false);
        assert!(
            cache
                .rules_with(&symbol, 1, || async {
                    called.store(true, Ordering::SeqCst);
                    Ok(CATALOGUE.into())
                })
                .await
                .is_err()
        );
        assert!(!called.load(Ordering::SeqCst));
        Ok(())
    }
}
