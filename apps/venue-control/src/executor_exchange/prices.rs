use super::*;
use venue_domain::domain::Asset;
use venue_gateway_binance::{BinanceMarkPrice, portfolio::UsdConversionEvidence};

const PRICE_TTL: std::time::Duration = std::time::Duration::from_millis(500);
const FAILURE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum PriceKey {
    Mark(Symbol),
    Index(Asset),
}

#[derive(Clone, Default)]
pub(super) struct SharedMarketPrices {
    entries: Arc<tokio::sync::Mutex<BTreeMap<PriceKey, Arc<tokio::sync::Mutex<PriceEntry>>>>>,
}

#[derive(Default)]
struct PriceEntry {
    current: Option<(Instant, Arc<Vec<u8>>)>,
    retry_after: Option<Instant>,
}

impl SharedMarketPrices {
    pub(super) async fn mark(
        &self,
        transport: &BinanceHttpTransport,
        symbol: &Symbol,
    ) -> Result<BinanceMarkPrice, BinanceExecutionError> {
        let payload = self
            .read_with(PriceKey::Mark(symbol.clone()), || async {
                transport
                    .fetch_usd_m_mark_price(&venue_gateway_binance::native_symbol(symbol))
                    .await
                    .map(|response| response.payload.to_vec())
                    .map_err(|_| BinanceExecutionError::Unavailable)
            })
            .await?;
        venue_gateway_binance::parse_mark_price(&payload, symbol, now_ms()?)
            .map_err(|_| BinanceExecutionError::Unavailable)
    }

    pub(super) async fn indices(
        &self,
        transport: &BinanceHttpTransport,
        assets: &BTreeSet<Asset>,
        generation: u64,
    ) -> Result<BTreeMap<Asset, UsdConversionEvidence>, BinanceExecutionError> {
        let mut required = assets
            .iter()
            .filter(|asset| asset.as_str() != "USDT")
            .cloned()
            .collect::<BTreeSet<_>>();
        if !required.is_empty() {
            required.insert(Asset::new("USDT").map_err(|_| BinanceExecutionError::Invalid)?);
        }
        let mut result = BTreeMap::new();
        for asset in required {
            let payload = self
                .read_with(PriceKey::Index(asset.clone()), || async {
                    transport
                        .fetch_usd_m_asset_index(&asset)
                        .await
                        .map(|response| response.payload.to_vec())
                        .map_err(|_| BinanceExecutionError::Unavailable)
                })
                .await?;
            let text = std::str::from_utf8(&payload).map_err(|_| BinanceExecutionError::Invalid)?;
            let value = venue_gateway_binance::portfolio::parse_usd_conversion_evidence(
                text,
                asset.clone(),
                generation,
                now_ms()?,
                5_000,
            )
            .map_err(|_| BinanceExecutionError::Unavailable)?;
            result.insert(asset, value);
        }
        Ok(result)
    }

    async fn read_with<F, Fut>(
        &self,
        key: PriceKey,
        load: F,
    ) -> Result<Arc<Vec<u8>>, BinanceExecutionError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, BinanceExecutionError>>,
    {
        let entry = self.entries.lock().await.entry(key).or_default().clone();
        let mut entry = entry.lock().await;
        let now = Instant::now();
        if let Some((received, payload)) = &entry.current {
            if now.duration_since(*received) < PRICE_TTL {
                return Ok(payload.clone());
            }
        }
        if entry.retry_after.is_some_and(|after| now < after) {
            return Err(BinanceExecutionError::Unavailable);
        }
        match load().await {
            Ok(payload) => {
                let payload = Arc::new(payload);
                entry.current = Some((Instant::now(), payload.clone()));
                entry.retry_after = None;
                Ok(payload)
            }
            Err(error) => {
                entry.retry_after = Some(Instant::now() + FAILURE_BACKOFF);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn two_hundred_accounts_share_prices_without_sharing_account_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = SharedMarketPrices::default();
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..200 {
            let (cache, loads) = (cache.clone(), loads.clone());
            tasks.spawn(async move {
                cache
                    .read_with(
                        PriceKey::Mark(
                            "BTC/USDT"
                                .parse()
                                .map_err(|_| BinanceExecutionError::Invalid)?,
                        ),
                        || async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            tokio::task::yield_now().await;
                            Ok(b"price fixture".to_vec())
                        },
                    )
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn expired_public_price_is_never_fallback_after_refresh_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = SharedMarketPrices::default();
        let key = PriceKey::Mark("BTC/USDT".parse()?);
        cache
            .read_with(key.clone(), || async { Ok(b"old".to_vec()) })
            .await?;
        let entry = cache
            .entries
            .lock()
            .await
            .get(&key)
            .ok_or("missing entry")?
            .clone();
        entry
            .lock()
            .await
            .current
            .as_mut()
            .ok_or("missing price")?
            .0 = Instant::now() - PRICE_TTL;
        assert!(
            cache
                .read_with(key.clone(), || async {
                    Err(BinanceExecutionError::Unavailable)
                })
                .await
                .is_err()
        );
        let called = AtomicBool::new(false);
        assert!(
            cache
                .read_with(key, || async {
                    called.store(true, Ordering::SeqCst);
                    Ok(b"new".to_vec())
                })
                .await
                .is_err()
        );
        assert!(!called.load(Ordering::SeqCst));
        Ok(())
    }
}
