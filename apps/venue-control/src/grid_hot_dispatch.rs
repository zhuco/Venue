//! Process-local acceleration from one committed Grid plan batch to the singleton exchange
//! adapter. PostgreSQL remains authority; losing or rejecting a token only selects cold signed
//! preflight and can never recreate a command.

use std::{collections::BTreeMap, sync::Arc};

use venue_domain::Symbol;
use venue_gateway_binance::BinanceInstrumentRules;

const MAX_GRID_HOT_DISPATCH_TOKENS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridHotDispatchToken {
    pub batch_id: String,
    pub batch_digest: [u8; 32],
    pub owner_user_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub private_generation: u64,
    pub private_observed_ms: u64,
    pub source_event_received_ms: u64,
    pub valid_until_ms: u64,
    pub rules: BinanceInstrumentRules,
}

impl GridHotDispatchToken {
    #[must_use]
    pub fn valid(&self) -> bool {
        !self.batch_id.is_empty()
            && self.batch_id.len() <= 64
            && !self.owner_user_id.is_empty()
            && !self.trading_account_id.is_empty()
            && !self.credential_id.is_empty()
            && self.private_generation > 0
            && self.private_observed_ms > 0
            && self.source_event_received_ms >= self.private_observed_ms
            && self.valid_until_ms >= self.source_event_received_ms
            && self.symbol == self.rules.instrument.symbol
            && self.rules.instrument.generation > 0
    }
}

#[derive(Clone, Default)]
pub struct GridHotDispatchCache {
    inner: Arc<parking_lot::Mutex<BTreeMap<String, GridHotDispatchToken>>>,
}

impl GridHotDispatchCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publishes only an exact, bounded token. A conflicting duplicate is removed so the
    /// consumer must use cold signed preflight rather than guessing which in-memory fact won.
    #[must_use]
    pub fn publish(&self, token: GridHotDispatchToken) -> bool {
        if !token.valid() {
            return false;
        }
        let mut tokens = self.inner.lock();
        if let Some(existing) = tokens.get(&token.batch_id) {
            if existing == &token {
                return true;
            }
            tokens.remove(&token.batch_id);
            return false;
        }
        if tokens.len() >= MAX_GRID_HOT_DISPATCH_TOKENS
            && let Some(oldest) = tokens
                .values()
                .min_by_key(|candidate| (candidate.valid_until_ms, candidate.batch_id.as_str()))
                .map(|candidate| candidate.batch_id.clone())
        {
            tokens.remove(&oldest);
        }
        tokens.insert(token.batch_id.clone(), token);
        true
    }

    /// Consumes at most once. Callers must validate every durable identity and freshness field;
    /// returning the token is not dispatch authority on its own.
    pub fn take(&self, batch_id: &str) -> Option<GridHotDispatchToken> {
        self.inner.lock().remove(batch_id)
    }

    pub fn invalidate_credential(&self, credential_id: &str) {
        self.inner
            .lock()
            .retain(|_, token| token.credential_id != credential_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(batch: &str) -> Result<GridHotDispatchToken, Box<dyn std::error::Error>> {
        let payload = include_str!(
            "../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        );
        let rules = venue_gateway_binance::parse_instrument_rules(payload, "BTC/USDT".parse()?, 7)?;
        Ok(GridHotDispatchToken {
            batch_id: batch.to_owned(),
            batch_digest: [3; 32],
            owner_user_id: "owner".to_owned(),
            trading_account_id: "account".to_owned(),
            credential_id: "credential".to_owned(),
            symbol: "BTC/USDT".parse()?,
            private_generation: 9,
            private_observed_ms: 100,
            source_event_received_ms: 101,
            valid_until_ms: 200,
            rules,
        })
    }

    #[test]
    fn token_is_one_shot_and_conflicting_duplicate_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = GridHotDispatchCache::new();
        let first = token("batch")?;
        assert!(cache.publish(first.clone()));
        assert!(cache.publish(first.clone()));
        let mut conflict = first;
        conflict.batch_digest = [4; 32];
        assert!(!cache.publish(conflict));
        assert!(cache.take("batch").is_none());

        assert!(cache.publish(token("once")?));
        assert!(cache.take("once").is_some());
        assert!(cache.take("once").is_none());
        Ok(())
    }

    #[test]
    fn invalidation_is_scoped_to_one_credential() -> Result<(), Box<dyn std::error::Error>> {
        let cache = GridHotDispatchCache::new();
        assert!(cache.publish(token("first")?));
        let mut other = token("second")?;
        other.credential_id = "other".to_owned();
        assert!(cache.publish(other));
        cache.invalidate_credential("credential");
        assert!(cache.take("first").is_none());
        assert!(cache.take("second").is_some());
        Ok(())
    }
}
