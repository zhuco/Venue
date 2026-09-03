use venue_execution::{
    AccountPhysicalGateway, AccountRecoveryRequest, AccountSymbolSet, SignedAccountSnapshot,
};

use super::{BinanceAccountGateway, BinanceAccountGatewayError};

impl BinanceAccountGateway {
    /// Rewinds only affected symbols for authenticated historical repair. No execution permit
    /// or account state is changed; the normal signed pagination and durable dedup still apply.
    pub fn replay_projection_fills_from(
        previous: Option<&str>,
        from: &std::collections::BTreeMap<venue_domain::Symbol, u64>,
    ) -> Result<Option<String>, BinanceAccountGatewayError> {
        if from.is_empty() {
            return Ok(previous.map(str::to_owned));
        }
        let mut cursor = super::parse_snapshot_fills_cursor(previous)
            .map_err(|_| BinanceAccountGatewayError::Binding)?;
        for (symbol, time) in from {
            let start = time
                .checked_sub(1)
                .filter(|time| *time > 0)
                .ok_or(BinanceAccountGatewayError::Binding)?;
            cursor.by_native_symbol.insert(
                crate::native_symbol(symbol),
                super::RecentFillsCursor {
                    observed_through_ms: start,
                    last_trade_id: None,
                    last_event_time_ms: None,
                },
            );
        }
        Ok(Some(cursor.encode()))
    }

    /// Collects a complete, normalized signed account snapshot for a secret-free durable read
    /// model. It grants no dispatch permit and never exposes raw PAPI payloads or credentials.
    pub fn signed_projection_snapshot(
        &mut self,
        previous_fills_cursor: Option<String>,
    ) -> Result<SignedAccountSnapshot, BinanceAccountGatewayError> {
        let configured_symbols = AccountSymbolSet::new(
            self.config.gateway_binding(),
            self.rules_by_symbol.keys().cloned(),
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let request = AccountRecoveryRequest::read_only(
            self.config.gateway_binding().clone(),
            configured_symbols,
            previous_fills_cursor,
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        <Self as AccountPhysicalGateway>::signed_account_snapshot(self, &request)
            .map_err(|_| BinanceAccountGatewayError::Readback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_cursor_rewinds_only_affected_symbol() -> Result<(), Box<dyn std::error::Error>> {
        let previous = "binance-fills-v1|BTCUSDT,1000,42,999;SOLUSDC,2000,87,1999";
        let from = [("SOL/USDC".parse()?, 1500)].into_iter().collect();
        assert_eq!(
            BinanceAccountGateway::replay_projection_fills_from(Some(previous), &from)?,
            Some("binance-fills-v1|BTCUSDT,1000,42,999;SOLUSDC,1499,,".to_owned())
        );
        assert!(
            BinanceAccountGateway::replay_projection_fills_from(Some("broken"), &from).is_err()
        );
        let invalid = [("SOL/USDC".parse()?, 0)].into_iter().collect();
        assert!(
            BinanceAccountGateway::replay_projection_fills_from(Some(previous), &invalid).is_err()
        );
        Ok(())
    }
}
