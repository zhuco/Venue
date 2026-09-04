use venue_execution::{
    AccountPhysicalGateway, AccountRecoveryRequest, AccountSymbolSet, SignedAccountSnapshot,
};

use super::{BinanceAccountGateway, BinanceAccountGatewayError};

/// One bounded, read-only snapshot job. Its HTTP awaits do not borrow the live user stream.
pub struct BinanceProjectionRead {
    transport: crate::BinanceHttpTransport,
    credentials: crate::BinanceCredentials,
    config: crate::BinanceConfig,
    rules: crate::BinanceInstrumentRules,
    connection_generation: u64,
    prior_generation: u64,
    generation: u64,
    attempt: u64,
    request: AccountRecoveryRequest,
}

pub struct BinanceCompletedProjection {
    read: BinanceProjectionRead,
    snapshot: SignedAccountSnapshot,
}

impl BinanceProjectionRead {
    pub async fn collect(self) -> Result<BinanceCompletedProjection, BinanceAccountGatewayError> {
        let snapshot = super::fetch_account_wide_snapshot(super::BinanceSnapshotCollection {
            transport: &self.transport,
            credentials: &self.credentials,
            config: &self.config,
            selected_rules: &self.rules,
            connection_generation: self.connection_generation,
            private_generation: self.generation,
            rules_generation: self.rules.instrument.generation,
            attempt_id: self.attempt,
            recovery: &self.request,
        })
        .await
        .map_err(|_| BinanceAccountGatewayError::Readback)?;
        Ok(BinanceCompletedProjection {
            read: self,
            snapshot,
        })
    }
}

impl BinanceCompletedProjection {
    pub fn snapshot(&self) -> &SignedAccountSnapshot {
        &self.snapshot
    }
}

impl BinanceAccountGateway {
    pub fn prepare_projection_read(
        &mut self,
        cursor: Option<String>,
    ) -> Result<BinanceProjectionRead, BinanceAccountGatewayError> {
        let symbols = AccountSymbolSet::new(
            self.config.gateway_binding(),
            self.rules_by_symbol.keys().cloned(),
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let request = AccountRecoveryRequest::read_only(
            self.config.gateway_binding().clone(),
            symbols,
            cursor,
        )
        .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let generation = self.next_private_generation()?;
        Ok(BinanceProjectionRead {
            transport: self.transport_for_private_generation(generation)?,
            credentials: crate::BinanceCredentials::from_secrets(
                self.credentials.api_key.clone(),
                self.credentials.api_secret.clone(),
            )
            .map_err(|_| BinanceAccountGatewayError::Credentials)?,
            config: self.config.clone(),
            rules: self.rules.clone(),
            connection_generation: self.connection_generation,
            prior_generation: self.private_generation,
            generation,
            attempt: self.take_attempt_id()?,
            request,
        })
    }

    /// Called only after the consumer has committed this exact signed baseline. A result from a
    /// replaced adapter or concurrent generation must never relabel buffered user-stream facts.
    pub fn accept_projection_read(
        &mut self,
        completed: BinanceCompletedProjection,
    ) -> Result<(), BinanceAccountGatewayError> {
        if completed.read.connection_generation != self.connection_generation
            || completed.read.prior_generation != self.private_generation
            || completed.read.config.gateway_binding() != self.config.gateway_binding()
        {
            return Err(BinanceAccountGatewayError::Binding);
        }
        self.transport = completed.read.transport;
        self.private_generation = completed.read.generation;
        self.rolling_dispatch_cache = None;
        Ok(())
    }
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
