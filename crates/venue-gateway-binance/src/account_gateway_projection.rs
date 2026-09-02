use venue_execution::{
    AccountPhysicalGateway, AccountRecoveryRequest, AccountSymbolSet, SignedAccountSnapshot,
};

use super::{BinanceAccountGateway, BinanceAccountGatewayError};

impl BinanceAccountGateway {
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
