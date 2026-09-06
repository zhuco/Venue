use super::*;

/// Corroborated absence within the ordinary-order retention window. This is no exchange order
/// status and grants no dispatch permit; the executor must still require an explicit drain.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct BinanceAbsentLimitOrder {
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub client_order_id: String,
    pub history_start_ms: u64,
    pub history_end_ms: u64,
    pub snapshot_observed_ms: u64,
    pub observed_ms: u64,
}

impl BinanceAccountGateway {
    /// A single -2013, an empty open-order list, or a current-only trade cursor is insufficient.
    /// Refuse any history row, including unrelated orders, rather than infer missing pagination.
    pub fn confirm_recent_absent_limit_order(
        &mut self,
        client_order_id: &str,
        created_ms: u64,
    ) -> Result<Option<BinanceAbsentLimitOrder>, BinanceAccountGatewayError> {
        crate::readback::validate_client_order_id(client_order_id)
            .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let start_ms = created_ms
            .checked_sub(60_000)
            .filter(|value| *value > 0)
            .ok_or(BinanceAccountGatewayError::Binding)?;
        let before_ms = now_ms()?;
        let exchange_ms = self
            .transport
            .signing_timestamp_ms()
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        if !recent_history_window(start_ms, before_ms) || before_ms.abs_diff(exchange_ms) > 1_000 {
            return Ok(None);
        }
        if !self.exact_limit_absent(client_order_id)? {
            return Ok(None);
        }
        let symbol = self.config.gateway_binding().symbol.clone();
        self.projection_symbols.insert(symbol.clone());
        let cursor = Self::replay_projection_fills_from(
            None,
            &BTreeMap::from([(symbol.clone(), start_ms)]),
        )?;
        let snapshot = self.signed_projection_snapshot(cursor)?;
        if snapshot
            .positions()
            .iter()
            .any(|position| position.quantity != rust_decimal::Decimal::ZERO)
            || !snapshot.open_orders().is_empty()
            || !snapshot.conditional_orders().is_empty()
            || !snapshot.fills().is_empty()
        {
            return Ok(None);
        }
        let history_end_ms = now_ms()?;
        if !recent_history_window(start_ms, history_end_ms) {
            return Ok(None);
        }
        let scope = self.absence_scope(history_end_ms)?;
        let request =
            crate::readback::build_recent_order_history_request(&scope, start_ms, history_end_ms)
                .map_err(|_| BinanceAccountGatewayError::Readback)?;
        let page = self
            .runtime
            .block_on(
                self.transport.execute_read(
                    &self.credentials,
                    &request,
                    self.transport
                        .signing_timestamp_ms()
                        .map_err(|_| BinanceAccountGatewayError::Readback)?,
                ),
            )
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        if !empty_history(&page.payload)? || !self.exact_limit_absent(client_order_id)? {
            return Ok(None);
        }
        let observed_ms = now_ms()?;
        let exchange_ms = self
            .transport
            .signing_timestamp_ms()
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        if observed_ms.saturating_sub(snapshot.observed_at_ms()) > 3_000
            || observed_ms.abs_diff(exchange_ms) > 1_000
        {
            return Ok(None);
        }
        Ok(Some(BinanceAbsentLimitOrder {
            trading_account_id: self.config.gateway_binding().trading_account_id.clone(),
            symbol,
            client_order_id: client_order_id.to_owned(),
            history_start_ms: start_ms,
            history_end_ms,
            snapshot_observed_ms: snapshot.observed_at_ms(),
            observed_ms,
        }))
    }

    fn absence_scope(
        &mut self,
        now: u64,
    ) -> Result<crate::BinancePrivateReadScope, BinanceAccountGatewayError> {
        let attempt = self.take_attempt_id()?;
        crate::BinancePrivateReadScope::new(
            &self.config,
            &self.rules,
            self.private_generation,
            attempt,
            now,
        )
        .map_err(|_| BinanceAccountGatewayError::Readback)
    }

    fn exact_limit_absent(
        &mut self,
        client_order_id: &str,
    ) -> Result<bool, BinanceAccountGatewayError> {
        let now = self
            .transport
            .signing_timestamp_ms()
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        let scope = self.absence_scope(now)?;
        let request = crate::build_exact_order_request(&scope, client_order_id)
            .map_err(|_| BinanceAccountGatewayError::Readback)?;
        match self.runtime.block_on(
            self.transport
                .execute_read(&self.credentials, &request, now),
        ) {
            Err(crate::BinanceTransportError::ApiRejected(-2013)) => Ok(true),
            Ok(_) => Ok(false),
            Err(_) => Err(BinanceAccountGatewayError::Readback),
        }
    }
}

fn recent_history_window(start: u64, end: u64) -> bool {
    start > 0
        && end
            .checked_sub(start)
            .is_some_and(|age| (120_000..=48 * 60 * 60 * 1000).contains(&age))
}

fn empty_history(payload: &[u8]) -> Result<bool, BinanceAccountGatewayError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| BinanceAccountGatewayError::Readback)?;
    value
        .as_array()
        .map(Vec::is_empty)
        .ok_or(BinanceAccountGatewayError::Readback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absence_refuses_incomplete_or_nonempty_history_and_expired_retention() {
        assert!(matches!(empty_history(b"[]"), Ok(true)));
        assert!(matches!(
            empty_history(b"[{\"status\":\"CANCELED\"}]"),
            Ok(false)
        ));
        assert!(empty_history(b"{\"code\":-2013}").is_err());
        assert!(empty_history(b"null").is_err());
        assert!(!recent_history_window(0, 120_000));
        assert!(!recent_history_window(1, 120_000));
        assert!(recent_history_window(1, 120_001));
        assert!(recent_history_window(1, 48 * 60 * 60 * 1000 + 1));
        assert!(!recent_history_window(1, 48 * 60 * 60 * 1000 + 2));
    }
}
