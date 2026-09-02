//! Testable signed-execution boundary. Production wiring will adapt this trait to the existing
//! Binance transport; mocks exercise idempotency and readback without a network path.

use std::collections::BTreeMap;

use venue_gateway_binance::BinanceCredentials;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRequest {
    pub command_id: String,
    pub client_order_id: String,
    pub trading_account_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReadback {
    Accepted,
    Reconciled,
    Rejected,
    Unknown,
}

/// Signed account baseline outcome used before a Pending activation can become Active. `Clean`
/// means the adapter checked permissions, balance, hedge positions and both ordinary/Algo order
/// surfaces; it is intentionally not inferred from a credential's prior verification record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountBaseline {
    Clean,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceExecutionError {
    #[error("Binance execution is unavailable")]
    Unavailable,
    #[error("Binance execution request is invalid")]
    Invalid,
}

#[allow(
    async_fn_in_trait,
    reason = "executor owns the only implementation boundary and requires no downstream trait contract"
)]
pub trait BinanceExecution {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError>;
    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError>;
}

#[allow(
    async_fn_in_trait,
    reason = "the singleton owns this narrow adapter boundary and requires no external implementation contract"
)]
pub trait BinanceActivationBaseline {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError>;
}

#[derive(Default)]
pub struct MockBinanceExecution {
    orders: BTreeMap<String, ExecutionReadback>,
    baselines: BTreeMap<String, AccountBaseline>,
}

impl MockBinanceExecution {
    pub fn set_readback(&mut self, client_order_id: String, state: ExecutionReadback) {
        self.orders.insert(client_order_id, state);
    }

    pub fn set_baseline(&mut self, trading_account_id: String, baseline: AccountBaseline) {
        self.baselines.insert(trading_account_id, baseline);
    }
}

impl BinanceExecution for MockBinanceExecution {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        if request.client_order_id.is_empty()
            || request.command_id.is_empty()
            || request.trading_account_id.is_empty()
        {
            return Err(BinanceExecutionError::Invalid);
        }
        Ok(*self
            .orders
            .entry(request.client_order_id.clone())
            .or_insert(ExecutionReadback::Accepted))
    }

    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        self.orders
            .get(&request.client_order_id)
            .copied()
            .ok_or(BinanceExecutionError::Unavailable)
    }
}

impl BinanceActivationBaseline for MockBinanceExecution {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        _credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        if trading_account_id.is_empty() {
            return Err(BinanceExecutionError::Invalid);
        }
        Ok(self
            .baselines
            .get(trading_account_id)
            .copied()
            .unwrap_or(AccountBaseline::Clean))
    }
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;

    use super::*;

    fn credentials() -> Result<BinanceCredentials, Box<dyn std::error::Error>> {
        Ok(BinanceCredentials::from_secrets(
            SecretString::from("a".repeat(32)),
            SecretString::from("b".repeat(32)),
        )?)
    }

    #[tokio::test]
    async fn mock_submit_is_stable_by_client_order_id() -> Result<(), Box<dyn std::error::Error>> {
        let request = ExecutionRequest {
            command_id: "command-a".into(),
            client_order_id: "client-a".into(),
            trading_account_id: "account-a".into(),
        };
        let mut exchange = MockBinanceExecution::default();
        assert_eq!(
            exchange.submit(&request, credentials()?).await?,
            ExecutionReadback::Accepted
        );
        exchange.set_readback("client-a".into(), ExecutionReadback::Reconciled);
        assert_eq!(
            exchange.submit(&request, credentials()?).await?,
            ExecutionReadback::Reconciled
        );
        assert_eq!(
            exchange.readback(&request, credentials()?).await?,
            ExecutionReadback::Reconciled
        );
        Ok(())
    }
}
