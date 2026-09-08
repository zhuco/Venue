//! Native protocols remain in adapters. These credentials are transient or encrypted with the
//! existing Control cipher; never serialize them into command payloads or diagnostics.
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use venue_control_protocol::accounts::SecretValue;
use venue_domain::domain::ExecutionCommand;
use venue_execution::{
    AccountGatewayResult, AccountPhysicalGateway, AccountRecoveryRequest, AccountSymbolSet,
    DurableAccountGateway, SignedAccountSnapshot,
};
use venue_gateway_api::{GatewayBinding, VenueId};

const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_BODY: usize = 2 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(tag = "venue", rename_all = "lowercase", deny_unknown_fields)]
pub enum StrategyCredentials {
    Bitget {
        api_key: SecretValue,
        api_secret: SecretValue,
        passphrase: SecretValue,
    },
    #[serde(rename = "bitget_copy")]
    BitgetCopy {
        api_key: SecretValue,
        api_secret: SecretValue,
        passphrase: SecretValue,
    },
    Bybit {
        api_key: SecretValue,
        api_secret: SecretValue,
    },
    Gate {
        api_key: SecretValue,
        api_secret: SecretValue,
    },
    Okx {
        api_key: SecretValue,
        api_secret: SecretValue,
        passphrase: SecretValue,
    },
    Hyperliquid {
        account_address: String,
        vault_address: Option<String>,
        api_wallet_address: String,
        private_key: SecretValue,
    },
}

impl StrategyCredentials {
    pub const fn venue(&self) -> VenueId {
        match self {
            Self::Bitget { .. } | Self::BitgetCopy { .. } => VenueId::Bitget,
            Self::Bybit { .. } => VenueId::Bybit,
            Self::Gate { .. } => VenueId::Gate,
            Self::Okx { .. } => VenueId::Okx,
            Self::Hyperliquid { .. } => VenueId::Hyperliquid,
        }
    }
    pub(crate) fn key_identity(&self) -> &str {
        match self {
            Self::Bitget { api_key, .. }
            | Self::BitgetCopy { api_key, .. }
            | Self::Bybit { api_key, .. }
            | Self::Gate { api_key, .. }
            | Self::Okx { api_key, .. } => api_key.expose(),
            Self::Hyperliquid {
                api_wallet_address, ..
            } => api_wallet_address,
        }
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("strategy exchange is unavailable or rejected its signed account contract")]
pub struct StrategyExchangeError;

pub(crate) enum StrategyGateway {
    Bitget(venue_gateway_bitget::BitgetAccountGateway),
    Bybit(venue_gateway_bybit::BybitAccountGateway),
    Gate(venue_gateway_gate::GateAccountGateway),
    Okx(venue_gateway_okx::OkxAccountGateway),
    Hyperliquid(venue_gateway_hyperliquid::HyperliquidAccountGateway),
}

macro_rules! gateway_call {
    ($gateway:expr, $method:ident $(, $arg:expr)*) => {
        match $gateway {
            StrategyGateway::Bitget(g) => g.$method($($arg),*).map_err(|_| StrategyExchangeError),
            StrategyGateway::Bybit(g) => g.$method($($arg),*).map_err(|_| StrategyExchangeError),
            StrategyGateway::Gate(g) => g.$method($($arg),*).map_err(|_| StrategyExchangeError),
            StrategyGateway::Okx(g) => g.$method($($arg),*).map_err(|_| StrategyExchangeError),
            StrategyGateway::Hyperliquid(g) => g.$method($($arg),*).map_err(|_| StrategyExchangeError),
        }
    };
}

impl StrategyGateway {
    pub(crate) fn market_facts_for_dispatch(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, &'static str> {
        match self {
            Self::Okx(gateway) => gateway.durable_market_facts_for_dispatch(),
            Self::Bitget(gateway) => gateway
                .durable_market_facts()
                .map_err(|_| "strategy_market_unavailable"),
            Self::Bybit(gateway) => gateway
                .durable_market_facts()
                .map_err(|_| "strategy_market_unavailable"),
            Self::Gate(gateway) => gateway
                .durable_market_facts()
                .map_err(|_| "strategy_market_unavailable"),
            Self::Hyperliquid(gateway) => gateway
                .durable_market_facts()
                .map_err(|_| "strategy_market_unavailable"),
        }
    }

    pub(crate) fn market_facts(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, StrategyExchangeError> {
        gateway_call!(self, durable_market_facts)
    }
    pub(crate) fn order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, StrategyExchangeError> {
        gateway_call!(self, durable_order_observation, command)
    }
    pub(crate) fn order_observation_detailed(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, String> {
        match self {
            Self::Bitget(gateway) => gateway
                .durable_order_observation(command)
                .map_err(|error| error.to_string()),
            Self::Bybit(gateway) => gateway
                .durable_order_observation(command)
                .map_err(|error| error.to_string()),
            Self::Gate(gateway) => gateway
                .durable_order_observation(command)
                .map_err(|error| error.to_string()),
            Self::Okx(gateway) => gateway
                .durable_order_observation(command)
                .map_err(|error| error.to_string()),
            Self::Hyperliquid(gateway) => gateway
                .durable_order_observation(command)
                .map_err(|error| error.to_string()),
        }
    }
    pub(crate) fn bybit_funding_detailed(
        &mut self,
        query: &venue_gateway_bybit::BybitFundingQuery,
    ) -> Result<venue_gateway_bybit::BybitFundingReadback, String> {
        match self {
            Self::Bybit(gateway) => gateway
                .settled_funding(query)
                .map_err(|error| error.to_string()),
            _ => Err("funding readback is available only for Bybit".to_owned()),
        }
    }
    /// Called only inside a bounded blocking worker: the existing adapters own their synchronous
    /// transport runtime. No runtime, credentials or nonce file is created per trading strategy.
    pub(crate) fn connect(
        binding: GatewayBinding,
        credentials: StrategyCredentials,
        nonce: u64,
    ) -> Result<Self, StrategyExchangeError> {
        Self::connect_detailed(binding, credentials, nonce).map_err(|_| StrategyExchangeError)
    }

    pub(crate) fn connect_detailed(
        binding: GatewayBinding,
        credentials: StrategyCredentials,
        nonce: u64,
    ) -> Result<Self, String> {
        if binding.venue != credentials.venue() {
            return Err("strategy credential venue does not match binding".to_owned());
        }
        let secret = |value: SecretValue| SecretString::from(value.expose().to_owned());
        Ok(match credentials {
            StrategyCredentials::BitgetCopy {
                api_key,
                api_secret,
                passphrase,
            } => {
                let credentials = venue_gateway_bitget::BitgetCredentials::from_copy_secrets(
                    secret(api_key),
                    secret(api_secret),
                    secret(passphrase),
                )
                .map_err(|error| error.to_string())?;
                Self::Bitget(
                    venue_gateway_bitget::BitgetAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
            StrategyCredentials::Bitget {
                api_key,
                api_secret,
                passphrase,
            } => {
                let credentials = venue_gateway_bitget::BitgetCredentials::from_secrets(
                    secret(api_key),
                    secret(api_secret),
                    secret(passphrase),
                )
                .map_err(|error| error.to_string())?;
                Self::Bitget(
                    venue_gateway_bitget::BitgetAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
            StrategyCredentials::Bybit {
                api_key,
                api_secret,
            } => {
                let credentials = venue_gateway_bybit::BybitCredentials::from_secrets(
                    secret(api_key),
                    secret(api_secret),
                )
                .map_err(|error| error.to_string())?;
                Self::Bybit(
                    venue_gateway_bybit::BybitAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
            StrategyCredentials::Gate {
                api_key,
                api_secret,
            } => {
                let credentials = venue_gateway_gate::GateCredentials::from_secrets(
                    secret(api_key),
                    secret(api_secret),
                )
                .map_err(|error| error.to_string())?;
                Self::Gate(
                    venue_gateway_gate::GateAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
            StrategyCredentials::Okx {
                api_key,
                api_secret,
                passphrase,
            } => {
                let credentials = venue_gateway_okx::OkxCredentials::from_secrets(
                    secret(api_key),
                    secret(api_secret),
                    secret(passphrase),
                )
                .map_err(|error| error.to_string())?;
                Self::Okx(
                    venue_gateway_okx::OkxAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        venue_gateway_okx::OkxTradeMode::Cross,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
            StrategyCredentials::Hyperliquid {
                account_address,
                vault_address,
                api_wallet_address,
                private_key,
            } => {
                let credentials = venue_gateway_hyperliquid::HyperliquidCredentials::from_secrets(
                    account_address,
                    vault_address,
                    api_wallet_address,
                    secret(private_key),
                )
                .map_err(|error| error.to_string())?;
                Self::Hyperliquid(
                    venue_gateway_hyperliquid::HyperliquidAccountGateway::connect_with_credentials(
                        binding,
                        credentials,
                        nonce,
                        TIMEOUT,
                        MAX_BODY,
                    )
                    .map_err(|error| error.to_string())?,
                )
            }
        })
    }
    pub(crate) fn identity(&mut self) -> Result<String, StrategyExchangeError> {
        gateway_call!(self, verified_account_identity)
    }
    pub(crate) fn verify_admission_symbol(&mut self) -> Result<(), StrategyExchangeError> {
        self.verify_strategy_symbols(&[])
    }
    pub(crate) fn verify_strategy_symbols(
        &mut self,
        symbols: &[venue_domain::Symbol],
    ) -> Result<(), StrategyExchangeError> {
        match self {
            Self::Bitget(gateway) => gateway
                .verify_copy_trading_symbols(symbols)
                .map_err(|_| StrategyExchangeError),
            _ => Ok(()),
        }
    }
    pub(crate) fn verify_permissions(&mut self) -> Result<(), StrategyExchangeError> {
        match self {
            Self::Bitget(gateway) => gateway
                .verify_strategy_permissions()
                .map_err(|_| StrategyExchangeError),
            Self::Gate(gateway) => gateway
                .verify_strategy_permissions()
                .map_err(|_| StrategyExchangeError),
            // These constructors already require their signed API permission/account contract.
            Self::Bybit(_) | Self::Okx(_) | Self::Hyperliquid(_) => Ok(()),
        }
    }
    pub(crate) fn snapshot(
        &mut self,
        binding: &GatewayBinding,
    ) -> Result<SignedAccountSnapshot, StrategyExchangeError> {
        let request = AccountRecoveryRequest::read_only(
            binding.clone(),
            AccountSymbolSet::single(binding),
            None,
        )
        .map_err(|_| StrategyExchangeError)?;
        gateway_call!(self, signed_account_snapshot, &request)
    }
    pub(crate) fn submit(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        match self {
            Self::Bitget(g) => g.execute_committed_with_context(command, context),
            Self::Bybit(g) => g.execute_committed_with_context(command, context),
            Self::Gate(g) => g.execute_committed_with_context(command, context),
            Self::Okx(g) => g.execute_committed_with_context(command, context),
            Self::Hyperliquid(g) => g.execute_committed_with_context(command, context),
        }
    }
    pub(crate) fn reconcile(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        match self {
            Self::Bitget(g) => g.reconcile_committed_with_context(command, context),
            Self::Bybit(g) => g.reconcile_committed_with_context(command, context),
            Self::Gate(g) => g.reconcile_committed_with_context(command, context),
            Self::Okx(g) => g.reconcile_committed_with_context(command, context),
            Self::Hyperliquid(g) => g.reconcile_committed_with_context(command, context),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StrategyCredentials;

    #[test]
    fn hyperliquid_strategy_credentials_preserve_optional_vault_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        for (encoded, expected) in [
            (
                r#"{"venue":"hyperliquid","account_address":"0x0000000000000000000000000000000000000001","vault_address":"0x0000000000000000000000000000000000000002","api_wallet_address":"0x0000000000000000000000000000000000000003","private_key":"secret"}"#,
                Some("0x0000000000000000000000000000000000000002"),
            ),
            (
                r#"{"venue":"hyperliquid","account_address":"0x0000000000000000000000000000000000000001","api_wallet_address":"0x0000000000000000000000000000000000000003","private_key":"secret"}"#,
                None,
            ),
        ] {
            let parsed: StrategyCredentials = serde_json::from_str(encoded)?;
            let StrategyCredentials::Hyperliquid { vault_address, .. } = parsed else {
                return Err("wrong credential venue".into());
            };
            assert_eq!(vault_address.as_deref(), expected);
        }
        Ok(())
    }
}
