//! Control-plane DTOs for the support based averaging strategy.
//!
//! These types describe ownership, configuration and projections only.  They do not contain
//! native order fields and do not grant permission to send an order.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::Symbol;
use venue_gateway_api::{GatewayMode, VenueId};

pub const SUPPORT_MARTINGALE_SCHEMA_VERSION: u16 = 1;
pub const SUPPORT_MARTINGALE_INSTANCES_PATH: &str = "/v2/strategies/support-martingale/instances";
pub const SUPPORT_MARTINGALE_LIFECYCLE_PATH: &str = "/v2/strategies/support-martingale/lifecycle";
pub const SUPPORT_MARTINGALE_DETAIL_PREFIX: &str = "/v2/strategies/support-martingale/instances/";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportMartingaleLifecycle {
    Stopped,
    Running,
    EntryPaused,
    IncreasePaused,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportMartingaleHealth {
    Healthy,
    NeedsAttention,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportMartingaleAction {
    Start,
    PauseEntry,
    PauseIncrease,
    Drain,
    Resume,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportMartingaleConfig {
    pub reference_venue: VenueId,
    pub execution_venue: VenueId,
    pub symbols: Vec<Symbol>,
    #[serde(with = "rust_decimal::serde::str")]
    pub total_budget: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub first_order_notional: Decimal,
    pub max_entries: u16,
    #[serde(with = "rust_decimal::serde::str")]
    pub size_multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_profit_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_profit_quote: Decimal,
    pub max_active_positions: u16,
}

impl SupportMartingaleConfig {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        if self.reference_venue != VenueId::Binance
            || self.execution_venue == VenueId::Binance
            || self.symbols.is_empty()
            || self.symbols.len() > 50
            || self.total_budget <= Decimal::ZERO
            || self.first_order_notional <= Decimal::ZERO
            || self.max_entries == 0
            || self.max_entries > 50
            || self.size_multiplier < Decimal::ONE
            || self.target_profit_rate <= Decimal::ZERO
            || self.minimum_profit_quote < Decimal::ZERO
            || self.max_active_positions == 0
            || self.max_active_positions > 50
            || usize::from(self.max_active_positions) > self.symbols.len()
            || self.first_order_notional > self.total_budget
            || self
                .symbols
                .iter()
                .enumerate()
                .any(|(index, symbol)| self.symbols[..index].contains(symbol))
        {
            return Err(SupportMartingaleProtocolError::Config);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportMartingaleCreateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub config: SupportMartingaleConfig,
}
impl SupportMartingaleCreateRequest {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        if self.schema_version != SUPPORT_MARTINGALE_SCHEMA_VERSION
            || self.request_id.trim().is_empty()
            || self.credential_id.trim().is_empty()
        {
            return Err(SupportMartingaleProtocolError::Identity);
        }
        self.config.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportMartingaleLifecycleRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub instance_id: String,
    pub expected_revision: u64,
    pub action: SupportMartingaleAction,
}
impl SupportMartingaleLifecycleRequest {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        if self.schema_version != SUPPORT_MARTINGALE_SCHEMA_VERSION
            || self.request_id.trim().is_empty()
            || self.instance_id.trim().is_empty()
            || self.expected_revision == 0
        {
            return Err(SupportMartingaleProtocolError::Identity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingaleInstance {
    pub instance_id: String,
    pub owner_user_id: String,
    pub credential_id: String,
    pub trading_account_id: String,
    pub execution_venue: VenueId,
    pub mode: GatewayMode,
    pub config: SupportMartingaleConfig,
    pub lifecycle: SupportMartingaleLifecycle,
    pub health: SupportMartingaleHealth,
    pub revision: u64,
    pub reserved_budget: Decimal,
    pub symbols: Vec<SupportMartingaleSymbolState>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingaleSymbolState {
    pub symbol: Symbol,
    pub cycle_id: Option<String>,
    pub layer: u16,
    pub average_price: Option<Decimal>,
    pub quantity: Decimal,
    pub invested: Decimal,
    pub take_profit_price: Option<Decimal>,
    pub net_pnl: Option<Decimal>,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingaleListItem {
    pub instance_id: String,
    pub execution_venue: VenueId,
    pub trading_account_id: String,
    pub lifecycle: SupportMartingaleLifecycle,
    pub health: SupportMartingaleHealth,
    pub revision: u64,
    pub symbol_count: u32,
    pub reserved_budget: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupportMartingaleProtocolError {
    #[error("support martingale identity is invalid")]
    Identity,
    #[error("support martingale configuration is invalid")]
    Config,
    #[error("support martingale account is invalid")]
    Account,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_binance_reference_and_non_binance_execution() {
        let config = SupportMartingaleConfig {
            reference_venue: VenueId::Binance,
            execution_venue: VenueId::Bybit,
            symbols: vec!["SOL/USDT".parse().unwrap(), "DOGE/USDT".parse().unwrap()],
            total_budget: Decimal::new(30, 0),
            first_order_notional: Decimal::new(2, 0),
            max_entries: 3,
            size_multiplier: Decimal::new(125, 2),
            target_profit_rate: Decimal::new(2, 2),
            minimum_profit_quote: Decimal::new(1, 2),
            max_active_positions: 2,
        };
        assert!(config.validate().is_ok());
    }
}
