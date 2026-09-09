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
pub const SUPPORT_MARTINGALE_PREFLIGHT_PATH: &str = "/v2/strategies/support-martingale/preflight";
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
    #[serde(default)]
    pub entry_mode: MartingaleEntryMode,
    /// Allows the strategy layer to treat a neutral BTC environment as admissible when its
    /// symbol-level support signal is ready. The runtime must still enforce the BTC guard.
    #[serde(default)]
    pub allow_btc_neutral: bool,
    #[serde(default)]
    pub symbol_parameters: Vec<MartingaleSymbolParameters>,
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
        if !self.valid_symbol_parameters()
            || self.reference_venue != VenueId::Binance
            || self.execution_venue == VenueId::Binance
            || self.symbols.is_empty()
            || self.symbols.len() > 30
            || self.total_budget <= Decimal::ZERO
            || self.first_order_notional <= Decimal::ZERO
            || self.max_entries == 0
            || self.max_entries > 10
            || self.size_multiplier < Decimal::ONE
            || self.target_profit_rate <= Decimal::ZERO
            || self.minimum_profit_quote < Decimal::ZERO
            || self.max_active_positions == 0
            || self.max_active_positions > 10
            || usize::from(self.max_active_positions) > self.symbols.len()
            || self.first_order_notional > self.total_budget
            || self
                .symbols
                .iter()
                .enumerate()
                .any(|(index, symbol)| self.symbols[..index].contains(symbol))
            || self
                .symbols
                .iter()
                .skip(1)
                .any(|symbol| symbol.quote() != self.symbols[0].quote())
        {
            return Err(SupportMartingaleProtocolError::Config);
        }
        Ok(())
    }

    fn valid_symbol_parameters(&self) -> bool {
        self.symbol_parameters
            .iter()
            .enumerate()
            .all(|(index, value)| {
                self.symbols.contains(&value.symbol)
                    && !self.symbol_parameters[..index]
                        .iter()
                        .any(|other| other.symbol == value.symbol)
                    && value.entry_price.is_none_or(|price| price > Decimal::ZERO)
                    && value.add_drop_rate > Decimal::ZERO
                    && value.add_drop_rate < Decimal::ONE
                    && match value.stop_loss {
                        None => true,
                        Some(MartingaleStopLoss::FixedPrice { price }) => {
                            price > Decimal::ZERO
                                && value.entry_price.is_none_or(|entry| price < entry)
                        }
                        Some(MartingaleStopLoss::AveragePricePercent { rate }) => {
                            rate > Decimal::ZERO && rate < Decimal::ONE
                        }
                    }
            })
            && (self.entry_mode != MartingaleEntryMode::FixedPrice
                || self.symbols.iter().all(|symbol| {
                    self.symbol_parameters
                        .iter()
                        .any(|value| value.symbol == *symbol && value.entry_price.is_some())
                }))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MartingaleEntryMode {
    #[default]
    Support,
    FixedPrice,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MartingaleSymbolParameters {
    pub symbol: Symbol,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub entry_price: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str")]
    pub add_drop_rate: Decimal,
    pub stop_loss: Option<MartingaleStopLoss>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum MartingaleStopLoss {
    FixedPrice {
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
    },
    AveragePricePercent {
        #[serde(with = "rust_decimal::serde::str")]
        rate: Decimal,
    },
}

impl MartingaleStopLoss {
    pub fn trigger_price(self, average: Decimal) -> Option<Decimal> {
        match self {
            Self::FixedPrice { price } if price > Decimal::ZERO => Some(price),
            Self::AveragePricePercent { rate }
                if average > Decimal::ZERO && rate > Decimal::ZERO && rate < Decimal::ONE =>
            {
                average.checked_mul(Decimal::ONE.checked_sub(rate)?)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportMartingalePreflightRequest {
    pub schema_version: u16,
    pub instance_id: String,
    pub expected_revision: u64,
}

impl SupportMartingalePreflightRequest {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        if self.schema_version != SUPPORT_MARTINGALE_SCHEMA_VERSION
            || self.instance_id.trim().is_empty()
            || self.expected_revision == 0
        {
            return Err(SupportMartingaleProtocolError::Identity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportMartingalePreflightCheckCode {
    LiveOnly,
    CredentialVerified,
    AccountExclusive,
    SignedFactsFresh,
    PositionMode,
    NoPositions,
    NoOpenOrders,
    NoUnknownResults,
    BudgetAvailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportMartingalePreflightCheckStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingalePreflightCheck {
    pub code: SupportMartingalePreflightCheckCode,
    pub status: SupportMartingalePreflightCheckStatus,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingalePreflightResponse {
    pub instance_id: String,
    pub revision: u64,
    pub checked_at_ms: u64,
    pub mode: GatewayMode,
    pub ready: bool,
    pub checks: Vec<SupportMartingalePreflightCheck>,
}

impl SupportMartingalePreflightResponse {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        const REQUIRED: [SupportMartingalePreflightCheckCode; 9] = [
            SupportMartingalePreflightCheckCode::LiveOnly,
            SupportMartingalePreflightCheckCode::CredentialVerified,
            SupportMartingalePreflightCheckCode::AccountExclusive,
            SupportMartingalePreflightCheckCode::SignedFactsFresh,
            SupportMartingalePreflightCheckCode::PositionMode,
            SupportMartingalePreflightCheckCode::NoPositions,
            SupportMartingalePreflightCheckCode::NoOpenOrders,
            SupportMartingalePreflightCheckCode::NoUnknownResults,
            SupportMartingalePreflightCheckCode::BudgetAvailable,
        ];
        if self.instance_id.trim().is_empty()
            || self.revision == 0
            || self.checked_at_ms == 0
            || self.mode != GatewayMode::Live
            || self.checks.len() != REQUIRED.len()
            || REQUIRED.iter().any(|code| {
                self.checks
                    .iter()
                    .filter(|check| check.code == *code)
                    .count()
                    != 1
            })
            || self.ready
                != self
                    .checks
                    .iter()
                    .all(|check| check.status == SupportMartingalePreflightCheckStatus::Passed)
        {
            return Err(SupportMartingaleProtocolError::Preflight);
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportMartingaleConfigUpdateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub instance_id: String,
    pub expected_revision: u64,
    pub config: SupportMartingaleConfig,
}

impl SupportMartingaleConfigUpdateRequest {
    pub fn validate(&self) -> Result<(), SupportMartingaleProtocolError> {
        if self.schema_version != SUPPORT_MARTINGALE_SCHEMA_VERSION
            || self.request_id.trim().is_empty()
            || self.instance_id.trim().is_empty()
            || self.expected_revision == 0
        {
            return Err(SupportMartingaleProtocolError::Identity);
        }
        self.config.validate()
    }
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
    pub health_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupportMartingaleListItem {
    pub instance_id: String,
    pub execution_venue: VenueId,
    pub trading_account_id: String,
    pub lifecycle: SupportMartingaleLifecycle,
    pub health: SupportMartingaleHealth,
    pub health_reason: Option<String>,
    pub revision: u64,
    pub symbol_count: u32,
    pub reserved_budget: Decimal,
    pub config: SupportMartingaleConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SupportMartingaleProtocolError {
    #[error("support martingale identity is invalid")]
    Identity,
    #[error("support martingale configuration is invalid")]
    Config,
    #[error("support martingale account is invalid")]
    Account,
    #[error("support martingale preflight response is invalid")]
    Preflight,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixed_config() -> Result<SupportMartingaleConfig, Box<dyn std::error::Error>> {
        Ok(serde_json::from_value(serde_json::json!({
            "entry_mode":"fixed_price", "reference_venue":"binance", "execution_venue":"bybit",
            "symbols":["SOL/USDT"], "symbol_parameters":[{
                "symbol":"SOL/USDT", "entry_price":"100", "add_drop_rate":"0.02",
                "stop_loss":{"method":"average_price_percent","rate":"0.1"}
            }], "total_budget":"100", "first_order_notional":"5", "max_entries":3,
            "size_multiplier":"1.25", "target_profit_rate":"0.005", "minimum_profit_quote":"0",
            "max_active_positions":1
        }))?)
    }

    #[test]
    fn fixed_entry_requires_each_symbol_and_valid_stop_terms()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = fixed_config()?;
        assert!(original.validate().is_ok());
        for rate in [Decimal::ZERO, Decimal::ONE, -Decimal::ONE] {
            let mut config = original.clone();
            config.symbol_parameters[0].add_drop_rate = rate;
            assert!(config.validate().is_err());
            config = original.clone();
            config.symbol_parameters[0].stop_loss =
                Some(MartingaleStopLoss::AveragePricePercent { rate });
            assert!(config.validate().is_err());
        }
        let mut config = original.clone();
        config.symbol_parameters[0].entry_price = None;
        assert!(config.validate().is_err());
        config = original.clone();
        config.symbols.push("ETH/USDT".parse()?);
        assert!(config.validate().is_err());
        config = original.clone();
        config
            .symbol_parameters
            .push(config.symbol_parameters[0].clone());
        assert!(config.validate().is_err());
        config = original;
        config.symbol_parameters[0].stop_loss = Some(MartingaleStopLoss::FixedPrice {
            price: Decimal::from(100),
        });
        assert!(config.validate().is_err());
        config.symbol_parameters[0].stop_loss = Some(MartingaleStopLoss::FixedPrice {
            price: Decimal::from(90),
        });
        assert!(config.validate().is_ok());
        Ok(())
    }

    #[test]
    fn old_configs_keep_support_mode_and_stops_are_explicit()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = fixed_config()?;
        assert_eq!(
            config.symbol_parameters[0]
                .stop_loss
                .and_then(|stop| stop.trigger_price(Decimal::from(80))),
            Some(Decimal::from(72))
        );
        let mut value = serde_json::to_value(config)?;
        let map = value.as_object_mut().ok_or("configuration object")?;
        map.remove("entry_mode");
        map.remove("symbol_parameters");
        let old: SupportMartingaleConfig = serde_json::from_value(value)?;
        assert!(old.validate().is_ok());
        assert_eq!(old.entry_mode, MartingaleEntryMode::Support);
        assert!(old.symbol_parameters.is_empty());
        Ok(())
    }

    #[test]
    fn old_configs_default_btc_neutral_gate_closed() -> Result<(), Box<dyn std::error::Error>> {
        let mut value = serde_json::to_value(fixed_config()?)?;
        value
            .as_object_mut()
            .ok_or("configuration object")?
            .remove("allow_btc_neutral");
        let old: SupportMartingaleConfig = serde_json::from_value(value)?;
        assert!(!old.allow_btc_neutral);
        Ok(())
    }

    #[test]
    fn accepts_binance_reference_and_non_binance_execution() {
        let config = SupportMartingaleConfig {
            entry_mode: Default::default(),
            allow_btc_neutral: false,
            symbol_parameters: Vec::new(),
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

    #[test]
    fn rejects_mixed_quote_assets_and_out_of_scope_counts() {
        let mut config = SupportMartingaleConfig {
            entry_mode: Default::default(),
            allow_btc_neutral: false,
            symbol_parameters: Vec::new(),
            reference_venue: VenueId::Binance,
            execution_venue: VenueId::Bybit,
            symbols: vec!["SOL/USDT".parse().unwrap(), "ETH/USDC".parse().unwrap()],
            total_budget: Decimal::new(30, 0),
            first_order_notional: Decimal::new(5, 0),
            max_entries: 4,
            size_multiplier: Decimal::ONE,
            target_profit_rate: Decimal::new(5, 3),
            minimum_profit_quote: Decimal::new(2, 2),
            max_active_positions: 2,
        };
        assert_eq!(
            config.validate(),
            Err(SupportMartingaleProtocolError::Config)
        );
        config.symbols = (0..31)
            .map(|index| format!("S{index}/USDT").parse().unwrap())
            .collect();
        config.max_active_positions = 10;
        assert_eq!(
            config.validate(),
            Err(SupportMartingaleProtocolError::Config)
        );
    }

    #[test]
    fn preflight_ready_requires_every_named_check_to_pass_once() {
        let checks = [
            SupportMartingalePreflightCheckCode::LiveOnly,
            SupportMartingalePreflightCheckCode::CredentialVerified,
            SupportMartingalePreflightCheckCode::AccountExclusive,
            SupportMartingalePreflightCheckCode::SignedFactsFresh,
            SupportMartingalePreflightCheckCode::PositionMode,
            SupportMartingalePreflightCheckCode::NoPositions,
            SupportMartingalePreflightCheckCode::NoOpenOrders,
            SupportMartingalePreflightCheckCode::NoUnknownResults,
            SupportMartingalePreflightCheckCode::BudgetAvailable,
        ]
        .into_iter()
        .map(|code| SupportMartingalePreflightCheck {
            code,
            status: SupportMartingalePreflightCheckStatus::Passed,
        })
        .collect();
        let mut response = SupportMartingalePreflightResponse {
            instance_id: "sm-1".into(),
            revision: 1,
            checked_at_ms: 1,
            mode: GatewayMode::Live,
            ready: true,
            checks,
        };
        assert!(response.validate().is_ok());
        response.checks[0].status = SupportMartingalePreflightCheckStatus::Failed;
        assert_eq!(
            response.validate(),
            Err(SupportMartingaleProtocolError::Preflight)
        );
    }
}
