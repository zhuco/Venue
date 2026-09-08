//! Independent Binance inventory market making; no grid level or rolling anchor contract.
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::Symbol;

pub const INVENTORY_MM_SCHEMA_VERSION: u16 = 1;
pub const INVENTORY_MM_PATH: &str = "/v2/strategies/inventory-mm";
pub const INVENTORY_MM_PREFLIGHT_PATH: &str = "/v2/strategies/inventory-mm/preflight";
pub const INVENTORY_MM_LIFECYCLE_PATH: &str = "/v2/strategies/inventory-mm/lifecycle";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryMmConfig {
    pub symbol: Symbol,
    #[serde(with = "rust_decimal::serde::str")]
    pub order_notional: Decimal,
    /// Distance of each quote from its reservation price, not the full bid/ask spread.
    #[serde(with = "rust_decimal::serde::str")]
    pub base_half_spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub volatility_multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub inventory_skew_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_leg_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_gross_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_net_notional: Decimal,
    /// Account-level USD loss guard, not isolated strategy PnL in the contract quote asset.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_loss_quote: Decimal,
    /// Account-level USD drawdown guard. Retains the persisted high-water mark on restart.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_drawdown_quote: Decimal,
    /// USD available-margin reserve before conversion into contract quote notional.
    #[serde(with = "rust_decimal::serde::str")]
    pub min_available_margin: Decimal,
    pub required_leverage: u8,
    pub quote_refresh_ms: u64,
}
impl InventoryMmConfig {
    pub fn validate(&self) -> Result<(), InventoryMmProtocolError> {
        let positive = [
            self.order_notional,
            self.base_half_spread_bps,
            self.volatility_multiplier,
            self.inventory_skew_bps,
            self.max_leg_notional,
            self.max_gross_notional,
            self.max_net_notional,
            self.max_loss_quote,
            self.max_drawdown_quote,
            self.min_available_margin,
        ];
        if positive
            .iter()
            .any(|v| *v <= Decimal::ZERO || *v == Decimal::MAX)
            || !matches!(self.symbol.quote(), "USDC" | "USDT")
            || self.max_leg_notional < self.order_notional
            || self.max_gross_notional < self.max_leg_notional
            || self.max_net_notional < self.order_notional
            || self.max_net_notional > self.max_leg_notional
            || self.base_half_spread_bps > Decimal::from(100)
            || self.inventory_skew_bps >= Decimal::from(1_000)
            || !(1..=20).contains(&self.required_leverage)
            || !(1_000..=60_000).contains(&self.quote_refresh_ms)
        {
            return Err(InventoryMmProtocolError);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryMmState {
    Stopped,
    StartPending,
    Running,
    StopPending,
    NeedsAttention,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryMmAction {
    Start,
    Stop,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryMmCreateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub config: InventoryMmConfig,
}
impl InventoryMmCreateRequest {
    pub fn validate(&self) -> Result<(), InventoryMmProtocolError> {
        if self.schema_version != INVENTORY_MM_SCHEMA_VERSION
            || !valid_id(&self.request_id)
            || !valid_id(&self.credential_id)
        {
            return Err(InventoryMmProtocolError);
        }
        self.config.validate()
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryMmLifecycleRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub instance_id: String,
    pub expected_revision: u64,
    pub action: InventoryMmAction,
}
impl InventoryMmLifecycleRequest {
    pub fn validate(&self) -> Result<(), InventoryMmProtocolError> {
        if self.schema_version != INVENTORY_MM_SCHEMA_VERSION
            || !valid_id(&self.request_id)
            || !valid_id(&self.instance_id)
            || self.expected_revision == 0
        {
            return Err(InventoryMmProtocolError);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryMmPreflightRequest {
    pub instance_id: String,
    pub expected_revision: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryMmPreflight {
    pub instance_id: String,
    pub revision: u64,
    pub checked_ms: u64,
    pub ready: bool,
    pub blockers: Vec<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryMmInstance {
    pub instance_id: String,
    pub owner_user_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub config: InventoryMmConfig,
    pub state: InventoryMmState,
    pub revision: u64,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub baseline_equity: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::str_option")]
    pub peak_equity: Option<Decimal>,
    pub attention: Option<String>,
    pub updated_ms: u64,
    pub last_quote_ms: Option<u64>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid inventory market making configuration or request")]
pub struct InventoryMmProtocolError;
fn valid_id(value: &str) -> bool {
    venue_domain::is_canonical_trading_account_id(value)
}
