//! User-scoped contracts for the Binance grid configuration and read model.
//!
//! These DTOs describe configuration and lifecycle intent only. They do not carry credentials,
//! normalized exchange parameters, or physical dispatch authority.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{OrderSide, PositionSide, Symbol, is_canonical_trading_account_id};

pub const GRID_SCHEMA_VERSION: u16 = 1;
pub const GRID_INSTANCES_PATH: &str = "/v2/grid/instances";
pub const GRID_LIFECYCLE_PATH: &str = "/v2/grid/lifecycle";
pub const MAX_GRID_LEVELS: u16 = 50;
pub const MIN_GRID_STALENESS_MS: u64 = 500;
pub const MAX_GRID_STALENESS_MS: u64 = 300_000;
pub const MIN_GRID_CONVERGENCE_MS: u64 = 1_000;
pub const MAX_GRID_CONVERGENCE_MS: u64 = 600_000;
pub const MAX_GRID_CONSECUTIVE_FAILURES: u16 = 100;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridConfig {
    #[serde(with = "rust_decimal::serde::str")]
    pub order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub spacing_rate: Decimal,
    pub grid_levels: u16,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_total_notional: Decimal,
    pub inventory_replenishment: GridInventoryReplenishment,
    pub profit_reduction: GridProfitReduction,
    pub reset_policy: GridResetPolicy,
}

impl GridConfig {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if !positive(self.order_notional)
            || !positive(self.spacing_rate)
            || self.spacing_rate >= Decimal::ONE
            || !(1..=MAX_GRID_LEVELS).contains(&self.grid_levels)
            || self.max_total_notional < self.order_notional
        {
            return Err(GridProtocolError::Config);
        }
        self.inventory_replenishment.validate()?;
        self.profit_reduction.validate()?;
        self.reset_policy.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridInventoryReplenishment {
    pub enabled: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_inventory_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_inventory_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_single_replenishment_notional: Decimal,
}

impl GridInventoryReplenishment {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if !positive(self.minimum_inventory_notional)
            || self.target_inventory_notional <= self.minimum_inventory_notional
            || !positive(self.max_single_replenishment_notional)
        {
            return Err(GridProtocolError::Config);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridProfitReduction {
    pub enabled: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub inventory_equity_multiple: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_unrealized_profit_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reduction_fraction: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_single_reduce_notional: Decimal,
}

impl GridProfitReduction {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if !positive(self.inventory_equity_multiple)
            || !unit_interval(self.minimum_unrealized_profit_rate)
            || !unit_interval(self.reduction_fraction)
            || (self.enabled && self.reduction_fraction.is_zero())
            || !positive(self.max_single_reduce_notional)
        {
            return Err(GridProtocolError::Config);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridResetPolicy {
    pub stale_market_ms: u64,
    pub stale_private_ms: u64,
    pub convergence_timeout_ms: u64,
    pub max_consecutive_failures: u16,
}

impl GridResetPolicy {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if !(MIN_GRID_STALENESS_MS..=MAX_GRID_STALENESS_MS).contains(&self.stale_market_ms)
            || !(MIN_GRID_STALENESS_MS..=MAX_GRID_STALENESS_MS).contains(&self.stale_private_ms)
            || !(MIN_GRID_CONVERGENCE_MS..=MAX_GRID_CONVERGENCE_MS)
                .contains(&self.convergence_timeout_ms)
            || !(1..=MAX_GRID_CONSECUTIVE_FAILURES).contains(&self.max_consecutive_failures)
        {
            return Err(GridProtocolError::Config);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridInstanceState {
    Draft,
    StartPending,
    Running,
    Paused,
    StopPending,
    Stopped,
    Blocked,
    ResetRequired,
    NeedsAttention,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridLifecycleAction {
    Start,
    Pause,
    Resume,
    Stop,
    Reset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridOrderRole {
    Open,
    Close,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridOrderSemanticKey {
    pub position_side: PositionSide,
    pub role: GridOrderRole,
    pub level: u16,
    pub sequence: u64,
}

impl GridOrderSemanticKey {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.position_side == PositionSide::Net
            || !(1..=MAX_GRID_LEVELS).contains(&self.level)
            || self.sequence == 0
        {
            return Err(GridProtocolError::OrderIdentity);
        }
        Ok(())
    }

    #[must_use]
    pub const fn order_side(&self) -> OrderSide {
        match (self.position_side, self.role) {
            (PositionSide::Long, GridOrderRole::Open)
            | (PositionSide::Short, GridOrderRole::Close) => OrderSide::Buy,
            (PositionSide::Long, GridOrderRole::Close)
            | (PositionSide::Short, GridOrderRole::Open) => OrderSide::Sell,
            // Validation rejects Net; returning a stable value keeps this accessor total without
            // turning externally decoded input into a panic path.
            (PositionSide::Net, _) => OrderSide::Buy,
        }
    }

    #[must_use]
    pub fn encoded(&self) -> String {
        let side = match self.position_side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
            PositionSide::Net => "net",
        };
        let role = match self.role {
            GridOrderRole::Open => "open",
            GridOrderRole::Close => "close",
        };
        format!("{side}:{role}:{}:{}", self.level, self.sequence)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridAnchor {
    pub revision: u64,
    pub instrument_generation: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub price_step: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_quantity: Decimal,
    pub source_native_trade_id: Option<String>,
    pub observed_ms: u64,
}

impl GridAnchor {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.revision == 0
            || self.instrument_generation == 0
            || !positive(self.price)
            || !positive(self.price_step)
            || !positive(self.grid_quantity)
            || self.observed_ms == 0
            || self
                .source_native_trade_id
                .as_deref()
                .is_some_and(|value| !bounded_plain(value, 1, 128))
        {
            return Err(GridProtocolError::Anchor);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridInstanceCreateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub config: GridConfig,
}

impl GridInstanceCreateRequest {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.schema_version != GRID_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.credential_id)
        {
            return Err(GridProtocolError::Identity);
        }
        self.config.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridConfigUpdateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub instance_id: String,
    pub expected_revision: u64,
    pub config: GridConfig,
}

impl GridConfigUpdateRequest {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.schema_version != GRID_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.instance_id)
            || self.expected_revision == 0
        {
            return Err(GridProtocolError::Identity);
        }
        self.config.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridLifecycleRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub instance_id: String,
    pub expected_revision: u64,
    pub action: GridLifecycleAction,
    pub risk_confirmed: bool,
    pub positions_remain_acknowledged: bool,
}

impl GridLifecycleRequest {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.schema_version != GRID_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.instance_id)
            || self.expected_revision == 0
        {
            return Err(GridProtocolError::Identity);
        }
        let flags_valid = match self.action {
            GridLifecycleAction::Start | GridLifecycleAction::Resume => {
                self.risk_confirmed && !self.positions_remain_acknowledged
            }
            GridLifecycleAction::Pause => {
                !self.risk_confirmed && !self.positions_remain_acknowledged
            }
            GridLifecycleAction::Stop => !self.risk_confirmed && self.positions_remain_acknowledged,
            // Reset only withdraws orders owned by this instance. Existing positions remain and
            // therefore require the same explicit acknowledgement as Stop.
            GridLifecycleAction::Reset => {
                !self.risk_confirmed && self.positions_remain_acknowledged
            }
        };
        if !flags_valid {
            return Err(GridProtocolError::Lifecycle);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GridInstanceSummary {
    pub schema_version: u16,
    pub instance_id: String,
    pub credential_id: String,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub state: GridInstanceState,
    pub revision: u64,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub config: GridConfig,
    pub anchor: Option<GridAnchor>,
    pub desired_digest: Option<String>,
    pub dirty: bool,
    pub convergence_started_ms: Option<u64>,
    pub consecutive_failures: u16,
    pub last_facts_ms: Option<u64>,
    pub attention_code: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

impl GridInstanceSummary {
    pub fn validate(&self) -> Result<(), GridProtocolError> {
        if self.schema_version != GRID_SCHEMA_VERSION
            || !canonical_id(&self.instance_id)
            || !canonical_id(&self.credential_id)
            || !canonical_id(&self.trading_account_id)
            || self.revision == 0
            || self.config_revision == 0
            || self.plan_revision == 0
            || self.created_ms == 0
            || self.updated_ms < self.created_ms
            || self
                .desired_digest
                .as_deref()
                .is_some_and(|value| !lower_hex_digest(value))
            || self
                .convergence_started_ms
                .is_some_and(|value| value == 0 || value > self.updated_ms)
            || self
                .last_facts_ms
                .is_some_and(|value| value == 0 || value > self.updated_ms)
            || self.consecutive_failures > MAX_GRID_CONSECUTIVE_FAILURES
            || (!self.dirty && self.convergence_started_ms.is_some())
            || (self.consecutive_failures > 0 && !self.dirty)
            || self
                .attention_code
                .as_deref()
                .is_some_and(|value| !bounded_plain(value, 1, 64))
            || matches!(
                self.state,
                GridInstanceState::Blocked
                    | GridInstanceState::ResetRequired
                    | GridInstanceState::NeedsAttention
            ) != self.attention_code.is_some()
        {
            return Err(GridProtocolError::Summary);
        }
        self.config.validate()?;
        if let Some(anchor) = &self.anchor {
            anchor.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GridProtocolError {
    #[error("grid identity or schema version is invalid")]
    Identity,
    #[error("grid configuration is invalid")]
    Config,
    #[error("grid lifecycle intent is invalid")]
    Lifecycle,
    #[error("grid order identity is invalid")]
    OrderIdentity,
    #[error("grid rolling anchor is invalid")]
    Anchor,
    #[error("grid summary is invalid")]
    Summary,
}

fn canonical_id(value: &str) -> bool {
    is_canonical_trading_account_id(value)
}

fn positive(value: Decimal) -> bool {
    value.is_sign_positive() && !value.is_zero()
}

fn unit_interval(value: Decimal) -> bool {
    value >= Decimal::ZERO && value <= Decimal::ONE
}

fn bounded_plain(value: &str, minimum: usize, maximum: usize) -> bool {
    let trimmed = value.trim();
    (minimum..=maximum).contains(&trimmed.chars().count()) && !value.chars().any(char::is_control)
}

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_1: &str = "00000000-0000-4000-8000-000000000001";
    const ID_2: &str = "00000000-0000-4000-8000-000000000002";

    fn config() -> GridConfig {
        GridConfig {
            order_notional: Decimal::new(5, 0),
            spacing_rate: Decimal::new(2, 3),
            grid_levels: 20,
            max_total_notional: Decimal::new(500, 0),
            inventory_replenishment: GridInventoryReplenishment {
                enabled: true,
                minimum_inventory_notional: Decimal::new(5, 0),
                target_inventory_notional: Decimal::new(15, 0),
                max_single_replenishment_notional: Decimal::new(5, 0),
            },
            profit_reduction: GridProfitReduction {
                enabled: true,
                inventory_equity_multiple: Decimal::new(3, 0),
                minimum_unrealized_profit_rate: Decimal::new(5, 2),
                reduction_fraction: Decimal::new(3, 1),
                max_single_reduce_notional: Decimal::new(25, 0),
            },
            reset_policy: GridResetPolicy {
                stale_market_ms: 5_000,
                stale_private_ms: 15_000,
                convergence_timeout_ms: 30_000,
                max_consecutive_failures: 3,
            },
        }
    }

    #[test]
    fn explicit_inventory_profit_and_reset_policies_are_bounded() {
        let mut value = config();
        value.inventory_replenishment.target_inventory_notional =
            value.inventory_replenishment.minimum_inventory_notional;
        assert_eq!(value.validate(), Err(GridProtocolError::Config));

        value = config();
        value.profit_reduction.reduction_fraction = Decimal::new(101, 2);
        assert_eq!(value.validate(), Err(GridProtocolError::Config));

        value = config();
        value.reset_policy.stale_private_ms = MAX_GRID_STALENESS_MS + 1;
        assert_eq!(value.validate(), Err(GridProtocolError::Config));
    }

    #[test]
    fn configuration_is_bounded_and_decimal_wire_values_are_strings()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = config();
        assert_eq!(value.validate(), Ok(()));
        let encoded = serde_json::to_value(&value)?;
        assert_eq!(encoded["order_notional"], "5");
        assert_eq!(encoded["spacing_rate"], "0.002");

        let mut invalid = value;
        invalid.grid_levels = MAX_GRID_LEVELS + 1;
        assert_eq!(invalid.validate(), Err(GridProtocolError::Config));
        Ok(())
    }

    #[test]
    fn semantic_key_covers_all_four_hedge_order_families() {
        for (position_side, role, expected) in [
            (PositionSide::Long, GridOrderRole::Open, OrderSide::Buy),
            (PositionSide::Long, GridOrderRole::Close, OrderSide::Sell),
            (PositionSide::Short, GridOrderRole::Open, OrderSide::Sell),
            (PositionSide::Short, GridOrderRole::Close, OrderSide::Buy),
        ] {
            let key = GridOrderSemanticKey {
                position_side,
                role,
                level: 1,
                sequence: 1,
            };
            assert_eq!(key.validate(), Ok(()));
            assert_eq!(key.order_side(), expected);
        }
    }

    #[test]
    fn lifecycle_requires_explicit_risk_and_stop_acknowledgements() {
        let mut request = GridLifecycleRequest {
            schema_version: GRID_SCHEMA_VERSION,
            request_id: ID_1.into(),
            instance_id: ID_2.into(),
            expected_revision: 1,
            action: GridLifecycleAction::Start,
            risk_confirmed: false,
            positions_remain_acknowledged: false,
        };
        assert_eq!(request.validate(), Err(GridProtocolError::Lifecycle));
        request.risk_confirmed = true;
        assert_eq!(request.validate(), Ok(()));
        request.action = GridLifecycleAction::Stop;
        request.risk_confirmed = false;
        assert_eq!(request.validate(), Err(GridProtocolError::Lifecycle));
        request.positions_remain_acknowledged = true;
        assert_eq!(request.validate(), Ok(()));
        request.action = GridLifecycleAction::Reset;
        assert_eq!(request.validate(), Ok(()));
    }

    #[test]
    fn create_request_rejects_unknown_wire_fields() -> Result<(), Box<dyn std::error::Error>> {
        let raw = format!(
            r#"{{"schema_version":1,"request_id":"{ID_1}","credential_id":"{ID_2}","symbol":"BTC/USDT","config":{{"order_notional":"5","spacing_rate":"0.002","grid_levels":20,"max_total_notional":"500"}},"secret":"no"}}"#
        );
        assert!(serde_json::from_str::<GridInstanceCreateRequest>(&raw).is_err());
        Ok(())
    }
}
