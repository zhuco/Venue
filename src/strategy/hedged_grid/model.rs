use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Amount, Asset, FieldState, OrderSide, Price, Symbol};

pub const HEDGED_GRID_SCHEMA_VERSION: u16 = 3;
pub const MIN_GRID_COUNT: u8 = 1;
pub const MAX_GRID_COUNT: u8 = 50;
/// Low inventory replenishment remains three 5-quote-asset grids; it is independent from grid
/// depth.
pub const INVENTORY_REPLENISH_GRID_COUNT: u8 = 3;

/// The grid deployment identity. It remains separate from scalping and is the sole source of the
/// selected exchange/account/symbol scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HedgedGridBinding {
    pub strategy_instance_id: String,
    pub run_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub config_version: String,
    pub owner_scope: String,
}

impl HedgedGridBinding {
    pub fn validate(&self) -> Result<(), HedgedGridError> {
        let owner_identity_invalid = [
            &self.strategy_instance_id,
            &self.run_id,
            &self.exchange,
            &self.account,
        ]
        .into_iter()
        .any(|value| !valid_binding_identity(value, 36));
        if owner_identity_invalid
            || !valid_binding_identity(&self.config_version, 64)
            || !valid_binding_identity(&self.owner_scope, 64)
        {
            return Err(HedgedGridError::Binding);
        }
        Ok(())
    }
}

fn valid_binding_identity(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Phase-one values are fixed.  The exchange supplies the normalized physical step and quantity
/// for each epoch; this release never owns tick, step, or native-symbol conversion rules.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HedgedGridParams {
    pub order_notional: Amount,
    #[serde(with = "rust_decimal::serde::str")]
    pub spacing_rate: Decimal,
    pub grid_count: u8,
    pub inventory_replenish_grid_count: u8,
}

impl HedgedGridParams {
    pub fn phase_one(grid_count: u8) -> Result<Self, HedgedGridError> {
        Self::fixed_release(
            Asset::new("USDC").map_err(|_| HedgedGridError::Params)?,
            grid_count,
        )
    }

    /// Builds the fixed low-balance release for the deployment's quote asset. Exchange-specific
    /// ticks, contract multipliers and quantity steps remain execution concerns.
    pub fn fixed_release(quote_asset: Asset, grid_count: u8) -> Result<Self, HedgedGridError> {
        if !matches!(quote_asset.as_str(), "USDC" | "USDT") {
            return Err(HedgedGridError::Params);
        }
        let params = Self {
            order_notional: Amount::new(quote_asset, Decimal::new(5, 0)),
            spacing_rate: Decimal::new(2, 3),
            grid_count,
            inventory_replenish_grid_count: INVENTORY_REPLENISH_GRID_COUNT,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn replenish_notional(&self) -> Amount {
        Amount::new(
            self.order_notional.asset.clone(),
            self.order_notional.value * Decimal::from(self.inventory_replenish_grid_count),
        )
    }

    pub fn validate(&self) -> Result<(), HedgedGridError> {
        if !matches!(self.order_notional.asset.as_str(), "USDC" | "USDT")
            || self.order_notional.value != Decimal::new(5, 0)
            || self.spacing_rate != Decimal::new(2, 3)
            || !(MIN_GRID_COUNT..=MAX_GRID_COUNT).contains(&self.grid_count)
            || self.inventory_replenish_grid_count != INVENTORY_REPLENISH_GRID_COUNT
        {
            return Err(HedgedGridError::Params);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridPosition {
    Long,
    Short,
}

/// Complete owned execution evidence accepted by the strategy. The runtime may submit unknown
/// maker evidence first and replay the same execution after signed readback proves its role.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnedGridFill {
    pub fill_id: String,
    pub private_generation: u64,
    pub source_order: GridOrderKey,
    pub fill_price: Price,
    pub complete: bool,
    pub maker: FieldState<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnedGridFillRecord {
    pub source_order: GridOrderIntent,
    pub fill_price: Price,
    #[serde(default)]
    pub private_generation: u64,
    pub maker: Option<bool>,
    pub grid_action_emitted: bool,
    /// Durable tombstone for a legacy maker fact discovered only after a fully drained Stop. It
    /// prevents replay from driving the retired epoch without claiming that a grid action ran.
    #[serde(default)]
    pub retired_without_action: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryDeficiency {
    pub long: bool,
    pub short: bool,
}

impl InventoryDeficiency {
    pub const fn any(&self) -> bool {
        self.long || self.short
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum InventoryRecoveryState {
    #[default]
    Inactive,
    Deficient {
        legs: InventoryDeficiency,
        first_seen_generation: u64,
    },
    AwaitingNextOwnedFill {
        armed_generation: u64,
    },
    ReanchorPending {
        fill_id: String,
        fill_price: Price,
    },
    Rebuilding {
        fill_id: String,
        fill_price: Price,
    },
}

impl GridPosition {
    pub const fn opening_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Buy,
            Self::Short => OrderSide::Sell,
        }
    }

    pub const fn closing_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Sell,
            Self::Short => OrderSide::Buy,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridOrderRole {
    Open,
    Close,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GridOrderKey {
    pub epoch: u64,
    pub position: GridPosition,
    pub role: GridOrderRole,
    /// Monotonic identity sequence within one position/role lane. It is not a permanent
    /// coordinate around the epoch anchor: rolling orders keep increasing this value while
    /// their physical prices follow the market in either direction.
    pub level: u64,
}

impl GridOrderKey {
    pub fn validate(&self) -> Result<(), HedgedGridError> {
        if self.epoch == 0 || self.level == 0 {
            return Err(HedgedGridError::OrderKey);
        }
        Ok(())
    }
}

/// One exchange-normalized grid epoch.  `step` and `grid_qty` are supplied by execution after
/// it has applied the current instrument rules; the strategy only preserves them consistently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PassiveBookFallbackAnchor {
    pub fill_id: String,
    pub fill_price: Price,
    pub anchor_price: Price,
    pub crossing_side: OrderSide,
    pub crossing_limit_price: Price,
    pub bid: Price,
    pub ask: Price,
    pub selected_at_ms: u64,
}

impl PassiveBookFallbackAnchor {
    pub fn validate(&self) -> Result<(), HedgedGridError> {
        let crossing_proven = match self.crossing_side {
            OrderSide::Buy => self.crossing_limit_price.value() >= self.ask.value(),
            OrderSide::Sell => self.crossing_limit_price.value() <= self.bid.value(),
        };
        if self.fill_id.is_empty()
            || self.selected_at_ms == 0
            || self.bid.value() >= self.ask.value()
            || self.anchor_price.value() < self.bid.value()
            || self.anchor_price.value() > self.ask.value()
            || !crossing_proven
        {
            return Err(HedgedGridError::Epoch);
        }
        Ok(())
    }

    pub fn matches_fill(&self, fill_id: &str, fill_price: Price) -> bool {
        self.fill_id == fill_id && self.fill_price == fill_price
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridEpoch {
    pub epoch: u64,
    pub anchor_price: Price,
    pub step: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_quantity: Decimal,
    /// Present only when an inventory-recovery fill anchor would make at least one complete-grid
    /// post-only order immediately marketable. The selected BBO midpoint and the crossing proof
    /// are persisted with the epoch before any install WAL is prepared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passive_book_fallback: Option<PassiveBookFallbackAnchor>,
}

impl GridEpoch {
    pub fn validate(&self, grid_count: u8) -> Result<(), HedgedGridError> {
        if self.epoch == 0
            || self.grid_quantity <= Decimal::ZERO
            || !(MIN_GRID_COUNT..=MAX_GRID_COUNT).contains(&grid_count)
            || self.passive_book_fallback.as_ref().is_some_and(|fallback| {
                fallback.validate().is_err() || fallback.anchor_price != self.anchor_price
            })
        {
            return Err(HedgedGridError::Epoch);
        }
        let outer = self.step.value() * Decimal::from(grid_count);
        if self.anchor_price.value() <= outer {
            return Err(HedgedGridError::Epoch);
        }
        Ok(())
    }

    pub fn price(
        &self,
        position: GridPosition,
        role: GridOrderRole,
        level: u8,
    ) -> Result<Price, HedgedGridError> {
        if level == 0 {
            return Err(HedgedGridError::OrderKey);
        }
        let distance = self.step.value() * Decimal::from(level);
        let lower = self.anchor_price.value() - distance;
        let upper = self.anchor_price.value() + distance;
        let value = match (position, role) {
            (GridPosition::Long, GridOrderRole::Open)
            | (GridPosition::Short, GridOrderRole::Close) => lower,
            (GridPosition::Short, GridOrderRole::Open)
            | (GridPosition::Long, GridOrderRole::Close) => upper,
        };
        Price::new(value).map_err(|_| HedgedGridError::Epoch)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridOrderIntent {
    pub key: GridOrderKey,
    pub side: OrderSide,
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub reduce_only: bool,
}

impl GridOrderIntent {
    pub fn validate(&self) -> Result<(), HedgedGridError> {
        self.key.validate()?;
        if self.quantity <= Decimal::ZERO
            || self.side
                != match self.key.role {
                    GridOrderRole::Open => self.key.position.opening_side(),
                    GridOrderRole::Close => self.key.position.closing_side(),
                }
            || self.reduce_only != matches!(self.key.role, GridOrderRole::Close)
        {
            return Err(HedgedGridError::Order);
        }
        Ok(())
    }
}

/// A complete, same-generation inventory observation supplied by the runtime after private
/// reconciliation.  Public prices are never accepted as a substitute for `mark_price` here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridInventory {
    pub private_generation: u64,
    /// Monotonic signed-readback observation cursor within a durable private generation.
    #[serde(default)]
    pub private_observed_at_ms: u64,
    pub mark_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_quantity: Decimal,
}

impl GridInventory {
    pub fn validate(&self) -> Result<(), HedgedGridError> {
        if self.private_generation == 0
            || self.long_quantity.is_sign_negative()
            || self.short_quantity.is_sign_negative()
        {
            return Err(HedgedGridError::Inventory);
        }
        Ok(())
    }

    pub fn notional(&self, position: GridPosition) -> Decimal {
        let quantity = match position {
            GridPosition::Long => self.long_quantity,
            GridPosition::Short => self.short_quantity,
        };
        quantity * self.mark_price.value()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridResetReason {
    Startup,
    InventoryLow,
    InventoryReplenished,
    Manual,
    Reconciliation,
    StructureInvalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridPhase {
    Recovering,
    CheckingInventory,
    ReplenishingInventory,
    ResettingGrid,
    Running,
    Stopping,
    BlockedUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridReplenishment {
    pub round: u64,
    pub private_generation: u64,
    pub position: GridPosition,
    pub target_notional: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridTransaction {
    pub id: String,
    pub source_fill_id: String,
    pub source_order: GridOrderKey,
    pub places: [GridOrderIntent; 2],
    pub cancel: GridOrderKey,
    /// The exact live order removed from the projection while the transaction is reserved.
    /// It permits an execution preflight failure to return to signed reconciliation before any
    /// child mutation has been submitted.
    #[serde(default)]
    pub cancelled_order: Option<GridOrderIntent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GridAction {
    /// Runtime freezes opening and settles/cancels only owned orders before calling
    /// `begin_replenishment`; it must not cancel external orders.
    Reset {
        reason: GridResetReason,
    },
    Place(GridOrderIntent),
    Replenish(GridReplenishment),
    Dispatch(GridTransaction),
    /// Replaces ordinary rolling for the first complete owned maker fill after both Hedge legs
    /// recover full closing capacity. Runtime persists this state before cancelling the old epoch.
    ReanchorAtFill {
        fill_id: String,
        fill_price: Price,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GridDecision {
    Noop,
    Actions(Vec<GridAction>),
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HedgedGridError {
    #[error("hedged-grid binding identity is invalid")]
    Binding,
    #[error("hedged-grid parameters differ from the fixed low-balance deployment release")]
    Params,
    #[error("grid epoch is invalid")]
    Epoch,
    #[error("grid order key is invalid")]
    OrderKey,
    #[error("grid order intent is invalid")]
    Order,
    #[error("private inventory observation is invalid")]
    Inventory,
    #[error("inventory generation regressed or was reused with different facts")]
    InventoryGeneration,
    #[error("the grid transition is invalid for the current phase")]
    Phase,
    #[error("the fill is not owned by the current managed grid")]
    UnknownFill,
    #[error("the fill identity conflicts with a prior immutable fill")]
    FillConflict,
    #[error("the complete owned fill lacks authoritative maker or price evidence")]
    FillEvidence,
    #[error("the grid cannot reserve a unique rolling transaction")]
    Rolling,
    #[error("the hedged-grid checkpoint schema cannot be migrated safely")]
    Checkpoint,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(exchange: &str) -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "grid_ada_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: exchange.to_owned(),
            account: "futures".to_owned(),
            symbol: "ADA/USDT".parse()?,
            config_version: "v1".to_owned(),
            owner_scope: "grid_ada_usdt_primary".to_owned(),
        })
    }

    #[test]
    fn binding_validation_is_exchange_agnostic_but_identity_strict()
    -> Result<(), Box<dyn std::error::Error>> {
        binding("future_adapter")?.validate()?;

        let mut blank = binding("future_adapter")?;
        blank.account.clear();
        assert_eq!(blank.validate(), Err(HedgedGridError::Binding));

        let mut non_canonical = binding("future_adapter")?;
        non_canonical.exchange = "future adapter".to_owned();
        assert_eq!(non_canonical.validate(), Err(HedgedGridError::Binding));

        let mut inconsistent_owner = binding("future_adapter")?;
        inconsistent_owner.run_id = "r".repeat(37);
        assert_eq!(inconsistent_owner.validate(), Err(HedgedGridError::Binding));
        Ok(())
    }
}
