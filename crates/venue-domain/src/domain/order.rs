use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Amount, FieldState, PositionSide, Price, Symbol};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderPurpose {
    Entry,
    Protection,
    TakeProfit,
    Reduce,
    ExposureTakeProfit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    New,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub client_order_id: FieldState<String>,
    pub symbol: Symbol,
    pub side: OrderSide,
    #[serde(default, skip_serializing_if = "FieldState::is_missing")]
    pub position_side: FieldState<PositionSide>,
    pub purpose: FieldState<OrderPurpose>,
    pub state: OrderState,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub filled_quantity: Decimal,
    pub limit_price: Option<Price>,
    pub average_price: FieldState<Price>,
    pub reduce_only: bool,
}

impl Order {
    pub fn validate(&self) -> Result<(), OrderError> {
        if self.order_id.trim().is_empty() {
            return Err(OrderError::EmptyId);
        }
        if !self.quantity.is_sign_positive()
            || self.quantity.is_zero()
            || self.filled_quantity.is_sign_negative()
            || self.filled_quantity > self.quantity
        {
            return Err(OrderError::Quantity);
        }
        if let FieldState::Known(purpose) = self.purpose {
            let protection = matches!(
                purpose,
                OrderPurpose::Protection
                    | OrderPurpose::TakeProfit
                    | OrderPurpose::Reduce
                    | OrderPurpose::ExposureTakeProfit
            );
            if protection != self.reduce_only {
                return Err(OrderError::ReduceOnly);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub fill_id: String,
    /// Venue-native monotonic execution/trade sequence normalized by the adapter. Timestamp and
    /// opaque identifier ordering are never substitutes when an order has multiple executions.
    #[serde(default, skip_serializing_if = "FieldState::is_missing")]
    pub execution_sequence: FieldState<u64>,
    pub order_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    #[serde(default, skip_serializing_if = "FieldState::is_missing")]
    pub position_side: FieldState<PositionSide>,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub price: Price,
    pub fee: FieldState<Amount>,
    pub realized_pnl: FieldState<Amount>,
    pub maker: FieldState<bool>,
    pub exchange_time_ms: Option<u64>,
}

impl Fill {
    pub fn validate(&self) -> Result<(), OrderError> {
        if self.fill_id.trim().is_empty() || self.order_id.trim().is_empty() {
            return Err(OrderError::EmptyId);
        }
        if !self.quantity.is_sign_positive() || self.quantity.is_zero() {
            return Err(OrderError::Quantity);
        }
        Ok(())
    }
}

impl FromStr for OrderState {
    type Err = OrderError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "new" => Ok(Self::New),
            "partially_filled" => Ok(Self::PartiallyFilled),
            "filled" => Ok(Self::Filled),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            "rejected" => Ok(Self::Rejected),
            "unknown" => Ok(Self::Unknown),
            _ => Err(OrderError::State),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OrderError {
    #[error("order and fill identifiers must not be empty")]
    EmptyId,
    #[error("order and fill quantity must be positive")]
    Quantity,
    #[error("entry must not be reduce-only, while protection and reduce must be reduce-only")]
    ReduceOnly,
    #[error("unknown normalized order state")]
    State,
}
