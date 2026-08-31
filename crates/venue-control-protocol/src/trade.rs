use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{ProtocolError, positive};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingAction {
    OpenLong,
    CloseLong,
    CloseShort,
    OpenShort,
    CancelSelectedOrder,
    CancelAllOrders,
    SelectSizePreset(usize),
    ClearSelection,
    CenterMarket,
}

impl TradingAction {
    #[must_use]
    pub const fn is_order_action(self) -> bool {
        matches!(
            self,
            Self::OpenLong | Self::CloseLong | Self::CloseShort | Self::OpenShort
        )
    }

    #[must_use]
    pub const fn is_close_action(self) -> bool {
        matches!(self, Self::CloseLong | Self::CloseShort)
    }

    #[must_use]
    pub const fn is_ui_only(self) -> bool {
        matches!(
            self,
            Self::SelectSizePreset(_) | Self::ClearSelection | Self::CenterMarket
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingOrderType {
    Limit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingTimeInForce {
    Gtc,
}

/// Secret-free semantic manual-trading request. The account Node must re-read positions and
/// working orders, normalize quantity through its exchange adapter, run risk, and append the
/// resulting command to the account WAL before any mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TradeIntent {
    pub action: TradingAction,
    pub quote_asset: String,
    pub order_type: TradingOrderType,
    pub time_in_force: TradingTimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub selected_price: Option<Decimal>,
    pub quote_notional: Option<Decimal>,
    /// UI-observed upper bound for a close. It never replaces the Node's signed position clamp.
    pub close_quantity_cap: Option<Decimal>,
    /// Optional explicit selection. `None` means the Node must select the most recent Working
    /// order within the enclosing account + symbol scope.
    pub selected_order_id: Option<String>,
}

impl TradeIntent {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.action.is_ui_only()
            || self.quote_asset.trim().is_empty()
            || self.order_type != TradingOrderType::Limit
            || self.time_in_force != TradingTimeInForce::Gtc
            || self.reduce_only != self.action.is_close_action()
        {
            return Err(ProtocolError::TradeIntent);
        }
        if self.action.is_order_action() {
            if self.selected_price.is_none_or(|value| !positive(value))
                || self.quote_notional.is_none_or(|value| !positive(value))
                || self.selected_order_id.is_some()
            {
                return Err(ProtocolError::TradeIntent);
            }
            if self.action.is_close_action() {
                if self.close_quantity_cap.is_none_or(|value| !positive(value)) {
                    return Err(ProtocolError::TradeIntent);
                }
            } else if self.close_quantity_cap.is_some() {
                return Err(ProtocolError::TradeIntent);
            }
            return Ok(());
        }
        if self.selected_price.is_some()
            || self.quote_notional.is_some()
            || self.close_quantity_cap.is_some()
        {
            return Err(ProtocolError::TradeIntent);
        }
        match self.action {
            TradingAction::CancelSelectedOrder => {
                if self
                    .selected_order_id
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Err(ProtocolError::TradeIntent);
                }
            }
            TradingAction::CancelAllOrders => {
                if self.selected_order_id.is_some() {
                    return Err(ProtocolError::TradeIntent);
                }
            }
            _ => return Err(ProtocolError::TradeIntent),
        }
        Ok(())
    }

    #[must_use]
    pub const fn reduce_only(&self) -> bool {
        self.reduce_only
    }
}
