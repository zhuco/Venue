use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    AccountRiskSnapshot, Amount, InstrumentMetadata, Order, OrderSide, PositionSide, Price, Symbol,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MmConfig {
    pub symbol: Symbol,
    pub order_notional: Amount,
    pub max_leg_notional: Amount,
    pub max_gross_notional: Amount,
    pub max_net_notional: Amount,
    #[serde(with = "rust_decimal::serde::str")]
    pub base_half_spread_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub inventory_skew_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub volatility_multiplier: Decimal,
    pub volatility_period: usize,
    pub refresh_interval_ms: u64,
    pub max_market_age_ms: u64,
    pub max_private_age_ms: u64,
    /// These account-wide guards use the signed account risk currency, not assumed stablecoin parity.
    pub max_loss_quote: Amount,
    pub max_drawdown_quote: Amount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MmControl {
    Run,
    Stop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MmQuote {
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    /// Domain close intent. A Hedge adapter must omit unsupported native reduceOnly fields.
    pub reduce_only: bool,
}

#[derive(Clone, Debug)]
pub struct MmInput {
    pub config: MmConfig,
    pub instrument: InstrumentMetadata,
    pub maximum_quantity: Decimal,
    pub maximum_price: Price,
    pub best_bid: Price,
    pub best_ask: Price,
    pub mark_price: Price,
    pub market_observed_at_ms: u64,
    pub long_quantity: Decimal,
    pub short_quantity: Decimal,
    /// All signed live orders on this symbol, including manual orders; terminal orders are ignored.
    pub live_orders: Vec<Order>,
    pub owned_order_ids: BTreeSet<String>,
    /// Full remaining durable Place intents not yet in live_orders. Never double-count a signed order.
    /// A pending cancellation remains in live_orders until signed terminal confirmation.
    pub pending_quotes: Vec<MmQuote>,
    pub unknown_results: bool,
    pub account: AccountRiskSnapshot,
    pub equity_baseline: Amount,
    pub equity_peak: Amount,
    /// Fresh margin-derived capacity for additional opens after all outstanding reservations.
    /// The host owns leverage verification and currency conversion; zero allows closes only.
    pub available_open_notional: Amount,
    pub volatility_bps: Decimal,
    pub previous_quotes_at_ms: Option<u64>,
    pub now_ms: u64,
    pub control: MmControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MmReason {
    Normal,
    RiskReduction,
    Replacement,
    PendingConfirmation,
    StaleFacts,
    UnknownResults,
    Stopped,
    LossLimit,
    DrawdownLimit,
    NoSafeQuote,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum MmAction {
    Keep {
        reason: MmReason,
    },
    /// Must obtain signed terminal facts and replan. No replacement may be sent from this plan.
    CancelThenReplan {
        order_ids: Vec<String>,
        reason: MmReason,
    },
    Quote {
        quotes: Vec<MmQuote>,
        reason: MmReason,
    },
    /// Loss/drawdown stops must be latched durably by the host until explicit operator restart.
    Halt {
        reason: MmReason,
        cancel_order_ids: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MmPlan {
    pub action: MmAction,
    #[serde(with = "rust_decimal::serde::str")]
    pub net_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub gross_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub half_spread_bps: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MmError {
    #[error("inventory market making configuration is invalid")]
    Config,
    #[error("inventory market making facts are invalid or inconsistent")]
    Facts,
    #[error("inventory market making decimal arithmetic overflow")]
    Arithmetic,
}
