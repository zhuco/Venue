use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{FieldState, Price, Symbol, UnknownReason};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketLevel {
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub symbol: Symbol,
    pub generation: u64,
    pub sequence: u64,
    pub exchange_time_ms: Option<u64>,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarketDelta {
    pub symbol: Symbol,
    pub generation: u64,
    pub first_sequence: u64,
    pub previous_sequence: Option<u64>,
    pub sequence: u64,
    pub exchange_time_ms: Option<u64>,
    pub bids: Vec<MarketLevel>,
    pub asks: Vec<MarketLevel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggressorSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicTrade {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub transaction_time_ms: u64,
    pub aggregate_trade_id: u64,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_quantity: Decimal,
    pub aggressor: FieldState<AggressorSide>,
}

/// One completed normalized public bar. Strategies consume only closed bars; an in-progress
/// exchange kline is never promoted into this domain fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicBar {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub sequence: u64,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub interval_ms: u64,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicTicker {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub transaction_time_ms: u64,
    pub update_id: u64,
    pub bid_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub bid_quantity: Decimal,
    pub ask_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub ask_quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarkFunding {
    pub symbol: Symbol,
    pub generation: u64,
    pub received_at_ms: u64,
    pub exchange_time_ms: u64,
    pub next_funding_time_ms: u64,
    pub mark_price: Price,
    pub index_price: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_rate: Decimal,
    pub estimated_settle_price: FieldState<Price>,
    pub predicted_funding_rate: FieldState<Decimal>,
    pub unknown_reason: Option<UnknownReason>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
pub enum MarketEvent {
    Snapshot(MarketSnapshot),
    Delta(MarketDelta),
    Trade(PublicTrade),
    Bar(PublicBar),
    Ticker(PublicTicker),
    MarkFunding(MarkFunding),
}
