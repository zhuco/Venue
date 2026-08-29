use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Amount, Asset, Price, Symbol};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Spot,
    LinearPerpetual,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Instrument {
    pub symbol: Symbol,
    pub market: MarketKind,
    pub settlement_asset: Option<Asset>,
    pub generation: u64,
    pub price_tick: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity_step: Decimal,
    pub minimum_notional: Amount,
}

impl Instrument {
    pub fn validate(&self) -> Result<(), InstrumentError> {
        if self.generation == 0 {
            return Err(InstrumentError::Generation);
        }
        if !self.quantity_step.is_sign_positive() || self.quantity_step.is_zero() {
            return Err(InstrumentError::QuantityStep);
        }
        if self.minimum_notional.value.is_sign_negative() {
            return Err(InstrumentError::MinimumNotional);
        }
        if matches!(self.market, MarketKind::LinearPerpetual) && self.settlement_asset.is_none() {
            return Err(InstrumentError::SettlementAsset);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentError {
    #[error("instrument generation must be positive")]
    Generation,
    #[error("quantity step must be positive")]
    QuantityStep,
    #[error("minimum notional cannot be negative")]
    MinimumNotional,
    #[error("linear perpetual requires a settlement asset")]
    SettlementAsset,
}
