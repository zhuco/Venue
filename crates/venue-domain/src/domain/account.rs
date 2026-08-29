use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::Asset;

/// Authoritative asset balance returned by a signed account readback.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountBalance {
    pub asset: Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub wallet_balance: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub available_balance: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub initial_margin: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub maintenance_margin: Decimal,
}

impl AccountBalance {
    pub fn validate(&self) -> Result<(), AccountError> {
        if self.wallet_balance.is_sign_negative()
            || self.available_balance.is_sign_negative()
            || self.initial_margin.is_sign_negative()
            || self.maintenance_margin.is_sign_negative()
        {
            return Err(AccountError::Negative);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AccountError {
    #[error("account balance and margin fields cannot be negative")]
    Negative,
}
