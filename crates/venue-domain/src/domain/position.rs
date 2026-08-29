use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::domain::{Price, Symbol};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Net,
    Long,
    Short,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub entry_price: Option<Price>,
    pub mark_price: Option<Price>,
}
