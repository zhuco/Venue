//! Inventory-sensitive two-sided quoting over normalized signed facts, without grid levels.
mod model;
mod planner;
mod volatility;

pub use model::*;
pub use planner::plan;
pub use volatility::MmVolatility;

#[cfg(test)]
mod tests;
