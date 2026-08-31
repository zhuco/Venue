//! Shared indicator catalog migrated from the frozen VenuePulse implementation.
//!
//! Algorithms remain transport-free and accept only normalized `venue-domain` facts at the
//! public boundary. Private floating-point samples preserve the verified legacy formulas without
//! introducing a second public market model.

mod core;
mod registry;
mod series;

pub mod book;
pub mod momentum;
pub mod price;
pub mod statistics;
pub mod trade;
pub mod trend;
pub mod volatility;
pub mod volume;

pub use core::{
    BarIndicator, BookIndicator, IndicatorError, IndicatorResult, Reset, ScalarPairIndicator,
    TradeIndicator, Warmup,
};
pub use registry::{IndicatorCatalog, IndicatorCategory, IndicatorDescriptor, IndicatorInput};

#[cfg(test)]
mod tests;
