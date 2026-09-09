//! Facts-driven grid using the existing planner and the shared PostgreSQL executor.
mod configuration;
mod planner;
pub use configuration::GridDepthUpdate;
mod progress;
mod runtime;
mod store;
pub use runtime::StrategyGridRuntime;
pub use store::{StrategyGridConfig, StrategyGridRecord, StrategyGridStore};
#[cfg(test)]
mod tests;
