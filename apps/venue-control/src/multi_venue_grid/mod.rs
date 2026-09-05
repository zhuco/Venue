//! Facts-driven grid using the existing planner and the shared PostgreSQL executor.
mod planner;
mod progress;
mod runtime;
mod store;
pub use runtime::StrategyGridRuntime;
pub use store::{StrategyGridConfig, StrategyGridRecord, StrategyGridStore};
#[cfg(test)]
mod tests;
