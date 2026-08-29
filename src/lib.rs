mod app;
mod backoff;
pub mod cli;
pub mod config;
pub mod controller;
mod credential_env;
mod deployment;
pub mod domain;
mod error;
pub mod exchange;
pub mod execution;
pub mod indicator;
mod log;
pub mod market;
pub mod risk;
pub mod runtime;
pub mod storage;
pub mod strategy;

pub use app::start;
pub use cli::Cli;
#[cfg(feature = "hedged-grid-binance")]
pub use deployment::start_hedged_grid_binance_deployment;
#[cfg(feature = "hedged-grid-bitget")]
pub use deployment::start_hedged_grid_bitget_deployment;
#[cfg(feature = "hedged-grid-gate")]
pub use deployment::start_hedged_grid_gate_deployment;
pub use error::{Error, Result};
