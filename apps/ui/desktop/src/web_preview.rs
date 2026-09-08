//! Local browser preview transports normalized public data only, never account state.
use crate::chart::ChartInterval;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{MarketSummary, UiBar};

#[cfg(target_arch = "wasm32")]
pub(crate) mod browser;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod server;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Series {
    pub symbol: String,
    pub interval: ChartInterval,
    pub bars: Vec<UiBar>,
    pub market: Option<MarketSummary>,
    pub status: String,
    pub price_scale: usize,
    pub quantity_scale: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Snapshot {
    pub selections: Vec<(String, ChartInterval)>,
    pub symbols: Vec<String>,
    pub series: Vec<Series>,
}
