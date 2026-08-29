use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

/// Normalized logical-risk unit required by durable risk projections.
pub trait RiskUnitValue: Clone + Debug + Eq + Ord + Serialize + DeserializeOwned {
    fn as_str(&self) -> &str;
}

/// Normalized logical-risk fact view shared by strategy and storage boundaries.
pub trait RiskFactValue<U>: Clone + Debug + Eq + Serialize + DeserializeOwned {
    fn fact_id(&self) -> &str;
    fn event_time_ms(&self) -> u64;
    fn valuation_generation(&self) -> u64;
    fn risk_unit(&self) -> &U;
}
