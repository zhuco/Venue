use serde::{Deserialize, Serialize};

pub const KOL_SOURCE_PATH: &str = "/v2/kol/source";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KolSourceRequest {
    pub credential_id: String,
    pub expected_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KolSourceSummary {
    pub trading_account_id: Option<String>,
    pub revision: u64,
    pub can_change: bool,
}
