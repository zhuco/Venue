use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct EventEnvelope {
    pub channel: String,
    pub data: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserFillsData {
    pub is_snapshot: bool,
    pub user: String,
    pub fills: Vec<UserFillRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserFillRow {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub time: u64,
    pub closed_pnl: String,
    pub oid: u64,
    pub cloid: Option<String>,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub crossed: Option<bool>,
}
