use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct EventEnvelope {
    pub channel: String,
    pub data: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UserFillsData {
    pub is_snapshot: Option<bool>,
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
    pub crossed: bool,
}

#[derive(Deserialize)]
pub(crate) struct PerpMetaResponse {
    pub universe: Vec<PerpMetaRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PerpMetaRow {
    pub name: String,
    pub sz_decimals: u32,
    pub max_leverage: u32,
    #[serde(default)]
    pub is_delisted: bool,
}

#[derive(Deserialize)]
pub(crate) struct BookData {
    pub coin: String,
    pub time: u64,
    pub levels: [Vec<BookLevel>; 2],
}

#[derive(Deserialize)]
pub(crate) struct BboData {
    pub coin: String,
    pub time: u64,
    pub bbo: [Option<BookLevel>; 2],
}

#[derive(Deserialize)]
pub(crate) struct BookLevel {
    pub px: String,
    pub sz: String,
    pub n: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClearinghouseState {
    pub asset_positions: Vec<AssetPositionRow>,
    pub margin_summary: MarginSummaryRow,
    pub cross_maintenance_margin_used: String,
    pub withdrawable: String,
    pub time: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MarginSummaryRow {
    pub account_value: String,
    pub total_margin_used: String,
}

#[derive(Deserialize)]
pub(crate) struct AssetPositionRow {
    #[serde(rename = "type")]
    pub kind: String,
    pub position: PositionRow,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PositionRow {
    pub coin: String,
    pub szi: String,
    pub entry_px: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenOrderRow {
    pub coin: String,
    pub limit_px: String,
    pub oid: serde_json::Value,
    pub orig_sz: String,
    pub reduce_only: bool,
    pub side: String,
    pub sz: String,
    pub timestamp: u64,
    pub cloid: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct OrderStatusEnvelope {
    pub status: String,
    pub order: Option<OrderStatusBody>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderStatusBody {
    pub order: OrderStatusOrder,
    pub status: String,
    pub status_timestamp: u64,
}

#[derive(Deserialize)]
pub(crate) struct OrderStatusOrder {
    pub coin: String,
    pub oid: u64,
    pub cloid: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsOrderUpdateRow {
    pub order: WsBasicOrderRow,
    pub status: String,
    pub status_timestamp: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WsBasicOrderRow {
    pub coin: String,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub orig_sz: String,
    pub cloid: Option<String>,
}
