use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Envelope<T> {
    pub code: String,
    pub data: Vec<T>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FillRow {
    pub inst_type: String,
    pub inst_id: String,
    pub bill_id: String,
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    pub fill_px: String,
    pub fill_sz: String,
    pub side: String,
    pub pos_side: String,
    pub fee_ccy: String,
    pub fee: String,
    pub ts: String,
}
