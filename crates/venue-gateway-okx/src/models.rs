use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Envelope<T> {
    pub code: String,
    #[serde(default)]
    pub msg: String,
    pub data: Vec<T>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstrumentRow {
    pub inst_type: String,
    pub inst_id: String,
    pub settle_ccy: String,
    pub tick_sz: String,
    pub lot_sz: String,
    pub min_sz: String,
    pub ct_val: String,
    pub ct_mult: String,
    pub ct_val_ccy: String,
    pub ct_type: String,
    pub state: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookPush {
    pub arg: BookArg,
    pub data: Vec<BookRow>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookArg {
    pub channel: String,
    pub inst_id: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookRow {
    pub asks: Vec<Vec<String>>,
    pub bids: Vec<Vec<String>>,
    pub ts: String,
    pub seq_id: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountConfigRow {
    pub uid: String,
    pub main_uid: String,
    pub acct_lv: String,
    pub pos_mode: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceRow {
    pub u_time: String,
    pub details: Vec<BalanceDetailRow>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceDetailRow {
    pub ccy: String,
    pub eq: String,
    pub avail_bal: String,
    pub imr: String,
    pub mmr: String,
    pub u_time: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionRow {
    pub inst_type: String,
    pub inst_id: String,
    pub pos_side: String,
    pub pos: String,
    pub avg_px: String,
    pub mark_px: String,
    pub u_time: String,
    #[serde(default)]
    pub p_time: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRow {
    pub inst_type: String,
    pub inst_id: String,
    pub ord_id: String,
    #[serde(default)]
    pub cl_ord_id: String,
    pub side: String,
    pub pos_side: String,
    pub sz: String,
    pub acc_fill_sz: String,
    pub px: String,
    pub avg_px: String,
    pub reduce_only: String,
    pub state: String,
    pub u_time: String,
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
    pub fill_time: String,
    pub exec_type: String,
}
