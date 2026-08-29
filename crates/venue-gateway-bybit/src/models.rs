use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Envelope<T> {
    pub ret_code: i64,
    pub result: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Page<T> {
    pub category: String,
    #[serde(default)]
    pub next_page_cursor: String,
    pub list: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionRow {
    pub symbol: String,
    pub order_id: String,
    #[serde(default)]
    pub order_link_id: String,
    pub side: String,
    pub exec_id: String,
    pub exec_price: String,
    pub exec_qty: String,
    pub exec_fee: String,
    pub exec_time: String,
    #[serde(default)]
    pub fee_currency: String,
    pub exec_type: String,
}
