use crate::i18n::Language;

#[derive(Clone, Copy)]
pub enum Key {
    Positions,
    Orders,
    Fills,
    CurrentSymbol,
    NoAccount,
    Waiting,
    Empty,
    Stale,
    Symbol,
    Side,
    Size,
    Price,
    Entry,
    Mark,
    State,
    Filled,
    Time,
    Instance,
    OrderId,
    FillId,
    ReduceOnly,
    Signed,
}

pub fn text(language: Language, key: Key) -> &'static str {
    let pair = match key {
        Key::Positions => ("持仓", "Positions"),
        Key::Orders => ("委托记录", "Orders"),
        Key::Fills => ("个人成交", "My trades"),
        Key::CurrentSymbol => ("仅当前交易对", "Current symbol"),
        Key::NoAccount => ("未选择交易帐户", "No trading account"),
        Key::Waiting => (
            "等待账户节点返回签名数据",
            "Waiting for signed account data",
        ),
        Key::Empty => (
            "暂无已投影记录（不代表账户已确认空仓）",
            "No projected records; account emptiness is not confirmed",
        ),
        Key::Stale => (
            "数据过期 / 连接异常，仅供查看",
            "Stale / disconnected — read only",
        ),
        Key::Symbol => ("交易对", "Symbol"),
        Key::Side => ("方向", "Side"),
        Key::Size => ("数量", "Size"),
        Key::Price => ("价格", "Price"),
        Key::Entry => ("开仓均价", "Entry"),
        Key::Mark => ("标记价格", "Mark"),
        Key::State => ("状态", "State"),
        Key::Filled => ("已成交", "Filled"),
        Key::Time => ("更新时间", "Updated"),
        Key::Instance => ("实例", "Instance"),
        Key::OrderId => ("委托号", "Order ID"),
        Key::FillId => ("成交号", "Fill ID"),
        Key::ReduceOnly => ("只减仓", "Reduce only"),
        Key::Signed => ("签名投影", "Signed projection"),
    };
    match language {
        Language::SimplifiedChinese => pair.0,
        Language::English => pair.1,
    }
}
