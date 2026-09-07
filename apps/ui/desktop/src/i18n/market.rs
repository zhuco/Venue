use super::Language;
use crate::market::MarketStatus;

pub(crate) fn message(
    language: Language,
    status: Option<MarketStatus>,
    reason: Option<&str>,
) -> (String, String) {
    let reason = reason.unwrap_or("");
    if language == Language::English {
        return (
            match status {
                Some(MarketStatus::Live) => "Market live",
                Some(MarketStatus::Stale) => "Market update delayed",
                Some(MarketStatus::LoadingHistory) => "Loading candles",
                Some(MarketStatus::Connecting) => "Connecting",
                Some(MarketStatus::Resyncing) => "Reconnecting",
                _ => "Market unavailable",
            }
            .into(),
            reason.into(),
        );
    }
    let reason_lower = reason.to_ascii_lowercase();
    let (title, hint) = if reason_lower.contains("clock") {
        (
            "正在校准行情时间",
            "正在自动同步行情时间。校准完成后恢复更新，无需手动修改系统时间。",
        )
    } else if reason_lower.contains("429") || reason_lower.contains("rate limit") {
        (
            "行情请求过于频繁",
            "交易所暂时限制了请求频率，稍后会自动重试。",
        )
    } else if reason_lower.contains("403") || reason_lower.contains("451") {
        (
            "行情接口访问受限",
            "行情服务拒绝了当前网络的请求，请检查网络或代理连接。",
        )
    } else if reason_lower.contains("not listed") || reason_lower.contains("missing btc") {
        (
            "当前交易所暂无此合约",
            "请从当前交易所的交易对列表中选择其他合约。",
        )
    } else if reason_lower.contains("invalid")
        || reason_lower.contains("timestamp")
        || reason_lower.contains("future")
        || reason_lower.contains("mismatch")
    {
        (
            "行情数据校验未通过",
            "本次返回的数据或时间异常，已忽略该次更新，正在等待有效行情。",
        )
    } else if status == Some(MarketStatus::Stale) || reason_lower.contains("market event timeout") {
        (
            "行情更新延迟",
            "一段时间未收到有效行情，当前显示的是上次数据。收到新行情后会自动恢复。",
        )
    } else if reason_lower.contains("timed out")
        || reason_lower.contains("timeout")
        || reason_lower.contains("deadline")
    {
        ("行情请求超时", "行情服务未及时响应，正在自动重试。")
    } else {
        match status {
            Some(MarketStatus::Live) => ("行情正常", "行情正在更新。"),
            Some(MarketStatus::LoadingHistory) => ("正在加载K线", "正在获取历史K线，请稍候。"),
            Some(MarketStatus::Connecting) => ("正在连接行情", "正在建立行情连接，请稍候。"),
            Some(MarketStatus::Resyncing) => {
                ("行情连接中断", "正在自动重连，连接恢复后会重新获取行情。")
            }
            _ => (
                "行情暂时不可用",
                "尚未取得有效行情，正在等待连接或自动重试。",
            ),
        }
    };
    let hint = if reason.starts_with("REST snapshots") {
        "行情通过定时快照更新，刷新间隔至少2秒；近期成交并非完整逐笔记录。"
    } else {
        hint
    };
    (title.into(), hint.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chinese_market_errors_distinguish_recovery_actions() {
        for (reason, expected) in [
            ("market event timeout", "行情更新延迟"),
            ("public display clock expired", "正在校准行情时间"),
            ("public HTTP 429", "行情请求过于频繁"),
            ("public HTTP 403", "行情接口访问受限"),
            (
                "Market not listed on selected exchange",
                "当前交易所暂无此合约",
            ),
            ("invalid public trade", "行情数据校验未通过"),
            ("public body read timed out", "行情请求超时"),
        ] {
            let (title, hint) = message(
                Language::SimplifiedChinese,
                Some(MarketStatus::Offline),
                Some(reason),
            );
            assert_eq!(title, expected);
            assert!(!hint.contains(reason));
        }
    }
}
