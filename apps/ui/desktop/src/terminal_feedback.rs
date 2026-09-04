use venue_control_protocol::{
    accounts::{AccountErrorCode, AccountErrorResponse},
    kol::{ExecutorCommandState, ExecutorCommandSummary},
};

use crate::i18n::Language;

pub(crate) fn show(ui: &mut egui::Ui, model: &crate::model::AppModel) {
    let language = model.preferences.language;
    let row = model
        .execution
        .terminal_executions
        .iter()
        .filter(|row| {
            row.origin == venue_control_protocol::kol::ExecutorCommandOrigin::Terminal
                && Some(row.trading_account_id.as_str())
                    == model.preferences.execution_account_id.as_deref()
                && row.symbol.to_string() == model.preferences.selected_symbol
                && model
                    .execution
                    .terminal_request_id
                    .as_deref()
                    .is_none_or(|id| row.request_id.as_deref() == Some(id))
        })
        .max_by_key(|row| row.created_ms);
    if let Some(error) = &model.execution.terminal_submission_error {
        ui.separator();
        ui.colored_label(crate::theme::SELL, error);
    } else if let Some(row) = row {
        ui.separator();
        let color = if matches!(
            row.state,
            ExecutorCommandState::Rejected | ExecutorCommandState::ReconcileRequired
        ) {
            crate::theme::WARNING
        } else {
            crate::theme::TEXT_SECONDARY
        };
        ui.colored_label(
            color,
            format!(
                "{} · {}",
                choose(language, "最近委托", "Latest command"),
                command_state(row.state, language)
            ),
        );
        if row.sanitized_error_code.is_some() {
            ui.colored_label(color, command_reason(row, language));
        }
        ui.add(egui::Label::new(egui::RichText::new(&row.command_id).small()).truncate())
            .on_hover_text(&row.command_id);
    } else if model.execution.terminal_request_id.is_some() {
        ui.separator();
        ui.colored_label(
            crate::theme::WARNING,
            choose(
                language,
                "已进入发送队列，等待服务端回执；此时不代表已挂单。",
                "Queued for submission; awaiting server receipt. Not yet a working order.",
            ),
        );
    }
    if let Some(id) = &model.execution.terminal_request_id {
        ui.add(egui::Label::new(egui::RichText::new(format!("request: {id}")).small()).truncate())
            .on_hover_text(id);
    }
}

pub(crate) fn http_error(status: u16, body: &[u8]) -> String {
    let code = serde_json::from_slice::<AccountErrorResponse>(body)
        .ok()
        .map(|error| error.code);
    let (name, reason) = match code {
        Some(AccountErrorCode::InvalidInput) => (
            "invalid_input",
            "下单参数无效，请检查金额、价格、订单类型和确认项",
        ),
        Some(AccountErrorCode::Unauthorized | AccountErrorCode::InvalidLogin) => {
            ("unauthorized", "登录已失效，请重新登录")
        }
        Some(AccountErrorCode::VerificationRequired) => (
            "verification_required",
            "API 未通过验证，或账户签名数据尚未就绪",
        ),
        Some(AccountErrorCode::Forbidden) => ("forbidden", "没有此交易账户的操作权限"),
        Some(AccountErrorCode::Conflict) => (
            "conflict",
            "账户状态、持仓、请求版本或运行归属冲突；需核对服务端记录",
        ),
        Some(AccountErrorCode::AccountInUse) => ("account_in_use", "账户仍被运行任务占用"),
        Some(AccountErrorCode::RateLimited) => ("rate_limited", "请求频率或账户队列达到上限"),
        Some(AccountErrorCode::NotFound) => ("not_found", "账户或指定委托不存在"),
        Some(AccountErrorCode::Unavailable) => (
            "unavailable",
            "服务暂不可用；请核对历史委托后再决定是否重试",
        ),
        Some(AccountErrorCode::UsernameUnavailable) | None => (
            "unrecognized_response",
            "无法识别安全错误信息；请核对历史委托，不直接重复提交",
        ),
    };
    format!("Control 拒绝请求 [{name}; HTTP {status}]：{reason}")
}

pub(crate) fn command_reason(summary: &ExecutorCommandSummary, language: Language) -> String {
    let Some(code) = summary.sanitized_error_code.as_deref() else {
        return "—".into();
    };
    if code.is_empty()
        || code.len() > 64
        || !code.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
    {
        return choose(
            language,
            "错误码格式异常，原文已隐藏",
            "Invalid error code; raw content hidden",
        )
        .into();
    }
    let (zh, en) = match code {
        "not_dispatched_invalid" => (
            "发送前校验未通过；可能涉及请求、数量/价格规则或签名账户事实。当前服务端未记录更细分原因。",
            "Pre-send validation failed for the request, order rules or signed account facts. The server did not record a finer reason.",
        ),
        "not_dispatched_unavailable" => (
            "发送前所需的账户数据、规则或服务不可用，未向交易所发单。",
            "Required account facts, rules or service unavailable before dispatch; not sent to the exchange.",
        ),
        "credential_unavailable" => (
            "执行器无法取得有效凭证，未向交易所发单。",
            "Executor credentials unavailable; not sent to the exchange.",
        ),
        "binance_rejected" => (
            "币安拒绝此委托；当前账本没有保存更细分的交易所原因。",
            "Binance rejected the order; the ledger has no finer exchange reason.",
        ),
        "dispatch_unknown"
        | "readback_unknown"
        | "restart_reconcile"
        | "readback_unavailable"
        | "readback_credentials_unavailable"
        | "signed_identity_missing" => (
            "提交结果尚未确认，等待签名查单；不要重复下单。",
            "Outcome unconfirmed; awaiting signed reconciliation. Do not submit a duplicate.",
        ),
        "signed_terminal_no_fill" => (
            "签名查单确认委托已结束且没有成交。",
            "Signed readback confirms a terminal order with no fill.",
        ),
        _ => (
            "服务端返回此安全错误码，需继续核对对应命令。",
            "Safe server error code; inspect the associated command for details.",
        ),
    };
    format!("[{code}] {}", choose(language, zh, en))
}

pub(crate) fn command_state(state: ExecutorCommandState, language: Language) -> &'static str {
    let (zh, en) = match state {
        ExecutorCommandState::Pending => ("已入账，等待执行", "Recorded; queued"),
        ExecutorCommandState::Sending => ("正在提交", "Sending"),
        ExecutorCommandState::Accepted => ("已接受，待确认", "Accepted; awaiting confirmation"),
        ExecutorCommandState::Rejected => ("已拒绝", "Rejected"),
        ExecutorCommandState::ReconcileRequired => ("结果未知，正在查单", "Unknown; reconciling"),
        ExecutorCommandState::Reconciled => (
            "命令已核对（非成交状态）",
            "Command reconciled; not fill status",
        ),
        ExecutorCommandState::Cancelled => ("发送前已取消", "Cancelled before dispatch"),
    };
    choose(language, zh, en)
}

fn choose(language: Language, zh: &'static str, en: &'static str) -> &'static str {
    match language {
        Language::SimplifiedChinese => zh,
        Language::English => en,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_failure_exposes_only_the_typed_error_code() {
        let message = http_error(
            409,
            br#"{"code":"verification_required","secret":"must-not-render"}"#,
        );
        assert!(message.contains("verification_required") && message.contains("HTTP 409"));
        assert!(!message.contains("must-not-render"));
        let malformed = http_error(502, b"APIKEY=must-not-render");
        assert!(malformed.contains("HTTP 502") && !malformed.contains("APIKEY"));
    }

    #[test]
    fn accepted_and_reconciled_commands_are_not_reported_as_fills() {
        assert_eq!(
            command_state(
                ExecutorCommandState::Reconciled,
                Language::SimplifiedChinese
            ),
            "命令已核对（非成交状态）"
        );
        assert_eq!(
            command_state(
                ExecutorCommandState::ReconcileRequired,
                Language::SimplifiedChinese
            ),
            "结果未知，正在查单"
        );
    }
}
