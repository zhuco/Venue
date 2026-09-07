use venue_control_protocol::{
    accounts::{AccountErrorCode, AccountErrorResponse},
    kol::{ExecutorCommandState, ExecutorCommandSummary},
};

use crate::i18n::Language;

pub(crate) fn message(model: &crate::model::AppModel) -> Option<String> {
    let language = model.preferences.language;
    let row = model
        .execution
        .terminal_executions
        .iter()
        .filter(|row| {
            row.origin == venue_control_protocol::kol::ExecutorCommandOrigin::Terminal
                && Some(row.trading_account_id.as_str())
                    == model.preferences.execution_account_id.as_deref()
                && (model.execution.terminal_request_id.is_some()
                    || row.symbol.to_string() == model.preferences.selected_symbol)
                && model
                    .execution
                    .terminal_request_id
                    .as_deref()
                    .is_none_or(|id| row.request_id.as_deref() == Some(id))
        })
        .max_by_key(|row| row.created_ms);
    if let Some(error) = &model.execution.terminal_submission_error {
        Some(error.clone())
    } else if let Some(row) = row.filter(|row| {
        row.sanitized_error_code.is_some()
            || matches!(
                row.state,
                ExecutorCommandState::Rejected
                    | ExecutorCommandState::ReconcileRequired
                    | ExecutorCommandState::Cancelled
            )
    }) {
        Some(
            format!(
                "{} · {} · {}",
                row.symbol,
                choose(language, "委托", "Order"),
                command_state(row.state, language)
            ) + &if row.sanitized_error_code.is_some() {
                format!(" · {}", command_reason(row, language))
            } else {
                String::new()
            },
        )
    } else {
        None
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
    describe_command_code(code, language)
}

fn describe_command_code(code: &str, language: Language) -> String {
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
        "position_not_dispatched" => (
            "服务重启前尚未发送市价操作，请根据最新持仓重新提交。",
            "Market action was not sent before restart. Review current positions before submitting again.",
        ),
        "position_changed_or_unavailable" => (
            "仓位已变化、账户数据暂不可用或权限已变化，未发送本次市价操作；请查看刷新后的持仓。",
            "Position, account data or permissions changed; market action was not sent. Check refreshed positions.",
        ),
        "reverse_position_not_flat" => (
            "原方向仍有持仓，未执行反向开仓；请检查运行中的策略及剩余仓位。",
            "The original leg is not flat; reverse opening was not sent. Check remaining exposure and running strategies.",
        ),
        "market_partial_fill" => (
            "市价委托结束但只部分成交，未继续反开；请查看剩余仓位。",
            "Market order ended with a partial fill; reversal was not continued. Check remaining exposure.",
        ),
        "market_not_filled" => (
            "市价委托结束且没有成交，未继续反开。",
            "Market order ended without a fill; reversal was not continued.",
        ),
        "not_dispatched_quantity_zero" => (
            "按交易对数量步长向下取整后为零，未发单。",
            "Quantity rounds down to zero at the symbol's step size; not sent.",
        ),
        "binance_-4164" => (
            "币安拒单：订单名义金额低于该交易对最低要求。",
            "Binance rejected the order: notional below the symbol minimum.",
        ),
        "binance_-1013" => (
            "币安拒单：请求消息无效。",
            "Binance rejected the order: invalid request message.",
        ),
        "binance_-2019" => (
            "币安拒单：保证金不足。",
            "Binance rejected the order: insufficient margin.",
        ),
        "binance_-5022" => (
            "币安拒单：Post Only 价格会立即成交，请重新选择挂单价格。",
            "Binance rejected Post Only because the price would take liquidity.",
        ),
        "binance_-1111" => (
            "币安拒单：数量或价格精度超出交易对限制。",
            "Binance rejected quantity or price precision.",
        ),
        "binance_-4061" => (
            "币安拒单：订单持仓方向与账户持仓模式不匹配。",
            "Binance rejected a position-side/account-mode mismatch.",
        ),
        "binance_-2015" => (
            "币安拒单：API Key、IP 白名单或交易权限被拒绝。",
            "Binance rejected the API key, IP whitelist or permissions.",
        ),
        "binance_-1021" => (
            "币安拒单：签名时间超出有效窗口，执行器需完成校时。",
            "Binance rejected the signed timestamp; executor clock synchronization is required.",
        ),
        "not_dispatched_invalid" => (
            "执行器未发送此委托；服务端未记录具体原因。此记录无法判断失败条件。",
            "The executor did not send this order. The server recorded no specific reason; the failed condition cannot be determined.",
        ),
        "not_dispatched_binding" => (
            "命令账户或交易对与执行连接不一致，未发送；需核对服务端命令路由。",
            "Command account or symbol does not match the execution connection; not sent. Check server routing.",
        ),
        "not_dispatched_identity" => (
            "命令标识缺失或订单标识格式无效，未发送；需核对服务端命令记录。",
            "Missing command identity or invalid order identity; not sent. Check the server command record.",
        ),
        "not_dispatched_quantity" => (
            "委托数量必须大于零，未发送。",
            "Order quantity must be greater than zero; not sent.",
        ),
        "not_dispatched_price" => (
            "委托价格或触发价格必须大于零，未发送。",
            "Order or trigger price must be greater than zero; not sent.",
        ),
        "not_dispatched_direction" => (
            "买卖方向、持仓方向与开平仓意图不一致，未发送；需核对命令参数。",
            "Order side, position side and open/close intent are inconsistent; not sent. Check command parameters.",
        ),
        "not_dispatched_order_type" => (
            "当前执行路径不支持该订单类型、有效方式或触发方式，未发送。",
            "This execution path does not support the order type, time in force or trigger type; not sent.",
        ),
        "not_dispatched_close_reservation" => (
            "开仓命令错误地携带了平仓预留数据，未发送；需修正服务端命令。",
            "An opening command unexpectedly contains close reservations; not sent. Server command needs correction.",
        ),
        "not_dispatched_cancel_target" => (
            "撤单目标缺失、标识冲突或撤单参数不一致，未发送撤单。",
            "Missing or conflicting cancel target, or inconsistent cancel parameters; cancel was not sent.",
        ),
        "not_dispatched_scope" => (
            "请求范围与执行连接或规则版本不一致，无法准备委托，未发送。",
            "Request scope does not match the execution connection or rule generation; order preparation failed, not sent.",
        ),
        "not_dispatched_instrument_rules" => (
            "交易对规则目录无法解析该合约的有效规则，未发送；需核对规则目录。",
            "The instrument catalogue could not resolve valid rules for this contract; not sent. Check the catalogue.",
        ),
        "not_dispatched_catalogue_unavailable" => (
            "公共交易对规则目录暂不可用，未发送。",
            "Public instrument catalogue is temporarily unavailable; not sent.",
        ),
        "not_dispatched_clock_unavailable" => (
            "执行器签名时钟尚未就绪，未发送。",
            "Executor signing clock is not ready; not sent.",
        ),
        "not_dispatched_signing" => (
            "执行器构造签名请求失败，未发送。",
            "Executor could not construct the signed request; not sent.",
        ),
        "not_dispatched_intent" => (
            "交易所适配器无法构造有效且明确的委托参数，未发送；需核对命令参数。",
            "The exchange adapter could not prepare a valid, unambiguous order intent; not sent. Check command parameters.",
        ),
        "not_dispatched_order_rules" => (
            "委托未通过该执行路径的交易对规则检查，未发送；并非交易所拒单。",
            "The order failed instrument-rule checks in this execution path; not sent, not an exchange rejection.",
        ),
        "not_dispatched_position" => (
            "平仓数量超出签名持仓可减范围，未发送。",
            "Close quantity exceeds the reducible signed position; not sent.",
        ),
        "not_dispatched_payload" => (
            "适配器构造的委托请求不完整或格式无效，未发送。",
            "The prepared order payload is incomplete or invalid; not sent.",
        ),
        "not_dispatched_account_facts" => (
            "该执行路径所需的签名账户事实未通过核验，未发送。",
            "Signed account facts required by this execution path did not validate; not sent.",
        ),
        "not_dispatched_ledger_conflict" => (
            "命令账本状态冲突，未发送；需核对对应命令记录。",
            "Command ledger state conflict; not sent. Check the command record.",
        ),
        "not_dispatched_unavailable" => (
            "执行服务在发送前不可用，未发送；服务端未记录具体不可用环节。",
            "Execution was unavailable before dispatch; not sent. The server did not record the specific unavailable stage.",
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
        ExecutorCommandState::Reconciled => ("已处理", "Processed"),
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
    fn exchange_reasons_keep_numeric_codes_and_hide_untrusted_content()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_control_protocol::kol::{
            ExecutorCommandOrigin, ExecutorCommandPhase, ExecutorOrderKind,
        };
        let mut summary = ExecutorCommandSummary {
            command_id: "fixture".into(),
            request_id: None,
            origin: ExecutorCommandOrigin::Terminal,
            phase: ExecutorCommandPhase::Open,
            trading_account_id: "fixture".into(),
            symbol: "DOGE/USDC".parse()?,
            position_side: Some(venue_domain::domain::PositionSide::Long),
            order_side: Some(venue_domain::domain::OrderSide::Buy),
            order_kind: ExecutorOrderKind::LimitPostOnly,
            requested_quantity: Some(1.into()),
            limit_price: Some(1.into()),
            state: ExecutorCommandState::Rejected,
            native_order_id: None,
            created_ms: 1,
            updated_ms: 1,
            sanitized_error_code: None,
        };
        for (code, reason) in [
            ("binance_-4164", "最低要求"),
            ("binance_-1013", "消息无效"),
            ("binance_-5022", "Post Only"),
            ("binance_-2019", "保证金不足"),
        ] {
            summary.sanitized_error_code = Some(code.into());
            let message = command_reason(&summary, Language::SimplifiedChinese);
            assert!(message.contains(code) && message.contains(reason));
        }
        summary.sanitized_error_code = Some("APIKEY=must-not-render".into());
        let message = command_reason(&summary, Language::SimplifiedChinese);
        assert!(message.contains("原文已隐藏") && !message.contains("must-not-render"));
        Ok(())
    }

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
    fn pre_dispatch_feedback_does_not_guess_unrecorded_causes() {
        for language in [Language::SimplifiedChinese, Language::English] {
            let unknown = describe_command_code("not_dispatched_invalid", language);
            assert!(!unknown.contains("签名账户") && !unknown.contains("signed account"));
            assert!(!unknown.contains("数量/价格") && !unknown.contains("order rules"));
            assert!(!unknown.contains("币安拒单") && !unknown.contains("Binance rejected"));
            for code in [
                "not_dispatched_binding",
                "not_dispatched_quantity",
                "not_dispatched_price",
                "not_dispatched_direction",
                "not_dispatched_order_type",
                "not_dispatched_identity",
                "not_dispatched_close_reservation",
                "not_dispatched_cancel_target",
                "not_dispatched_scope",
                "not_dispatched_instrument_rules",
                "not_dispatched_catalogue_unavailable",
                "not_dispatched_clock_unavailable",
                "not_dispatched_signing",
                "not_dispatched_intent",
                "not_dispatched_order_rules",
                "not_dispatched_position",
                "not_dispatched_payload",
                "not_dispatched_account_facts",
                "not_dispatched_ledger_conflict",
            ] {
                let reason = describe_command_code(code, language);
                assert!(reason.contains(code));
                assert!(
                    !reason.contains("服务端返回此安全错误码")
                        && !reason.contains("Safe server error code")
                );
                assert!(!reason.contains("币安拒单") && !reason.contains("Binance rejected"));
            }
        }
        let rules = describe_command_code(
            "not_dispatched_instrument_rules",
            Language::SimplifiedChinese,
        );
        assert!(rules.contains("规则目录"));
        assert!(!rules.contains("账户") && !rules.contains("名义"));
    }

    #[test]
    fn accepted_and_reconciled_commands_are_not_reported_as_fills() {
        assert_eq!(
            command_state(
                ExecutorCommandState::Reconciled,
                Language::SimplifiedChinese
            ),
            "已处理"
        );
        assert_eq!(
            command_state(
                ExecutorCommandState::ReconcileRequired,
                Language::SimplifiedChinese
            ),
            "结果未知，正在查单"
        );
    }

    #[test]
    fn normal_submission_is_silent_but_failures_keep_the_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_control_protocol::kol::{
            ExecutorCommandOrigin, ExecutorCommandPhase, ExecutorOrderKind,
        };
        let mut model = crate::model::AppModel::new(crate::model::Preferences::default());
        model.preferences.execution_account_id = Some("account-fixture".into());
        model.preferences.selected_symbol = "DOGE/USDC".into();
        model
            .execution
            .begin_terminal_submission("request-fixture".into());
        assert!(render_feedback(&model).is_empty());
        model
            .execution
            .terminal_executions
            .push(ExecutorCommandSummary {
                command_id: "command-fixture".into(),
                request_id: Some("request-fixture".into()),
                origin: ExecutorCommandOrigin::Terminal,
                phase: ExecutorCommandPhase::Open,
                trading_account_id: "account-fixture".into(),
                symbol: "DOGE/USDC".parse()?,
                position_side: Some(venue_domain::PositionSide::Long),
                order_side: Some(venue_domain::OrderSide::Buy),
                order_kind: ExecutorOrderKind::LimitPostOnly,
                requested_quantity: Some(1.into()),
                limit_price: Some(1.into()),
                state: ExecutorCommandState::Pending,
                native_order_id: None,
                created_ms: 1,
                updated_ms: 1,
                sanitized_error_code: None,
            });
        for state in [
            ExecutorCommandState::Pending,
            ExecutorCommandState::Sending,
            ExecutorCommandState::Accepted,
            ExecutorCommandState::Reconciled,
        ] {
            model.execution.terminal_executions[0].state = state;
            assert!(render_feedback(&model).is_empty());
        }
        model.execution.terminal_executions[0].state = ExecutorCommandState::Rejected;
        model.execution.terminal_executions[0].sanitized_error_code = Some("binance_-2019".into());
        let rendered = render_feedback(&model);
        assert!(rendered.contains("保证金不足") && rendered.contains("已拒绝"));
        assert!(!rendered.contains("command-fixture") && !rendered.contains("request-fixture"));
        model.execution.terminal_executions[0].state = ExecutorCommandState::ReconcileRequired;
        model.execution.terminal_executions[0].sanitized_error_code =
            Some("dispatch_unknown".into());
        assert!(render_feedback(&model).contains("不要重复下单"));
        model.preferences.execution_account_id = Some("other-account".into());
        assert!(render_feedback(&model).is_empty());
        model.execution.terminal_submission_error = Some("提交连接失败".into());
        assert!(render_feedback(&model).contains("提交连接失败"));
        Ok(())
    }

    fn render_feedback(model: &crate::model::AppModel) -> String {
        message(model).unwrap_or_default()
    }
}
