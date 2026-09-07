use eframe::egui::{self, Color32, RichText, Stroke};
use venue_control_protocol::TradingAction;
use venue_control_protocol::kol::{
    TERMINAL_SCHEMA_VERSION, TerminalAction, TerminalCancelRequest, TerminalOrderKind,
    TerminalOrderRequest,
};

use crate::i18n::{TextKey, text};
use crate::{client::ControlClient, model::AppModel, theme};

pub fn show(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    if let Some(action) = controls(ui, model) {
        apply_action(model, client, action, ui.ctx());
    }
    if let Some(side) = model.trade_dock.market_close_requested.take() {
        submit_market_close(model, client, side);
    }
}

// Rendering produces the same semantic action as keyboard input, with no network side effect.
pub(crate) fn controls(ui: &mut egui::Ui, model: &mut AppModel) -> Option<TradingAction> {
    model.refresh_trading_price(ui.ctx());
    egui::ScrollArea::vertical()
        .id_salt("trade-dock-scroll")
        .auto_shrink([false, false])
        .show(ui, |ui| compact_controls(ui, model))
        .inner
}

fn compact_controls(ui: &mut egui::Ui, model: &mut AppModel) -> Option<TradingAction> {
    let language = model.preferences.language;
    let private_ready = model
        .execution
        .private_ready(model.preferences.execution_account_id.as_deref(), now_ms());
    let symbol = model.preferences.selected_symbol.clone();
    let (base, quote) = symbol_assets(&symbol);
    let mut action = None;
    ui.spacing_mut().item_spacing = egui::vec2(6.0, 4.0);
    ui.spacing_mut().interact_size.y = 22.0;
    ui.spacing_mut().button_padding = egui::vec2(6.0, 3.0);

    ui.horizontal(|ui| {
        let scope = if model.account_switch_pending.is_some() {
            "账户切换中 / Account switch pending"
        } else if model.preferences.execution_account_id.is_none() {
            crate::i18n::text(language, crate::i18n::TextKey::NoExecutionAccount)
        } else if model.market_worker_failed {
            "行情不可用 / Market unavailable"
        } else if private_ready {
            "LIVE"
        } else {
            label(language, "账户数据待刷新", "Account data awaiting refresh")
        };
        ui.label(
            RichText::new(format!("● {scope}  {symbol}"))
                .size(12.0)
                .color(if private_ready {
                    theme::BUY
                } else {
                    theme::WARNING
                }),
        )
        .on_hover_text(label(
            language,
            "手动开仓不等待持仓刷新；账户权限由服务端验证，平仓仍需新鲜持仓。",
            "Manual opens do not wait for positions; the server verifies ownership. Closes still require fresh positions.",
        ));
    });
    ui.separator();
    ui.horizontal(|ui| {
        ui.strong("Limit · Post Only");
        ui.checkbox(&mut model.preferences.trading.post_only, "Post Only");
    });
    ui.columns(2, |columns| {
        columns[0].horizontal(|ui| {
            ui.small(label(language, "价格", "Price"));
            ui.small(&quote);
        });
        let mut input = model.trade_dock.price_input.clone();
        if columns[0]
            .add(
                egui::TextEdit::singleline(&mut input)
                    .hint_text(text(language, TextKey::LimitPriceHint))
                    .desired_width(f32::INFINITY),
            )
            .changed()
        {
            model
                .trade_dock
                .edit_price(input, columns[0].ctx().input(|input| input.time));
        }
        columns[1].horizontal(|ui| {
            ui.small(text(language, TextKey::TradeSizeAmount));
            let previous = model.trade_dock.amount_in_base;
            egui::ComboBox::from_id_salt("trade-amount-unit")
                .selected_text(if previous { &base } else { &quote })
                .width(60.0)
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut model.trade_dock.amount_in_base, false, &quote);
                    ui.selectable_value(&mut model.trade_dock.amount_in_base, true, &base);
                });
            if previous != model.trade_dock.amount_in_base {
                model.trade_dock.amount_input.clear();
                model.trade_dock.armed_action = None;
            }
        });
        let preset =
            model.preferences.trading.size_presets[model.trade_dock.selected_size_preset.min(4)];
        let hint = if model.trade_dock.amount_in_base {
            text(language, TextKey::EnterBaseSize).to_owned()
        } else {
            preset.normalize().to_string()
        };
        if columns[1]
            .add(
                egui::TextEdit::singleline(&mut model.trade_dock.amount_input)
                    .hint_text(hint)
                    .desired_width(f32::INFINITY),
            )
            .changed()
        {
            model.trade_dock.armed_action = None;
        }
    });
    ui.horizontal(|ui| {
        let width = ((ui.available_width() - ui.spacing().item_spacing.x * 4.0) / 5.0).max(24.0);
        for index in 0..crate::trading::SIZE_PRESET_COUNT {
            let value = model.preferences.trading.size_presets[index];
            let title = value.normalize().to_string();
            let key = model
                .preferences
                .trading
                .hotkeys
                .key_for(TradingAction::SelectSizePreset(index))
                .map_or("—", |key| key.label());
            if ui
                .add_sized(
                    [width, 25.0],
                    egui::Button::selectable(
                        model.trade_dock.selected_size_preset == index
                            && model.trade_dock.amount_input.is_empty(),
                        RichText::new(title).size(11.0),
                    )
                    .wrap_mode(egui::TextWrapMode::Truncate),
                )
                .on_hover_text(format!("{} {quote} · {key}", value.normalize()))
                .clicked()
            {
                action = Some(TradingAction::SelectSizePreset(index));
            }
        }
    });
    let primary = [
        TradingAction::OpenLong,
        TradingAction::CloseLong,
        TradingAction::OpenShort,
        TradingAction::CloseShort,
    ];
    let columns = if ui.available_width() >= 480.0 { 4 } else { 2 };
    for row in primary.chunks(columns) {
        ui.horizontal(|ui| {
            let width = (ui.available_width() - ui.spacing().item_spacing.x * (columns - 1) as f32)
                / columns as f32;
            for candidate in row {
                if let Some(clicked) = action_button(ui, model, *candidate, width) {
                    action = Some(clicked);
                }
            }
        });
    }
    let now = ui.ctx().input(|input| input.time);
    if let Some(reason) = action_disabled_reason(model, TradingAction::OpenLong, now) {
        ui.colored_label(theme::WARNING, reason);
    } else if model.preferences.trading.hotkeys_enabled {
        ui.small(label(
            language,
            "快捷键已启用；输入框聚焦或弹窗打开时暂停。",
            "Hotkeys active; paused while editing text or while a dialog is open.",
        ));
    } else {
        ui.small(label(
            language,
            "快捷键已在设置中关闭。",
            "Hotkeys are disabled in Settings.",
        ));
    }
    ui.horizontal(|ui| {
        let width = (ui.available_width() - ui.spacing().item_spacing.x) / 2.0;
        for (side, action, title) in [
            (
                venue_domain::PositionSide::Long,
                TradingAction::CloseLong,
                label(language, "市价平多", "Market Close Long"),
            ),
            (
                venue_domain::PositionSide::Short,
                TradingAction::CloseShort,
                label(language, "市价平空", "Market Close Short"),
            ),
        ] {
            let quantity = model
                .execution
                .position_quantity(&symbol, side)
                .unwrap_or(rust_decimal::Decimal::ZERO);
            let armed = model.trade_dock.armed_action == Some(action);
            if ui
                .add_enabled(
                    private_ready && quantity > rust_decimal::Decimal::ZERO,
                    egui::Button::new(if armed {
                        format!("确认 {title}")
                    } else {
                        title.to_owned()
                    })
                    .min_size(egui::vec2(width, 28.0)),
                )
                .on_hover_text(label(
                    language,
                    "第二次点击确认；Executor 下单前按最新签名仓位再次裁剪。",
                    "Click twice to confirm; Executor re-clips against the latest signed position.",
                ))
                .clicked()
            {
                if armed {
                    model.trade_dock.market_close_requested = Some(side);
                } else {
                    model.trade_dock.armed_action = Some(action);
                }
            }
        }
    });
    ui.horizontal(|ui| {
        let width = (ui.available_width() - ui.spacing().item_spacing.x) / 2.0;
        for candidate in [
            TradingAction::CancelSelectedOrder,
            TradingAction::CancelAllOrders,
        ] {
            if let Some(clicked) = action_button(ui, model, candidate, width) {
                action = Some(clicked);
            }
        }
    });
    if let Some(armed) = model.trade_dock.armed_action {
        ui.colored_label(theme::WARNING, action_name(language, armed));
    }
    action
}

fn symbol_assets(symbol: &str) -> (String, String) {
    symbol.split_once('/').map_or_else(
        || ("BASE".to_owned(), "QUOTE".to_owned()),
        |(base, quote)| (base.to_owned(), quote.to_owned()),
    )
}

fn action_palette(action: TradingAction) -> (Color32, Color32, Color32) {
    match action {
        TradingAction::OpenLong => (Color32::WHITE, Color32::from_rgb(10, 103, 76), theme::BUY),
        TradingAction::CloseLong => (theme::BUY, theme::BG_SECONDARY, theme::BUY),
        TradingAction::CloseShort => (theme::SELL, theme::BG_SECONDARY, theme::SELL),
        TradingAction::OpenShort => (Color32::WHITE, Color32::from_rgb(132, 35, 52), theme::SELL),
        TradingAction::CancelSelectedOrder => {
            (theme::TEXT_PRIMARY, theme::BG_SECONDARY, theme::DIVIDER)
        }
        TradingAction::CancelAllOrders => (theme::WARNING, theme::BG_SECONDARY, theme::WARNING),
        _ => (theme::TEXT_PRIMARY, theme::BG_SECONDARY, theme::DIVIDER),
    }
}
pub(crate) fn action_button(
    ui: &mut egui::Ui,
    model: &AppModel,
    action: TradingAction,
    width: f32,
) -> Option<TradingAction> {
    let title = action_name(model.preferences.language, action);
    let key = model
        .preferences
        .trading
        .hotkeys
        .key_for(action)
        .map_or("—", |key| key.label());
    let disabled_reason = action_disabled_reason(model, action, ui.ctx().input(|input| input.time));
    let enabled = disabled_reason.is_none();
    let (text_color, fill, stroke) = action_palette(action);
    let response = ui.add_enabled(
        enabled,
        egui::Button::new(
            RichText::new(format!("{title}  {key}"))
                .size(12.0)
                .strong()
                .color(text_color),
        )
        .fill(fill)
        .stroke(Stroke::new(1.0, stroke))
        .min_size(egui::vec2(width, 30.0)),
    );
    let response = if let Some(reason) = disabled_reason {
        response.on_disabled_hover_text(reason)
    } else {
        response
    };
    response.clicked().then_some(action)
}

fn action_disabled_reason(model: &AppModel, action: TradingAction, now: f64) -> Option<String> {
    let language = model.preferences.language;
    if !matches!(action, TradingAction::OpenLong | TradingAction::OpenShort)
        && !model
            .execution
            .private_ready(model.preferences.execution_account_id.as_deref(), now_ms())
    {
        return Some(
            label(
                language,
                "私有账户数据已过期，刷新后才可平仓或撤单；手动开仓不受此限制",
                "Private data is stale; refresh before closing or cancelling. Manual opens remain available",
            )
            .to_owned(),
        );
    }
    if action == TradingAction::CancelAllOrders {
        return Some(
            label(
                language,
                "不提供全撤；请在当前委托表选择一张订单后精确撤销",
                "Cancel all is unavailable; select one open order for exact cancellation",
            )
            .to_owned(),
        );
    }
    if action == TradingAction::CancelSelectedOrder {
        return terminal_cancel_selection(model).err().map(|error| {
            label(
                language,
                match error {
                    CancelSelectionError::Missing => "请先在当前委托表点击有交易所委托号的一行",
                    CancelSelectionError::ScopeChanged => {
                        "所选委托不属于当前交易账户或交易对，请重新选择"
                    }
                    CancelSelectionError::Disappeared => {
                        "所选委托已不在最新活动委托投影中，请刷新后重新选择"
                    }
                    CancelSelectionError::Pending => "该委托的撤单结果未确认，请勿重复提交",
                },
                match error {
                    CancelSelectionError::Missing => {
                        "Select an open-order row with an exchange order ID first"
                    }
                    CancelSelectionError::ScopeChanged => {
                        "The selected order is outside the current account or symbol; select again"
                    }
                    CancelSelectionError::Disappeared => {
                        "The selected order is absent from the latest open-order projection"
                    }
                    CancelSelectionError::Pending => "Cancellation is unconfirmed; do not resubmit",
                },
            )
            .to_owned()
        });
    }
    if !model.preferences.trading.post_only {
        return Some(
            label(
                language,
                "请启用 Post Only；当前限价终端仅发送 Maker 单",
                "Enable Post Only; the limit terminal only sends maker orders",
            )
            .to_owned(),
        );
    }
    if !action.is_order_action() {
        return Some(
            label(
                language,
                "该操作尚未接入 Binance Executor",
                "This action is not connected to the Binance Executor yet",
            )
            .to_owned(),
        );
    }
    terminal_request_parts(model, action, now)
        .err()
        .map(|error| {
            let chinese = match error {
                crate::trading::TradePlanError::MissingPrice => "请先点击图表或订单簿选择限价",
                crate::trading::TradePlanError::ExpiredPrice => "所选价格已过期，请重新选择",
                crate::trading::TradePlanError::InvalidPrice => "所选价格无效",
                crate::trading::TradePlanError::InvalidSize => "请输入有效的下单金额",
                crate::trading::TradePlanError::NoPosition => "对应方向没有可平仓位",
                crate::trading::TradePlanError::UiOnlyAction => {
                    "未选择可交易的 Binance 账户或交易对"
                }
            };
            match language {
                crate::i18n::Language::SimplifiedChinese => chinese.to_owned(),
                crate::i18n::Language::English => error.to_string(),
            }
        })
}

pub fn apply_action(
    model: &mut AppModel,
    client: &ControlClient,
    action: TradingAction,
    context: &egui::Context,
) {
    model.synchronize_trading_scope();
    model.refresh_trading_price(context);
    match action {
        TradingAction::SelectSizePreset(index) => {
            if index < crate::trading::SIZE_PRESET_COUNT {
                model.trade_dock.selected_size_preset = index;
                model.trade_dock.amount_input.clear();
                model.trade_dock.amount_in_base = false;
                model.trade_dock.armed_action = None;
            }
            return;
        }
        TradingAction::ClearSelection => {
            model.trade_dock.clear_selection();
            return;
        }
        TradingAction::CenterMarket => {
            model.follow_latest_requested = true;
            return;
        }
        _ => {}
    }
    if action == TradingAction::CancelAllOrders {
        if let Some(reason) =
            action_disabled_reason(model, action, context.input(|input| input.time))
        {
            local_failure(model, reason);
        }
        return;
    }
    if action == TradingAction::CancelSelectedOrder {
        if let Some(reason) =
            action_disabled_reason(model, action, context.input(|input| input.time))
        {
            local_failure(model, reason);
            return;
        }
        let selection = match terminal_cancel_selection(model) {
            Ok(selection) => selection.clone(),
            Err(_) => {
                local_failure(model, "选中委托已不能撤销 [selection_unavailable]".into());
                return;
            }
        };
        let request = TerminalCancelRequest {
            replacement_price: None,
            schema_version: TERMINAL_SCHEMA_VERSION,
            request_id: model.next_terminal_request_id(),
            credential_id: selection.credential_id.clone(),
            symbol: selection.symbol.clone(),
            native_order_id: selection.native_order_id.clone(),
        };
        let request_id = request.request_id.clone();
        match client.send_terminal_cancel(request, model.confirmed_account_scope()) {
            Ok(()) => {
                model.execution.chart_orders.submitted_cancel(
                    selection,
                    request_id.clone(),
                    context,
                );
                model.execution.begin_terminal_submission(request_id);
                model.trade_dock.clear_order_selection();
                model.notice("Submitted exact order cancellation to the Binance Executor ledger");
            }
            Err(error) => local_failure(model, format!("撤单未提交 [local_rejected]：{error}")),
        }
        return;
    }
    if let Some(reason) = action_disabled_reason(model, action, context.input(|input| input.time)) {
        local_failure(model, reason);
        return;
    }
    model.trade_dock.armed_action = Some(action);
    let request = match build_terminal_request(model, action, context.input(|input| input.time)) {
        Ok(request) => request,
        Err(error) => {
            local_failure(model, format!("下单未提交 [local_rejected]：{error}"));
            return;
        }
    };
    let request_id = request.request_id.clone();
    match client.send_terminal(request, model.confirmed_account_scope()) {
        Ok(()) => {
            context.request_repaint();
            model.execution.begin_terminal_submission(request_id);
            model.trade_dock.armed_action = None;
            model.notice(format!(
                "Submitted {:?} to the Binance Executor ledger",
                action
            ));
        }
        Err(error) => local_failure(model, format!("下单未提交 [local_rejected]：{error}")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CancelSelectionError {
    Missing,
    ScopeChanged,
    Disappeared,
    Pending,
}

fn terminal_cancel_selection(
    model: &AppModel,
) -> Result<&crate::trading::TerminalOrderSelection, CancelSelectionError> {
    if model
        .confirmed_account_scope()
        .is_none_or(|s| s.venue != venue_control_protocol::VenueId::Binance)
    {
        return Err(CancelSelectionError::ScopeChanged);
    }
    let selection = model
        .trade_dock
        .terminal_order_selection
        .as_ref()
        .ok_or(CancelSelectionError::Missing)?;
    let selected_credential = model
        .account_overview
        .as_ref()
        .and_then(|overview| overview.selected_credential_id.as_deref());
    if selected_credential != Some(selection.credential_id.as_str())
        || model.preferences.execution_account_id.as_deref()
            != Some(selection.trading_account_id.as_str())
        || model.preferences.selected_symbol != selection.symbol.to_string()
    {
        return Err(CancelSelectionError::ScopeChanged);
    }
    let projection = model
        .execution
        .private_projection_for(Some(&selection.trading_account_id))
        .filter(|projection| projection.credential_id == selection.credential_id)
        .ok_or(CancelSelectionError::ScopeChanged)?;
    if !projection.open_orders.iter().any(|order| {
        order.symbol == selection.symbol
            && order.native_order_id.as_deref() == Some(selection.native_order_id.as_str())
    }) {
        return Err(CancelSelectionError::Disappeared);
    }
    if model.execution.chart_orders.is_pending(selection) {
        return Err(CancelSelectionError::Pending);
    }
    Ok(selection)
}

struct TerminalRequestParts {
    credential_id: String,
    symbol: venue_domain::Symbol,
    action: TerminalAction,
    price: rust_decimal::Decimal,
    quote_notional: rust_decimal::Decimal,
    close_quantity_cap: Option<rust_decimal::Decimal>,
}

fn terminal_request_parts(
    model: &AppModel,
    action: TradingAction,
    now: f64,
) -> Result<TerminalRequestParts, crate::trading::TradePlanError> {
    if model.market_worker_failed
        || model.preferences.market_server != crate::model::MarketServer::Binance
    {
        return Err(crate::trading::TradePlanError::UiOnlyAction);
    }
    if model
        .selected_execution_credential()
        .is_none_or(|c| c.venue != venue_control_protocol::VenueId::Binance)
    {
        return Err(crate::trading::TradePlanError::UiOnlyAction);
    }
    let credential_id = model
        .account_overview
        .as_ref()
        .and_then(|overview| overview.selected_credential_id.clone())
        .ok_or(crate::trading::TradePlanError::UiOnlyAction)?;
    let symbol = model
        .preferences
        .selected_symbol
        .parse()
        .map_err(|_| crate::trading::TradePlanError::UiOnlyAction)?;
    let price = model
        .trade_dock
        .selected_price
        .ok_or(crate::trading::TradePlanError::MissingPrice)?;
    if model
        .trade_dock
        .price_remaining_seconds(now, model.preferences.trading.price_validity_seconds)
        .is_none()
    {
        return Err(crate::trading::TradePlanError::ExpiredPrice);
    }
    let quote_notional = model
        .trade_dock
        .quote_notional(&model.preferences.trading, price)?;
    let terminal_action = match action {
        TradingAction::OpenLong => TerminalAction::OpenLong,
        TradingAction::CloseLong => TerminalAction::CloseLong,
        TradingAction::OpenShort => TerminalAction::OpenShort,
        TradingAction::CloseShort => TerminalAction::CloseShort,
        _ => return Err(crate::trading::TradePlanError::UiOnlyAction),
    };
    if terminal_action.is_close()
        && !model
            .execution
            .private_ready(model.preferences.execution_account_id.as_deref(), now_ms())
    {
        return Err(crate::trading::TradePlanError::NoPosition);
    }
    let close_cap = terminal_action
        .is_close()
        .then(|| {
            model.execution.position_quantity(
                &model.preferences.selected_symbol,
                terminal_action.position_side(),
            )
        })
        .flatten()
        .filter(|quantity| *quantity > rust_decimal::Decimal::ZERO);
    if terminal_action.is_close() && close_cap.is_none() {
        return Err(crate::trading::TradePlanError::NoPosition);
    }
    Ok(TerminalRequestParts {
        credential_id,
        symbol,
        action: terminal_action,
        price,
        quote_notional,
        close_quantity_cap: close_cap,
    })
}

fn build_terminal_request(
    model: &mut AppModel,
    action: TradingAction,
    now: f64,
) -> Result<TerminalOrderRequest, crate::trading::TradePlanError> {
    let TerminalRequestParts {
        credential_id,
        symbol,
        action,
        price,
        quote_notional,
        close_quantity_cap,
    } = terminal_request_parts(model, action, now)?;
    Ok(TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: model.next_terminal_request_id(),
        credential_id,
        symbol,
        action,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional,
        limit_price: Some(price),
        close_quantity_cap,
        market_risk_confirmed: false,
    })
}

fn submit_market_close(
    model: &mut AppModel,
    client: &ControlClient,
    side: venue_domain::PositionSide,
) {
    crate::execution_view::submit_confirmed_close(model, client, side);
}

fn local_failure(model: &mut AppModel, reason: String) {
    model.execution.terminal_request_id = None;
    model.execution.terminal_submission_error = Some(reason.clone());
    model.notice(reason);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

const fn label<'a>(language: crate::i18n::Language, chinese: &'a str, english: &'a str) -> &'a str {
    match language {
        crate::i18n::Language::SimplifiedChinese => chinese,
        crate::i18n::Language::English => english,
    }
}

pub(crate) const fn action_name(
    language: crate::i18n::Language,
    action: TradingAction,
) -> &'static str {
    match action {
        TradingAction::OpenLong => label(language, "开多", "Open Long"),
        TradingAction::CloseLong => label(language, "平多", "Close Long"),
        TradingAction::CloseShort => label(language, "平空", "Close Short"),
        TradingAction::OpenShort => label(language, "开空", "Open Short"),
        TradingAction::CancelSelectedOrder => label(language, "撤选中", "Cancel Selected"),
        TradingAction::CancelAllOrders => label(language, "撤全部", "Cancel All"),
        TradingAction::SelectSizePreset(_) => label(language, "数量预设", "Size Preset"),
        TradingAction::ClearSelection => label(language, "清除", "Clear"),
        TradingAction::CenterMarket => label(language, "回到市场", "Center Market"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn manual_open_is_available_without_private_projection_but_close_is_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = AppModel::new(crate::model::Preferences::default());
        model.account_overview = Some(serde_json::from_value(serde_json::json!({
            "user":{"user_id":"fixture-user","username":"fixture"},
            "credentials":[{
                "credential_id":"fixture-credential", "label":"fixture", "venue":"binance",
                "masked_key":"***", "trading_account_id":"fixture-account", "verification":"verified",
                "verified_ms":1, "expires_ms":null, "api_reachable":true, "dual_position":true,
                "account_mode":null, "has_exposure":false
            }], "selected_credential_id":"fixture-credential"
        }))?);
        model.preferences.execution_account_id = Some("fixture-account".into());
        model.preferences.selected_symbol = "DOGE/USDC".into();
        model
            .trade_dock
            .select_price(rust_decimal::Decimal::new(8663, 5), 1.0)?;
        assert!(model.execution.private_projection.is_none());
        for action in [TradingAction::OpenLong, TradingAction::OpenShort] {
            assert!(action_disabled_reason(&model, action, 1.0).is_none());
            assert!(
                terminal_request_parts(&model, action, 1.0)?
                    .close_quantity_cap
                    .is_none()
            );
        }
        for action in [
            TradingAction::CloseLong,
            TradingAction::CloseShort,
            TradingAction::CancelSelectedOrder,
        ] {
            assert!(action_disabled_reason(&model, action, 1.0).is_some());
        }
        model.account_overview = None;
        assert!(action_disabled_reason(&model, TradingAction::OpenLong, 1.0).is_some());
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod switch_regressions {
    use super::*;
    use crate::account_scope::{
        Scoped,
        tests::{id, model, overview},
    };
    fn subscribe(client: &ControlClient, model: &AppModel) {
        let scope = model.confirmed_account_scope().unwrap();
        client.subscribe_terminal(Scoped {
            value: venue_control_protocol::kol::TerminalProjectionRequest {
                schema_version: 1,
                credential_id: scope.credential_id.clone(),
                symbols: vec!["BTC/USDC".parse().unwrap()],
            },
            scope,
        });
    }
    #[test]
    fn selection_pending_same_frame_mouse_hotkey_and_builders_send_zero() {
        let mut model = model();
        let (client, probe) = ControlClient::fixture();
        subscribe(&client, &model);
        model.trade_dock.select_price(100.into(), 0.0).unwrap();
        let old_request = build_terminal_request(&mut model, TradingAction::OpenLong, 0.0).unwrap();
        let context = egui::Context::default();
        model.preferences.trading.hotkeys_enabled = true;
        let mut output = context.run_ui(
            egui::RawInput {
                events: vec![egui::Event::Key {
                    key: egui::Key::A,
                    physical_key: Some(egui::Key::A),
                    pressed: true,
                    repeat: false,
                    modifiers: Default::default(),
                }],
                ..Default::default()
            },
            |ui| {
                model.begin_account_selection(id(2));
                // Exercise the mouse action sink and the real keyboard decoder in one frame.
                apply_action(&mut model, &client, TradingAction::OpenLong, ui.ctx());
                let actions: Vec<_> = ui.input(|i| {
                    i.events
                        .iter()
                        .filter_map(|e| {
                            crate::trading::hotkey_action(e, &model.preferences.trading)
                        })
                        .collect()
                });
                assert_eq!(actions, vec![TradingAction::OpenLong]);
                for action in actions {
                    apply_action(&mut model, &client, action, ui.ctx());
                }
                assert_eq!(probe.count(), 0);
            },
        );
        output.textures_delta.clear();
        for action in [
            TradingAction::OpenLong,
            TradingAction::OpenShort,
            TradingAction::CloseLong,
            TradingAction::CloseShort,
            TradingAction::CancelSelectedOrder,
            TradingAction::CancelAllOrders,
        ] {
            apply_action(&mut model, &client, action, &context);
            if action.is_order_action() {
                assert!(build_terminal_request(&mut model, action, 0.0).is_err());
            }
        }
        assert!(
            client
                .send_terminal(old_request, model.confirmed_account_scope())
                .is_err()
        );
        assert!(
            client
                .send_terminal_cancel(
                    TerminalCancelRequest {
                        replacement_price: None,
                        schema_version: TERMINAL_SCHEMA_VERSION,
                        request_id: id(81),
                        credential_id: id(1),
                        symbol: "BTC/USDC".parse().unwrap(),
                        native_order_id: "1".into()
                    },
                    model.confirmed_account_scope()
                )
                .is_err()
        );
        assert!(
            client
                .send_position_action(
                    venue_control_protocol::terminal_position::TerminalPositionActionRequest {
                        schema_version: 1,
                        request_id: id(82),
                        credential_id: id(1),
                        symbol: "BTC/USDC".parse().unwrap(),
                        position_side: venue_domain::PositionSide::Long,
                        quantity: 1.into(),
                        action: venue_control_protocol::terminal_position::PositionAction::Close,
                        market_risk_confirmed: true,
                    },
                    model.confirmed_account_scope()
                )
                .is_err()
        );
        assert_eq!(probe.count(), 0);
    }
    #[test]
    fn selection_request_scope_comes_from_confirmed_account_and_stale_generation_is_rejected() {
        let mut model = model();
        let (client, probe) = ControlClient::fixture();
        subscribe(&client, &model);
        model.trade_dock.select_price(100.into(), 0.0).unwrap();
        let request = build_terminal_request(&mut model, TradingAction::OpenLong, 0.0).unwrap();
        let old = model.confirmed_account_scope().unwrap();
        client
            .send_terminal(request.clone(), Some(old.clone()))
            .unwrap();
        let submitted = probe.take_order();
        assert_eq!(submitted.scope, old);
        assert_eq!(submitted.value.credential_id, old.credential_id);
        for server in crate::model::MarketServer::ALL.into_iter().skip(1) {
            model.select_market_server(server);
            model.select_trading_price("BTC/USDC", 100.into(), &egui::Context::default());
            assert!(model.trade_dock.selected_price.is_none());
            assert!(build_terminal_request(&mut model, TradingAction::OpenLong, 0.0).is_err());
            assert_eq!(model.confirmed_account_scope(), Some(old.clone()));
            client
                .send_terminal_cancel(
                    TerminalCancelRequest {
                        replacement_price: None,
                        schema_version: TERMINAL_SCHEMA_VERSION,
                        request_id: id(81),
                        credential_id: id(1),
                        symbol: "BTC/USDC".parse().unwrap(),
                        native_order_id: "1".into(),
                    },
                    model.confirmed_account_scope(),
                )
                .unwrap();
            assert_eq!(probe.take_cancel().scope, old);
        }
        model.begin_account_selection(id(2));
        model.apply_account_overview(overview(2));
        model.begin_account_selection(id(1));
        model.apply_account_overview(overview(1));
        subscribe(&client, &model);
        assert!(client.send_terminal(request, Some(old)).is_err());
        assert_eq!(probe.count(), 0);
    }
}
