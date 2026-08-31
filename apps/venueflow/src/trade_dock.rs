use eframe::egui::{self, Color32, RichText, Stroke};
use venue_control_protocol::{StrategyLifecycle, TradingAction};

use crate::{client::ControlClient, model::AppModel, theme, trading::build_trade_intent};

pub fn show(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    if let Some(action) = controls(ui, model) {
        apply_action(model, client, action, ui.ctx());
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
    let strategy = model.selected_trading_strategy();
    let symbol = model.preferences.selected_symbol.clone();
    let (base, quote) = symbol_assets(&symbol);
    let mut action = None;
    ui.spacing_mut().item_spacing = egui::vec2(6.0, 4.0);
    ui.spacing_mut().interact_size.y = 22.0;
    ui.spacing_mut().button_padding = egui::vec2(6.0, 3.0);

    ui.horizontal(|ui| {
        let running = strategy
            .as_ref()
            .is_some_and(|strategy| strategy.lifecycle == StrategyLifecycle::Running);
        let scope = if model.preferences.execution_account_id.is_none() {
            crate::i18n::text(language, crate::i18n::TextKey::NoExecutionAccount)
        } else if running {
            "LIVE"
        } else {
            crate::i18n::text(language, crate::i18n::TextKey::TradingUnavailable)
        };
        ui.label(
            RichText::new(format!("● {scope}  {symbol}"))
                .size(12.0)
                .color(if running { theme::BUY } else { theme::WARNING }),
        )
        .on_hover_text(label(
            language,
            "下单须有精确账户、交易对和运行中的作用域；服务端再次验证。",
            "An exact account, symbol and running scope are required; the server revalidates.",
        ));
    });
    ui.separator();
    ui.columns(2, |columns| {
        columns[0].horizontal(|ui| {
            ui.small(label(language, "价格", "Price"));
            let price = model.trade_dock.selected_price.map_or_else(
                || "—".to_owned(),
                |value| model.format_market_price(&symbol, value),
            );
            ui.label(RichText::new(price).size(15.0).strong().color(theme::BRAND))
                .on_hover_text(label(
                    language,
                    "点击图表或盘口选择限价",
                    "Select a limit price on the chart or book",
                ));
        });
        columns[1].horizontal(|ui| {
            ui.small(label(language, "金额", "Amount"));
            let amount = model
                .preferences
                .trading
                .size_presets
                .get(model.trade_dock.selected_size_preset)
                .map_or_else(|| "—".to_owned(), |value| value.normalize().to_string());
            ui.label(
                RichText::new(format!("{amount} {quote}"))
                    .size(15.0)
                    .strong(),
            );
        });
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
                        model.trade_dock.selected_size_preset == index,
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
        TradingAction::CloseShort,
        TradingAction::OpenShort,
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
    if strategy.is_some() {
        ui.separator();
        ui.horizontal_wrapped(|ui| {
            let long = strategy.as_ref().map_or_else(
                || "—".to_owned(),
                |s| s.long_quantity.normalize().to_string(),
            );
            let short = strategy.as_ref().map_or_else(
                || "—".to_owned(),
                |s| s.short_quantity.normalize().to_string(),
            );
            let pnl = strategy
                .as_ref()
                .and_then(|s| s.unrealized_pnl)
                .map_or_else(|| "—".to_owned(), |value| value.normalize().to_string());
            ui.label(
                RichText::new(format!("{} {long} {base}", label(language, "多", "Long")))
                    .size(11.0)
                    .color(theme::BUY),
            );
            ui.label(
                RichText::new(format!("{} {short} {base}", label(language, "空", "Short")))
                    .size(11.0)
                    .color(theme::SELL),
            );
            ui.label(
                RichText::new(format!("PnL {pnl} {quote}"))
                    .size(11.0)
                    .color(theme::TEXT_SECONDARY),
            );
        });
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
fn action_button(
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
    let enabled = action_enabled(model, action);
    let (text_color, fill, stroke) = action_palette(action);
    ui.add_enabled(
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
    )
    .clicked()
    .then_some(action)
}

fn action_enabled(model: &AppModel, action: TradingAction) -> bool {
    let Some(strategy) = model.selected_trading_strategy() else {
        return false;
    };
    if strategy.lifecycle != StrategyLifecycle::Running {
        return false;
    }
    match action {
        TradingAction::OpenLong | TradingAction::OpenShort => {
            model.trade_dock.selected_price.is_some()
        }
        TradingAction::CloseLong => {
            model.trade_dock.selected_price.is_some()
                && strategy.long_quantity > rust_decimal::Decimal::ZERO
        }
        TradingAction::CloseShort => {
            model.trade_dock.selected_price.is_some()
                && strategy.short_quantity > rust_decimal::Decimal::ZERO
        }
        TradingAction::CancelSelectedOrder | TradingAction::CancelAllOrders => true,
        _ => true,
    }
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
    let Some(strategy) = model.selected_trading_strategy() else {
        model.notice("Trading action rejected: no exact account and symbol scope");
        return;
    };
    if strategy.lifecycle != StrategyLifecycle::Running {
        model.notice("Trading action rejected: selected strategy is not Running");
        return;
    }
    model.trade_dock.armed_action = Some(action);
    let intent = match build_trade_intent(
        &strategy,
        &model.preferences.trading,
        &model.trade_dock,
        action,
        context.input(|input| input.time),
    ) {
        Ok(intent) => intent,
        Err(error) => {
            model.notice(format!("Trading action rejected: {error}"));
            return;
        }
    };
    let request = model.begin_trade_command(&strategy, intent, now_ms());
    if request.validate().is_err() {
        model.notice("Trading action rejected: malformed semantic intent");
        return;
    }
    match client.send(request.clone()) {
        Ok(()) => {
            model.record_submission(request);
            model.trade_dock.armed_action = None;
            model.notice(format!("Submitted {:?} semantic intent", action));
        }
        Err(error) => model.notice(format!("Trading request rejected locally: {error}")),
    }
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
        TradingAction::CancelSelectedOrder => label(language, "撤当前", "Cancel Current"),
        TradingAction::CancelAllOrders => label(language, "撤全部", "Cancel All"),
        TradingAction::SelectSizePreset(_) => label(language, "数量预设", "Size Preset"),
        TradingAction::ClearSelection => label(language, "清除", "Clear"),
        TradingAction::CenterMarket => label(language, "回到市场", "Center Market"),
    }
}
