use eframe::egui::{self, Align, Color32, Frame, Layout, Margin, RichText, Stroke};
use venue_control_protocol::{StrategyLifecycle, TradingAction};

use crate::{client::ControlClient, model::AppModel, theme, trading::build_trade_intent};

pub fn show(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    let language = model.preferences.language;
    let strategy = model.selected_trading_strategy();
    let symbol = strategy.as_ref().map_or_else(
        || model.preferences.selected_symbol.clone(),
        |strategy| strategy.symbol.to_string(),
    );
    let (base_asset, quote_asset) = symbol_assets(&symbol);
    let selected_size = model
        .preferences
        .trading
        .size_presets
        .get(model.trade_dock.selected_size_preset)
        .copied();

    ui.horizontal(|ui| {
        if let Some(strategy) = &strategy {
            ui.colored_label(
                theme::SELL,
                RichText::new(format!("● {}", strategy.mode)).strong(),
            );
            ui.strong(strategy.venue.to_string());
            ui.monospace(strategy.symbol.to_string());
        } else {
            ui.colored_label(
                theme::WARNING,
                RichText::new(label(language, "● 未选择作用域", "● No scope")).strong(),
            );
            ui.monospace(&symbol);
        }
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            status_badge(
                ui,
                if model.preferences.trading.hotkeys_enabled {
                    "KEY ON"
                } else {
                    "KEY OFF"
                },
                model.preferences.trading.hotkeys_enabled,
            );
            status_badge(
                ui,
                if model.preferences.trading.post_only {
                    "PO ON"
                } else {
                    "PO OFF"
                },
                model.preferences.trading.post_only,
            );
            status_badge(ui, "GTC", false);
            status_badge(ui, "LMT", false);
        });
    });

    ui.add_space(6.0);
    summary_frame().show(ui, |ui| {
        ui.columns(2, |columns| {
            columns[0].label(
                RichText::new(label(language, "选中价格", "SELECTED PRICE"))
                    .small()
                    .color(theme::TEXT_SECONDARY),
            );
            match model.trade_dock.selected_price {
                Some(price) => {
                    columns[0].monospace(
                        RichText::new(model.format_market_price(&symbol, price))
                            .size(20.0)
                            .strong()
                            .color(theme::BRAND_HOVER),
                    );
                }
                None => {
                    columns[0].label(
                        RichText::new(label(
                            language,
                            "点击图表或盘口选择价格",
                            "Click chart or book to select",
                        ))
                        .color(theme::WARNING),
                    );
                }
            }
            columns[1].label(
                RichText::new(label(language, "默认数量", "DEFAULT SIZE"))
                    .small()
                    .color(theme::TEXT_SECONDARY),
            );
            if let Some(size) = selected_size {
                columns[1].monospace(
                    RichText::new(format!("{} {quote_asset}", size.normalize()))
                        .size(20.0)
                        .strong(),
                );
            }
        });
        if let Some(action) = model.trade_dock.armed_action {
            ui.add_space(3.0);
            ui.colored_label(
                theme::WARNING,
                format!(
                    "{}: {}",
                    label(language, "待执行", "ARMED"),
                    action_name(language, action)
                ),
            );
        }
    });

    ui.add_space(6.0);
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(format!(
                "{} · {}",
                label(language, "数量", "SIZE"),
                quote_asset
            ))
            .strong(),
        );
        let spacing = ui.spacing().item_spacing.x;
        let button_width = ((ui.available_width() - spacing * 4.0)
            / crate::trading::SIZE_PRESET_COUNT as f32)
            .max(44.0);
        for index in 0..crate::trading::SIZE_PRESET_COUNT {
            let Some(value) = model.preferences.trading.size_presets.get(index).copied() else {
                continue;
            };
            let key = model
                .preferences
                .trading
                .hotkeys
                .key_for(TradingAction::SelectSizePreset(index))
                .map_or("—", |key| key.label());
            let selected = model.trade_dock.selected_size_preset == index;
            if ui
                .add_sized(
                    [button_width, 30.0],
                    egui::Button::selectable(selected, format!("{}  [{key}]", value.normalize())),
                )
                .clicked()
            {
                apply_action(model, client, TradingAction::SelectSizePreset(index));
            }
        }
    });

    ui.add_space(6.0);
    ui.horizontal(|ui| {
        let spacing = ui.spacing().item_spacing.x;
        let button_width = ((ui.available_width() - spacing * 3.0) / 4.0).max(64.0);
        action_button(
            ui,
            model,
            client,
            TradingAction::OpenLong,
            label(language, "开多", "Open Long"),
            button_width,
        );
        action_button(
            ui,
            model,
            client,
            TradingAction::CloseLong,
            label(language, "平多", "Close Long"),
            button_width,
        );
        action_button(
            ui,
            model,
            client,
            TradingAction::CloseShort,
            label(language, "平空", "Close Short"),
            button_width,
        );
        action_button(
            ui,
            model,
            client,
            TradingAction::OpenShort,
            label(language, "开空", "Open Short"),
            button_width,
        );
    });

    ui.add_space(4.0);
    ui.horizontal(|ui| {
        let button_width = ((ui.available_width() - ui.spacing().item_spacing.x) / 2.0).max(90.0);
        action_button(
            ui,
            model,
            client,
            TradingAction::CancelSelectedOrder,
            label(language, "撤当前", "Cancel Current"),
            button_width,
        );
        action_button(
            ui,
            model,
            client,
            TradingAction::CancelAllOrders,
            label(language, "撤全部", "Cancel All"),
            button_width,
        );
    });

    ui.add_space(6.0);
    position_frame().show(ui, |ui| {
        if let Some(strategy) = &strategy {
            ui.columns(3, |columns| {
                position_value(
                    &mut columns[0],
                    "LONG",
                    &format!("{} {base_asset}", strategy.long_quantity.normalize()),
                    theme::BUY,
                );
                position_value(
                    &mut columns[1],
                    "SHORT",
                    &format!("{} {base_asset}", strategy.short_quantity.normalize()),
                    theme::SELL,
                );
                if let Some(pnl) = strategy.unrealized_pnl {
                    position_value(
                        &mut columns[2],
                        "PnL",
                        &format!("{:+} {quote_asset}", pnl.round_dp(2).normalize()),
                        theme::value_color(pnl.to_string().parse::<f64>().unwrap_or(0.0)),
                    );
                } else {
                    position_value(&mut columns[2], "PnL", "—", theme::TEXT_SECONDARY);
                }
            });
        } else {
            ui.label(
                RichText::new(label(
                    language,
                    "选择一个运行中的交易作用域后显示持仓",
                    "Select a running trading scope to show positions",
                ))
                .color(theme::TEXT_SECONDARY),
            );
        }
    });

    if let Some(strategy) = &strategy
        && strategy.lifecycle != StrategyLifecycle::Running
    {
        ui.add_space(4.0);
        ui.colored_label(
            theme::WARNING,
            label(
                language,
                "当前作用域未运行，交易动作已锁定",
                "Current scope is not Running; trade actions are locked",
            ),
        );
    }

    ui.add_space(6.0);
    ui.separator();
    ui.add_space(3.0);
    ui.horizontal(|ui| {
        let hotkey_status = if model.preferences.trading.hotkeys_enabled {
            label(language, "快捷键 已启用", "Hotkeys enabled")
        } else {
            label(language, "快捷键 已停用", "Hotkeys disabled")
        };
        ui.label(RichText::new(hotkey_status).small().color(
            if model.preferences.trading.hotkeys_enabled {
                theme::BUY
            } else {
                theme::TEXT_SECONDARY
            },
        ))
        .on_hover_text(label(
            language,
            "按钮与键盘共用同一 TradingAction",
            "Buttons and keyboard share the same TradingAction",
        ));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            if ui
                .button(format!(
                    "⚙ {}",
                    label(language, "交易设置", "Trading Settings")
                ))
                .clicked()
            {
                model.trading_settings_requested = true;
            }
        });
    });
}

fn status_badge(ui: &mut egui::Ui, text: &str, active: bool) {
    Frame::new()
        .fill(if active {
            Color32::from_rgb(24, 49, 42)
        } else {
            theme::BG_SECONDARY
        })
        .stroke(Stroke::new(
            1.0,
            if active { theme::BUY } else { theme::DIVIDER },
        ))
        .corner_radius(3)
        .inner_margin(Margin::symmetric(6, 2))
        .show(ui, |ui| {
            ui.label(RichText::new(text).monospace().small());
        });
}

fn summary_frame() -> Frame {
    Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .corner_radius(3)
        .inner_margin(Margin::same(9))
}

fn position_frame() -> Frame {
    Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .corner_radius(3)
        .inner_margin(Margin::same(7))
}

fn position_value(ui: &mut egui::Ui, title: &str, value: &str, color: Color32) {
    ui.label(RichText::new(title).small().strong().color(color));
    ui.monospace(RichText::new(value).strong());
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
    model: &mut AppModel,
    client: &ControlClient,
    action: TradingAction,
    title: &str,
    width: f32,
) {
    let key = model
        .preferences
        .trading
        .hotkeys
        .key_for(action)
        .map_or("—", |key| key.label());
    let enabled = action_enabled(model, action);
    let (text_color, fill, stroke) = action_palette(action);
    if ui
        .add_enabled(
            enabled,
            egui::Button::new(
                RichText::new(format!("{title}  {key}"))
                    .strong()
                    .color(text_color),
            )
            .fill(fill)
            .stroke(Stroke::new(1.0, stroke))
            .min_size(egui::vec2(width, 34.0)),
        )
        .clicked()
    {
        apply_action(model, client, action);
    }
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

pub fn apply_action(model: &mut AppModel, client: &ControlClient, action: TradingAction) {
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

const fn action_name(language: crate::i18n::Language, action: TradingAction) -> &'static str {
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
