use eframe::egui::{self, Align, Align2, Frame, Layout, Margin, RichText, Stroke};
use venue_control_protocol::TradingAction;

use crate::{model::AppModel, theme, trading::TradingKey};

pub fn show(context: &egui::Context, open: &mut bool, model: &mut AppModel) {
    if !*open {
        return;
    }
    let language = model.preferences.language;
    let quote_asset = model.selected_trading_strategy().map_or_else(
        || quote_from_symbol(&model.preferences.selected_symbol).to_owned(),
        |strategy| strategy.symbol.quote().to_owned(),
    );
    let post_only_label = if model.preferences.trading.post_only {
        "ON"
    } else {
        "OFF"
    };

    egui::Window::new(label(language, "交易设置", "Trading Settings"))
        .open(open)
        .resizable(false)
        .collapsible(false)
        .anchor(Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .default_width(720.0)
        .show(context, |ui| {
            ui.spacing_mut().item_spacing = egui::vec2(8.0, 7.0);

            section_title(
                ui,
                label(language, "订单参数", "ORDER PARAMETERS"),
                label(language, "固定限价单", "Fixed limit orders"),
            );
            settings_frame().show(ui, |ui| {
                ui.columns(4, |columns| {
                    setting_value(
                        &mut columns[0],
                        label(language, "订单类型", "Order Type"),
                        "LIMIT",
                    );
                    setting_value(&mut columns[1], "TIF", "GTC");
                    columns[2].label(
                        RichText::new("Post Only")
                            .small()
                            .color(theme::TEXT_SECONDARY),
                    );
                    columns[2].checkbox(
                        &mut model.preferences.trading.post_only,
                        post_only_label,
                    );
                    setting_value(
                        &mut columns[3],
                        label(language, "数量单位", "Size Unit"),
                        &quote_asset,
                    );
                });
            });

            section_title(
                ui,
                label(language, "数量预设", "SIZE PRESETS"),
                &format!(
                    "{} · {}",
                    label(language, "数值与快捷键", "Value and shortcut"),
                    quote_asset
                ),
            );
            settings_frame().show(ui, |ui| {
                ui.columns(crate::trading::SIZE_PRESET_COUNT, |columns| {
                    for index in 0..crate::trading::SIZE_PRESET_COUNT {
                        preset_editor(&mut columns[index], model, index, &quote_asset, language);
                    }
                });
            });

            section_title(
                ui,
                label(language, "交易快捷键", "TRADING HOTKEYS"),
                label(language, "冲突绑定会自动互换", "Conflicts swap automatically"),
            );
            settings_frame().show(ui, |ui| {
                ui.columns(2, |columns| {
                    hotkey_group(
                        &mut columns[0],
                        model,
                        "trading-position-hotkeys",
                        label(language, "开仓与平仓", "Position actions"),
                        language,
                        &position_hotkey_rows(),
                    );
                    hotkey_group(
                        &mut columns[1],
                        model,
                        "trading-order-hotkeys",
                        label(language, "订单与视图", "Orders and view"),
                        language,
                        &order_hotkey_rows(),
                    );
                });
            });

            ui.add_space(3.0);
            footer_frame().show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.checkbox(
                        &mut model.preferences.trading.hotkeys_enabled,
                        label(language, "启用交易快捷键", "Enable trading hotkeys"),
                    );
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.label(
                            RichText::new(label(
                                language,
                                "修改立即保存到偏好设置",
                                "Changes are saved to preferences",
                            ))
                            .small()
                            .color(theme::TEXT_SECONDARY),
                        );
                    });
                });
            });

            if !model.preferences.trading.validate() {
                ui.colored_label(
                    theme::SELL,
                    label(
                        language,
                        "所有数量预设必须大于 0",
                        "Every size preset must be greater than zero",
                    ),
                );
            }
        });
}

fn section_title(ui: &mut egui::Ui, title: &str, detail: &str) {
    ui.add_space(5.0);
    ui.horizontal(|ui| {
        ui.label(RichText::new(title).strong());
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            ui.label(
                RichText::new(detail)
                    .small()
                    .color(theme::TEXT_SECONDARY),
            );
        });
    });
}

fn settings_frame() -> Frame {
    Frame::new()
        .fill(theme::PANEL)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .corner_radius(4)
        .inner_margin(Margin::same(10))
}

fn footer_frame() -> Frame {
    Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .corner_radius(4)
        .inner_margin(Margin::symmetric(10, 7))
}

fn setting_value(ui: &mut egui::Ui, title: &str, value: &str) {
    ui.label(
        RichText::new(title)
            .small()
            .color(theme::TEXT_SECONDARY),
    );
    ui.monospace(RichText::new(value).strong());
}

fn preset_editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    index: usize,
    quote_asset: &str,
    language: crate::i18n::Language,
) {
    let action = TradingAction::SelectSizePreset(index);
    ui.horizontal(|ui| {
        ui.label(
            RichText::new(format!(
                "{} {}",
                label(language, "档位", "Preset"),
                index + 1
            ))
            .small()
            .strong(),
        );
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            hotkey_combo(ui, model, action, 42.0);
        });
    });
    if let Some(value) = model.preferences.trading.size_presets.get_mut(index) {
        let mut numeric = value.to_string().parse::<f64>().unwrap_or(1.0);
        let width = ui.available_width();
        if ui
            .add_sized(
                [width, 26.0],
                egui::DragValue::new(&mut numeric)
                    .range(0.01..=1_000_000_000.0)
                    .suffix(format!(" {quote_asset}")),
            )
            .changed()
            && let Some(decimal) = rust_decimal::Decimal::from_f64_retain(numeric)
        {
            *value = decimal;
        }
    }
}

fn hotkey_group(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    grid_id: &'static str,
    title: &str,
    language: crate::i18n::Language,
    rows: &[(TradingAction, &'static str, &'static str)],
) {
    ui.label(
        RichText::new(title)
            .small()
            .strong()
            .color(theme::TEXT_SECONDARY),
    );
    ui.add_space(2.0);
    egui::Grid::new(grid_id)
        .num_columns(2)
        .min_col_width(118.0)
        .spacing([12.0, 6.0])
        .show(ui, |ui| {
            for (action, chinese, english) in rows {
                ui.label(label(language, chinese, english));
                hotkey_combo(ui, model, *action, 76.0);
                ui.end_row();
            }
        });
}

fn hotkey_combo(ui: &mut egui::Ui, model: &mut AppModel, action: TradingAction, width: f32) {
    let Some(current) = model.preferences.trading.hotkeys.key_for(action) else {
        return;
    };
    let mut selected = current;
    egui::ComboBox::from_id_salt(("trading-hotkey", action))
        .width(width)
        .selected_text(current.label())
        .show_ui(ui, |ui| {
            for key in TradingKey::ALL {
                ui.selectable_value(&mut selected, key, key.label());
            }
        });
    if selected != current {
        model.preferences.trading.hotkeys.assign(action, selected);
    }
}

fn position_hotkey_rows() -> [(TradingAction, &'static str, &'static str); 4] {
    [
        (TradingAction::OpenLong, "开多", "Open Long"),
        (TradingAction::CloseLong, "平多", "Close Long"),
        (TradingAction::OpenShort, "开空", "Open Short"),
        (TradingAction::CloseShort, "平空", "Close Short"),
    ]
}

fn order_hotkey_rows() -> [(TradingAction, &'static str, &'static str); 4] {
    [
        (
            TradingAction::CancelSelectedOrder,
            "撤当前",
            "Cancel Current",
        ),
        (TradingAction::CancelAllOrders, "撤全部", "Cancel All"),
        (TradingAction::ClearSelection, "清除选中", "Clear Selection"),
        (TradingAction::CenterMarket, "回到市场", "Center Market"),
    ]
}

fn quote_from_symbol(symbol: &str) -> &str {
    symbol.split_once('/').map_or("QUOTE", |(_, quote)| quote)
}

const fn label<'a>(language: crate::i18n::Language, chinese: &'a str, english: &'a str) -> &'a str {
    match language {
        crate::i18n::Language::SimplifiedChinese => chinese,
        crate::i18n::Language::English => english,
    }
}
