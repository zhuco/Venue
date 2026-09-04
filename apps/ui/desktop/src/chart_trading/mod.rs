mod alerts;
mod menu;
mod order_tags;
mod overlays;
mod settings;
#[cfg(test)]
mod tests;

pub(crate) use alerts::{AlertBook, show_alerts};
pub(crate) use alerts::{notification, poll};
pub(crate) use menu::menu_button;
pub(crate) use order_tags::{OrderTagState, apply_interaction};
pub(crate) use overlays::{ChartOverlay, collect, draw};
pub(crate) use settings::ChartTradingSettings;

use crate::{i18n::Language, model::AppModel};
use eframe::egui;

fn label(language: Language, zh: &'static str, en: &'static str) -> &'static str {
    match language {
        Language::SimplifiedChinese => zh,
        Language::English => en,
    }
}

pub(crate) fn quick_order(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &crate::client::ControlClient,
    symbol: &str,
    settings: &ChartTradingSettings,
) {
    if !settings.quick_order {
        return;
    }
    ui.push_id(("chart-quick-order", symbol), |ui| {
        ui.horizontal_wrapped(|ui| {
            ui.spacing_mut().item_spacing = egui::vec2(6.0, 4.0);
            ui.add_enabled_ui(symbol == model.preferences.selected_symbol, |ui| {
                if settings.quick_amount {
                    let quote = symbol.split_once('/').map_or("", |(_, quote)| quote);
                    ui.label(if model.trade_dock.amount_in_base {
                        symbol.split_once('/').map_or("", |(base, _)| base)
                    } else {
                        quote
                    });
                    let preset = model.preferences.trading.size_presets
                        [model.trade_dock.selected_size_preset.min(4)];
                    let hint = if model.trade_dock.amount_in_base {
                        label(model.preferences.language, "数量", "Quantity").to_owned()
                    } else {
                        preset.normalize().to_string()
                    };
                    if ui
                        .add(
                            egui::TextEdit::singleline(&mut model.trade_dock.amount_input)
                                .desired_width(68.0)
                                .hint_text(hint),
                        )
                        .changed()
                    {
                        model.trade_dock.armed_action = None;
                    }
                }
                for (visible, action) in [
                    (
                        settings.quick_buy,
                        venue_control_protocol::TradingAction::OpenLong,
                    ),
                    (
                        settings.quick_sell,
                        venue_control_protocol::TradingAction::OpenShort,
                    ),
                ] {
                    if visible
                        && let Some(action) =
                            crate::trade_dock::action_button(ui, model, action, 108.0)
                    {
                        crate::trade_dock::apply_action(model, client, action, ui.ctx());
                    }
                }
            })
            .response
            .on_disabled_hover_text(label(
                model.preferences.language,
                "先切换到该交易对再快捷下单",
                "Select this symbol before placing an order",
            ));
            ui.weak("Post Only");
        });
    });
}
