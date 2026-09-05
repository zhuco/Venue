use eframe::egui;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::label;
use crate::{model::AppModel, theme};

const MAX_ALERTS: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PriceAlert {
    pub symbol: String,
    pub price: Decimal,
    pub active: bool,
    #[serde(skip)]
    previous: Option<(u64, Decimal)>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct AlertBook {
    pub items: Vec<PriceAlert>,
    #[serde(skip)]
    pub notification: Option<String>,
}

impl AlertBook {
    pub fn add(&mut self, symbol: &str, price: Decimal) -> bool {
        if price <= Decimal::ZERO
            || self.items.len() >= MAX_ALERTS
            || symbol.parse::<venue_domain::Symbol>().is_err()
            || self
                .items
                .iter()
                .any(|alert| alert.symbol == symbol && alert.price == price && alert.active)
        {
            return false;
        }
        self.items.push(PriceAlert {
            symbol: symbol.to_owned(),
            price,
            active: true,
            previous: None,
        });
        true
    }

    pub fn observe(&mut self, symbol: &str, time: u64, price: Decimal) -> Vec<Decimal> {
        let mut triggered = Vec::new();
        if price <= Decimal::ZERO {
            return triggered;
        }
        for alert in self
            .items
            .iter_mut()
            .filter(|alert| alert.active && alert.symbol == symbol)
        {
            if let Some((previous_time, previous)) = alert.previous {
                if time <= previous_time {
                    continue;
                }
                if (previous < alert.price && price >= alert.price)
                    || (previous > alert.price && price <= alert.price)
                {
                    alert.active = false;
                    triggered.push(alert.price);
                }
            }
            alert.previous = Some((time, price));
        }
        triggered
    }
}

pub(crate) fn poll(model: &mut AppModel) {
    let now = crate::account_center::now_ms();
    for quote in model.local_quotes.values() {
        if quote.exchange_time_ms > now.saturating_add(2_000)
            || now.saturating_sub(quote.exchange_time_ms) > 15_000
            || now.saturating_sub(quote.received_ms) > 15_000
        {
            continue;
        }
        let triggered = model.preferences.chart_alerts.observe(
            &quote.symbol,
            quote.exchange_time_ms,
            quote.last,
        );
        for price in triggered {
            model.preferences.chart_alerts.notification = Some(format!(
                "{} {} · {} {}",
                label(model.preferences.language, "价格提醒", "Price alert"),
                quote.symbol,
                label(model.preferences.language, "已触及", "Reached"),
                price.normalize()
            ));
        }
    }
}

pub(crate) fn notification(context: &egui::Context, model: &mut AppModel) {
    let Some(message) = model.preferences.chart_alerts.notification.clone() else {
        return;
    };
    egui::Window::new(label(model.preferences.language, "价格提醒", "Price alert"))
        .id(egui::Id::new("chart-alert-notification"))
        .collapsible(false)
        .resizable(false)
        .anchor(egui::Align2::RIGHT_TOP, [-18.0, 70.0])
        .show(context, |ui| {
            ui.colored_label(theme::WARNING, message);
            if ui
                .button(label(model.preferences.language, "知道了", "Dismiss"))
                .clicked()
            {
                model.preferences.chart_alerts.notification = None;
            }
        });
}

pub(crate) fn show_alerts(ui: &mut egui::Ui, model: &mut AppModel, symbol: &str) {
    let language = model.preferences.language;
    ui.horizontal_wrapped(|ui| {
        ui.weak(label(
            language,
            "价格提醒 · 仅终端运行时",
            "Price alerts · while terminal is running",
        ));
        let key = ui.make_persistent_id(("price-alert-input", symbol));
        let mut input = ui
            .data_mut(|data| data.get_temp::<String>(key))
            .unwrap_or_default();
        ui.add(
            egui::TextEdit::singleline(&mut input)
                .desired_width(90.0)
                .hint_text(label(language, "提醒价格", "Target price")),
        );
        let parsed = input
            .trim()
            .parse::<Decimal>()
            .ok()
            .filter(|price| *price > Decimal::ZERO);
        if ui
            .add_enabled(
                parsed.is_some() && model.preferences.chart_alerts.items.len() < MAX_ALERTS,
                egui::Button::new(label(language, "添加", "Add")),
            )
            .clicked()
            && let Some(price) = parsed
            && model.preferences.chart_alerts.add(symbol, price)
        {
            input.clear();
        }
        ui.data_mut(|data| data.insert_temp(key, input));
        ui.menu_button(label(language, "管理", "Manage"), |ui| {
            let mut remove = None;
            for (index, alert) in model
                .preferences
                .chart_alerts
                .items
                .iter_mut()
                .enumerate()
                .filter(|(_, alert)| alert.symbol == symbol)
            {
                ui.horizontal(|ui| {
                    if ui
                        .checkbox(&mut alert.active, alert.price.normalize().to_string())
                        .changed()
                    {
                        alert.previous = None;
                    }
                    if ui.small_button("×").clicked() {
                        remove = Some(index);
                    }
                });
            }
            if let Some(index) = remove {
                model.preferences.chart_alerts.items.remove(index);
            }
            if !model
                .preferences
                .chart_alerts
                .items
                .iter()
                .any(|alert| alert.symbol == symbol)
            {
                ui.weak(label(language, "暂无价格提醒", "No price alerts"));
            }
        });
    });
}
