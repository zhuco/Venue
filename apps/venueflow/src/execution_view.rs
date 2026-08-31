mod text;
use crate::{model::AppModel, theme};
use eframe::egui;
use std::sync::Arc;
use text::{Key, text};
use venue_control_protocol::{ExecutionFactBinding, ExecutionFactsSnapshot, GatewayMode, VenueId};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Tab {
    #[default]
    Positions,
    Orders,
    Fills,
}

#[derive(Debug, Default)]
pub struct ExecutionViewState {
    pub facts: Option<Arc<ExecutionFactsSnapshot>>,
    pub error: Option<String>,
    received_ms: u64,
    tab: Tab,
    current_symbol: bool,
}

impl ExecutionViewState {
    pub fn order_matches(
        &self,
        strategy: &venue_control_protocol::StrategySummary,
        order_id: &str,
        now: u64,
    ) -> bool {
        self.fresh(now)
            && self.facts.as_ref().is_some_and(|facts| {
                facts.orders.iter().any(|row| {
                    row.order_id == order_id
                        && fresh_time(row.observed_ms, now)
                        && row.binding.venue == strategy.venue
                        && row.binding.mode == strategy.mode
                        && row.binding.trading_account_id == strategy.trading_account_id
                        && row.binding.symbol == strategy.symbol
                        && row.binding.instance_id == strategy.instance_id
                        && row.binding.config_epoch == strategy.config_epoch
                })
            })
    }
    pub fn apply(&mut self, facts: ExecutionFactsSnapshot) {
        if facts.validate().is_err()
            || self
                .facts
                .as_ref()
                .is_some_and(|old| old.generated_ms > facts.generated_ms)
        {
            self.error = Some("Invalid or regressing execution facts".into());
            return;
        }
        self.received_ms = crate::account_center::now_ms();
        self.facts = Some(Arc::new(facts));
        self.error = None;
    }

    pub fn fresh(&self, now: u64) -> bool {
        self.error.is_none()
            && fresh_time(self.received_ms, now)
            && self
                .facts
                .as_ref()
                .is_some_and(|facts| fresh_time(facts.generated_ms, now))
    }
}

fn fresh_time(time: u64, now: u64) -> bool {
    time > 0 && time <= now.saturating_add(2_000) && now.saturating_sub(time) <= 15_000
}

fn matches_account(
    binding: &ExecutionFactBinding,
    venue: VenueId,
    account: &str,
    symbol: Option<&str>,
) -> bool {
    binding.venue == venue
        && binding.mode == GatewayMode::Live
        && binding.trading_account_id == account
        && symbol.is_none_or(|symbol| binding.symbol.to_string() == symbol)
}

pub fn show(ui: &mut egui::Ui, model: &mut AppModel) {
    let language = model.preferences.language;
    ui.horizontal_wrapped(|ui| {
        for (tab, key) in [
            (Tab::Positions, Key::Positions),
            (Tab::Orders, Key::Orders),
            (Tab::Fills, Key::Fills),
        ] {
            ui.selectable_value(&mut model.execution.tab, tab, text(language, key));
        }
        ui.separator();
        ui.checkbox(
            &mut model.execution.current_symbol,
            text(language, Key::CurrentSymbol),
        );
    });
    ui.separator();
    let selected = model.account_overview.as_ref().and_then(|overview| {
        overview.credentials.iter().find(|credential| {
            Some(&credential.credential_id) == overview.selected_credential_id.as_ref()
        })
    });
    let Some((venue, account)) =
        selected.and_then(|c| c.trading_account_id.clone().map(|id| (c.venue, id)))
    else {
        ui.weak(text(language, Key::NoAccount));
        return;
    };
    if model.preferences.execution_account_id.as_deref() != Some(account.as_str()) {
        ui.weak(text(language, Key::NoAccount));
        return;
    }
    let Some(facts) = model.execution.facts.clone() else {
        ui.weak(text(language, Key::Waiting));
        if let Some(error) = &model.execution.error {
            ui.colored_label(theme::WARNING, error);
        }
        return;
    };
    let now = crate::account_center::now_ms();
    let fresh = model.execution.fresh(now) && model.snapshot_online && model.event_stream_online;
    ui.horizontal(|ui| {
        ui.small(format!(
            "{} · {}",
            text(language, Key::Signed),
            timestamp(facts.generated_ms)
        ));
        if !fresh {
            ui.colored_label(theme::WARNING, text(language, Key::Stale));
        }
    });
    let symbol = model
        .execution
        .current_symbol
        .then(|| model.preferences.selected_symbol.clone());
    let included =
        |b: &ExecutionFactBinding| matches_account(b, venue, &account, symbol.as_deref());
    let mut selected_row: Option<(ExecutionFactBinding, Option<String>)> = None;
    let mut count = 0;
    egui::ScrollArea::both()
        .id_salt("execution-table-scroll")
        .show(ui, |ui| {
            egui::Grid::new(("execution-table", model.execution.tab as u8))
                .striped(true)
                .min_col_width(72.0)
                .spacing([18.0, 8.0])
                .show(ui, |ui| {
                    let headings: &[Key] = match model.execution.tab {
                        Tab::Positions => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Size,
                            Key::Entry,
                            Key::Mark,
                            Key::Instance,
                            Key::Time,
                        ],
                        Tab::Orders => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Price,
                            Key::Size,
                            Key::Filled,
                            Key::State,
                            Key::ReduceOnly,
                            Key::Instance,
                            Key::OrderId,
                            Key::Time,
                        ],
                        Tab::Fills => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Price,
                            Key::Size,
                            Key::Instance,
                            Key::OrderId,
                            Key::FillId,
                            Key::Time,
                        ],
                    };
                    for key in headings {
                        ui.weak(text(language, *key));
                    }
                    ui.end_row();
                    match model.execution.tab {
                        Tab::Positions => {
                            for row in facts.positions.iter().filter(|row| included(&row.binding)) {
                                count += 1;
                                if ui
                                    .selectable_label(false, row.binding.symbol.to_string())
                                    .clicked()
                                {
                                    selected_row = Some((row.binding.clone(), None));
                                }
                                ui.label(format!("{:?}", row.position_side));
                                decimal(ui, Some(row.quantity));
                                decimal(ui, row.entry_price);
                                decimal(ui, row.mark_price);
                                ui.label(&row.binding.instance_id);
                                ui.weak(timestamp(row.observed_ms));
                                ui.end_row();
                            }
                        }
                        Tab::Orders => {
                            for row in facts.orders.iter().filter(|row| included(&row.binding)) {
                                count += 1;
                                let actionable = fresh
                                    && fresh_time(row.observed_ms, now)
                                    && model.snapshot.as_ref().is_some_and(|s| {
                                        s.strategies.iter().any(|s| {
                                            s.venue == row.binding.venue
                                                && s.mode == row.binding.mode
                                                && s.trading_account_id
                                                    == row.binding.trading_account_id
                                                && s.symbol == row.binding.symbol
                                                && s.instance_id == row.binding.instance_id
                                                && s.config_epoch == row.binding.config_epoch
                                        })
                                    });
                                if ui
                                    .add_enabled(
                                        actionable,
                                        egui::Button::selectable(
                                            model.trade_dock.selected_order_id.as_deref()
                                                == Some(row.order_id.as_str()),
                                            row.binding.symbol.to_string(),
                                        ),
                                    )
                                    .clicked()
                                {
                                    selected_row =
                                        Some((row.binding.clone(), Some(row.order_id.clone())));
                                }
                                ui.label(format!("{:?} / {:?}", row.side, row.position_side));
                                decimal(ui, row.limit_price);
                                decimal(ui, Some(row.quantity));
                                decimal(ui, row.filled_quantity);
                                ui.label(
                                    row.state
                                        .map_or_else(|| "—".into(), |state| format!("{state:?}")),
                                );
                                ui.label(if row.reduce_only { "✓" } else { "—" });
                                ui.label(&row.binding.instance_id);
                                ui.monospace(&row.order_id);
                                ui.weak(timestamp(row.observed_ms));
                                ui.end_row();
                            }
                        }
                        Tab::Fills => {
                            for row in facts
                                .fills
                                .iter()
                                .rev()
                                .filter(|row| included(&row.binding))
                            {
                                count += 1;
                                ui.label(row.binding.symbol.to_string());
                                ui.label(format!("{:?}", row.side));
                                decimal(ui, Some(row.price));
                                decimal(ui, Some(row.quantity));
                                ui.label(&row.binding.instance_id);
                                ui.monospace(&row.order_id);
                                ui.monospace(&row.fill_id);
                                ui.weak(timestamp(row.occurred_ms));
                                ui.end_row();
                            }
                        }
                    }
                });
        });
    if count == 0 {
        ui.weak(text(language, Key::Empty));
    }
    if let Some((binding, order)) = selected_row {
        model.select_symbol(binding.symbol.to_string());
        model.preferences.selected_instance = Some(binding.instance_id);
        model.synchronize_trading_scope();
        model.trade_dock.selected_order_id = order;
    }
}

fn decimal(ui: &mut egui::Ui, value: Option<rust_decimal::Decimal>) {
    ui.monospace(value.map_or_else(|| "—".into(), |v| v.normalize().to_string()));
}

fn timestamp(ms: u64) -> String {
    crate::chart::format_timeline_label(ms, crate::chart::ChartInterval::OneHour)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_and_future_facts_are_not_actionable() {
        assert!(!fresh_time(0, 100));
        assert!(!fresh_time(1, 20_000));
        assert!(!fresh_time(25_000, 20_000));
        assert!(fresh_time(19_000, 20_000));
    }
    #[test]
    fn account_filter_never_crosses_venue_account_or_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = ExecutionFactBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "account-a".into(),
            symbol: "BTC/USDC".parse()?,
            instance_id: "one".into(),
            config_epoch: 1,
        };
        assert!(matches_account(
            &binding,
            VenueId::Binance,
            "account-a",
            None
        ));
        assert!(!matches_account(
            &binding,
            VenueId::Binance,
            "account-b",
            None
        ));
        assert!(!matches_account(
            &binding,
            VenueId::Binance,
            "account-a",
            Some("ETH/USDC")
        ));
        assert!(!matches_account(
            &binding,
            VenueId::Bybit,
            "account-a",
            None
        ));
        Ok(())
    }
}
