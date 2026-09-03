mod text;
use crate::{model::AppModel, theme};
use eframe::egui;
use std::sync::Arc;
use text::{Key, text};
use venue_control_protocol::kol::{ExecutorCommandSummary, TerminalAccountProjection};
use venue_control_protocol::{ExecutionFactBinding, ExecutionFactsSnapshot, GatewayMode, VenueId};
use venue_domain::OrderState;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Tab {
    #[default]
    Positions,
    CurrentOrders,
    OrderHistory,
    Fills,
    PositionHistory,
    Bots,
    Assets,
}

#[derive(Debug, Default)]
pub struct ExecutionViewState {
    pub facts: Option<Arc<ExecutionFactsSnapshot>>,
    pub error: Option<String>,
    pub private_error: Option<String>,
    pub private_projection: Option<Arc<TerminalAccountProjection>>,
    pub terminal_executions: Vec<ExecutorCommandSummary>,
    received_ms: u64,
    tab: Tab,
    current_symbol: bool,
}

impl ExecutionViewState {
    pub fn apply_private(&mut self, projection: Option<TerminalAccountProjection>) {
        if let Some(projection) = projection {
            if projection.validate().is_err()
                || self.private_projection.as_ref().is_some_and(|old| {
                    old.credential_id == projection.credential_id
                        && old.observed_ms > projection.observed_ms
                })
            {
                self.private_error =
                    Some("Invalid or regressing private account projection".into());
                return;
            }
            self.received_ms = crate::account_center::now_ms();
            self.private_projection = Some(Arc::new(projection));
            self.private_error = None;
        } else {
            self.private_error = None;
        }
    }

    pub fn apply_terminal_executions(&mut self, executions: Vec<ExecutorCommandSummary>) {
        if executions.iter().any(|summary| summary.validate().is_err()) {
            self.private_error = Some("Invalid terminal execution history".into());
        } else if executions.len() == 1 {
            let summary = executions[0].clone();
            self.terminal_executions
                .retain(|old| old.command_id != summary.command_id);
            self.terminal_executions.insert(0, summary);
        } else {
            self.terminal_executions = executions;
        }
    }

    pub fn private_fresh(&self, now: u64) -> bool {
        self.private_error.is_none()
            && fresh_time(self.received_ms, now)
            && self
                .private_projection
                .as_ref()
                .is_some_and(|projection| fresh_time(projection.observed_ms, now))
    }

    pub fn position_quantity(
        &self,
        symbol: &str,
        side: venue_domain::PositionSide,
    ) -> Option<rust_decimal::Decimal> {
        self.private_projection
            .as_ref()?
            .positions
            .iter()
            .find(|position| {
                position.symbol.to_string() == symbol && position.position_side == side
            })
            .map(|position| position.quantity)
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
            (Tab::CurrentOrders, Key::CurrentOrders),
            (Tab::OrderHistory, Key::OrderHistory),
            (Tab::Fills, Key::Fills),
            (Tab::PositionHistory, Key::PositionHistory),
            (Tab::Bots, Key::Bots),
            (Tab::Assets, Key::Assets),
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
    if model.execution.tab != Tab::Bots {
        if let Some(projection) = model.execution.private_projection.clone()
            && projection.trading_account_id == account
        {
            show_private_projection(ui, model, &projection);
        } else {
            ui.weak(text(language, Key::Waiting));
            ui.small("等待唯一 Binance Executor 返回签名私有账户投影");
            if let Some(error) = &model.execution.private_error {
                ui.colored_label(theme::WARNING, error);
            }
        }
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
    match model.execution.tab {
        Tab::Positions | Tab::CurrentOrders | Tab::Assets => {
            ui.weak(text(language, Key::CurrentSource));
        }
        Tab::Fills => {
            ui.weak(text(language, Key::FillsScope));
        }
        Tab::Bots => {
            ui.weak(text(language, Key::BotsSource));
        }
        Tab::OrderHistory | Tab::PositionHistory => {}
    }
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
                        Tab::CurrentOrders => &[
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
                        Tab::OrderHistory | Tab::PositionHistory => &[],
                        Tab::Bots => &[
                            Key::Symbol,
                            Key::Instance,
                            Key::Kind,
                            Key::State,
                            Key::Long,
                            Key::Short,
                        ],
                        Tab::Assets => &[Key::Asset, Key::Equity, Key::Available],
                    };
                    for key in headings {
                        ui.weak(text(language, *key));
                    }
                    ui.end_row();
                    match model.execution.tab {
                        Tab::Positions => {
                            for row in facts.positions.iter().filter(|row| included(&row.binding)) {
                                count += 1;
                                if symbol_link(
                                    ui,
                                    &row.binding.symbol,
                                    &model.preferences.selected_symbol,
                                ) {
                                    selected_row = Some((row.binding.clone(), None));
                                }
                                ui.label(format!("{:?}", row.position_side));
                                market_quantity(ui, model, &row.binding.symbol, Some(row.quantity));
                                market_price(ui, model, &row.binding.symbol, row.entry_price);
                                market_price(ui, model, &row.binding.symbol, row.mark_price);
                                ui.label(&row.binding.instance_id);
                                ui.weak(timestamp(row.observed_ms));
                                ui.end_row();
                            }
                        }
                        Tab::CurrentOrders => {
                            for row in facts
                                .orders
                                .iter()
                                .filter(|row| included(&row.binding) && is_current_order(row.state))
                            {
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
                                if symbol_link(
                                    ui,
                                    &row.binding.symbol,
                                    &model.preferences.selected_symbol,
                                ) {
                                    selected_row = Some((
                                        row.binding.clone(),
                                        actionable.then(|| row.order_id.clone()),
                                    ));
                                }
                                ui.label(format!("{:?} / {:?}", row.side, row.position_side));
                                market_price(ui, model, &row.binding.symbol, row.limit_price);
                                market_quantity(ui, model, &row.binding.symbol, Some(row.quantity));
                                market_quantity(
                                    ui,
                                    model,
                                    &row.binding.symbol,
                                    row.filled_quantity,
                                );
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
                        Tab::OrderHistory | Tab::PositionHistory => {}
                        Tab::Bots => {
                            if let Some(snapshot) = &model.snapshot {
                                for strategy in snapshot.strategies.iter().filter(|strategy| {
                                    strategy.venue == venue
                                        && strategy.mode == GatewayMode::Live
                                        && strategy.trading_account_id == account
                                        && symbol.as_ref().is_none_or(|selected| {
                                            strategy.symbol.to_string() == *selected
                                        })
                                }) {
                                    count += 1;
                                    ui.label(strategy.symbol.to_string());
                                    ui.label(&strategy.instance_id);
                                    ui.label(format!("{:?}", strategy.kind));
                                    ui.label(format!("{:?}", strategy.lifecycle));
                                    decimal(ui, Some(strategy.long_quantity));
                                    decimal(ui, Some(strategy.short_quantity));
                                    ui.end_row();
                                }
                            }
                        }
                        Tab::Assets => {
                            if let Some(account_summary) =
                                model.snapshot.as_ref().and_then(|snapshot| {
                                    snapshot.accounts.iter().find(|summary| {
                                        summary.venue == venue
                                            && summary.mode == GatewayMode::Live
                                            && summary.trading_account_id == account
                                    })
                                })
                            {
                                for balance in &account_summary.balances {
                                    count += 1;
                                    ui.label(balance.asset.to_string());
                                    decimal(ui, Some(balance.equity));
                                    decimal(ui, balance.available_margin);
                                    ui.end_row();
                                }
                            }
                        }
                    }
                });
        });
    match model.execution.tab {
        Tab::OrderHistory => {
            ui.weak(text(language, Key::OrderHistoryUnavailable));
        }
        Tab::PositionHistory => {
            ui.weak(text(language, Key::PositionHistoryUnavailable));
        }
        _ if count == 0 => {
            ui.weak(text(language, Key::Empty));
        }
        _ => {}
    }
    if let Some((binding, order)) = selected_row {
        model.select_symbol(binding.symbol.to_string());
        model.follow_latest_requested = true;
        model.preferences.selected_instance = Some(binding.instance_id);
        model.synchronize_trading_scope();
        model.trade_dock.selected_order_id = order;
    }
}

fn show_private_projection(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    projection: &TerminalAccountProjection,
) {
    let language = model.preferences.language;
    let now = crate::account_center::now_ms();
    let fresh = model.execution.private_fresh(now);
    ui.horizontal(|ui| {
        ui.small(format!(
            "{} · {}",
            text(language, Key::Signed),
            timestamp(projection.observed_ms)
        ));
        if !fresh {
            ui.colored_label(theme::WARNING, text(language, Key::Stale));
        }
    });
    ui.weak(text(language, Key::CurrentSource));
    let selected_symbol = model
        .execution
        .current_symbol
        .then(|| model.preferences.selected_symbol.clone());
    let included = |symbol: &venue_domain::Symbol| {
        selected_symbol
            .as_ref()
            .is_none_or(|selected| symbol.to_string() == *selected)
    };
    let mut count = 0_usize;
    let mut requested_symbol = None;
    egui::ScrollArea::both()
        .id_salt("private-execution-table-scroll")
        .show(ui, |ui| {
            egui::Grid::new(("private-execution-table", model.execution.tab as u8))
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
                            Key::Time,
                        ],
                        Tab::CurrentOrders => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Price,
                            Key::Size,
                            Key::Filled,
                            Key::State,
                            Key::ReduceOnly,
                            Key::OrderId,
                            Key::Time,
                        ],
                        Tab::OrderHistory => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Price,
                            Key::Size,
                            Key::State,
                            Key::OrderId,
                            Key::Time,
                        ],
                        Tab::Fills => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Price,
                            Key::Size,
                            Key::OrderId,
                            Key::FillId,
                            Key::Time,
                        ],
                        Tab::PositionHistory => &[
                            Key::Symbol,
                            Key::Side,
                            Key::Size,
                            Key::Entry,
                            Key::Mark,
                            Key::Time,
                        ],
                        Tab::Assets => &[Key::Asset, Key::Equity, Key::Available],
                        Tab::Bots => &[],
                    };
                    for key in headings {
                        ui.weak(text(language, *key));
                    }
                    ui.end_row();
                    match model.execution.tab {
                        Tab::Positions => {
                            for row in projection
                                .positions
                                .iter()
                                .filter(|row| included(&row.symbol))
                            {
                                count += 1;
                                if symbol_link(ui, &row.symbol, &model.preferences.selected_symbol)
                                {
                                    requested_symbol = Some(row.symbol.to_string());
                                }
                                ui.label(format!("{:?}", row.position_side));
                                market_quantity(ui, model, &row.symbol, Some(row.quantity));
                                market_price(ui, model, &row.symbol, row.entry_price);
                                market_price(ui, model, &row.symbol, row.mark_price);
                                ui.weak(timestamp(projection.observed_ms));
                                ui.end_row();
                            }
                        }
                        Tab::CurrentOrders => {
                            for row in projection
                                .open_orders
                                .iter()
                                .filter(|row| included(&row.symbol))
                            {
                                count += 1;
                                if symbol_link(ui, &row.symbol, &model.preferences.selected_symbol)
                                {
                                    requested_symbol = Some(row.symbol.to_string());
                                }
                                ui.label(format!("{:?} / {:?}", row.order_side, row.position_side));
                                market_price(ui, model, &row.symbol, row.limit_price);
                                market_quantity(ui, model, &row.symbol, Some(row.quantity));
                                market_quantity(ui, model, &row.symbol, row.filled_quantity);
                                ui.label(format!("{:?}", row.state));
                                ui.label(if row.reduce_only { "✓" } else { "—" });
                                ui.monospace(
                                    row.native_order_id
                                        .as_deref()
                                        .map_or(row.client_order_id.as_str(), |value| value),
                                );
                                ui.weak(row.created_ms.map_or_else(|| "—".into(), timestamp));
                                ui.end_row();
                            }
                        }
                        Tab::OrderHistory => {
                            for row in model.execution.terminal_executions.iter().filter(|row| {
                                row.trading_account_id == projection.trading_account_id
                                    && included(&row.symbol)
                            }) {
                                count += 1;
                                ui.label(row.symbol.to_string());
                                ui.label(format!("{:?} / {:?}", row.order_side, row.position_side));
                                market_price(ui, model, &row.symbol, row.limit_price);
                                market_quantity(ui, model, &row.symbol, row.requested_quantity);
                                ui.label(format!("{:?}", row.state));
                                ui.monospace(
                                    row.native_order_id
                                        .as_deref()
                                        .map_or(row.command_id.as_str(), |value| value),
                                );
                                ui.weak(timestamp(row.updated_ms));
                                ui.end_row();
                            }
                        }
                        Tab::Fills => {
                            for row in projection.fills.iter().filter(|row| included(&row.symbol)) {
                                count += 1;
                                ui.label(row.symbol.to_string());
                                ui.label(format!("{:?} / {:?}", row.order_side, row.position_side));
                                market_price(ui, model, &row.symbol, Some(row.price));
                                market_quantity(ui, model, &row.symbol, Some(row.quantity));
                                ui.monospace(&row.native_order_id);
                                ui.monospace(&row.native_trade_id);
                                ui.weak(row.occurred_ms.map_or_else(|| "—".into(), timestamp));
                                ui.end_row();
                            }
                        }
                        Tab::PositionHistory => {
                            for entry in projection
                                .position_history
                                .iter()
                                .filter(|entry| included(&entry.position.symbol))
                            {
                                let row = &entry.position;
                                count += 1;
                                ui.label(row.symbol.to_string());
                                ui.label(format!("{:?}", row.position_side));
                                market_quantity(ui, model, &row.symbol, Some(row.quantity));
                                market_price(ui, model, &row.symbol, row.entry_price);
                                market_price(ui, model, &row.symbol, row.mark_price);
                                ui.weak(timestamp(entry.observed_ms));
                                ui.end_row();
                            }
                        }
                        Tab::Assets => {
                            for row in &projection.assets {
                                count += 1;
                                ui.label(&row.asset);
                                decimal(ui, Some(row.equity));
                                decimal(ui, row.available_margin);
                                ui.end_row();
                            }
                        }
                        Tab::Bots => {}
                    }
                });
        });
    if count == 0 {
        ui.weak(text(language, Key::Empty));
    }
    if let Some(symbol) = requested_symbol {
        model.select_symbol(symbol);
        model.follow_latest_requested = true;
    }
}

fn symbol_link(ui: &mut egui::Ui, symbol: &venue_domain::Symbol, selected_symbol: &str) -> bool {
    let value = symbol.to_string();
    ui.add(
        egui::Button::selectable(value == selected_symbol, &value)
            .frame(false)
            .sense(egui::Sense::click()),
    )
    .on_hover_text("打开或切换到该交易对图表")
    .clicked()
}

fn market_price(
    ui: &mut egui::Ui,
    model: &AppModel,
    symbol: &venue_domain::Symbol,
    value: Option<rust_decimal::Decimal>,
) {
    ui.monospace(market_price_text(model, symbol, value));
}

fn market_price_text(
    model: &AppModel,
    symbol: &venue_domain::Symbol,
    value: Option<rust_decimal::Decimal>,
) -> String {
    value.map_or_else(
        || "—".into(),
        |value| model.format_market_price(&symbol.to_string(), value),
    )
}

fn market_quantity(
    ui: &mut egui::Ui,
    model: &AppModel,
    symbol: &venue_domain::Symbol,
    value: Option<rust_decimal::Decimal>,
) {
    ui.monospace(value.map_or_else(
        || "—".into(),
        |value| model.format_market_quantity(&symbol.to_string(), value),
    ));
}

fn is_current_order(state: Option<OrderState>) -> bool {
    state.is_none()
        || matches!(
            state,
            Some(OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown)
        )
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
    fn current_and_historical_orders_are_separated_without_hiding_unknown_rows() {
        assert!(is_current_order(None));
        assert!(is_current_order(Some(OrderState::New)));
        assert!(is_current_order(Some(OrderState::PartiallyFilled)));
        assert!(is_current_order(Some(OrderState::Unknown)));
        assert!(!is_current_order(Some(OrderState::Filled)));
        assert!(!is_current_order(Some(OrderState::Cancelled)));
        assert!(!is_current_order(Some(OrderState::Expired)));
        assert!(!is_current_order(Some(OrderState::Rejected)));
    }
    #[test]
    fn private_mark_price_uses_the_exchange_symbol_precision()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = AppModel::new(crate::model::Preferences::default());
        model.local_precisions.insert("SOL/USDC".into(), (4, 3));
        let symbol: venue_domain::Symbol = "SOL/USDC".parse()?;
        assert_eq!(
            market_price_text(
                &model,
                &symbol,
                Some(rust_decimal::Decimal::new(123_456_789, 6))
            ),
            "123.4568"
        );
        Ok(())
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
