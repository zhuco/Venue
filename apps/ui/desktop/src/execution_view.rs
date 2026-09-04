mod position_actions;
mod text;
use crate::{
    client::ControlClient,
    model::AppModel,
    theme,
    trading::{TerminalOrderSelection, TradeDockState},
};
use eframe::egui;
pub(crate) use position_actions::submit_confirmed_close;
use std::sync::Arc;
use text::{Key, text};
use venue_control_protocol::kol::{ExecutorCommandSummary, TerminalAccountProjection};

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
    pub private_error: Option<String>,
    pub terminal_executions_error: Option<String>,
    pub private_projection: Option<Arc<TerminalAccountProjection>>,
    pub terminal_executions: Vec<ExecutorCommandSummary>,
    pub terminal_request_id: Option<String>,
    pub terminal_submission_error: Option<String>,
    pub grid: crate::grid_view::GridViewState,
    pub leader_bot: crate::leader_bot_view::LeaderBotView,
    position_actions: position_actions::PositionActions,
    pub(crate) chart_orders: crate::chart_trading::OrderTagState,
    private_received_ms: u64,
    tab: Tab,
    pub(crate) current_symbol: bool,
}

impl ExecutionViewState {
    pub fn begin_terminal_submission(&mut self, request_id: String) {
        self.terminal_request_id = Some(request_id);
        self.terminal_submission_error = None;
    }

    pub fn position_submission_failed(&mut self, id: &str, definitive: bool) {
        self.position_actions.submission_failed(id, definitive);
        self.chart_orders.submission_failed(id, definitive);
    }

    pub fn apply_private(
        &mut self,
        projection: Option<TerminalAccountProjection>,
        trade_dock: &mut TradeDockState,
    ) {
        if let Some(projection) = projection {
            if projection.validate().is_err()
                || self.private_projection.as_ref().is_some_and(|old| {
                    old.credential_id == projection.credential_id
                        && old.observed_ms > projection.observed_ms
                })
            {
                #[cfg(not(target_arch = "wasm32"))]
                tracing::warn!("invalid or regressing private account projection");
                self.private_error =
                    Some("Invalid or regressing private account projection".into());
                return;
            }
            if trade_dock
                .terminal_order_selection
                .as_ref()
                .is_some_and(|selection| !selection_exists(selection, &projection))
            {
                trade_dock.clear_order_selection();
            }
            self.chart_orders.observe(&projection);
            self.private_received_ms = crate::account_center::now_ms();
            self.private_projection = Some(Arc::new(projection));
            self.private_error = None;
        } else {
            trade_dock.clear_order_selection();
            self.private_projection = None;
            self.private_received_ms = 0;
            self.private_error = None;
        }
    }

    pub fn apply_terminal_executions(&mut self, executions: Vec<ExecutorCommandSummary>) {
        if executions.iter().any(|summary| summary.validate().is_err()) {
            self.terminal_executions_error = Some("Invalid terminal execution history".into());
        } else {
            self.position_actions.completed(&executions);
            self.chart_orders.completed(&executions);
            if let Some(projection) = &self.private_projection {
                self.chart_orders.observe(projection);
            }
            if self.terminal_request_id.as_deref().is_some_and(|id| {
                executions
                    .iter()
                    .any(|summary| summary.request_id.as_deref() == Some(id))
            }) {
                self.terminal_submission_error = None;
            }
            self.terminal_executions = executions;
            self.terminal_executions_error = None;
        }
    }

    pub fn apply_terminal_execution(&mut self, summary: ExecutorCommandSummary) {
        if summary.validate().is_err() {
            self.terminal_executions_error = Some("Invalid terminal execution receipt".into());
            return;
        }
        if self.terminal_request_id.is_some() && summary.request_id == self.terminal_request_id {
            self.terminal_submission_error = None;
        }
        self.terminal_executions
            .retain(|old| old.command_id != summary.command_id);
        self.terminal_executions.insert(0, summary);
        self.position_actions.completed(&self.terminal_executions);
        self.chart_orders.completed(&self.terminal_executions);
        if let Some(projection) = &self.private_projection {
            self.chart_orders.observe(projection);
        }
        self.terminal_executions.truncate(500);
    }

    pub fn private_projection_for(
        &self,
        trading_account_id: Option<&str>,
    ) -> Option<&TerminalAccountProjection> {
        let trading_account_id = trading_account_id?;
        self.private_projection
            .as_deref()
            .filter(|projection| projection.trading_account_id == trading_account_id)
    }

    pub fn private_ready(&self, trading_account_id: Option<&str>, now: u64) -> bool {
        self.private_error.is_none()
            && fresh_time(self.private_received_ms, now)
            && self
                .private_projection_for(trading_account_id)
                .is_some_and(|projection| fresh_time(projection.observed_ms, now))
    }

    pub(crate) fn private_received_ms(&self) -> u64 {
        self.private_received_ms
    }

    fn refresh_warning(&self, observed_ms: u64, now: u64) -> Key {
        if self.private_error.is_some() {
            Key::ConnectionRetry
        } else if observed_ms > now.saturating_add(2_000)
            || self.private_received_ms > now.saturating_add(2_000)
        {
            Key::ClockMismatch
        } else if !fresh_time(self.private_received_ms, now) {
            Key::RefreshDelayed
        } else {
            Key::AccountDelayed
        }
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
}

fn fresh_time(time: u64, now: u64) -> bool {
    time > 0 && time <= now.saturating_add(2_000) && now.saturating_sub(time) <= 15_000
}

fn selection_exists(
    selection: &TerminalOrderSelection,
    projection: &TerminalAccountProjection,
) -> bool {
    selection.credential_id == projection.credential_id
        && selection.trading_account_id == projection.trading_account_id
        && projection.open_orders.iter().any(|order| {
            order.symbol == selection.symbol
                && order.native_order_id.as_deref() == Some(selection.native_order_id.as_str())
        })
}

pub fn show(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
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
    let Some(account) = selected.and_then(|c| c.trading_account_id.clone()) else {
        ui.weak(text(language, Key::NoAccount));
        return;
    };
    if model.preferences.execution_account_id.as_deref() != Some(account.as_str()) {
        ui.weak(text(language, Key::NoAccount));
        return;
    }
    if model.execution.tab == Tab::Bots {
        if let Some(credential) = selected.cloned() {
            crate::leader_bot_view::show(ui, model, client, &credential);
            crate::grid_view::show(ui, model, client, &credential, &account);
        }
        return;
    }
    if let Some(projection) = model.execution.private_projection.clone()
        && projection.trading_account_id == account
        && Some(projection.credential_id.as_str())
            == selected.map(|credential| credential.credential_id.as_str())
    {
        show_private_projection(ui, model, client, &projection);
    } else {
        ui.weak(text(language, Key::Waiting));
        if let Some(error) = &model.execution.private_error {
            ui.colored_label(theme::WARNING, error);
        }
    }
}

fn show_private_projection(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    projection: &TerminalAccountProjection,
) {
    let language = model.preferences.language;
    let now = crate::account_center::now_ms();
    if model.execution.tab != Tab::Positions
        && !model
            .execution
            .private_ready(Some(&projection.trading_account_id), now)
    {
        let warning = ui.colored_label(
            theme::WARNING,
            text(
                language,
                model.execution.refresh_warning(projection.observed_ms, now),
            ),
        );
        if let Some(error) = &model.execution.private_error {
            warning.on_hover_text(error);
        }
    }
    if model.execution.tab == Tab::OrderHistory {
        ui.small(text(language, Key::OrderHistoryScope));
        if let Some(error) = &model.execution.terminal_executions_error {
            ui.colored_label(theme::WARNING, error);
        }
    }
    if model.execution.tab == Tab::PositionHistory {
        ui.small(text(language, Key::PositionHistoryScope));
    }
    if model.execution.tab == Tab::Fills {
        ui.small(text(language, Key::FillsScope));
    }
    if model.execution.tab == Tab::Assets {
        ui.small(text(language, Key::AssetsScope));
    }
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
    let mut requested_order = None;
    let mut requested_position = None;
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
                            Key::CurrentPnl,
                            Key::Actions,
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
                            Key::Reason,
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
                                .filter(|row| included(&row.symbol) && !row.quantity.is_zero())
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
                                position_pnl(ui, row);
                                if let Some(action) =
                                    position_actions::row_buttons(ui, model, projection, row)
                                {
                                    requested_position = Some(action);
                                }
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
                                let selection =
                                    row.native_order_id.as_deref().map(|native_order_id| {
                                        TerminalOrderSelection {
                                            credential_id: projection.credential_id.clone(),
                                            trading_account_id: projection
                                                .trading_account_id
                                                .clone(),
                                            symbol: row.symbol.clone(),
                                            native_order_id: native_order_id.to_owned(),
                                        }
                                    });
                                let selected = selection.as_ref().is_some_and(|selection| {
                                    model
                                        .trade_dock
                                        .terminal_order_selection
                                        .as_ref()
                                        .is_some_and(|current| current == selection)
                                });
                                let symbol = row.symbol.to_string();
                                if ui
                                    .add(
                                        egui::Button::selectable(selected, &symbol)
                                            .frame(false)
                                            .sense(egui::Sense::click()),
                                    )
                                    .on_hover_text(if selection.is_some() {
                                        "选择此委托并打开对应交易对图表"
                                    } else {
                                        "打开对应交易对图表；此行缺少交易所委托号，不能撤单"
                                    })
                                    .clicked()
                                {
                                    if let Some(selection) = selection.clone() {
                                        requested_order = Some(selection);
                                    } else {
                                        requested_symbol = Some(symbol);
                                    }
                                }
                                ui.label(format!("{:?} / {:?}", row.order_side, row.position_side));
                                market_price(ui, model, &row.symbol, row.limit_price);
                                market_quantity(ui, model, &row.symbol, Some(row.quantity));
                                market_quantity(ui, model, &row.symbol, row.filled_quantity);
                                ui.label(format!("{:?}", row.state));
                                ui.label(if row.reduce_only { "✓" } else { "—" });
                                if let Some(native_order_id) = row.native_order_id.as_deref() {
                                    if ui
                                        .selectable_label(selected, native_order_id)
                                        .on_hover_text(
                                            "选择此委托用于精确撤单；同时打开对应交易对图表",
                                        )
                                        .clicked()
                                    {
                                        requested_order = selection;
                                    }
                                } else {
                                    ui.monospace(&row.client_order_id)
                                        .on_hover_text("缺少交易所委托号，不能精确撤单");
                                }
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
                                ui.label(crate::terminal_feedback::command_state(
                                    row.state, language,
                                ));
                                ui.add_sized(
                                    [260.0, 32.0],
                                    egui::Label::new(crate::terminal_feedback::command_reason(
                                        row, language,
                                    ))
                                    .truncate(),
                                )
                                .on_hover_text(
                                    crate::terminal_feedback::command_reason(row, language),
                                );
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
    position_actions::show_confirmation(ui, model, client, requested_position);
    if let Some(selection) = requested_order {
        model.select_symbol(selection.symbol.to_string());
        model.trade_dock.select_terminal_order(selection);
        model.follow_latest_requested = true;
    } else if let Some(symbol) = requested_symbol {
        model.select_symbol(symbol);
        model.follow_latest_requested = true;
    }
}

fn position_pnl(ui: &mut egui::Ui, position: &venue_control_protocol::kol::TerminalPosition) {
    let pnl = position_pnl_value(position);
    if let Some(pnl) = pnl {
        let color = if pnl >= rust_decimal::Decimal::ZERO {
            theme::BUY
        } else {
            theme::SELL
        };
        ui.colored_label(color, format!("{:.4}", pnl.round_dp(4)));
    } else {
        ui.label("—");
    }
}

pub(crate) fn position_pnl_value(
    position: &venue_control_protocol::kol::TerminalPosition,
) -> Option<rust_decimal::Decimal> {
    position
        .entry_price
        .zip(position.mark_price)
        .and_then(|(entry, mark)| {
            let movement = match position.position_side {
                venue_domain::PositionSide::Long => mark.checked_sub(entry),
                venue_domain::PositionSide::Short => entry.checked_sub(mark),
                venue_domain::PositionSide::Net => mark.checked_sub(entry),
            }?;
            movement.checked_mul(position.quantity)
        })
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

fn decimal(ui: &mut egui::Ui, value: Option<rust_decimal::Decimal>) {
    ui.monospace(value.map_or_else(|| "—".into(), |v| v.normalize().to_string()));
}

fn timestamp(ms: u64) -> String {
    crate::chart::format_timeline_label(ms, crate::chart::ChartInterval::OneHour)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_projection(account: &str, observed_ms: u64) -> TerminalAccountProjection {
        TerminalAccountProjection {
            schema_version: venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: "00000000-0000-4000-8000-000000000002".into(),
            trading_account_id: account.into(),
            observed_ms,
            persisted_ms: observed_ms,
            private_generation: 1,
            position_mode: venue_control_protocol::kol::TerminalPositionMode::Hedge,
            positions: vec![],
            position_history: vec![],
            open_orders: vec![],
            fills: vec![],
            assets: vec![],
        }
    }

    fn open_order(symbol: venue_domain::Symbol) -> venue_control_protocol::kol::TerminalOpenOrder {
        venue_control_protocol::kol::TerminalOpenOrder {
            client_order_id: "client-order-a".into(),
            native_order_id: Some("123456".into()),
            symbol,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: rust_decimal::Decimal::ONE,
            filled_quantity: Some(rust_decimal::Decimal::ZERO),
            limit_price: Some(rust_decimal::Decimal::new(100, 0)),
            post_only: true,
            time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
            reduce_only: false,
            state: venue_control_protocol::kol::TerminalOrderState::New,
            created_ms: Some(18_000),
        }
    }

    #[test]
    fn positions_render_four_decimal_pnl_and_each_row_actions_without_update_time()
    -> Result<(), Box<dyn std::error::Error>> {
        use rust_decimal::Decimal;
        let mut model = AppModel::new(crate::model::Preferences::default());
        model.preferences.language = crate::i18n::Language::SimplifiedChinese;
        let mut projection = private_projection(
            "00000000-0000-4000-8000-000000000003",
            crate::account_center::now_ms(),
        );
        for side in [
            venue_domain::PositionSide::Long,
            venue_domain::PositionSide::Short,
        ] {
            projection
                .positions
                .push(venue_control_protocol::kol::TerminalPosition {
                    symbol: "SOL/USDC".parse()?,
                    position_side: side,
                    quantity: Decimal::ONE,
                    entry_price: Some(Decimal::from(100)),
                    mark_price: Some(Decimal::new(100123456, 6)),
                });
        }
        let context = egui::Context::default();
        let mut output = context.run_ui(egui::RawInput::default(), |ui| {
            for row in &projection.positions {
                position_pnl(ui, row);
                assert!(position_actions::row_buttons(ui, &model, &projection, row).is_none());
            }
        });
        output.textures_delta.clear();
        fn collect(shape: &egui::Shape, text: &mut String) {
            match shape {
                egui::Shape::Text(value) => {
                    text.push_str(&value.galley.job.text);
                    text.push('\n');
                }
                egui::Shape::Vec(values) => values.iter().for_each(|value| collect(value, text)),
                _ => (),
            }
        }
        let mut rendered = String::new();
        for shape in &output.shapes {
            collect(&shape.shape, &mut rendered);
        }
        assert!(rendered.contains("0.1235") && rendered.contains("-0.1235"));
        assert_eq!(rendered.matches("平仓").count(), 2);
        assert_eq!(rendered.matches("反开").count(), 2);
        assert!(!rendered.contains("更新时间"));
        Ok(())
    }

    #[test]
    fn stale_and_future_facts_are_not_actionable() {
        assert!(!fresh_time(0, 100));
        assert!(!fresh_time(1, 20_000));
        assert!(!fresh_time(25_000, 20_000));
        assert!(fresh_time(19_000, 20_000));
    }

    #[test]
    fn refresh_warning_distinguishes_connection_clock_and_data_delays() {
        let mut state = ExecutionViewState {
            private_received_ms: 19_000,
            ..Default::default()
        };
        assert!(matches!(
            state.refresh_warning(1, 20_000),
            Key::AccountDelayed
        ));
        state.private_received_ms = 1;
        assert!(matches!(
            state.refresh_warning(19_000, 20_000),
            Key::RefreshDelayed
        ));
        assert!(matches!(
            state.refresh_warning(25_000, 20_000),
            Key::ClockMismatch
        ));
        state.private_error = Some("HTTP 503".into());
        assert!(matches!(
            state.refresh_warning(19_000, 20_000),
            Key::ConnectionRetry
        ));
    }

    #[test]
    fn empty_private_response_removes_old_account_data_and_readiness() {
        let mut state = ExecutionViewState {
            private_projection: Some(Arc::new(private_projection(
                "00000000-0000-4000-8000-000000000001",
                19_000,
            ))),
            private_received_ms: 19_500,
            ..ExecutionViewState::default()
        };
        state.apply_private(None, &mut TradeDockState::default());
        assert!(state.private_projection.is_none());
        assert_eq!(state.private_received_ms, 0);
        assert!(!state.private_ready(Some("00000000-0000-4000-8000-000000000001"), 20_000));
    }

    #[test]
    fn complete_history_snapshot_replaces_even_a_single_row()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_control_protocol::kol::{
            ExecutorCommandOrigin, ExecutorCommandPhase, ExecutorCommandState, ExecutorOrderKind,
        };
        let symbol: venue_domain::Symbol = "SOL/USDC".parse()?;
        let summary = |suffix: &str| ExecutorCommandSummary {
            command_id: format!("00000000-0000-4000-8000-0000000000{suffix}"),
            request_id: Some(format!("00000000-0000-4000-8000-0000000001{suffix}")),
            trading_account_id: "00000000-0000-4000-8000-000000000001".into(),
            symbol: symbol.clone(),
            position_side: Some(venue_domain::PositionSide::Long),
            origin: ExecutorCommandOrigin::Terminal,
            phase: ExecutorCommandPhase::Open,
            order_kind: ExecutorOrderKind::LimitPostOnly,
            order_side: Some(venue_domain::OrderSide::Buy),
            requested_quantity: Some(rust_decimal::Decimal::ONE),
            limit_price: Some(rust_decimal::Decimal::from(100)),
            state: ExecutorCommandState::Reconciled,
            native_order_id: Some(suffix.into()),
            created_ms: 1,
            updated_ms: 2,
            sanitized_error_code: None,
        };
        let first = summary("01");
        let second = summary("02");
        first.validate()?;
        second.validate()?;
        let mut state = ExecutionViewState::default();
        state.begin_terminal_submission(first.request_id.clone().ok_or("request missing")?);
        state.terminal_submission_error = Some("request timed out".into());
        state.apply_terminal_executions(vec![second.clone()]);
        assert!(state.terminal_submission_error.is_some());
        state.apply_terminal_executions(vec![first.clone(), second.clone()]);
        assert!(state.terminal_submission_error.is_none());
        state.apply_terminal_executions(vec![second]);
        assert_eq!(state.terminal_executions.len(), 1);
        state.apply_terminal_execution(first);
        assert_eq!(state.terminal_executions.len(), 2);
        state.apply_terminal_executions(vec![]);
        assert!(state.terminal_executions.is_empty());
        Ok(())
    }

    #[test]
    fn private_readiness_uses_the_exact_selected_account_and_private_receive_time() {
        let mut state = ExecutionViewState {
            private_projection: Some(Arc::new(private_projection("account-a", 19_000))),
            private_received_ms: 19_500,
            ..ExecutionViewState::default()
        };
        assert!(state.private_ready(Some("account-a"), 20_000));
        assert!(!state.private_ready(Some("account-b"), 20_000));
        assert!(!state.private_ready(None, 20_000));
        state.private_received_ms = 1;
        assert!(!state.private_ready(Some("account-a"), 20_000));
    }
    #[test]
    fn refreshed_projection_clears_a_selected_order_only_after_it_disappears()
    -> Result<(), Box<dyn std::error::Error>> {
        let account_id = "00000000-0000-4000-8000-000000000001";
        let symbol: venue_domain::Symbol = "SOL/USDC".parse()?;
        let mut first = private_projection(account_id, 19_000);
        first.open_orders.push(open_order(symbol.clone()));
        let mut state = ExecutionViewState::default();
        let mut trade_dock = TradeDockState::default();
        trade_dock.select_terminal_order(TerminalOrderSelection {
            credential_id: first.credential_id.clone(),
            trading_account_id: first.trading_account_id.clone(),
            symbol,
            native_order_id: "123456".into(),
        });

        state.apply_private(Some(first), &mut trade_dock);
        assert!(trade_dock.terminal_order_selection.is_some());

        state.apply_private(
            Some(private_projection(account_id, 20_000)),
            &mut trade_dock,
        );
        assert!(trade_dock.terminal_order_selection.is_none());
        assert!(trade_dock.selected_order_id.is_none());
        Ok(())
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
    fn current_position_pnl_respects_hedge_side() -> Result<(), Box<dyn std::error::Error>> {
        let symbol: venue_domain::Symbol = "SOL/USDC".parse()?;
        let position = |position_side| venue_control_protocol::kol::TerminalPosition {
            symbol: symbol.clone(),
            position_side,
            quantity: rust_decimal::Decimal::new(2, 0),
            entry_price: Some(rust_decimal::Decimal::new(100, 0)),
            mark_price: Some(rust_decimal::Decimal::new(103, 0)),
        };
        assert_eq!(
            position_pnl_value(&position(venue_domain::PositionSide::Long)),
            Some(rust_decimal::Decimal::new(6, 0))
        );
        assert_eq!(
            position_pnl_value(&position(venue_domain::PositionSide::Short)),
            Some(rust_decimal::Decimal::new(-6, 0))
        );
        Ok(())
    }
}
