use eframe::egui::{self, Align2, Color32, FontId, RichText, Stroke};
mod presentation;
mod status_bar;
#[cfg(test)]
mod tests;
use egui_tiles::{Behavior, TileId, Tiles, UiResponse};
use venue_control_protocol::{
    AggressorSide, CommandState, ConnectionState, ControlAction, ControlCommandRequest,
    HealthState, MarketSummary, StrategyLifecycle, StrategySummary,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::market::MarketSelection;
use crate::{
    client::ControlClient,
    i18n::{Language, TextKey, text},
    model::{
        AppModel, MarketQuote, PendingConfirmation, WorkspaceKind, decimal_to_f64, format_decimal,
        freshness_age_ms, requires_operator_confirmation,
    },
    theme,
    workspace::{Pane, PaneKind, Workspaces},
};

pub struct PaneBehavior<'a> {
    pub model: &'a mut AppModel,
    pub client: &'a ControlClient,
}

impl Behavior<Pane> for PaneBehavior<'_> {
    fn pane_ui(&mut self, ui: &mut egui::Ui, _tile_id: TileId, pane: &mut Pane) -> UiResponse {
        theme::panel_frame().show(ui, |ui| match pane.kind {
            PaneKind::MarketWatch => show_market_watch(ui, self.model),
            PaneKind::Chart => show_chart(ui, pane, self.model),
            PaneKind::OrderBook => show_order_book(ui, pane, self.model),
            PaneKind::TradeTape => show_trade_tape(ui, pane, self.model),
            PaneKind::Accounts => show_accounts(ui, self.model),
            PaneKind::Strategies => show_strategies(ui, self.model),
            PaneKind::Execution => crate::execution_view::show(ui, self.model),
            PaneKind::CopyRelations => crate::copy_relation_view::show(ui, self.model, self.client),
            PaneKind::Ledger => show_ledger(ui, self.model),
            PaneKind::TradeDock => crate::trade_dock::show(ui, self.model, self.client),
            PaneKind::Control => show_control(ui, self.model, self.client),
            PaneKind::Diagnostics => show_diagnostics(ui, self.model),
        });
        UiResponse::None
    }

    fn tab_title_for_pane(&mut self, pane: &Pane) -> egui::WidgetText {
        pane.title(self.model.preferences.language).into()
    }

    fn is_tab_closable(&self, _tiles: &Tiles<Pane>, _tile_id: TileId) -> bool {
        true
    }

    fn gap_width(&self, _style: &egui::Style) -> f32 {
        4.0
    }
}

pub fn show_top_bar(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    show_modules: &mut bool,
    show_trading_settings: &mut bool,
    show_execution_account: &mut bool,
    show_symbol_picker: &mut bool,
) {
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .inner_margin(egui::Margin::symmetric(10, 3))
        .show(ui, |ui| {
            let language = model.preferences.language;
            let mut reset_requested = false;
            let picker_requested = std::cell::Cell::new(*show_symbol_picker);
            let mut symbol_filter = model.symbol_filter.clone();
            let mut market_server = model.preferences.market_server;
            let account_overview = model.account_overview.clone();
            let mut account_selection_requested = None;
            let account_label = model
                .preferences
                .execution_account_id
                .as_deref()
                .map(|account| {
                    format!(
                        "{} {}",
                        text(language, TextKey::ExecutionAccount),
                        short_account(account)
                    )
                })
                .unwrap_or_else(|| {
                    model
                        .account_overview
                        .as_ref()
                        .map(|v| {
                            format!(
                                "{} · {}",
                                v.user.username,
                                text(language, TextKey::ExecutionAccount)
                            )
                        })
                        .unwrap_or_else(|| text(language, TextKey::LoginAccount).to_owned())
                });
            let ((), search_response) = egui::containers::Sides::new()
                .shrink_left()
                .height(48.0)
                .show(
                    ui,
                    |ui| {
                        ui.horizontal_centered(|ui| {
                            ui.label(
                                RichText::new("VENUEFLOW")
                                    .strong()
                                    .size(16.0)
                                    .color(theme::BRAND_HOVER),
                            );
                            ui.separator();
                            for workspace in WorkspaceKind::ALL {
                                if ui
                                    .selectable_label(
                                        workspaces.active == workspace,
                                        workspace.label(language),
                                    )
                                    .clicked()
                                {
                                    workspaces.active = workspace;
                                }
                            }
                            ui.separator();
                            let selected_symbol = model.preferences.selected_symbol.clone();
                            egui::ScrollArea::horizontal()
                                .id_salt("favorite-symbol-tabs")
                                .auto_shrink([false, true])
                                .show(ui, |ui| {
                                    ui.horizontal_centered(|ui| {
                                        let mut tabs = model.preferences.favorite_symbols.clone();
                                        if !tabs.contains(&selected_symbol) {
                                            tabs.push(selected_symbol.clone());
                                        }
                                        for symbol in tabs {
                                            let quote = local_quote(model, &symbol);
                                            let details = quote.map_or_else(
                                                || "—  —".to_owned(),
                                                |quote| {
                                                    format!(
                                                        "{} {:+.2}%",
                                                        model.format_market_price(
                                                            &symbol, quote.last
                                                        ),
                                                        quote.change_percent_24h
                                                    )
                                                },
                                            );
                                            let selected =
                                                model.preferences.selected_symbol == symbol;
                                            let detail_color =
                                                quote.map_or(theme::TEXT_SECONDARY, |quote| {
                                                    theme::value_color(decimal_to_f64(
                                                        quote.change_percent_24h,
                                                    ))
                                                });
                                            let response = ui.add_sized(
                                                [150.0, 44.0],
                                                egui::Button::new(symbol_tab_text(
                                                    &symbol,
                                                    &details,
                                                    detail_color,
                                                ))
                                                .fill(if selected {
                                                    theme::PANEL
                                                } else {
                                                    theme::BG_SECONDARY
                                                }),
                                            );
                                            if selected {
                                                ui.painter().line_segment(
                                                    [
                                                        response.rect.left_top(),
                                                        response.rect.right_top(),
                                                    ],
                                                    Stroke::new(2.0, theme::BRAND),
                                                );
                                            }
                                            if response.clicked() {
                                                model.select_symbol(symbol);
                                                workspaces.follow_dynamic_charts_latest();
                                            }
                                        }
                                        if ui
                                            .add_sized([32.0, 44.0], egui::Button::new("+"))
                                            .clicked()
                                        {
                                            picker_requested.set(true);
                                        }
                                    });
                                });
                        })
                        .inner
                    },
                    |ui| {
                        if ui
                            .button(text(language, TextKey::TradingSettings))
                            .clicked()
                        {
                            *show_trading_settings = true;
                        }
                        if ui.button(text(language, TextKey::Modules)).clicked() {
                            *show_modules = true;
                        }
                        if ui.button(text(language, TextKey::ResetLayout)).clicked() {
                            reset_requested = true;
                        }
                        let user_label = account_overview
                            .as_ref()
                            .map(|a| a.user.username.as_str())
                            .unwrap_or_else(|| text(language, TextKey::LoginAccount));
                        if ui.button(user_label).clicked() {
                            *show_execution_account = true;
                        }
                        if let Some(overview) = &account_overview {
                            egui::ComboBox::from_id_salt("execution-account-selection")
                                .selected_text(&account_label)
                                .show_ui(ui, |ui| {
                                    for credential in &overview.credentials {
                                        let selected = overview.selected_credential_id.as_deref()
                                            == Some(credential.credential_id.as_str());
                                        if ui
                                            .add_enabled(
                                                credential
                                                    .selectable(crate::account_center::now_ms()),
                                                egui::Button::selectable(
                                                    selected,
                                                    format!(
                                                        "{} · {}",
                                                        credential.label, credential.masked_key
                                                    ),
                                                ),
                                            )
                                            .clicked()
                                        {
                                            account_selection_requested =
                                                Some(credential.credential_id.clone());
                                        }
                                    }
                                    if ui
                                        .button(text(language, TextKey::SelectExecutionAccount))
                                        .clicked()
                                    {
                                        *show_execution_account = true;
                                    }
                                });
                        }
                        egui::ComboBox::from_id_salt("market-server")
                            .width(125.0)
                            .selected_text(format!(
                                "{}: {}",
                                text(language, TextKey::MarketServer),
                                market_server.label()
                            ))
                            .show_ui(ui, |ui| {
                                for server in crate::model::MarketServer::ALL {
                                    ui.selectable_value(&mut market_server, server, server.label());
                                }
                            });
                        let filter_before = symbol_filter.clone();
                        let search_response = ui.add_sized(
                            [180.0, 30.0],
                            egui::TextEdit::singleline(&mut symbol_filter)
                                .horizontal_align(egui::Align::LEFT)
                                .vertical_align(egui::Align::Center)
                                .hint_text(match language {
                                    Language::SimplifiedChinese => "搜索交易对",
                                    Language::English => "Search markets",
                                }),
                        );
                        if search_response.has_focus() && symbol_filter == filter_before {
                            let fallback = ui.input(|input| {
                                input
                                    .events
                                    .iter()
                                    .filter_map(|event| match event {
                                        egui::Event::Text(value) | egui::Event::Paste(value) => {
                                            Some(value.as_str())
                                        }
                                        _ => None,
                                    })
                                    .collect::<String>()
                            });
                            if !fallback.is_empty() {
                                symbol_filter.push_str(&fallback);
                            }
                        }
                        if search_response.has_focus() || symbol_filter != filter_before {
                            picker_requested.set(true);
                        }
                        search_response
                    },
                );
            if model.preferences.market_server != market_server {
                model.preferences.market_server = market_server;
                if account_selection_requested.is_some() {
                    model.account_selection_requested = account_selection_requested;
                }
            }
            model.symbol_filter = symbol_filter;
            *show_symbol_picker = picker_requested.get();
            if reset_requested {
                workspaces.restore_active();
                model.notice("Restored the active workspace layout");
            }
            crate::symbol_picker::show(&search_response, show_symbol_picker, model, workspaces);
        });
}
pub fn show_status_bar(ui: &mut egui::Ui, model: &AppModel) {
    status_bar::show(ui, model);
}
pub fn show_confirmation(context: &egui::Context, model: &mut AppModel, client: &ControlClient) {
    let Some(mut pending) = model.pending_confirmation.take() else {
        return;
    };
    let expected = pending.request.expected_confirmation();
    let mut keep_open = true;
    let language = model.preferences.language;
    egui::Window::new(text(language, TextKey::ConfirmAction))
        .collapsible(false)
        .resizable(false)
        .anchor(Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .show(context, |ui| {
            ui.colored_label(theme::WARNING, text(language, TextKey::IntentWarning));
            ui.separator();
            ui.label(format!("Venue: {}", pending.request.venue));
            ui.label(format!("Mode: {}", pending.request.mode));
            ui.label(format!("Account: {}", pending.request.trading_account_id));
            ui.label(format!("Instance: {}", pending.request.instance_id));
            ui.label(format!("Symbol: {}", pending.request.symbol));
            ui.label(format!(
                "Config epoch: {}",
                pending.request.expected_config_epoch
            ));
            ui.label(format!("Action: {}", pending.request.action.as_str()));
            ui.add_space(6.0);
            ui.label(text(language, TextKey::TypeConfirmation));
            ui.monospace(&expected);
            ui.text_edit_singleline(&mut pending.typed);
            ui.horizontal(|ui| {
                if ui.button(text(language, TextKey::Cancel)).clicked() {
                    keep_open = false;
                }
                let enabled = pending.typed == expected;
                if ui
                    .add_enabled(
                        enabled,
                        egui::Button::new(text(language, TextKey::SubmitIntent)),
                    )
                    .clicked()
                {
                    if let Some(request) = pending.confirmed_request() {
                        send_command(model, client, request);
                    }
                    keep_open = false;
                }
            });
        });
    if keep_open {
        model.pending_confirmation = Some(pending);
    }
}
pub fn show_modules(
    context: &egui::Context,
    open: &mut bool,
    workspaces: &mut Workspaces,
    language: Language,
) {
    let visibility = workspaces.pane_visibility(language);
    egui::Window::new(text(language, TextKey::WorkspaceModules))
        .open(open)
        .resizable(false)
        .show(context, |ui| {
            for (tile_id, title, mut visible) in visibility {
                if ui.checkbox(&mut visible, title).changed() {
                    workspaces.set_visible(tile_id, visible);
                }
            }
        });
}
fn connection_badge(ui: &mut egui::Ui, state: ConnectionState, language: Language) {
    let (label, color) = match state {
        ConnectionState::Connecting => (text(language, TextKey::Connecting), theme::WARNING),
        ConnectionState::Live => (text(language, TextKey::LiveData), theme::BUY),
        ConnectionState::Degraded => (text(language, TextKey::Degraded), theme::WARNING),
        ConnectionState::Offline => (text(language, TextKey::Offline), theme::SELL),
    };
    ui.colored_label(color, RichText::new(label).strong());
}
fn show_market_watch(ui: &mut egui::Ui, model: &mut AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::Markets),
        text(language, TextKey::MarketSource),
    );
    let mut symbols = available_symbols(model);
    symbols.extend(model.preferences.favorite_symbols.iter().cloned());
    symbols.sort_by(|left, right| {
        favorite_rank(&model.preferences.favorite_symbols, left)
            .cmp(&favorite_rank(&model.preferences.favorite_symbols, right))
            .then_with(|| left.cmp(right))
    });
    symbols.dedup();
    egui::ScrollArea::vertical().show(ui, |ui| {
        egui::Grid::new("market-watch-grid")
            .striped(true)
            .num_columns(3)
            .show(ui, |ui| {
                ui.strong(text(language, TextKey::Symbol));
                ui.strong(text(language, TextKey::Last));
                ui.strong(text(language, TextKey::Source));
                ui.end_row();
                for symbol in symbols {
                    if ui
                        .selectable_label(model.preferences.selected_symbol == symbol, &symbol)
                        .clicked()
                    {
                        model.select_symbol(symbol.clone());
                        model.follow_latest_requested = true;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        if let Some(last) = model
                            .local_markets
                            .view_for_symbol(&symbol)
                            .and_then(|market| market.last)
                        {
                            ui.monospace(model.format_market_price(&symbol, last));
                            ui.colored_label(theme::BUY, "BINANCE");
                        } else if let Some(projected) = market(model, &symbol) {
                            ui.monospace(model.format_market_price(&symbol, projected.last));
                            ui.colored_label(theme::TEXT_SECONDARY, "CONTROL");
                        } else {
                            ui.monospace("—");
                            ui.colored_label(theme::TEXT_SECONDARY, "BINANCE");
                        }
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        if let Some(projected) = market(model, &symbol) {
                            ui.monospace(model.format_market_price(&symbol, projected.last));
                            ui.colored_label(theme::TEXT_SECONDARY, "CONTROL");
                        } else {
                            ui.monospace("—");
                            ui.colored_label(theme::TEXT_SECONDARY, "CONTROL");
                        }
                    }
                    ui.end_row();
                }
            });
    });
}
fn show_chart(ui: &mut egui::Ui, pane: &mut Pane, model: &mut AppModel) {
    let language = model.preferences.language;
    let settings_key = pane.settings_key();
    #[cfg(not(target_arch = "wasm32"))]
    let history_status = MarketSelection::binance_usd_m(
        pane.symbol
            .as_deref()
            .unwrap_or(&model.preferences.selected_symbol),
        pane.interval,
    )
    .ok()
    .and_then(|selection| model.local_markets.view(&selection))
    .and_then(|view| {
        if let Some(error) = &view.history_error {
            Some(error.clone())
        } else if view.history_loading {
            Some(text(language, TextKey::LoadingHistory).into())
        } else if view.history_exhausted || view.bars.len() >= crate::market::MAX_BARS {
            Some(text(language, TextKey::HistoryBoundary).into())
        } else {
            None
        }
    });
    #[cfg(target_arch = "wasm32")]
    let history_status: Option<String> = None;
    let settings_requested = show_chart_toolbar(ui, pane, language, history_status.as_deref());
    if settings_requested {
        model.indicator_settings_requested = true;
        model.indicator_target = Some(settings_key.clone());
    }
    let settings = model
        .preferences
        .chart_overrides
        .get(&settings_key)
        .unwrap_or(&model.preferences.chart)
        .clone();
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol)
        .to_owned();
    let highlighted_price = (symbol == model.preferences.selected_symbol)
        .then(|| model.trade_dock.highlighted_price(ui.ctx()))
        .flatten();
    #[cfg(not(target_arch = "wasm32"))]
    if let Ok(selection) = MarketSelection::binance_usd_m(&symbol, pane.interval)
        && let Err(error) =
            model
                .local_markets
                .configure_chart(&settings_key, &selection, settings.engine_config())
    {
        ui.colored_label(theme::WARNING, error.to_string());
        return;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.chart_view(&settings_key) {
        let (price_scale, quantity_scale) = model.market_scales(&symbol);
        let chart = presentation::sample(
            ui,
            ("chart-display", pane.instance),
            (
                local.selection.clone(),
                local.generation,
                local.status,
                local.bars.len(),
                local.bars.first().map(|bar| bar.open_time_ms),
                settings.clone(),
            ),
            model.preferences.trading.chart_cadence,
            || (local.bars.clone(), local.studies.clone(), local.last),
        );
        let selected_price = crate::chart_view::candle_plot(
            ui,
            &chart.0,
            &chart.1,
            &mut pane.viewport,
            language,
            &settings,
            (price_scale, quantity_scale),
            pane.interval,
            chart.2,
            highlighted_price,
        );
        let selection = local.selection.clone();
        let near_start = pane.viewport.right_offset() > 0
            && pane.viewport.right_offset() + pane.viewport.visible_bars() + 32 >= chart.0.len();
        let manual = std::mem::take(&mut pane.history_requested);
        if (manual || near_start)
            && let Some(request) = model.local_markets.begin_history(&selection, manual)
        {
            model.history_requests.push(request);
        }
        if let Some(price) = selected_price {
            model.select_trading_price(&symbol, price, ui.ctx());
        }
        if settings_requested {
            model.indicator_settings_requested = true;
            model.indicator_target = Some(settings_key);
        }
        return;
    }
    pane_heading(ui, &symbol, text(language, TextKey::ControlFallback));
    let Some(market) = market(model, &symbol) else {
        empty(ui, text(language, TextKey::NoMarket));
        return;
    };
    ui.horizontal_wrapped(|ui| {
        ui.label(format!(
            "Last {}",
            model.format_market_price(&symbol, market.last)
        ));
        ui.label(format!(
            "Bid {}",
            model.format_market_price(&symbol, market.bid)
        ));
        ui.label(format!(
            "Ask {}",
            model.format_market_price(&symbol, market.ask)
        ));
        for indicator in market.indicators.iter().take(6) {
            ui.colored_label(
                theme::BRAND_HOVER,
                format!("{} {}", indicator.name, format_decimal(indicator.value, 3)),
            )
            .on_hover_text(format!(
                "{} · observed {}",
                indicator.source_version, indicator.observed_ms
            ));
        }
    });
    let chart = presentation::sample(
        ui,
        ("chart-display-control", pane.instance),
        (
            symbol.clone(),
            pane.interval,
            model.connection,
            settings.clone(),
            market.bars.len(),
        ),
        model.preferences.trading.chart_cadence,
        || (market.bars.clone(), market.last),
    );
    let selected_price = crate::chart_view::candle_plot(
        ui,
        &chart.0,
        &[],
        &mut pane.viewport,
        language,
        &settings,
        (8, 8),
        pane.interval,
        Some(chart.1),
        highlighted_price,
    );
    if let Some(price) = selected_price {
        model.select_trading_price(&symbol, price, ui.ctx());
    }
    if settings_requested {
        model.indicator_settings_requested = true;
        model.indicator_target = Some(settings_key);
    }
}
fn show_chart_toolbar(
    ui: &mut egui::Ui,
    pane: &mut Pane,
    language: Language,
    history_status: Option<&str>,
) -> bool {
    let mut settings_requested = false;
    ui.horizontal_wrapped(|ui| {
        if ui
            .button(format!("⚙ {}", text(language, TextKey::Indicators)))
            .clicked()
        {
            settings_requested = true;
        }
        ui.separator();
        for interval in crate::chart::ChartInterval::ALL {
            if ui
                .selectable_label(pane.interval == interval, interval.label())
                .clicked()
            {
                pane.interval = interval;
                pane.viewport.reset();
            }
        }
        ui.separator();
        if ui.small_button(text(language, TextKey::Fit)).clicked() {
            pane.viewport.reset();
        }
        if ui.small_button(text(language, TextKey::Follow)).clicked() {
            pane.viewport.follow_latest();
        }
        if let Some(status) = history_status {
            ui.weak(status);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if ui
            .small_button(text(language, TextKey::OlderBars))
            .clicked()
        {
            pane.history_requested = true;
        }
        ui.colored_label(
            theme::TEXT_SECONDARY,
            format!(
                "{} {} · {}",
                pane.viewport.visible_bars(),
                text(language, TextKey::Bars),
                if pane.viewport.right_offset() == 0 {
                    text(language, TextKey::Live)
                } else {
                    text(language, TextKey::History)
                }
            ),
        );
    });
    settings_requested
}
// Chart painting lives in chart_view to keep this UI entrypoint compositional.
fn show_order_book(ui: &mut egui::Ui, pane: &Pane, model: &mut AppModel) {
    let language = model.preferences.language;
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol)
        .to_owned();
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.view_for_symbol(&symbol) {
        let scope = (local.selection.clone(), local.generation, local.status);
        let book = presentation::sample(
            ui,
            ("book-display", pane.instance),
            scope.clone(),
            model.preferences.trading.book_cadence,
            || {
                (
                    local.asks.clone(),
                    local.bids.clone(),
                    local.last,
                    local.bid,
                    local.ask,
                )
            },
        );
        let trades = presentation::sample(
            ui,
            ("tape-display", pane.instance),
            scope,
            model.preferences.trading.tape_cadence,
            || local.trades.clone(),
        );
        let selected_price = crate::order_book_view::show(
            ui,
            pane.instance,
            &book.0,
            &book.1,
            &trades,
            book.2,
            book.3,
            book.4,
            language,
            model,
            &symbol,
        );
        if let Some(price) = selected_price {
            model.select_trading_price(&symbol, price, ui.ctx());
        }
        return;
    }
    let Some(market) = market(model, &symbol) else {
        empty(ui, text(language, TextKey::NoBook));
        return;
    };
    let book = presentation::sample(
        ui,
        ("book-display-control", pane.instance),
        (symbol.clone(), model.connection),
        model.preferences.trading.book_cadence,
        || {
            (
                market.asks.clone(),
                market.bids.clone(),
                market.last,
                market.bid,
                market.ask,
            )
        },
    );
    let trades = presentation::sample(
        ui,
        ("tape-display-control", pane.instance),
        (symbol.clone(), model.connection),
        model.preferences.trading.tape_cadence,
        || market.trades.clone(),
    );
    let selected_price = crate::order_book_view::show(
        ui,
        pane.instance,
        &book.0,
        &book.1,
        &trades,
        Some(book.2),
        Some(book.3),
        Some(book.4),
        language,
        model,
        &symbol,
    );
    if let Some(price) = selected_price {
        model.select_trading_price(&symbol, price, ui.ctx());
    }
}
fn show_trade_tape(ui: &mut egui::Ui, pane: &Pane, model: &AppModel) {
    let language = model.preferences.language;
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    pane_heading(ui, text(language, TextKey::TradeTape), symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.view_for_symbol(symbol) {
        let trades = presentation::sample(
            ui,
            ("standalone-tape", pane.instance),
            (local.selection.clone(), local.generation, local.status),
            model.preferences.trading.tape_cadence,
            || local.trades.clone(),
        );
        if trades.is_empty() {
            empty(ui, text(language, TextKey::NoTrades));
        } else {
            show_trade_rows(ui, pane.instance, &trades, language, model, symbol);
        }
        return;
    }
    let Some(market) = market(model, symbol) else {
        empty(ui, text(language, TextKey::NoTrades));
        return;
    };
    let trades = presentation::sample(
        ui,
        ("standalone-tape-control", pane.instance),
        (symbol.to_owned(), model.connection),
        model.preferences.trading.tape_cadence,
        || market.trades.clone(),
    );
    show_trade_rows(ui, pane.instance, &trades, language, model, symbol);
}
fn show_trade_rows(
    ui: &mut egui::Ui,
    instance: u32,
    trades: &[venue_control_protocol::UiTrade],
    language: Language,
    model: &AppModel,
    symbol: &str,
) {
    egui::ScrollArea::vertical().show(ui, |ui| {
        egui::Grid::new(format!("tape-{instance}"))
            .striped(true)
            .show(ui, |ui| {
                ui.strong(text(language, TextKey::Time));
                ui.strong(text(language, TextKey::Price));
                ui.strong(text(language, TextKey::Quantity));
                ui.end_row();
                for trade in trades.iter().rev().take(80) {
                    let color = match trade.aggressor {
                        AggressorSide::Buy => theme::BUY,
                        AggressorSide::Sell => theme::SELL,
                        AggressorSide::Unknown => theme::TEXT_SECONDARY,
                    };
                    ui.monospace(trade.occurred_ms.to_string());
                    ui.colored_label(color, model.format_market_price(symbol, trade.price));
                    ui.monospace(model.format_market_quantity(symbol, trade.quantity));
                    ui.end_row();
                }
            });
    });
}
fn show_accounts(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::Accounts),
        text(language, TextKey::AccountsSource),
    );
    let Some(snapshot) = &model.snapshot else {
        empty(ui, text(language, TextKey::WaitingControl));
        return;
    };
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("accounts-grid")
            .striped(true)
            .show(ui, |ui| {
                for heading in [
                    TextKey::Venue,
                    TextKey::Mode,
                    TextKey::Account,
                    TextKey::Health,
                    TextKey::Equity,
                    TextKey::Available,
                    TextKey::UnrealizedPnl,
                    TextKey::PrivateGeneration,
                    TextKey::WriterGeneration,
                    TextKey::ReconciledAge,
                ] {
                    ui.strong(text(language, heading));
                }
                ui.end_row();
                for account in &snapshot.accounts {
                    ui.label(account.venue.to_string());
                    ui.label(account.mode.to_string());
                    ui.monospace(short_account(&account.trading_account_id));
                    health_label(ui, account.health);
                    let balance_equity = account
                        .balances
                        .iter()
                        .map(|balance| {
                            format!("{} {}", balance.asset, format_decimal(balance.equity, 2))
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    ui.monospace(if balance_equity.is_empty() {
                        account
                            .equity
                            .map_or_else(|| "—".to_owned(), |value| format_decimal(value, 2))
                    } else {
                        balance_equity
                    });
                    let balance_margin = account
                        .balances
                        .iter()
                        .map(|balance| {
                            let value = balance
                                .available_margin
                                .map_or_else(|| "—".to_owned(), |value| format_decimal(value, 2));
                            format!("{} {value}", balance.asset)
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    ui.monospace(if balance_margin.is_empty() {
                        account
                            .available_margin
                            .map_or_else(|| "—".to_owned(), |value| format_decimal(value, 2))
                    } else {
                        balance_margin
                    });
                    if let Some(value) = account.unrealized_pnl {
                        let pnl = decimal_to_f64(value);
                        ui.colored_label(theme::value_color(pnl), format!("{pnl:+.2}"));
                    } else {
                        ui.monospace("—");
                    }
                    ui.monospace(account.private_generation.to_string());
                    ui.monospace(account.writer_generation.to_string());
                    ui.monospace(format_freshness(freshness_age_ms(
                        snapshot.generated_ms,
                        account.last_reconciled_ms,
                    )));
                    ui.end_row();
                }
            });
        ui.separator();
        ui.colored_label(
            theme::WARNING,
            text(language, TextKey::AccountProjectionCaveat),
        );
        ui.small(text(language, TextKey::AccountAuthorityCaveat));
    });
}
fn show_strategies(ui: &mut egui::Ui, model: &mut AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::Strategies),
        text(language, TextKey::StrategiesSubtitle),
    );
    let strategies = model
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.strategies.clone())
        .unwrap_or_default();
    if strategies.is_empty() {
        empty(ui, text(language, TextKey::NoStrategies));
        return;
    }
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("strategies-grid")
            .striped(true)
            .show(ui, |ui| {
                for heading in [
                    TextKey::Instance,
                    TextKey::Kind,
                    TextKey::Venue,
                    TextKey::Mode,
                    TextKey::Symbol,
                    TextKey::State,
                    TextKey::Orders,
                    TextKey::Long,
                    TextKey::Short,
                    TextKey::Pnl,
                    TextKey::Epoch,
                ] {
                    ui.strong(text(language, heading));
                }
                ui.end_row();
                for strategy in strategies {
                    if ui
                        .selectable_label(
                            model.preferences.selected_instance.as_deref()
                                == Some(strategy.instance_id.as_str()),
                            &strategy.instance_id,
                        )
                        .clicked()
                    {
                        model.preferences.selected_instance = Some(strategy.instance_id.clone());
                        model.select_symbol(strategy.symbol.to_string());
                    }
                    ui.label(format!("{:?}", strategy.kind));
                    ui.label(strategy.venue.to_string());
                    ui.label(strategy.mode.to_string());
                    ui.label(strategy.symbol.to_string());
                    lifecycle_label(ui, strategy.lifecycle);
                    ui.monospace(strategy.open_orders.to_string());
                    ui.monospace(format_decimal(strategy.long_quantity, 4));
                    ui.monospace(format_decimal(strategy.short_quantity, 4));
                    match (strategy.realized_pnl, strategy.unrealized_pnl) {
                        (Some(realized), Some(unrealized)) => {
                            let pnl = decimal_to_f64(realized + unrealized);
                            ui.colored_label(theme::value_color(pnl), format!("{pnl:+.2}"));
                        }
                        _ => {
                            ui.monospace("—");
                        }
                    }
                    ui.monospace(strategy.config_epoch.to_string());
                    ui.end_row();
                    if let Some(attention) = &strategy.attention {
                        ui.label("");
                        ui.colored_label(theme::WARNING, attention);
                        ui.end_row();
                    }
                }
            });
    });
}
fn show_ledger(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::ReceiptLedger),
        text(language, TextKey::LedgerSubtitle),
    );
    let Some(snapshot) = &model.snapshot else {
        empty(ui, text(language, TextKey::NoLedger));
        return;
    };
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("ledger-grid").striped(true).show(ui, |ui| {
            for heading in [
                TextKey::Observed,
                TextKey::Instance,
                TextKey::Action,
                TextKey::State,
                TextKey::Receipt,
                TextKey::Detail,
            ] {
                ui.strong(text(language, heading));
            }
            ui.end_row();
            for entry in snapshot.ledger.iter().rev().take(500) {
                ui.monospace(entry.occurred_ms.to_string());
                ui.label(&entry.instance_id);
                ui.label(&entry.action);
                ui.label(&entry.state);
                ui.monospace(&entry.receipt_id);
                if entry.detail.trim().is_empty() {
                    ui.colored_label(theme::TEXT_SECONDARY, text(language, TextKey::None));
                } else if matches!(entry.state.as_str(), "rejected" | "unknown") {
                    ui.colored_label(
                        theme::SELL,
                        format!(
                            "{}: {}",
                            text(language, TextKey::FailureReason),
                            entry.detail
                        ),
                    );
                } else {
                    ui.label(&entry.detail);
                }
                ui.end_row();
            }
        });
    });
}
fn show_control(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::LifecycleControl),
        text(language, TextKey::ControlSubtitle),
    );
    let strategies = model
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.strategies.clone())
        .unwrap_or_default();
    if strategies.is_empty() {
        empty(ui, text(language, TextKey::NoControl));
        return;
    }
    if model.preferences.selected_instance.is_none() {
        model.preferences.selected_instance = strategies.first().map(|row| row.instance_id.clone());
    }
    egui::ComboBox::from_id_salt("control-instance")
        .selected_text(
            model
                .preferences
                .selected_instance
                .as_deref()
                .unwrap_or(text(language, TextKey::SelectInstance)),
        )
        .show_ui(ui, |ui| {
            for strategy in &strategies {
                ui.selectable_value(
                    &mut model.preferences.selected_instance,
                    Some(strategy.instance_id.clone()),
                    format!(
                        "{} · {} · {}",
                        strategy.instance_id, strategy.venue, strategy.symbol
                    ),
                );
            }
        });
    let selected = model
        .preferences
        .selected_instance
        .as_deref()
        .and_then(|id| strategies.iter().find(|row| row.instance_id == id))
        .cloned();
    let Some(strategy) = selected else {
        return;
    };
    ui.separator();
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Venue),
        strategy.venue
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Mode),
        strategy.mode
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Account),
        strategy.trading_account_id
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Symbol),
        strategy.symbol
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Epoch),
        strategy.config_epoch
    ));
    lifecycle_label(ui, strategy.lifecycle);
    if let Some(attention) = &strategy.attention {
        ui.colored_label(theme::WARNING, attention);
    }
    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        for (action, label) in [
            (ControlAction::Pause, text(language, TextKey::Pause)),
            (ControlAction::Resume, text(language, TextKey::Resume)),
            (ControlAction::Stop, text(language, TextKey::Stop)),
            (ControlAction::Flatten, text(language, TextKey::Flatten)),
        ] {
            let button = if action == ControlAction::Flatten {
                egui::Button::new(RichText::new(label).color(theme::SELL))
            } else {
                egui::Button::new(label)
            };
            if ui.add(button).clicked() {
                submit_or_confirm(model, client, &strategy, action);
            }
        }
    });
    ui.separator();
    ui.small(text(language, TextKey::StopSemantics));
    ui.small(text(language, TextKey::ConfirmationSemantics));
    if !model.commands.is_empty() {
        ui.separator();
        ui.strong(text(language, TextKey::SessionReceipts));
        egui::ScrollArea::vertical()
            .max_height(170.0)
            .show(ui, |ui| {
                for command in model.commands.iter().take(16) {
                    ui.horizontal_wrapped(|ui| {
                        ui.monospace(&command.request.request_id);
                        ui.label(command.request.action.as_str());
                        ui.label(command.request.mode.to_string());
                        ui.monospace(&command.request.trading_account_id);
                        ui.label(command.request.symbol.to_string());
                        ui.label(&command.request.instance_id);
                        ui.monospace(format!("epoch {}", command.request.expected_config_epoch));
                    });
                    match (&command.latest_receipt, &command.terminal_receipt) {
                        (_, Some(receipt)) => {
                            receipt_state_label(ui, receipt.state);
                            ui.monospace(format!("final receipt {}", receipt.receipt_id));
                            if !receipt.detail.is_empty() {
                                ui.small(&receipt.detail);
                            }
                        }
                        (Some(receipt), None) => {
                            receipt_state_label(ui, receipt.state);
                            ui.small(
                                "accepted; awaiting a final Applied / Rejected / Unknown receipt",
                            );
                        }
                        (None, None) => {
                            ui.colored_label(theme::WARNING, "submitted; awaiting receipt");
                        }
                    }
                    ui.separator();
                }
            });
    }
}
fn submit_or_confirm(
    model: &mut AppModel,
    client: &ControlClient,
    strategy: &StrategySummary,
    action: ControlAction,
) {
    let request = model.begin_command(strategy, action, now_ms());
    if requires_operator_confirmation(action) {
        model.pending_confirmation = Some(PendingConfirmation::new(request));
    } else {
        send_command(model, client, request);
    }
}
fn send_command(model: &mut AppModel, client: &ControlClient, request: ControlCommandRequest) {
    match client.send(request.clone()) {
        Ok(()) => {
            model.record_submission(request.clone());
            model.notice(format!(
                "Submitted {} intent for {}",
                request.action.as_str(),
                request.instance_id
            ));
        }
        Err(error) => model.notice(format!("Control request rejected locally: {error}")),
    }
}
fn show_diagnostics(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::Diagnostics),
        text(language, TextKey::DiagnosticsSubtitle),
    );
    connection_badge(ui, model.connection, language);
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::RuntimeProjection),
        model.control_connection.map_or(
            text(language, TextKey::AwaitingSnapshot).to_owned(),
            |state| format!("{state:?}")
        )
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::Endpoint),
        endpoint_label(&model.preferences.endpoint)
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::SnapshotPolling),
        if model.snapshot_online {
            text(language, TextKey::Online)
        } else {
            text(language, TextKey::Offline)
        }
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::EventStream),
        if model.event_stream_online {
            text(language, TextKey::Online)
        } else {
            text(language, TextKey::Offline)
        }
    ));
    ui.label(format!(
        "{}: {}",
        text(language, TextKey::LastEventId),
        model
            .last_event_id
            .map_or(text(language, TextKey::None).to_owned(), |event_id| {
                event_id.to_string()
            })
    ));
    if let Some(snapshot) = &model.snapshot {
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::Schema),
            snapshot.schema_version
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::Generated),
            snapshot.generated_ms
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::Accounts),
            snapshot.accounts.len()
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::Strategies),
            snapshot.strategies.len()
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::Markets),
            snapshot.markets.len()
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::LedgerRows),
            snapshot.ledger.len()
        ));
    }
    ui.separator();
    ui.strong(text(language, TextKey::AuthorityCoverage));
    ui.label(text(language, TextKey::LiveProjection));
    ui.label(text(language, TextKey::ReceiptProjection));
    ui.colored_label(theme::WARNING, text(language, TextKey::WalNotProjected));
    ui.colored_label(theme::WARNING, text(language, TextKey::UnknownNotProjected));
    ui.colored_label(
        theme::WARNING,
        text(language, TextKey::CapabilityNotProjected),
    );
    ui.separator();
    #[cfg(not(target_arch = "wasm32"))]
    {
        ui.strong(text(language, TextKey::LocalPublicData));
        ui.label(text(language, TextKey::LocalVenueLive));
        ui.label(format!(
            "{}: {} · {}: {}",
            text(language, TextKey::Subscriptions),
            model.local_markets.selections().count(),
            text(language, TextKey::Generation),
            model.local_markets.generation()
        ));
        ui.label(format!(
            "{}: {}",
            text(language, TextKey::CatalogSymbols),
            model.local_symbols.len()
        ));
        ui.label(text(
            language,
            if model.local_proxy_detected {
                TextKey::ProxyEnabled
            } else {
                TextKey::ProxyDisabled
            },
        ));
        if let Some(error) = &model.local_catalog_error {
            ui.colored_label(theme::WARNING, error);
        }
        ui.label(text(language, TextKey::FixedEndpoints));
        ui.separator();
    }
    ui.strong(text(language, TextKey::RecentNotices));
    for notice in &model.notices {
        ui.small(notice);
    }
    ui.separator();
    ui.colored_label(
        theme::TEXT_SECONDARY,
        text(language, TextKey::PublicBoundary),
    );
}
fn receipt_state_label(ui: &mut egui::Ui, state: CommandState) {
    let color = match state {
        CommandState::Applied => theme::BUY,
        CommandState::Accepted => theme::WARNING,
        CommandState::Rejected | CommandState::Unknown => theme::SELL,
    };
    ui.colored_label(color, format!("{state:?}"));
}
fn format_freshness(age_ms: Option<u64>) -> String {
    age_ms.map_or("unknown".to_owned(), |age_ms| {
        if age_ms < 1_000 {
            format!("{age_ms} ms")
        } else {
            format!("{:.1} s", age_ms as f64 / 1_000.0)
        }
    })
}
fn market<'a>(model: &'a AppModel, symbol: &str) -> Option<&'a MarketSummary> {
    model
        .snapshot
        .as_ref()?
        .markets
        .iter()
        .find(|market| market.symbol.to_string() == symbol)
}
pub(crate) fn available_symbols(model: &AppModel) -> Vec<String> {
    #[cfg(not(target_arch = "wasm32"))]
    if !model.local_symbols.is_empty() {
        return model.local_symbols.clone();
    }
    model
        .snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .markets
                .iter()
                .map(|market| market.symbol.to_string())
                .collect()
        })
        .unwrap_or_default()
}
pub(crate) fn favorite_rank(favorites: &[String], symbol: &str) -> usize {
    favorites
        .iter()
        .position(|favorite| favorite == symbol)
        .unwrap_or(favorites.len())
}
pub(crate) fn local_quote<'a>(model: &'a AppModel, symbol: &str) -> Option<&'a MarketQuote> {
    model.local_quotes.get(symbol)
}
fn symbol_tab_text(symbol: &str, details: &str, detail_color: Color32) -> egui::WidgetText {
    let mut job = egui::text::LayoutJob::default();
    job.append(
        symbol,
        0.0,
        egui::TextFormat {
            font_id: FontId::proportional(12.5),
            color: theme::TEXT_PRIMARY,
            ..Default::default()
        },
    );
    job.append(
        &format!("\n{details}"),
        0.0,
        egui::TextFormat {
            font_id: FontId::monospace(11.0),
            color: detail_color,
            ..Default::default()
        },
    );
    job.into()
}
pub(crate) fn pane_heading(ui: &mut egui::Ui, title: &str, subtitle: &str) {
    ui.horizontal(|ui| {
        ui.strong(title);
        ui.colored_label(theme::TEXT_SECONDARY, subtitle);
    });
    ui.separator();
}

pub(crate) fn empty(ui: &mut egui::Ui, message: &str) {
    ui.centered_and_justified(|ui| {
        ui.colored_label(theme::TEXT_SECONDARY, message);
    });
}

fn lifecycle_label(ui: &mut egui::Ui, lifecycle: StrategyLifecycle) {
    let color = match lifecycle {
        StrategyLifecycle::Running => theme::BUY,
        StrategyLifecycle::Paused | StrategyLifecycle::Rebuilding => theme::WARNING,
        StrategyLifecycle::NeedsAttention => theme::SELL,
        StrategyLifecycle::Starting | StrategyLifecycle::Stopping | StrategyLifecycle::Stopped => {
            theme::TEXT_SECONDARY
        }
    };
    ui.colored_label(color, format!("{:?}", lifecycle));
}

fn health_label(ui: &mut egui::Ui, health: HealthState) {
    let color = match health {
        HealthState::Healthy => theme::BUY,
        HealthState::Recovering | HealthState::NeedsAttention => theme::WARNING,
        HealthState::Stopped | HealthState::Unknown => theme::TEXT_SECONDARY,
    };
    ui.colored_label(color, format!("{:?}", health));
}

fn short_account(account: &str) -> String {
    if account.len() <= 13 {
        account.to_owned()
    } else {
        format!("{}…{}", &account[..8], &account[account.len() - 4..])
    }
}

fn endpoint_label(endpoint: &str) -> &str {
    if endpoint.trim().is_empty() {
        "same origin"
    } else {
        endpoint
    }
}

fn now_ms() -> u64 {
    crate::account_center::now_ms()
}
