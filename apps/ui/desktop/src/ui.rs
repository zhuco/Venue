use eframe::egui::{self, Align2, FontId, RichText, Stroke};
mod presentation;
mod status_bar;
mod tab_prices;
#[cfg(test)]
mod tests;
mod top_bar;
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
            PaneKind::Chart => show_chart(ui, pane, self.model, self.client),
            PaneKind::OrderBook => show_order_book(ui, pane, self.model),
            PaneKind::TradeTape => show_trade_tape(ui, pane, self.model),
            PaneKind::Accounts => show_accounts(ui, self.model),
            PaneKind::Strategies => show_strategies(ui, self.model),
            PaneKind::Execution => crate::execution_view::show(ui, self.model, self.client),
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

    fn min_size(&self) -> f32 {
        // Keep both panes in the lower trading row tall enough for every action button.
        300.0
    }
}

pub use top_bar::show as show_top_bar;

fn show_symbol_tabs(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
    picker_requested: &std::cell::Cell<bool>,
) {
    let mut tabs = model.preferences.favorite_symbols.clone();
    if !tabs.contains(&model.preferences.selected_symbol) {
        tabs.push(model.preferences.selected_symbol.clone());
    }
    let mut close_requested = None;
    let now = crate::market_prices::now_ms();
    let cache_id = ui.make_persistent_id("symbol-tab-prices");
    let mut cache = ui.data_mut(|data| {
        data.get_temp::<tab_prices::TabPrices>(cache_id)
            .unwrap_or_default()
    });
    cache.retain_tabs(model.preferences.market_server, &tabs);
    for symbol in tabs {
        #[cfg(not(target_arch = "wasm32"))]
        let unlisted = !model.local_symbols.is_empty() && !model.local_symbols.contains(&symbol);
        #[cfg(target_arch = "wasm32")]
        let unlisted = false;
        let quote = local_quote(model, &symbol);
        let latest = cache.observe(&symbol, model.last_trade_for_tab(&symbol, now));
        let details = format!(
            "{} {}",
            latest
                .map(|price| model.format_market_price(&symbol, price.value))
                .unwrap_or_else(|| match model.preferences.language {
                    Language::SimplifiedChinese if unlisted => "该所无此合约".into(),
                    Language::English if unlisted => "Not listed".into(),
                    Language::SimplifiedChinese => "读取中".into(),
                    Language::English => "Loading".into(),
                }),
            quote
                .map(|quote| format!("{:+.2}%", quote.change_percent_24h))
                .unwrap_or_else(|| "—".into()),
        );
        let selected = model.preferences.selected_symbol == symbol;
        let detail_color = quote.map_or(theme::TEXT_SECONDARY, |quote| {
            theme::value_color(decimal_to_f64(quote.change_percent_24h))
        });
        let (rect, response) =
            ui.allocate_exact_size(egui::vec2(152.0, 48.0), egui::Sense::click());
        let response = if let Some(price) = latest {
            let age = now.saturating_sub(price.event_ms) / 1000;
            response.on_hover_text(match model.preferences.language {
                Language::SimplifiedChinese => format!("最近市场成交价 · {age} 秒前更新"),
                Language::English => format!("Last market trade · updated {age}s ago"),
            })
        } else {
            response
        };
        ui.painter().rect_filled(
            rect,
            0.0,
            if selected {
                theme::DIVIDER
            } else if response.hovered() {
                theme::BG_SECONDARY
            } else {
                theme::BG_PRIMARY
            },
        );
        if selected {
            ui.painter().line_segment(
                [rect.left_top(), rect.right_top()],
                Stroke::new(2.0, theme::BRAND),
            );
        }
        let name_rect = ui.painter().text(
            rect.left_top() + egui::vec2(10.0, 6.0),
            Align2::LEFT_TOP,
            &symbol,
            if selected {
                theme::emphasis_font(14.0)
            } else {
                FontId::proportional(14.0)
            },
            if selected {
                theme::TEXT_PRIMARY
            } else {
                theme::TEXT_SECONDARY
            },
        );
        ui.painter().text(
            egui::pos2(rect.left() + 10.0, name_rect.bottom() + 2.8),
            Align2::LEFT_TOP,
            details,
            FontId::proportional(11.0),
            detail_color,
        );
        let close_rect = egui::Rect::from_min_max(
            egui::pos2(rect.right() - 26.0, rect.top()),
            rect.right_bottom(),
        );
        let close = ui.interact(
            close_rect,
            ui.make_persistent_id(("close-symbol-tab", &symbol)),
            egui::Sense::click(),
        );
        if response.hovered() || close.hovered() {
            ui.painter().text(
                close_rect.center(),
                Align2::CENTER_CENTER,
                "×",
                FontId::proportional(16.0),
                if close.hovered() {
                    theme::TEXT_PRIMARY
                } else {
                    theme::TEXT_SECONDARY
                },
            );
        }
        if close.clicked() {
            close_requested = Some(symbol.clone());
        } else if response.clicked() {
            model.select_symbol(symbol);
            workspaces.follow_dynamic_charts_latest();
        }
    }
    ui.data_mut(|data| data.insert_temp(cache_id, cache));
    if let Some(symbol) = close_requested
        && model.close_symbol_tab(&symbol)
    {
        workspaces.follow_dynamic_charts_latest();
        if model.preferences.favorite_symbols.is_empty() {
            picker_requested.set(true);
        }
    }
    if ui.add_sized([40.0, 48.0], egui::Button::new("+")).clicked() {
        picker_requested.set(true);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn window_controls(ui: &mut egui::Ui) {
    if ui.add_sized([32.0, 26.0], egui::Button::new("×")).clicked() {
        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
    }
    if ui.add_sized([32.0, 26.0], egui::Button::new("□")).clicked() {
        toggle_maximized(ui.ctx());
    }
    if ui.add_sized([32.0, 26.0], egui::Button::new("—")).clicked() {
        ui.ctx()
            .send_viewport_cmd(egui::ViewportCommand::Minimized(true));
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn toggle_maximized(context: &egui::Context) {
    let maximized = context.input(|input| input.viewport().maximized.unwrap_or(false));
    context.send_viewport_cmd(egui::ViewportCommand::Maximized(!maximized));
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
                            .market_prices(&symbol, crate::market_prices::now_ms())
                            .reference_price()
                        {
                            ui.monospace(model.format_market_price(&symbol, last));
                            ui.colored_label(theme::BUY, model.preferences.market_server.label());
                        } else if let Some(projected) = market(model, &symbol) {
                            ui.monospace(model.format_market_price(&symbol, projected.last));
                            ui.colored_label(theme::TEXT_SECONDARY, "CONTROL");
                        } else {
                            ui.monospace("—");
                            ui.colored_label(
                                theme::TEXT_SECONDARY,
                                model.preferences.market_server.label(),
                            );
                        }
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        if let Some(last) = model
                            .market_prices(&symbol, crate::market_prices::now_ms())
                            .reference_price()
                        {
                            ui.monospace(model.format_market_price(&symbol, last));
                        } else {
                            ui.monospace("—");
                        }
                        ui.colored_label(
                            theme::TEXT_SECONDARY,
                            model.preferences.market_server.label(),
                        );
                    }
                    ui.end_row();
                }
            });
    });
}
fn show_chart(ui: &mut egui::Ui, pane: &mut Pane, model: &mut AppModel, client: &ControlClient) {
    let language = model.preferences.language;
    let settings_key = pane.settings_key();
    let settings_requested = show_chart_toolbar(ui, pane, language);
    if settings_requested {
        model.indicator_settings_requested = true;
        model.indicator_target = Some(settings_key.clone());
    }
    let mut settings = model
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
    crate::chart_trading::quick_order(ui, model, client, &symbol, &pane.trading_display);
    if pane.trading_display.alerts {
        crate::chart_trading::show_alerts(ui, model, &symbol);
    }
    if pane.trading_display.liquidation {
        ui.weak(if language == Language::SimplifiedChinese {
            "暂无强平价数据"
        } else {
            "Liquidation price unavailable"
        });
    }
    let overlays = crate::chart_trading::collect(model, &symbol, &pane.trading_display);
    #[cfg(not(target_arch = "wasm32"))]
    pane.analysis.select(
        MarketSelection::for_server(model.preferences.market_server, &symbol, pane.interval).ok(),
    );
    #[cfg(not(target_arch = "wasm32"))]
    if let Ok(selection) =
        MarketSelection::for_server(model.preferences.market_server, &symbol, pane.interval)
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
        ui.horizontal_wrapped(|ui| {
            ui.weak(format!("{} · {}", model.preferences.market_server.label(), local.selection.binding.symbol));
            let now = crate::market_prices::now_ms();
            if let Some(funding) = local.funding.as_ref() {
                let (label, tooltip) = presentation::funding_display(funding, now, language);
                ui.label(label).on_hover_text(tooltip);
            } else {
                ui.weak(if language == Language::SimplifiedChinese { "Funding — · 等待来源" } else { "Funding — · waiting for source" });
            }
            if let Some(interest) = local.open_interest_current.as_ref() {
                let (label, tooltip) = presentation::open_interest_display(interest, now, language);
                ui.label(label).on_hover_text(tooltip);
            } else {
                let reason = local.open_interest_error.as_deref().unwrap_or("loading");
                ui.weak(format!("OI — · {reason}"));
            }
            let locally_sampled = local.open_interest_history.last().is_some_and(|sample|
                sample.time_source == venue_domain::MarketTimeSource::LocalObservation);
            if locally_sampled {
                ui.weak("OI · 本地5m采样").on_hover_text(
                    "公开快照由本机开始记录；历史不可回补，时间为本机收到快照的时刻，变化幅度仅作估算");
            }
            let history_stale = presentation::open_interest_history_stale(
                local.open_interest_history.last(), now);
            if history_stale {
                ui.colored_label(theme::WARNING, "OI history stale").on_hover_text(
                    local.open_interest_history_error.as_deref().unwrap_or(
                        "Last verified 5m OI sample is over 15 minutes old"));
            }
            let changes = if history_stale { [None; 5] } else {
                venue_indicators::chart::open_interest::changes(&local.open_interest_history)
            };
            if let Some(state) = (!locally_sampled).then_some(changes[0]).flatten()
                .and_then(|change| model.local_markets.base_minute_facts(&local.selection.binding)
                .and_then(|minutes| venue_indicators::chart::open_interest::price_oi_state(change, minutes))) {
                use venue_indicators::chart::open_interest::PriceOiState;
                let label = match state {
                    PriceOiState::PriceUpOiUp => "Price↑ OI↑",
                    PriceOiState::PriceUpOiDown => "Price↑ OI↓",
                    PriceOiState::PriceDownOiUp => "Price↓ OI↑",
                    PriceOiState::PriceDownOiDown => "Price↓ OI↓",
                };
                ui.weak(format!("{label} · 5m"))
                    .on_hover_text("Price and OI compare the same completed 5m interval; descriptive only");
            }
            for (label, change) in ["5m", "15m", "1h", "4h", "24h"].into_iter().zip(changes) {
                let label = if locally_sampled { format!("{label}本地") } else { label.to_owned() };
                if let Some(change) = change {
                    ui.weak(format!("{label} {}%", format_decimal(change.change_percent, 2)))
                        .on_hover_text(format!("{} OI samples: {} → {} ms UTC",
                            if locally_sampled { "Locally observed, approximate" } else { "Exchange" },
                            change.baseline_time_ms, change.latest_time_ms));
                } else {
                    ui.weak(format!("{label} —")).on_hover_text(
                        if local.open_interest_history.is_empty() {
                            local.open_interest_history_error.as_deref().unwrap_or(
                                "No verified 5m OI history for this selected market; current OI is not backfilled")
                        } else if history_stale {
                            "Last verified 5m OI sample is over 15 minutes old"
                        } else {
                            "Insufficient completed OI samples for this comparison window"
                        });
                }
            }
            if let Some(error) = &local.study_error {
                ui.colored_label(theme::WARNING, "Indicator —")
                    .on_hover_text(error);
            }
        });
        if local.bars.is_empty() {
            if let Some(bars) = model.local_markets.chart_preview(&local.selection) {
                let rect = ui.available_rect_before_wrap();
                ui.add_enabled_ui(false, |ui| {
                    let _ = crate::chart_view::candle_plot(
                        ui,
                        bars,
                        &[],
                        &mut pane.viewport,
                        language,
                        &settings,
                        model.market_scales(&symbol),
                        model.market_price_tick(&symbol),
                        pane.interval,
                        None,
                        None,
                        &pane.trading_display,
                        &[],
                        (None, None),
                        None,
                        Some(&local.selection.binding),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                });
                crate::chart_view::loading::preview_badge(ui, rect, language);
                return;
            }
            crate::chart_view::loading::show(
                ui,
                language,
                &symbol,
                pane.interval.label(),
                matches!(
                    local.status,
                    crate::market::MarketStatus::Offline
                        | crate::market::MarketStatus::Resyncing
                        | crate::market::MarketStatus::Stale
                ),
            );
            if settings_requested {
                model.indicator_settings_requested = true;
                model.indicator_target = Some(settings_key);
            }
            return;
        }
        use crate::chart_view::analysis::{
            AnalysisAction, AnalysisMode, AvwapAnchor, FixedProfileRange,
        };
        let binding = local.selection.binding.clone();
        let anchors = model
            .preferences
            .analysis_anchors
            .iter()
            .filter(|anchor| anchor.pane_instance == pane.instance && anchor.binding == binding)
            .cloned()
            .collect::<Vec<_>>();
        let fixed = model
            .preferences
            .fixed_profile_ranges
            .iter()
            .find(|range| range.pane_instance == pane.instance && range.binding == binding)
            .cloned();
        settings.profile.fixed_start_ms = fixed.as_ref().map_or(0, |range| range.start_ms);
        settings.profile.fixed_end_ms = fixed.as_ref().map_or(0, |range| range.end_ms);
        ui.horizontal_wrapped(|ui| {
            if settings.microstructure.heatmap {
                let active = pane.heatmap_history_scope.as_ref() == Some(&local.selection);
                let title = if active {
                    if language == Language::SimplifiedChinese { "停止历史热图" } else { "Stop history heatmap" }
                } else if language == Language::SimplifiedChinese {
                    "加载可见热图历史"
                } else { "Load visible heatmap history" };
                if ui.small_button(title).on_hover_text(if language == Language::SimplifiedChinese {
                    "按当前交易所、交易对和可见时间范围逐页补齐同源1m K线；公开数据缓存在本机"
                } else {
                    "Page same-market 1m candles for this visible range; public history is cached locally"
                }).clicked() {
                    pane.heatmap_history_scope = if active { None } else { Some(local.selection.clone()) };
                }
            }
            if ui.add_enabled(anchors.len() < 8, egui::Button::new("＋ AVWAP")).clicked() {
                pane.analysis.mode = AnalysisMode::AddAnchor;
            }
            for anchor in &anchors {
                if ui.small_button(format!("A{} ↔", anchor.id)).on_hover_text("Click or drag to the target candle").clicked() {
                    pane.analysis.mode = AnalysisMode::MoveAnchor(anchor.id);
                }
                if ui.small_button(format!("A{} ×", anchor.id)).on_hover_text("Delete anchor").clicked() {
                    model.preferences.analysis_anchors.retain(|item| item.id != anchor.id);
                }
            }
            if !anchors.is_empty() && ui.small_button(if language == Language::SimplifiedChinese {
                "清空本图 AVWAP"
            } else { "Clear chart AVWAP" }).clicked() {
                model.preferences.analysis_anchors.retain(|item|
                    item.pane_instance != pane.instance || item.binding != binding);
                pane.analysis.mode = AnalysisMode::None;
            }
            if ui.small_button("Select Profile").clicked() { pane.analysis.mode = AnalysisMode::FixedStart; }
            if let Some(range) = &fixed {
                if ui.small_button("FR start").clicked() { pane.analysis.mode = AnalysisMode::MoveFixedStart(range.end_ms); }
                if ui.small_button("FR end").clicked() { pane.analysis.mode = AnalysisMode::MoveFixedEnd(range.start_ms); }
                if ui.small_button("FR ×").clicked() {
                    model.preferences.fixed_profile_ranges.retain(|item| item.pane_instance != pane.instance || item.binding != binding);
                }
            }
            if pane.analysis.mode != AnalysisMode::None {
                ui.colored_label(theme::WARNING, "Click candle · Esc cancels");
            }
        });
        let (price_scale, quantity_scale) = model.market_scales(&symbol);
        let prices = model.market_prices(&symbol, crate::market_prices::now_ms());
        let chart = presentation::sample_revision(
            ui,
            ("chart-display", pane.instance),
            (local.selection.clone(), local.generation, settings.clone()),
            Some((local.revision, prices)),
            model.preferences.trading.chart_cadence,
            || {
                (
                    local.bars.clone(),
                    local.studies.clone(),
                    prices.reference_price(),
                    prices.bid,
                    prices.ask,
                    prices.reference().map(|price| price.event_ms),
                    prices.reference().map(|price| price.received_ms),
                )
            },
        );
        crate::latency_evidence::prepare_market(
            &format!("{:?}", model.preferences.market_server),
            local.generation,
            &symbol,
            chart.5,
            chart.6,
            if pane.trading_display.last_price && pane.trading_display.price_lines {
                chart.2
            } else {
                None
            },
        );
        let selected_price = crate::chart_view::candle_plot(
            ui,
            &chart.0,
            &chart.1,
            &mut pane.viewport,
            language,
            &settings,
            (price_scale, quantity_scale),
            model.market_price_tick(&symbol),
            pane.interval,
            chart.2,
            highlighted_price,
            &pane.trading_display,
            &overlays,
            (chart.3, chart.4),
            model
                .market_depth(&symbol, crate::market_prices::now_ms())
                .map(|book| (book.bids.as_slice(), book.asks.as_slice())),
            Some(&local.selection.binding),
            model.local_markets.base_minutes(&local.selection.binding),
            model.local_markets.session_days(&local.selection.binding),
            model
                .local_markets
                .base_minute_facts(&local.selection.binding),
            model
                .local_markets
                .base_minute_forming_fact(&local.selection.binding),
            Some((&anchors, &mut pane.analysis)),
            Some(local.bar_revision),
            Some(&local.open_interest_history),
        );
        if let Some(action) = pane.analysis.action.take() {
            match action {
                AnalysisAction::AddAnchor { time_ms, price } => {
                    if anchors.len() < 8 {
                        let id = model
                            .preferences
                            .analysis_anchors
                            .iter()
                            .map(|item| item.id)
                            .max()
                            .unwrap_or(0)
                            .saturating_add(1);
                        let palette = [
                            [240, 185, 11],
                            [90, 200, 250],
                            [159, 122, 234],
                            [14, 203, 129],
                            [246, 70, 93],
                            [253, 138, 0],
                            [183, 138, 247],
                            [91, 159, 255],
                        ];
                        model.preferences.analysis_anchors.push(AvwapAnchor {
                            id,
                            pane_instance: pane.instance,
                            binding: binding.clone(),
                            open_time_ms: time_ms,
                            reference_price: price,
                            color: palette[anchors.len()],
                        });
                    }
                }
                AnalysisAction::MoveAnchor { id, time_ms, price } => {
                    if let Some(anchor) =
                        model
                            .preferences
                            .analysis_anchors
                            .iter_mut()
                            .find(|anchor| {
                                anchor.id == id
                                    && anchor.pane_instance == pane.instance
                                    && anchor.binding == binding
                            })
                    {
                        anchor.open_time_ms = time_ms;
                        anchor.reference_price = price;
                    }
                }
                AnalysisAction::SetFixedRange { start_ms, end_ms } => {
                    model.preferences.fixed_profile_ranges.retain(|item| {
                        item.pane_instance != pane.instance || item.binding != binding
                    });
                    model
                        .preferences
                        .fixed_profile_ranges
                        .push(FixedProfileRange {
                            pane_instance: pane.instance,
                            binding: binding.clone(),
                            start_ms,
                            end_ms,
                        });
                    let saved = model
                        .preferences
                        .chart_overrides
                        .entry(settings_key.clone())
                        .or_insert_with(|| settings.clone());
                    saved.profile.fixed_range = true;
                    saved.profile.fixed_start_ms = 0;
                    saved.profile.fixed_end_ms = 0;
                }
            }
            ui.ctx().request_repaint();
        }
        crate::latency_evidence::clear_market();
        let render_key = egui::Id::new(("chart-history-rendered", pane.instance));
        let rendered = (local.selection.clone(), local.generation);
        if ui
            .ctx()
            .data(|data| data.get_temp::<(crate::market::MarketSelection, u64)>(render_key))
            != Some(rendered.clone())
        {
            tracing::info!(target: "venueflow::chart_loading", generation = local.generation, symbol = %local.selection.binding.symbol,
                interval = local.selection.interval.label(), bars = chart.0.len(), "Chart initial history rendered");
            ui.ctx()
                .data_mut(|data| data.insert_temp(render_key, rendered));
        }
        let selection = local.selection.clone();
        {
            let visible = pane.viewport.visible_range(chart.0.len());
            if let Some(first) = chart.0.get(visible.start) {
                let mut minute_start = first.open_time_ms;
                if settings.microstructure.heatmap {
                    minute_start = minute_start.saturating_sub(
                        u64::from(settings.microstructure.lookback_hours) * 3_600_000,
                    );
                }
                if settings.session.sr_1h {
                    minute_start = minute_start.saturating_sub(72 * 3_600_000);
                } else if settings.session.sr_15m {
                    minute_start = minute_start.saturating_sub(24 * 3_600_000);
                }
                if settings.microstructure.show_cvd
                    && settings.microstructure.cvd_reset_mode
                        == venue_indicators::chart::CvdResetMode::UtcDaily
                {
                    minute_start = minute_start.min(first.open_time_ms / 86_400_000 * 86_400_000);
                }
                if settings.profile.fixed_range
                    && let Some(range) = &fixed
                {
                    minute_start = minute_start.min(range.start_ms);
                }
                if !anchors.is_empty() {
                    minute_start = minute_start.min(
                        anchors
                            .iter()
                            .map(|anchor| anchor.open_time_ms)
                            .min()
                            .unwrap_or(minute_start),
                    );
                }
                if pane.viewport.right_offset() == 0
                    && !(settings.microstructure.heatmap
                        && pane.heatmap_history_scope.as_ref() == Some(&selection))
                {
                    // The default view warms only a small 1m window. Older model coverage
                    // is requested when the user navigates history; absent coverage stays visible.
                    minute_start = minute_start
                        .max(crate::account_center::now_ms().saturating_sub(6 * 3_600_000));
                }
                let visible_end_ms =
                    chart
                        .0
                        .get(visible.end.saturating_sub(1))
                        .map_or(first.open_time_ms, |bar| {
                            bar.open_time_ms
                                .saturating_add(selection.interval.duration_ms().saturating_sub(1))
                        });
                if settings.needs_minute_source(!anchors.is_empty()) {
                    if let Some(request) = model.local_markets.begin_shared_history(
                        &binding,
                        crate::chart::ChartInterval::OneMinute,
                        minute_start,
                        visible_end_ms,
                    ) {
                        model.shared_history_requests.push(request);
                    }
                }
                if settings.needs_day_source() {
                    let week = venue_indicators::chart::session_levels::session_start(
                        first.open_time_ms,
                        venue_indicators::chart::session_levels::SessionPeriod::Weekly,
                    )
                    .unwrap_or(first.open_time_ms);
                    let day_start = week.saturating_sub(7 * 86_400_000);
                    if let Some(request) = model.local_markets.begin_shared_history(
                        &binding,
                        crate::chart::ChartInterval::OneDay,
                        day_start,
                        visible_end_ms,
                    ) {
                        model.shared_history_requests.push(request);
                    }
                }
            }
        }
        let near_start = pane.viewport.visible_bars() > chart.0.len()
            || (pane.viewport.right_offset() > 0
                && pane.viewport.right_offset() + pane.viewport.visible_bars() + 32
                    >= chart.0.len());
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
    #[cfg(not(target_arch = "wasm32"))]
    if model.preferences.market_server == crate::model::MarketServer::Binance {
        crate::chart_view::loading::show(
            ui,
            language,
            &symbol,
            pane.interval.label(),
            model.market_worker_failed,
        );
        return;
    }
    #[cfg(all(target_arch = "wasm32", feature = "preview"))]
    {
        if let Some(series) = model.browser_market.series(&symbol, Some(pane.interval)) {
            pane_heading(
                ui,
                &symbol,
                &format!("Binance · {} · read only", series.status),
            );
            let prices = model.market_prices(&symbol, crate::market_prices::now_ms());
            let chart = presentation::sample_revision(
                ui,
                ("preview-chart", pane.instance),
                (symbol.clone(), pane.interval, series.generation),
                Some((series.revision, prices)),
                model.preferences.trading.chart_cadence,
                || (series.bars.clone(), prices),
            );
            let _ = crate::chart_view::candle_plot(
                ui,
                &chart.0,
                &[],
                &mut pane.viewport,
                language,
                &settings,
                (series.price_scale, series.quantity_scale),
                model.market_price_tick(&symbol),
                pane.interval,
                chart.1.reference_price(),
                None,
                &pane.trading_display,
                &overlays,
                (chart.1.bid, chart.1.ask),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            );
        } else {
            empty(ui, &model.browser_market.status);
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
        || (market.bars.clone(), market.last, market.bid, market.ask),
    );
    let selected_price = crate::chart_view::candle_plot(
        ui,
        &chart.0,
        &[],
        &mut pane.viewport,
        language,
        &settings,
        (8, 8),
        model.market_price_tick(&symbol),
        pane.interval,
        Some(chart.1),
        highlighted_price,
        &pane.trading_display,
        &overlays,
        (Some(chart.2), Some(chart.3)),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    if let Some(price) = selected_price {
        model.select_trading_price(&symbol, price, ui.ctx());
    }
    if settings_requested {
        model.indicator_settings_requested = true;
        model.indicator_target = Some(settings_key);
    }
}
fn show_chart_toolbar(ui: &mut egui::Ui, pane: &mut Pane, language: Language) -> bool {
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
        crate::chart_trading::menu_button(ui, &mut pane.trading_display, language);
        ui.separator();
        if ui.small_button(text(language, TextKey::Fit)).clicked() {
            pane.viewport.reset();
        }
        if ui
            .small_button(if language == Language::SimplifiedChinese {
                "更早K线"
            } else {
                "Older candles"
            })
            .clicked()
        {
            pane.history_requested = true;
        }
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
        let scope = (local.selection.clone(), local.generation);
        let prices = model.market_prices(&symbol, crate::market_prices::now_ms());
        let depth = model.market_depth(&symbol, crate::market_prices::now_ms());
        let book = presentation::sample_revision(
            ui,
            ("book-display", pane.instance),
            scope.clone(),
            Some((
                prices,
                depth.map(|view| (view.selection.clone(), view.generation, view.revision)),
            )),
            model.preferences.trading.book_cadence,
            || {
                (
                    depth.map_or_else(Vec::new, |view| view.asks.clone()),
                    depth.map_or_else(Vec::new, |view| view.bids.clone()),
                    prices.reference_price(),
                    prices.bid,
                    prices.ask,
                )
            },
        );
        let trades = presentation::sample_revision(
            ui,
            ("tape-display", pane.instance),
            scope,
            Some(local.revision),
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
    #[cfg(all(target_arch = "wasm32", feature = "preview"))]
    {
        if let Some(series) = model.browser_market.series(&symbol, None) {
            let public_now = model
                .browser_market
                .public_now_ms(crate::account_center::now_ms());
            let prices = model.market_prices(&symbol, crate::market_prices::now_ms());
            let depth = model.browser_market.depth_series(&symbol, public_now);
            let tape = model.browser_market.tape_series(&symbol);
            let scope = (symbol.clone(), series.generation);
            let book = presentation::sample_revision(
                ui,
                ("preview-book", pane.instance),
                scope.clone(),
                Some((
                    prices,
                    depth.map(|view| (view.interval, view.generation, view.revision)),
                )),
                model.preferences.trading.book_cadence,
                || {
                    (
                        depth.map_or_else(Vec::new, |view| view.asks.clone()),
                        depth.map_or_else(Vec::new, |view| view.bids.clone()),
                        prices,
                    )
                },
            );
            let trades = presentation::sample_revision(
                ui,
                ("preview-tape", pane.instance),
                scope,
                Some(tape.map(|view| (view.interval, view.generation, view.revision))),
                model.preferences.trading.tape_cadence,
                || tape.map_or_else(Vec::new, |view| view.trades.clone()),
            );
            let _ = crate::order_book_view::show(
                ui,
                pane.instance,
                &book.0,
                &book.1,
                &trades,
                book.2.reference_price(),
                book.2.bid,
                book.2.ask,
                language,
                model,
                &symbol,
            );
        } else {
            empty(ui, text(language, TextKey::NoBook));
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
        let trades = presentation::sample_revision(
            ui,
            ("standalone-tape", pane.instance),
            (local.selection.clone(), local.generation),
            Some(local.revision),
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
    #[cfg(all(target_arch = "wasm32", feature = "preview"))]
    {
        if let Some(series) = model.browser_market.tape_series(symbol) {
            let trades = presentation::sample_revision(
                ui,
                ("standalone-preview-tape", pane.instance),
                (symbol.to_owned(), series.generation),
                Some((series.interval, series.revision)),
                model.preferences.trading.tape_cadence,
                || series.trades.clone(),
            );
            show_trade_rows(ui, pane.instance, &trades, language, model, symbol);
        } else {
            empty(ui, text(language, TextKey::NoTrades));
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
    ui.horizontal_wrapped(|ui| {
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
        ui.monospace(format!(
            "{}: {}",
            text(language, TextKey::Account),
            short_account(&strategy.trading_account_id)
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
    });
    if let Some(attention) = &strategy.attention {
        ui.small(RichText::new(attention).color(theme::WARNING));
    }
    ui.add_space(4.0);
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
    ui.horizontal_wrapped(|ui| {
        ui.small(text(language, TextKey::StopSemantics));
        ui.small(text(language, TextKey::ConfirmationSemantics));
    });
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
    #[cfg(all(target_arch = "wasm32", feature = "preview"))]
    return None;
    #[cfg(not(target_arch = "wasm32"))]
    if model.market_generation > 0 || model.market_worker_failed {
        return None;
    }
    if model.preferences.market_server != crate::model::MarketServer::Binance {
        return None;
    }
    model
        .snapshot
        .as_ref()?
        .markets
        .iter()
        .find(|market| market.symbol.to_string() == symbol)
}
pub(crate) fn available_symbols(model: &AppModel) -> Vec<String> {
    #[cfg(all(target_arch = "wasm32", feature = "preview"))]
    if let Some(snapshot) = &model.browser_market.snapshot {
        return snapshot.symbols.clone();
    }
    #[cfg(not(target_arch = "wasm32"))]
    if model.preferences.market_server != crate::model::MarketServer::Binance {
        return model.local_symbols.clone();
    }
    #[cfg(not(target_arch = "wasm32"))]
    if model.market_generation > 0 || model.market_worker_failed || !model.local_symbols.is_empty()
    {
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
