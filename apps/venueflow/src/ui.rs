use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, RichText, Sense, Stroke};
use egui_tiles::{Behavior, TileId, Tiles, UiResponse};
use venue_control_protocol::{
    AggressorSide, CommandState, ConnectionState, ControlAction, ControlCommandRequest,
    HealthState, MarketSummary, StrategyLifecycle, StrategySummary, UiBar,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::market::{LocalMarketView, MarketSelection};
use crate::{
    chart::{ChartStudyPoint, PriceRange, bar_center_x, bar_index_at_x},
    chart_settings::ChartDisplaySettings,
    client::ControlClient,
    i18n::{Language, TextKey, text},
    model::{
        AppModel, MarketQuote, PendingConfirmation, SymbolGroup, WorkspaceKind, decimal_to_f64,
        format_decimal, freshness_age_ms, requires_operator_confirmation,
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
            PaneKind::CopyRelations => show_copy_relations(ui, self.model),
            PaneKind::Ledger => show_ledger(ui, self.model),
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
    show_settings: &mut bool,
    show_symbol_picker: &mut bool,
) {
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .inner_margin(egui::Margin::symmetric(10, 4))
        .show(ui, |ui| {
            let language = model.preferences.language;
            let connection = model.connection;
            let mut reset_requested = false;
            let (picker_response, ()) = egui::containers::Sides::new()
                .shrink_left()
                .height(46.0)
                .show(
                    ui,
                    |ui| {
                        ui.horizontal(|ui| {
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
                            let selected_quote = local_quote(model, &selected_symbol);
                            let selected_details = selected_quote.map_or_else(
                                || "—  —".to_owned(),
                                |quote| {
                                    format!(
                                        "{} {:+.2}%",
                                        model.format_market_price(&selected_symbol, quote.last),
                                        quote.change_percent_24h
                                    )
                                },
                            );
                            let picker_response = ui.add_sized(
                                [150.0, 42.0],
                                egui::Button::new(symbol_tab_text(
                                    &format!("{}  ▼", selected_symbol),
                                    &selected_details,
                                    selected_quote.map_or(theme::TEXT_SECONDARY, |quote| {
                                        theme::value_color(decimal_to_f64(quote.change_percent_24h))
                                    }),
                                )),
                            );
                            ui.painter().line_segment(
                                [
                                    picker_response.rect.left_top(),
                                    picker_response.rect.right_top(),
                                ],
                                Stroke::new(2.0, theme::BRAND),
                            );
                            if picker_response.clicked() {
                                *show_symbol_picker = !*show_symbol_picker;
                            }
                            ui.separator();
                            egui::ScrollArea::horizontal()
                                .id_salt("favorite-symbol-tabs")
                                .auto_shrink([false, true])
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        let favorites = model.preferences.favorite_symbols.clone();
                                        for symbol in favorites
                                            .into_iter()
                                            .filter(|symbol| symbol != &selected_symbol)
                                        {
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
                                            if ui
                                                .add_sized(
                                                    [150.0, 42.0],
                                                    egui::Button::selectable(
                                                        selected,
                                                        symbol_tab_text(
                                                            &symbol,
                                                            &details,
                                                            detail_color,
                                                        ),
                                                    ),
                                                )
                                                .clicked()
                                            {
                                                model.preferences.selected_symbol = symbol;
                                                workspaces.follow_dynamic_charts_latest();
                                            }
                                        }
                                        if ui.small_button("+").clicked() {
                                            *show_symbol_picker = true;
                                        }
                                    });
                                });
                            picker_response
                        })
                        .inner
                    },
                    |ui| {
                        if ui.button(text(language, TextKey::Settings)).clicked() {
                            *show_settings = true;
                        }
                        if ui.button(text(language, TextKey::Modules)).clicked() {
                            *show_modules = true;
                        }
                        if ui.button(text(language, TextKey::ResetLayout)).clicked() {
                            reset_requested = true;
                        }
                        connection_badge(ui, connection, language);
                    },
                );
            if reset_requested {
                workspaces.restore_active();
                model.notice("Restored the active workspace layout");
            }
            show_symbol_picker_popup(&picker_response, show_symbol_picker, model, workspaces);
        });
}

fn show_symbol_picker_popup(
    anchor: &egui::Response,
    open: &mut bool,
    model: &mut AppModel,
    workspaces: &mut Workspaces,
) {
    if !*open {
        return;
    }
    if anchor
        .ctx
        .input(|input| input.key_pressed(egui::Key::Escape))
    {
        *open = false;
        return;
    }
    let language = model.preferences.language;
    let mut symbols = available_symbols(model);
    symbols.extend(model.preferences.favorite_symbols.iter().cloned());
    symbols.sort_by(|left, right| {
        favorite_rank(&model.preferences.favorite_symbols, left)
            .cmp(&favorite_rank(&model.preferences.favorite_symbols, right))
            .then_with(|| left.cmp(right))
    });
    symbols.dedup();
    let normalized_filter = model
        .symbol_filter
        .trim()
        .to_ascii_uppercase()
        .replace(['/', '-', '_'], "");
    let filtered = symbols
        .into_iter()
        .filter(|symbol| {
            let group_match = match model.symbol_group {
                SymbolGroup::Favorites => model.preferences.favorite_symbols.contains(symbol),
                SymbolGroup::Usdc => symbol.ends_with("/USDC"),
                SymbolGroup::Usdt => symbol.ends_with("/USDT"),
                SymbolGroup::All => true,
            };
            let normalized_symbol = symbol.replace('/', "");
            group_match
                && (normalized_filter.is_empty() || normalized_symbol.contains(&normalized_filter))
        })
        .collect::<Vec<_>>();
    let mut selected = anchor
        .ctx
        .input(|input| input.key_pressed(egui::Key::Enter))
        .then(|| filtered.first().cloned())
        .flatten();
    egui::Popup::from_response(anchor)
        .open_bool(open)
        .close_behavior(egui::PopupCloseBehavior::CloseOnClickOutside)
        .align(egui::RectAlign::BOTTOM_START)
        .gap(4.0)
        .width(590.0)
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(Stroke::new(1.0, theme::DIVIDER)),
        )
        .show(|ui| {
            ui.set_min_width(560.0);
            let filter_before_input = model.symbol_filter.clone();
            let search = ui.add(
                egui::TextEdit::singleline(&mut model.symbol_filter)
                    .desired_width(f32::INFINITY)
                    .hint_text("BTCUSDC / BTC/USDC"),
            );
            if model.symbol_filter == filter_before_input {
                let fallback_text = ui.input(|input| {
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
                if !fallback_text.is_empty() {
                    model.symbol_filter.push_str(&fallback_text);
                } else if ui.input(|input| input.key_pressed(egui::Key::Backspace)) {
                    model.symbol_filter.pop();
                }
            }
            if model.symbol_filter.is_empty() {
                search.request_focus();
            }
            if search.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Tab)) {
                search.request_focus();
            }
            ui.horizontal(|ui| {
                for (group, label) in [
                    (SymbolGroup::Favorites, "★ 收藏"),
                    (SymbolGroup::All, "全部"),
                    (SymbolGroup::Usdc, "USDC"),
                    (SymbolGroup::Usdt, "USDT"),
                ] {
                    ui.selectable_value(&mut model.symbol_group, group, label);
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(format!("{} markets", filtered.len()));
                });
            });
            ui.separator();
            egui::ScrollArea::vertical()
                .max_height(520.0)
                .show(ui, |ui| {
                    egui::Grid::new("symbol-picker-grid")
                        .striped(true)
                        .num_columns(5)
                        .min_col_width(110.0)
                        .show(ui, |ui| {
                            ui.strong("★");
                            ui.strong(text(language, TextKey::Symbol));
                            ui.strong(text(language, TextKey::Last));
                            ui.strong("24h %");
                            ui.strong("24h Quote Volume");
                            ui.end_row();
                            for symbol in &filtered {
                                let favorite = model.preferences.favorite_symbols.contains(symbol);
                                if ui.small_button(if favorite { "★" } else { "☆" }).clicked() {
                                    if favorite {
                                        model
                                            .preferences
                                            .favorite_symbols
                                            .retain(|item| item != symbol);
                                    } else {
                                        model.preferences.favorite_symbols.push(symbol.clone());
                                    }
                                }
                                if ui
                                    .selectable_label(
                                        model.preferences.selected_symbol == *symbol,
                                        symbol,
                                    )
                                    .clicked()
                                {
                                    selected = Some(symbol.clone());
                                }
                                if let Some(quote) = local_quote(model, symbol) {
                                    ui.monospace(model.format_market_price(symbol, quote.last));
                                    ui.colored_label(
                                        if quote.change_percent_24h >= rust_decimal::Decimal::ZERO {
                                            theme::BUY
                                        } else {
                                            theme::SELL
                                        },
                                        format!("{:+.2}%", quote.change_percent_24h),
                                    );
                                    ui.monospace(format_decimal(quote.quote_volume_24h, 0));
                                } else {
                                    ui.monospace("—");
                                    ui.monospace("—");
                                    ui.monospace("—");
                                }
                                ui.end_row();
                            }
                        });
                });
        });
    if let Some(symbol) = selected {
        model.preferences.selected_symbol = symbol;
        model.symbol_filter.clear();
        workspaces.follow_dynamic_charts_latest();
        *open = false;
    }
}

pub fn show_status_bar(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    let generated = model
        .snapshot
        .as_ref()
        .map_or(text(language, TextKey::NoSnapshot).to_owned(), |snapshot| {
            format!("snapshot {}", snapshot.generated_ms)
        });
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .inner_margin(egui::Margin::symmetric(10, 5))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.small(format!(
                    "Control API: {}",
                    endpoint_label(&model.preferences.endpoint)
                ));
                ui.separator();
                ui.small(generated);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(market) = model
                    .local_markets
                    .view_for_symbol(&model.preferences.selected_symbol)
                {
                    ui.separator();
                    ui.small(format!(
                        "DATA=BINANCE LIVE · {:?} · {} ms",
                        market.status,
                        market.latency_ms.unwrap_or_default()
                    ));
                }
                if let Some(receipt) = model.last_terminal_receipt() {
                    ui.separator();
                    ui.small(format!(
                        "final receipt {} · {:?}",
                        receipt.receipt_id, receipt.state
                    ));
                }
                if let Some(error) = &model.last_error {
                    ui.separator();
                    ui.colored_label(
                        theme::SELL,
                        format!("{}: {error}", text(language, TextKey::ControlError)),
                    );
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(text(language, TextKey::ControlBoundary));
                });
            });
        });
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
                        model.preferences.selected_symbol = symbol.clone();
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
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = local_market(model, symbol, pane.interval) {
        let (price_scale, quantity_scale) = model.market_scales(symbol);
        let settings_requested = show_chart_toolbar(ui, pane, language);
        candle_plot(
            ui,
            &local.bars,
            &local.studies,
            &mut pane.viewport,
            language,
            &model.preferences.chart,
            (price_scale, quantity_scale),
        );
        if settings_requested {
            model.indicator_settings_requested = true;
        }
        return;
    }
    pane_heading(ui, symbol, text(language, TextKey::ControlFallback));
    let Some(market) = market(model, symbol) else {
        empty(ui, text(language, TextKey::NoMarket));
        return;
    };
    ui.horizontal_wrapped(|ui| {
        ui.label(format!(
            "Last {}",
            model.format_market_price(symbol, market.last)
        ));
        ui.label(format!(
            "Bid {}",
            model.format_market_price(symbol, market.bid)
        ));
        ui.label(format!(
            "Ask {}",
            model.format_market_price(symbol, market.ask)
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
    let settings_requested = show_chart_toolbar(ui, pane, language);
    candle_plot(
        ui,
        &market.bars,
        &[],
        &mut pane.viewport,
        language,
        &model.preferences.chart,
        (8, 8),
    );
    if settings_requested {
        model.indicator_settings_requested = true;
    }
}

fn show_chart_toolbar(ui: &mut egui::Ui, pane: &mut Pane, language: Language) -> bool {
    let mut settings_requested = false;
    ui.horizontal(|ui| {
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

fn candle_plot(
    ui: &mut egui::Ui,
    all_bars: &[UiBar],
    all_studies: &[ChartStudyPoint],
    viewport: &mut crate::chart::ChartViewport,
    language: Language,
    settings: &ChartDisplaySettings,
    scales: (usize, usize),
) {
    let (price_scale, quantity_scale) = scales;
    let height = ui.available_height().max(120.0);
    let (response, painter) = ui.allocate_painter(
        egui::vec2(ui.available_width(), height),
        Sense::click_and_drag(),
    );
    if all_bars.is_empty() {
        painter.text(
            response.rect.center(),
            Align2::CENTER_CENTER,
            text(language, TextKey::NoCandles),
            FontId::proportional(14.0),
            theme::TEXT_SECONDARY,
        );
        return;
    }

    let plot_rect = response.rect.shrink2(egui::vec2(8.0, 8.0));
    let pointer_ratio = response.hover_pos().map_or(1.0, |point| {
        ((point.x - plot_rect.left()) / plot_rect.width()).clamp(0.0, 1.0)
    });
    if response.hovered() {
        let wheel = ui.input(|input| input.smooth_scroll_delta.y);
        if wheel.abs() > f32::EPSILON {
            viewport.zoom_by_steps(
                all_bars.len(),
                pointer_ratio,
                if wheel > 0.0 { 1 } else { -1 },
            );
        }
    }
    if response.dragged() {
        viewport.pan_by_drag_total(all_bars.len(), plot_rect.width(), response.drag_delta().x);
    }
    if response.drag_stopped() {
        viewport.pan_by_drag_total(all_bars.len(), plot_rect.width(), response.drag_delta().x);
        viewport.finish_drag();
    }

    let range = viewport.visible_range(all_bars.len());
    let bars = &all_bars[range];
    let has_local_studies = !all_studies.is_empty();
    let sub_count = if has_local_studies {
        [
            settings.rsi.enabled,
            settings.macd.enabled,
            settings.atr.enabled,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
    } else {
        0
    };
    let volume_ratio = if settings.show_volume { 0.12 } else { 0.0 };
    let sub_ratio = 0.145 * sub_count as f32;
    let price_ratio = (1.0 - volume_ratio - sub_ratio).clamp(0.42, 0.86);
    let price_rect = Rect::from_min_max(
        plot_rect.min,
        Pos2::new(
            plot_rect.right(),
            plot_rect.top() + plot_rect.height() * price_ratio,
        ),
    );
    let mut cursor = price_rect.bottom() + 4.0;
    let volume_rect = settings.show_volume.then(|| {
        let rect = Rect::from_min_max(
            Pos2::new(plot_rect.left(), cursor),
            Pos2::new(
                plot_rect.right(),
                cursor + plot_rect.height() * volume_ratio - 4.0,
            ),
        );
        cursor = rect.bottom() + 4.0;
        rect
    });
    let sub_height = if sub_count == 0 {
        0.0
    } else {
        (plot_rect.bottom() - cursor - 4.0 * (sub_count.saturating_sub(1)) as f32)
            / sub_count as f32
    };
    let mut next_sub_rect = || {
        let rect = Rect::from_min_max(
            Pos2::new(plot_rect.left(), cursor),
            Pos2::new(plot_rect.right(), cursor + sub_height),
        );
        cursor = rect.bottom() + 4.0;
        rect
    };
    let rsi_rect = (has_local_studies && settings.rsi.enabled).then(&mut next_sub_rect);
    let macd_rect = (has_local_studies && settings.macd.enabled).then(&mut next_sub_rect);
    let atr_rect = (has_local_studies && settings.atr.enabled).then(&mut next_sub_rect);
    let Some(price_range) = PriceRange::from_bars(bars) else {
        return;
    };
    let width = price_rect.width() / bars.len() as f32;
    let price_y = |price: f64| {
        price_range
            .price_to_y(price_rect.top(), price_rect.height(), price)
            .unwrap_or(price_rect.center().y)
    };
    for index in 0..=4 {
        let y = price_rect.top() + price_rect.height() * index as f32 / 4.0;
        painter.line_segment(
            [
                Pos2::new(price_rect.left(), y),
                Pos2::new(price_rect.right(), y),
            ],
            Stroke::new(1.0, theme::CHART_GRID),
        );
        if let Some(price) = price_range.y_to_price(price_rect.top(), price_rect.height(), y) {
            painter.text(
                Pos2::new(price_rect.right() - 3.0, y - 2.0),
                Align2::RIGHT_BOTTOM,
                format_f64_trimmed(price, price_scale),
                FontId::monospace(f32::from(settings.chart_text_size)),
                theme::TEXT_SECONDARY,
            );
        }
    }
    let vertical_grid_count = (price_rect.width() / 140.0).round().clamp(4.0, 12.0) as usize;
    for index in 0..=vertical_grid_count {
        let x = price_rect.left() + price_rect.width() * index as f32 / vertical_grid_count as f32;
        painter.line_segment(
            [
                Pos2::new(x, price_rect.top()),
                Pos2::new(x, plot_rect.bottom()),
            ],
            Stroke::new(1.0, theme::CHART_GRID),
        );
    }
    let maximum_volume = bars
        .iter()
        .map(|bar| decimal_to_f64(bar.volume))
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    for (index, bar) in bars.iter().enumerate() {
        let open = decimal_to_f64(bar.open);
        let close = decimal_to_f64(bar.close);
        let x = bar_center_x(price_rect.left(), price_rect.width(), bars.len(), index)
            .unwrap_or(price_rect.left());
        let color = if close >= open {
            theme::BUY
        } else {
            theme::SELL
        };
        painter.line_segment(
            [
                Pos2::new(x, price_y(decimal_to_f64(bar.low))),
                Pos2::new(x, price_y(decimal_to_f64(bar.high))),
            ],
            Stroke::new(1.0, color),
        );
        let top = price_y(open.max(close));
        let bottom = price_y(open.min(close));
        let body = Rect::from_min_max(
            Pos2::new(x - width * 0.31, top),
            Pos2::new(x + width * 0.31, bottom.max(top + 1.0)),
        );
        painter.rect_filled(body, 0.5, color);
        if let Some(volume_rect) = volume_rect {
            let volume_height =
                (decimal_to_f64(bar.volume) / maximum_volume) as f32 * volume_rect.height();
            painter.rect_filled(
                Rect::from_min_max(
                    Pos2::new(x - width * 0.31, volume_rect.bottom() - volume_height),
                    Pos2::new(x + width * 0.31, volume_rect.bottom()),
                ),
                0.5,
                Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 150),
            );
        }
    }
    if has_local_studies {
        draw_price_studies(&painter, price_rect, bars, all_studies, price_y, settings);
        if let Some(rect) = rsi_rect {
            draw_rsi_study(&painter, rect, bars, all_studies, settings);
        }
        if let Some(rect) = macd_rect {
            draw_macd_study(&painter, rect, bars, all_studies, settings);
        }
        if let Some(rect) = atr_rect {
            draw_atr_study(&painter, rect, bars, all_studies, settings);
        }
    }
    if let Some(last) = bars.last() {
        let y = price_y(decimal_to_f64(last.close));
        painter.line_segment(
            [
                Pos2::new(price_rect.left(), y),
                Pos2::new(price_rect.right(), y),
            ],
            Stroke::new(1.0, theme::BRAND_HOVER),
        );
    }
    if let Some(pointer) = response
        .hover_pos()
        .filter(|point| plot_rect.contains(*point))
    {
        painter.line_segment(
            [
                Pos2::new(pointer.x, plot_rect.top()),
                Pos2::new(pointer.x, plot_rect.bottom()),
            ],
            Stroke::new(1.0, theme::TEXT_SECONDARY),
        );
        if price_rect.contains(pointer) {
            painter.line_segment(
                [
                    Pos2::new(price_rect.left(), pointer.y),
                    Pos2::new(price_rect.right(), pointer.y),
                ],
                Stroke::new(1.0, theme::TEXT_SECONDARY),
            );
            if let Some(price) =
                price_range.y_to_price(price_rect.top(), price_rect.height(), pointer.y)
            {
                painter.text(
                    Pos2::new(price_rect.right() - 4.0, pointer.y - 4.0),
                    Align2::RIGHT_BOTTOM,
                    format_f64_trimmed(price, price_scale),
                    FontId::monospace(f32::from(settings.chart_text_size)),
                    theme::TEXT_PRIMARY,
                );
            }
        }
        if let Some(index) =
            bar_index_at_x(price_rect.left(), price_rect.width(), bars.len(), pointer.x)
        {
            let bar = &bars[index];
            painter.text(
                price_rect.left_top() + egui::vec2(6.0, 6.0),
                Align2::LEFT_TOP,
                format!(
                    "{}  O {}  H {}  L {}  C {}  V {}",
                    bar.open_time_ms,
                    format_decimal(bar.open, price_scale),
                    format_decimal(bar.high, price_scale),
                    format_decimal(bar.low, price_scale),
                    format_decimal(bar.close, price_scale),
                    format_decimal(bar.volume, quantity_scale),
                ),
                FontId::monospace(f32::from(settings.chart_text_size)),
                theme::TEXT_PRIMARY,
            );
        }
    }
}

fn draw_price_studies(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    price_y: impl Fn(f64) -> f32 + Copy,
    settings: &ChartDisplaySettings,
) {
    if settings.sma.enabled {
        draw_study_line(
            painter,
            rect,
            bars,
            studies,
            |p| p.sma,
            price_y,
            Stroke::new(settings.sma.line_width(), settings.sma.color()),
        );
    }
    if settings.ema.enabled {
        draw_study_line(
            painter,
            rect,
            bars,
            studies,
            |p| p.ema,
            price_y,
            Stroke::new(settings.ema.line_width(), settings.ema.color()),
        );
    }
    if settings.bollinger.enabled {
        for (selector, color) in [
            (
                (|p: &ChartStudyPoint| p.bollinger_upper) as fn(&ChartStudyPoint) -> _,
                settings.bollinger.color(),
            ),
            (
                |p: &ChartStudyPoint| p.bollinger_middle,
                settings.bollinger.secondary_color(),
            ),
            (
                |p: &ChartStudyPoint| p.bollinger_lower,
                settings.bollinger.color(),
            ),
        ] {
            draw_study_line(
                painter,
                rect,
                bars,
                studies,
                selector,
                price_y,
                Stroke::new(settings.bollinger.line_width(), color),
            );
        }
    }
    if settings.vwap.enabled {
        draw_study_line(
            painter,
            rect,
            bars,
            studies,
            |p| p.vwap,
            price_y,
            Stroke::new(settings.vwap.line_width(), settings.vwap.color()),
        );
    }
    painter.text(
        rect.left_top() + egui::vec2(6.0, 22.0),
        Align2::LEFT_TOP,
        format!(
            "SMA{}  EMA{}  BOLL{},{}  VWAP",
            settings.sma_period,
            settings.ema_period,
            settings.bollinger_period,
            settings.bollinger_multiplier_hundredths as f32 / 100.0
        ),
        FontId::monospace(f32::from(settings.chart_text_size)),
        theme::TEXT_SECONDARY,
    );
}

fn draw_study_line(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    selector: fn(&ChartStudyPoint) -> Option<rust_decimal::Decimal>,
    value_y: impl Fn(f64) -> f32,
    stroke: Stroke,
) {
    let mut previous = None;
    for (index, bar) in bars.iter().enumerate() {
        let current = study_at(studies, bar.open_time_ms)
            .and_then(selector)
            .and_then(|value| {
                bar_center_x(rect.left(), rect.width(), bars.len(), index)
                    .map(|x| Pos2::new(x, value_y(decimal_to_f64(value))))
            });
        if let (Some(left), Some(right)) = (previous, current) {
            painter.line_segment([left, right], stroke);
        }
        previous = current;
    }
}

fn draw_rsi_study(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    settings: &ChartDisplaySettings,
) {
    let y = |value: f64| rect.bottom() - (value.clamp(0.0, 100.0) as f32 / 100.0) * rect.height();
    for level in [30.0, 50.0, 70.0] {
        let color = if level == 50.0 {
            theme::TEXT_SECONDARY
        } else {
            Color32::from_rgb(91, 69, 122)
        };
        painter.line_segment(
            [
                Pos2::new(rect.left(), y(level)),
                Pos2::new(rect.right(), y(level)),
            ],
            Stroke::new(0.75, color),
        );
    }
    draw_study_line(
        painter,
        rect,
        bars,
        studies,
        |point| point.rsi,
        y,
        Stroke::new(settings.rsi.line_width(), settings.rsi.color()),
    );
    painter.text(
        rect.left_top() + egui::vec2(4.0, 2.0),
        Align2::LEFT_TOP,
        format!("RSI {}", settings.rsi_period),
        FontId::monospace(f32::from(settings.chart_text_size)),
        theme::TEXT_SECONDARY,
    );
}

fn draw_macd_study(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    settings: &ChartDisplaySettings,
) {
    let maximum = bars
        .iter()
        .filter_map(|bar| study_at(studies, bar.open_time_ms))
        .flat_map(|point| [point.macd, point.macd_signal, point.macd_histogram])
        .flatten()
        .map(decimal_to_f64)
        .map(f64::abs)
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let y = |value: f64| rect.center().y - (value / maximum) as f32 * rect.height() * 0.44;
    painter.line_segment(
        [
            Pos2::new(rect.left(), rect.center().y),
            Pos2::new(rect.right(), rect.center().y),
        ],
        Stroke::new(0.75, theme::TEXT_SECONDARY),
    );
    let width = rect.width() / bars.len().max(1) as f32;
    for (index, bar) in bars.iter().enumerate() {
        let Some(value) =
            study_at(studies, bar.open_time_ms).and_then(|point| point.macd_histogram)
        else {
            continue;
        };
        let value = decimal_to_f64(value);
        let Some(x) = bar_center_x(rect.left(), rect.width(), bars.len(), index) else {
            continue;
        };
        painter.rect_filled(
            Rect::from_two_pos(
                Pos2::new(x - width * 0.28, y(0.0)),
                Pos2::new(x + width * 0.28, y(value)),
            ),
            0.0,
            if value >= 0.0 {
                theme::BUY
            } else {
                theme::SELL
            },
        );
    }
    draw_study_line(
        painter,
        rect,
        bars,
        studies,
        |point| point.macd,
        y,
        Stroke::new(settings.macd.line_width(), settings.macd.color()),
    );
    draw_study_line(
        painter,
        rect,
        bars,
        studies,
        |point| point.macd_signal,
        y,
        Stroke::new(settings.macd.line_width(), settings.macd.secondary_color()),
    );
    painter.text(
        rect.left_top() + egui::vec2(4.0, 2.0),
        Align2::LEFT_TOP,
        format!(
            "MACD {},{},{}",
            settings.macd_fast_period, settings.macd_slow_period, settings.macd_signal_period
        ),
        FontId::monospace(f32::from(settings.chart_text_size)),
        theme::TEXT_SECONDARY,
    );
}

fn draw_atr_study(
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    studies: &[ChartStudyPoint],
    settings: &ChartDisplaySettings,
) {
    let maximum = bars
        .iter()
        .filter_map(|bar| study_at(studies, bar.open_time_ms).and_then(|point| point.atr))
        .map(decimal_to_f64)
        .fold(0.0_f64, f64::max)
        .max(f64::EPSILON);
    let y = |value: f64| rect.bottom() - (value / maximum) as f32 * rect.height() * 0.82;
    draw_study_line(
        painter,
        rect,
        bars,
        studies,
        |point| point.atr,
        y,
        Stroke::new(settings.atr.line_width(), settings.atr.color()),
    );
    painter.text(
        rect.left_top() + egui::vec2(4.0, 2.0),
        Align2::LEFT_TOP,
        format!("ATR {}", settings.atr_period),
        FontId::monospace(f32::from(settings.chart_text_size)),
        theme::TEXT_SECONDARY,
    );
}

fn study_at(studies: &[ChartStudyPoint], open_time_ms: u64) -> Option<&ChartStudyPoint> {
    studies
        .binary_search_by_key(&open_time_ms, |point| point.open_time_ms)
        .ok()
        .and_then(|index| studies.get(index))
}

fn show_order_book(ui: &mut egui::Ui, pane: &Pane, model: &AppModel) {
    let language = model.preferences.language;
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    pane_heading(ui, text(language, TextKey::OrderBook), symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.view_for_symbol(symbol) {
        if local.asks.is_empty() || local.bids.is_empty() {
            empty(ui, text(language, TextKey::NoBook));
        } else {
            show_book_rows(
                ui,
                pane.instance,
                &local.asks,
                &local.bids,
                language,
                model,
                symbol,
            );
        }
        return;
    }
    let Some(market) = market(model, symbol) else {
        empty(ui, text(language, TextKey::NoBook));
        return;
    };
    show_book_rows(
        ui,
        pane.instance,
        &market.asks,
        &market.bids,
        language,
        model,
        symbol,
    );
}

fn show_book_rows(
    ui: &mut egui::Ui,
    instance: u32,
    asks: &[venue_control_protocol::UiBookLevel],
    bids: &[venue_control_protocol::UiBookLevel],
    language: Language,
    model: &AppModel,
    symbol: &str,
) {
    egui::Grid::new(format!("book-{instance}"))
        .striped(true)
        .num_columns(3)
        .show(ui, |ui| {
            ui.strong(text(language, TextKey::Side));
            ui.strong(text(language, TextKey::Price));
            ui.strong(text(language, TextKey::Quantity));
            ui.end_row();
            for level in asks.iter().rev().take(10) {
                ui.colored_label(theme::SELL, text(language, TextKey::Ask));
                ui.monospace(model.format_market_price(symbol, level.price));
                ui.monospace(model.format_market_quantity(symbol, level.quantity));
                ui.end_row();
            }
            for level in bids.iter().take(10) {
                ui.colored_label(theme::BUY, text(language, TextKey::Bid));
                ui.monospace(model.format_market_price(symbol, level.price));
                ui.monospace(model.format_market_quantity(symbol, level.quantity));
                ui.end_row();
            }
        });
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
        if local.trades.is_empty() {
            empty(ui, text(language, TextKey::NoTrades));
        } else {
            show_trade_rows(ui, pane.instance, &local.trades, language, model, symbol);
        }
        return;
    }
    let Some(market) = market(model, symbol) else {
        empty(ui, text(language, TextKey::NoTrades));
        return;
    };
    show_trade_rows(ui, pane.instance, &market.trades, language, model, symbol);
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
                    ui.monospace(format_decimal(account.equity, 2));
                    ui.monospace(format_decimal(account.available_margin, 2));
                    let pnl = decimal_to_f64(account.unrealized_pnl);
                    ui.colored_label(theme::value_color(pnl), format!("{pnl:+.2}"));
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
                        model.preferences.selected_symbol = strategy.symbol.to_string();
                    }
                    ui.label(format!("{:?}", strategy.kind));
                    ui.label(strategy.venue.to_string());
                    ui.label(strategy.mode.to_string());
                    ui.label(strategy.symbol.to_string());
                    lifecycle_label(ui, strategy.lifecycle);
                    ui.monospace(strategy.open_orders.to_string());
                    ui.monospace(format_decimal(strategy.long_quantity, 4));
                    ui.monospace(format_decimal(strategy.short_quantity, 4));
                    let pnl = decimal_to_f64(strategy.realized_pnl + strategy.unrealized_pnl);
                    ui.colored_label(theme::value_color(pnl), format!("{pnl:+.2}"));
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

fn show_copy_relations(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    pane_heading(
        ui,
        text(language, TextKey::CopyRelations),
        text(language, TextKey::CopySubtitle),
    );
    let Some(snapshot) = &model.snapshot else {
        empty(ui, text(language, TextKey::NoCopy));
        return;
    };
    egui::Grid::new("copy-grid").striped(true).show(ui, |ui| {
        for heading in [
            TextKey::Leader,
            TextKey::Follower,
            TextKey::Symbol,
            TextKey::Target,
            TextKey::Actual,
            TextKey::Drift,
            TextKey::State,
        ] {
            ui.strong(text(language, heading));
        }
        ui.end_row();
        for relation in &snapshot.copy_relations {
            ui.label(&relation.leader_id);
            ui.label(&relation.follower_instance_id);
            ui.label(relation.symbol.to_string());
            ui.monospace(format_decimal(relation.target_exposure, 4));
            ui.monospace(format_decimal(relation.actual_exposure, 4));
            let drift = decimal_to_f64(relation.drift);
            ui.colored_label(theme::value_color(-drift.abs()), format!("{drift:+.4}"));
            ui.label(format!("{:?}", relation.status));
            ui.end_row();
        }
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
                ui.label(&entry.detail);
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

fn available_symbols(model: &AppModel) -> Vec<String> {
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

fn favorite_rank(favorites: &[String], symbol: &str) -> usize {
    favorites
        .iter()
        .position(|favorite| favorite == symbol)
        .unwrap_or(favorites.len())
}

fn local_quote<'a>(model: &'a AppModel, symbol: &str) -> Option<&'a MarketQuote> {
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

fn format_f64_trimmed(value: f64, precision: usize) -> String {
    let formatted = format!("{value:.precision$}");
    if formatted.contains('.') {
        formatted
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    } else {
        formatted
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn local_market<'a>(
    model: &'a AppModel,
    symbol: &str,
    interval: crate::chart::ChartInterval,
) -> Option<&'a LocalMarketView> {
    let selection = MarketSelection::binance_usd_m(symbol, interval).ok()?;
    model.local_markets.view(&selection)
}

fn pane_heading(ui: &mut egui::Ui, title: &str, subtitle: &str) {
    ui.horizontal(|ui| {
        ui.strong(title);
        ui.colored_label(theme::TEXT_SECONDARY, subtitle);
    });
    ui.separator();
}

fn empty(ui: &mut egui::Ui, message: &str) {
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
