use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, RichText, Sense, Stroke};
use egui_tiles::{Behavior, TileId, Tiles, UiResponse};
use venue_control_protocol::{
    AggressorSide, CommandState, ConnectionState, ControlAction, ControlCommandRequest,
    HealthState, MarketSummary, StrategyLifecycle, StrategySummary, UiBar,
};

#[cfg(not(target_arch = "wasm32"))]
use crate::market::{LocalMarketView, MarketSelection, MarketStatus};
use crate::{
    chart::{PriceRange, bar_center_x, bar_index_at_x},
    client::ControlClient,
    model::{
        AppModel, PendingConfirmation, WorkspaceKind, decimal_to_f64, format_decimal,
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
            PaneKind::CopyRelations => show_copy_relations(ui, self.model),
            PaneKind::Ledger => show_ledger(ui, self.model),
            PaneKind::Control => show_control(ui, self.model, self.client),
            PaneKind::Diagnostics => show_diagnostics(ui, self.model),
        });
        UiResponse::None
    }

    fn tab_title_for_pane(&mut self, pane: &Pane) -> egui::WidgetText {
        pane.title().into()
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
) {
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .stroke(Stroke::new(1.0, theme::DIVIDER))
        .inner_margin(egui::Margin::symmetric(12, 8))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new("VENUEFLOW")
                        .strong()
                        .size(18.0)
                        .color(theme::BRAND_HOVER),
                );
                ui.separator();
                for workspace in WorkspaceKind::ALL {
                    if ui
                        .selectable_label(workspaces.active == workspace, workspace.label())
                        .clicked()
                    {
                        workspaces.active = workspace;
                    }
                }
                ui.separator();
                let mut symbols = model
                    .snapshot
                    .as_ref()
                    .map(|snapshot| {
                        snapshot
                            .markets
                            .iter()
                            .map(|market| market.symbol.to_string())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                symbols.extend(
                    ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT"]
                        .into_iter()
                        .map(str::to_owned),
                );
                symbols.push(model.preferences.selected_symbol.clone());
                symbols.sort();
                symbols.dedup();
                egui::ComboBox::from_id_salt("selected-symbol")
                    .selected_text(&model.preferences.selected_symbol)
                    .width(120.0)
                    .show_ui(ui, |ui| {
                        for symbol in symbols {
                            ui.selectable_value(
                                &mut model.preferences.selected_symbol,
                                symbol.clone(),
                                symbol,
                            );
                        }
                    });
                ui.separator();
                connection_badge(ui, model.connection);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Settings").clicked() {
                        *show_settings = true;
                    }
                    if ui.button("Modules").clicked() {
                        *show_modules = true;
                    }
                    if ui.button("Reset layout").clicked() {
                        workspaces.restore_active();
                        model.notice("Restored the active workspace layout");
                    }
                });
            });
        });
}

pub fn show_status_bar(ui: &mut egui::Ui, model: &AppModel) {
    let generated = model
        .snapshot
        .as_ref()
        .map_or("no snapshot".to_owned(), |snapshot| {
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
                    ui.colored_label(theme::SELL, format!("Control error: {error}"));
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small("EXEC=CONTROL · no credentials in UI · no direct mutation");
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
    egui::Window::new("Confirm high-risk control action")
        .collapsible(false)
        .resizable(false)
        .anchor(Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .show(context, |ui| {
            ui.colored_label(
                theme::WARNING,
                "This request is only an intent. The service and account node will revalidate it.",
            );
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
            ui.label("Type the exact confirmation:");
            ui.monospace(&expected);
            ui.text_edit_singleline(&mut pending.typed);
            ui.horizontal(|ui| {
                if ui.button("Cancel").clicked() {
                    keep_open = false;
                }
                let enabled = pending.typed == expected;
                if ui
                    .add_enabled(enabled, egui::Button::new("Submit intent"))
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

pub fn show_settings(
    context: &egui::Context,
    open: &mut bool,
    model: &mut AppModel,
    reconnect: &mut bool,
) {
    egui::Window::new("VenueFlow settings")
        .open(open)
        .resizable(false)
        .show(context, |ui| {
            ui.label("Control API base URL");
            ui.text_edit_singleline(&mut model.preferences.endpoint);
            ui.small("Web builds may leave this empty to use the current origin.");
            if ui.button("Reconnect").clicked() {
                *reconnect = true;
            }
            ui.separator();
            ui.label("Local Binance USD-M symbol (canonical BASE/USDT or BASE/USDC)");
            ui.text_edit_singleline(&mut model.preferences.selected_symbol);
            #[cfg(not(target_arch = "wasm32"))]
            ui.small("Native only · fixed Binance LIVE public endpoints · no API key.");
            #[cfg(target_arch = "wasm32")]
            ui.small("Web builds remain Control-only and do not connect to exchanges.");
            ui.separator();
            ui.add(
                egui::Slider::new(&mut model.preferences.ui_scale, 0.85..=1.35).text("UI scale"),
            );
            ui.checkbox(&mut model.preferences.show_status_bar, "Show status bar");
            ui.separator();
            ui.colored_label(
                theme::TEXT_SECONDARY,
                "The configurable URL above is only for Venue Control; local public market endpoints are fixed in native code.",
            );
        });
}

pub fn show_modules(context: &egui::Context, open: &mut bool, workspaces: &mut Workspaces) {
    let visibility = workspaces.pane_visibility();
    egui::Window::new("Workspace modules")
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

fn connection_badge(ui: &mut egui::Ui, state: ConnectionState) {
    let (label, color) = match state {
        ConnectionState::Connecting => ("CONNECTING", theme::WARNING),
        ConnectionState::Live => ("LIVE DATA", theme::BUY),
        ConnectionState::Degraded => ("DEGRADED", theme::WARNING),
        ConnectionState::Offline => ("OFFLINE", theme::SELL),
    };
    ui.colored_label(color, RichText::new(label).strong());
}

fn show_market_watch(ui: &mut egui::Ui, model: &mut AppModel) {
    pane_heading(
        ui,
        "Markets",
        "Control discovery · Binance local execution prices",
    );
    let rows = model
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.markets.clone())
        .unwrap_or_default();
    egui::ScrollArea::vertical().show(ui, |ui| {
        egui::Grid::new("market-watch-grid")
            .striped(true)
            .num_columns(3)
            .show(ui, |ui| {
                ui.strong("Symbol");
                ui.strong("Last");
                ui.strong("24h");
                ui.end_row();
                for market in rows {
                    let symbol = market.symbol.to_string();
                    if ui
                        .selectable_label(model.preferences.selected_symbol == symbol, &symbol)
                        .clicked()
                    {
                        model.preferences.selected_symbol = symbol;
                    }
                    ui.monospace(format_decimal(market.last, 4));
                    let change = decimal_to_f64(market.change_percent_24h);
                    ui.colored_label(theme::value_color(change), format!("{change:+.2}%"));
                    ui.end_row();
                }
            });
    });
}

fn show_chart(ui: &mut egui::Ui, pane: &mut Pane, model: &AppModel) {
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = local_market(model, symbol, pane.interval) {
        pane_heading(ui, symbol, "DATA=BINANCE LIVE · local public REST/WS");
        ui.horizontal_wrapped(|ui| {
            local_status_label(ui, local.status);
            if let Some(last) = local.last {
                ui.label(format!("Last {}", format_decimal(last, 4)));
            }
            if let Some(bid) = local.bid {
                ui.label(format!("Bid {}", format_decimal(bid, 4)));
            }
            if let Some(ask) = local.ask {
                ui.label(format!("Ask {}", format_decimal(ask, 4)));
            }
            ui.colored_label(
                theme::TEXT_SECONDARY,
                format!("latency {} ms", local.latency_ms.unwrap_or_default()),
            );
            if let Some(detail) = &local.status_detail {
                ui.colored_label(theme::WARNING, detail);
            }
        });
        show_chart_toolbar(ui, pane);
        candle_plot(ui, &local.bars, &mut pane.viewport);
        return;
    }
    pane_heading(ui, symbol, "Control projection fallback");
    let Some(market) = market(model, symbol) else {
        empty(ui, "No market projection is available for this symbol.");
        return;
    };
    ui.horizontal_wrapped(|ui| {
        ui.label(format!("Last {}", format_decimal(market.last, 4)));
        ui.label(format!("Bid {}", format_decimal(market.bid, 4)));
        ui.label(format!("Ask {}", format_decimal(market.ask, 4)));
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
    show_chart_toolbar(ui, pane);
    candle_plot(ui, &market.bars, &mut pane.viewport);
}

fn show_chart_toolbar(ui: &mut egui::Ui, pane: &mut Pane) {
    ui.horizontal(|ui| {
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
        if ui.small_button("Fit").clicked() {
            pane.viewport.reset();
        }
        if ui.small_button("Follow").clicked() {
            pane.viewport.follow_latest();
        }
        ui.colored_label(
            theme::TEXT_SECONDARY,
            format!(
                "{} bars{}",
                pane.viewport.visible_bars(),
                if pane.viewport.right_offset() == 0 {
                    " · LIVE"
                } else {
                    " · HISTORY"
                }
            ),
        );
    });
}

fn candle_plot(ui: &mut egui::Ui, all_bars: &[UiBar], viewport: &mut crate::chart::ChartViewport) {
    let height = ui.available_height().max(120.0);
    let (response, painter) = ui.allocate_painter(
        egui::vec2(ui.available_width(), height),
        Sense::click_and_drag(),
    );
    if all_bars.is_empty() {
        painter.text(
            response.rect.center(),
            Align2::CENTER_CENTER,
            "No candles",
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
        let drag_delta = ui.input(|input| input.pointer.delta().x);
        viewport.pan_by_drag(all_bars.len(), plot_rect.width(), drag_delta);
    }

    let range = viewport.visible_range(all_bars.len());
    let bars = &all_bars[range];
    let price_rect = Rect::from_min_max(
        plot_rect.min,
        Pos2::new(
            plot_rect.right(),
            plot_rect.top() + plot_rect.height() * 0.78,
        ),
    );
    let volume_rect = Rect::from_min_max(
        Pos2::new(plot_rect.left(), price_rect.bottom() + 5.0),
        plot_rect.max,
    );
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
            Stroke::new(1.0, Color32::from_rgba_unmultiplied(0x20, 0x35, 0x43, 120)),
        );
        if let Some(price) = price_range.y_to_price(price_rect.top(), price_rect.height(), y) {
            painter.text(
                Pos2::new(price_rect.right() - 3.0, y - 2.0),
                Align2::RIGHT_BOTTOM,
                format!("{price:.4}"),
                FontId::monospace(10.0),
                theme::TEXT_SECONDARY,
            );
        }
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
                    format!("{price:.4}"),
                    FontId::monospace(11.0),
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
                    format_decimal(bar.open, 4),
                    format_decimal(bar.high, 4),
                    format_decimal(bar.low, 4),
                    format_decimal(bar.close, 4),
                    format_decimal(bar.volume, 3),
                ),
                FontId::monospace(11.0),
                theme::TEXT_PRIMARY,
            );
        }
    }
}

fn show_order_book(ui: &mut egui::Ui, pane: &Pane, model: &AppModel) {
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    pane_heading(ui, "Order book", symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.view_for_symbol(symbol) {
        show_book_rows(ui, pane.instance, &local.asks, &local.bids);
        return;
    }
    let Some(market) = market(model, symbol) else {
        empty(ui, "No order-book projection.");
        return;
    };
    show_book_rows(ui, pane.instance, &market.asks, &market.bids);
}

fn show_book_rows(
    ui: &mut egui::Ui,
    instance: u32,
    asks: &[venue_control_protocol::UiBookLevel],
    bids: &[venue_control_protocol::UiBookLevel],
) {
    egui::Grid::new(format!("book-{instance}"))
        .striped(true)
        .num_columns(3)
        .show(ui, |ui| {
            ui.strong("Side");
            ui.strong("Price");
            ui.strong("Quantity");
            ui.end_row();
            for level in asks.iter().rev().take(10) {
                ui.colored_label(theme::SELL, "ASK");
                ui.monospace(format_decimal(level.price, 4));
                ui.monospace(format_decimal(level.quantity, 4));
                ui.end_row();
            }
            for level in bids.iter().take(10) {
                ui.colored_label(theme::BUY, "BID");
                ui.monospace(format_decimal(level.price, 4));
                ui.monospace(format_decimal(level.quantity, 4));
                ui.end_row();
            }
        });
}

fn show_trade_tape(ui: &mut egui::Ui, pane: &Pane, model: &AppModel) {
    let symbol = pane
        .symbol
        .as_deref()
        .unwrap_or(&model.preferences.selected_symbol);
    pane_heading(ui, "Trade tape", symbol);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(local) = model.local_markets.view_for_symbol(symbol) {
        show_trade_rows(ui, pane.instance, &local.trades);
        return;
    }
    let Some(market) = market(model, symbol) else {
        empty(ui, "No trade projection.");
        return;
    };
    show_trade_rows(ui, pane.instance, &market.trades);
}

fn show_trade_rows(ui: &mut egui::Ui, instance: u32, trades: &[venue_control_protocol::UiTrade]) {
    egui::ScrollArea::vertical().show(ui, |ui| {
        egui::Grid::new(format!("tape-{instance}"))
            .striped(true)
            .show(ui, |ui| {
                ui.strong("Time");
                ui.strong("Price");
                ui.strong("Qty");
                ui.end_row();
                for trade in trades.iter().rev().take(80) {
                    let color = match trade.aggressor {
                        AggressorSide::Buy => theme::BUY,
                        AggressorSide::Sell => theme::SELL,
                        AggressorSide::Unknown => theme::TEXT_SECONDARY,
                    };
                    ui.monospace(trade.occurred_ms.to_string());
                    ui.colored_label(color, format_decimal(trade.price, 4));
                    ui.monospace(format_decimal(trade.quantity, 4));
                    ui.end_row();
                }
            });
    });
}

fn show_accounts(ui: &mut egui::Ui, model: &AppModel) {
    pane_heading(ui, "Accounts", "secret-free query projections");
    let Some(snapshot) = &model.snapshot else {
        empty(ui, "Waiting for the Control API.");
        return;
    };
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("accounts-grid")
            .striped(true)
            .show(ui, |ui| {
                for heading in [
                    "Venue",
                    "Mode",
                    "Account",
                    "Health",
                    "Equity",
                    "Available",
                    "uPnL",
                    "Private gen",
                    "Writer gen",
                    "Reconciled age",
                ] {
                    ui.strong(heading);
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
            "WAL state · runtime Unknown fence · capability freshness: not projected by Control v2",
        );
        ui.small(
            "VenueFlow shows writer/private generations and reconciliation age only; it does not infer missing authority from health or ledger text.",
        );
    });
}

fn show_strategies(ui: &mut egui::Ui, model: &mut AppModel) {
    pane_heading(ui, "Strategies", "Grid · Scalping · Copy");
    let strategies = model
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.strategies.clone())
        .unwrap_or_default();
    if strategies.is_empty() {
        empty(ui, "No strategy instances were returned.");
        return;
    }
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("strategies-grid")
            .striped(true)
            .show(ui, |ui| {
                for heading in [
                    "Instance", "Kind", "Venue", "Mode", "Symbol", "State", "Orders", "Long",
                    "Short", "PnL", "Epoch",
                ] {
                    ui.strong(heading);
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
    pane_heading(ui, "Copy relations", "target exposure and durable drift");
    let Some(snapshot) = &model.snapshot else {
        empty(ui, "Waiting for copy projections.");
        return;
    };
    egui::Grid::new("copy-grid").striped(true).show(ui, |ui| {
        for heading in [
            "Leader", "Follower", "Symbol", "Target", "Actual", "Drift", "State",
        ] {
            ui.strong(heading);
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
    pane_heading(
        ui,
        "Receipt ledger",
        "read-only execution and control receipts",
    );
    let Some(snapshot) = &model.snapshot else {
        empty(ui, "Waiting for ledger projections.");
        return;
    };
    egui::ScrollArea::both().show(ui, |ui| {
        egui::Grid::new("ledger-grid").striped(true).show(ui, |ui| {
            for heading in [
                "Observed", "Instance", "Action", "State", "Receipt", "Detail",
            ] {
                ui.strong(heading);
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
    pane_heading(
        ui,
        "Lifecycle control",
        "semantic intents; server revalidation required",
    );
    let strategies = model
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.strategies.clone())
        .unwrap_or_default();
    if strategies.is_empty() {
        empty(ui, "No controllable strategy projection is available.");
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
                .unwrap_or("Select instance"),
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
    ui.label(format!("Venue: {}", strategy.venue));
    ui.label(format!("Mode: {}", strategy.mode));
    ui.label(format!("Account: {}", strategy.trading_account_id));
    ui.label(format!("Symbol: {}", strategy.symbol));
    ui.label(format!("Config epoch: {}", strategy.config_epoch));
    lifecycle_label(ui, strategy.lifecycle);
    if let Some(attention) = &strategy.attention {
        ui.colored_label(theme::WARNING, attention);
    }
    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        for (action, label) in [
            (ControlAction::Pause, "Pause"),
            (ControlAction::Resume, "Resume"),
            (ControlAction::Stop, "Stop"),
            (ControlAction::Flatten, "Flatten"),
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
    ui.small("Stop cancels only the selected instance's owned orders and preserves residual custody. Flatten additionally requests signed zero-position convergence.");
    ui.small("Pause, Stop, and Flatten require an exact typed scope confirmation. The account node remains authoritative and must independently revalidate every intent.");
    if !model.commands.is_empty() {
        ui.separator();
        ui.strong("This session's command receipts");
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
    pane_heading(ui, "Diagnostics", "control-plane client state");
    connection_badge(ui, model.connection);
    ui.label(format!(
        "Runtime projection: {}",
        model
            .control_connection
            .map_or("awaiting snapshot".to_owned(), |state| format!("{state:?}"))
    ));
    ui.label(format!(
        "Endpoint: {}",
        endpoint_label(&model.preferences.endpoint)
    ));
    ui.label(format!(
        "Snapshot polling: {}",
        if model.snapshot_online {
            "online"
        } else {
            "offline"
        }
    ));
    ui.label(format!(
        "SSE stream: {}",
        if model.event_stream_online {
            "online"
        } else {
            "offline"
        }
    ));
    ui.label(format!(
        "Last event ID: {}",
        model
            .last_event_id
            .map_or("none".to_owned(), |event_id| event_id.to_string())
    ));
    if let Some(snapshot) = &model.snapshot {
        ui.label(format!("Schema: {}", snapshot.schema_version));
        ui.label(format!("Generated: {}", snapshot.generated_ms));
        ui.label(format!("Accounts: {}", snapshot.accounts.len()));
        ui.label(format!("Strategies: {}", snapshot.strategies.len()));
        ui.label(format!("Markets: {}", snapshot.markets.len()));
        ui.label(format!("Ledger rows: {}", snapshot.ledger.len()));
    }
    ui.separator();
    ui.strong("Authority coverage in Control v2");
    ui.label("TEST/LIVE, account, strategy, private generation, writer generation: projected");
    ui.label("Command Accepted/Applied/Rejected/Unknown receipts: projected");
    ui.colored_label(theme::WARNING, "WAL state: not projected");
    ui.colored_label(theme::WARNING, "Runtime Unknown fence: not projected");
    ui.colored_label(
        theme::WARNING,
        "Capability evidence freshness: not projected",
    );
    ui.separator();
    #[cfg(not(target_arch = "wasm32"))]
    {
        ui.strong("Local public market data");
        ui.label("Venue: Binance USD-M · mode: LIVE · identity: none");
        ui.label(format!(
            "Subscriptions: {} · generation: {}",
            model.local_markets.selections().count(),
            model.local_markets.generation()
        ));
        ui.label("Fixed endpoints: fapi.binance.com + fstream.binance.com");
        ui.separator();
    }
    ui.strong("Recent notices");
    for notice in &model.notices {
        ui.small(notice);
    }
    ui.separator();
    ui.colored_label(theme::TEXT_SECONDARY, "The native UI may read Binance public market endpoints. It cannot read credentials, private streams, PostgreSQL, WAL, checkpoints, artifacts, or any exchange mutation path.");
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

#[cfg(not(target_arch = "wasm32"))]
fn local_market<'a>(
    model: &'a AppModel,
    symbol: &str,
    interval: crate::chart::ChartInterval,
) -> Option<&'a LocalMarketView> {
    let selection = MarketSelection::binance_usd_m(symbol, interval).ok()?;
    model.local_markets.view(&selection)
}

#[cfg(not(target_arch = "wasm32"))]
fn local_status_label(ui: &mut egui::Ui, status: MarketStatus) {
    let color = match status {
        MarketStatus::Live => theme::BUY,
        MarketStatus::LoadingHistory | MarketStatus::Connecting | MarketStatus::Resyncing => {
            theme::WARNING
        }
        MarketStatus::Stale | MarketStatus::Offline => theme::SELL,
    };
    ui.colored_label(color, RichText::new(format!("{status:?}")).strong());
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
