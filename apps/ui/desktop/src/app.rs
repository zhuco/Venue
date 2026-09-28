use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

use crate::{
    client::{ClientEvent, ControlClient},
    model::{AppModel, Preferences},
    settings_panel::{self, SettingsPanelState},
    theme, ui,
    workspace::Workspaces,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    market::MarketSelection,
    market_client::{LocalMarketClient, LocalMarketClientEvent},
};
use eframe::egui;
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
mod market_events;
mod persistence;

const STORAGE_KEY: &str = "venueflow-state-v1";
const PERSISTED_SCHEMA_VERSION: u16 = 9;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
struct FrameTelemetry {
    started: Option<Instant>,
    cpu_ms: Vec<f32>,
    reported_full_window: bool,
    last_memory_sample: Option<Instant>,
    source_peak_bytes: usize,
    chart_cache_peak_bytes: usize,
    async_reserved_peak_bytes: usize,
    async_reserved_last_bytes: usize,
    estimated_indicator_peak_bytes: usize,
    estimated_indicator_last_bytes: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl FrameTelemetry {
    fn memory_sample_due(&self) -> bool {
        !self.reported_full_window
            && self
                .last_memory_sample
                .is_none_or(|last| last.elapsed() >= Duration::from_secs(5))
    }

    fn record_memory(
        &mut self,
        source_bytes: usize,
        chart_cache_bytes: usize,
        async_reserved_bytes: usize,
    ) {
        self.last_memory_sample = Some(Instant::now());
        self.source_peak_bytes = self.source_peak_bytes.max(source_bytes);
        self.chart_cache_peak_bytes = self.chart_cache_peak_bytes.max(chart_cache_bytes);
        self.async_reserved_peak_bytes = self.async_reserved_peak_bytes.max(async_reserved_bytes);
        self.async_reserved_last_bytes = async_reserved_bytes;
        self.estimated_indicator_last_bytes = source_bytes
            .saturating_add(chart_cache_bytes)
            .saturating_add(async_reserved_bytes);
        self.estimated_indicator_peak_bytes = self
            .estimated_indicator_peak_bytes
            .max(self.estimated_indicator_last_bytes);
    }

    fn record(&mut self, previous_frame_cpu_s: Option<f32>) {
        if self.reported_full_window {
            return;
        }
        let Some(seconds) = previous_frame_cpu_s.filter(|value| value.is_finite() && *value >= 0.0)
        else {
            return;
        };
        let started = self.started.get_or_insert_with(Instant::now);
        self.cpu_ms.push(seconds * 1_000.0);
        if started.elapsed() >= Duration::from_secs(15 * 60) {
            self.report("full");
            self.reported_full_window = true;
        }
    }

    fn report(&self, window: &str) {
        if self.cpu_ms.is_empty() {
            return;
        }
        let mut sorted = self.cpu_ms.clone();
        sorted.sort_by(f32::total_cmp);
        let elapsed_s = self
            .started
            .map_or(0.0, |started| started.elapsed().as_secs_f64());
        tracing::info!(target: "venueflow::frame_performance", window,
            elapsed_s, samples = sorted.len(),
            p50_ms = percentile(&sorted, 50), p95_ms = percentile(&sorted, 95),
            p99_ms = percentile(&sorted, 99), max_ms = sorted.last().copied().unwrap_or(0.0),
            "Visible frame CPU time including UI and rendering, excluding vsync wait");
        tracing::info!(target: "venueflow::indicator_performance", window,
            source_peak_bytes = self.source_peak_bytes,
            chart_cache_peak_bytes = self.chart_cache_peak_bytes,
            async_reserved_peak_bytes = self.async_reserved_peak_bytes,
            async_reserved_last_bytes = self.async_reserved_last_bytes,
            estimated_indicator_peak_bytes = self.estimated_indicator_peak_bytes,
            estimated_indicator_last_bytes = self.estimated_indicator_last_bytes,
            "Five-second samples of indicator sources, chart caches and reserved heatmap worker inputs; other UI and renderer allocations excluded");
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn percentile(sorted: &[f32], percent: usize) -> f32 {
    let rank = sorted.len().saturating_mul(percent).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
struct PersistedState {
    schema_version: u16,
    preferences: Preferences,
    workspaces: Workspaces,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            schema_version: PERSISTED_SCHEMA_VERSION,
            preferences: Preferences::default(),
            workspaces: Workspaces::default(),
        }
    }
}

pub struct VenueFlowApp {
    model: AppModel,
    workspaces: Workspaces,
    client: ControlClient,
    account_center: crate::account_center::AccountCenter,
    connected_endpoint: String,
    #[cfg(not(target_arch = "wasm32"))]
    market_client: Option<LocalMarketClient>,
    #[cfg(not(target_arch = "wasm32"))]
    market_server: crate::model::MarketServer,
    #[cfg(not(target_arch = "wasm32"))]
    market_generation: u64,
    show_modules: bool,
    show_settings: bool,
    show_trading_settings: bool,
    show_execution_account: bool,
    settings_state: SettingsPanelState,
    show_symbol_picker: bool,
    reconnect: bool,
    #[cfg(not(target_arch = "wasm32"))]
    frame_telemetry: FrameTelemetry,
}

impl VenueFlowApp {
    pub fn new(creation_context: &eframe::CreationContext<'_>, default_endpoint: String) -> Self {
        theme::apply(&creation_context.egui_ctx);
        let (mut persisted, recovered) = persistence::load(creation_context.storage);
        #[cfg(all(target_arch = "wasm32", feature = "preview"))]
        {
            persisted.preferences.market_server = crate::model::MarketServer::Binance;
            persisted.preferences.endpoint.clear();
        }
        persisted.workspaces.upgrade_trading_tables();
        if persisted.preferences.endpoint.trim().is_empty() {
            persisted.preferences.endpoint = default_endpoint;
        }
        let mut model = AppModel::new(persisted.preferences);
        if recovered {
            let message = match model.preferences.language {
                crate::i18n::Language::SimplifiedChinese => "布局配置无法读取，已恢复可用布局。",
                crate::i18n::Language::English => {
                    "Layout could not be read; a usable layout was restored."
                }
            };
            model.last_error = Some(message.into());
            model.notice(message);
        }
        let client = ControlClient::connect(
            model.preferences.endpoint.clone(),
            creation_context.egui_ctx.clone(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        let (model, market_client) = match LocalMarketClient::start_with_context(
            model.preferences.market_server,
            Some(creation_context.egui_ctx.clone()),
        ) {
            Ok(client) => (model, Some(client)),
            Err(error) => {
                let mut model = model;
                model.market_worker_failed = true;
                model.local_catalog_error = Some(format!("Market worker unavailable: {error}"));
                (model, None)
            }
        };
        Self {
            connected_endpoint: model.preferences.endpoint.clone(),
            account_center: crate::account_center::AccountCenter::new(&model.preferences.endpoint),
            #[cfg(not(target_arch = "wasm32"))]
            market_server: model.preferences.market_server,
            #[cfg(not(target_arch = "wasm32"))]
            market_generation: model.market_generation,
            model,
            workspaces: persisted.workspaces,
            client,
            #[cfg(not(target_arch = "wasm32"))]
            market_client,
            show_modules: false,
            show_settings: false,
            show_trading_settings: false,
            show_execution_account: false,
            settings_state: SettingsPanelState::default(),
            show_symbol_picker: false,
            reconnect: false,
            #[cfg(not(target_arch = "wasm32"))]
            frame_telemetry: FrameTelemetry::default(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn synchronize_local_markets(&mut self, context: &egui::Context) {
        let fallback_symbol = self.model.preferences.selected_symbol.clone();
        let active_charts: Vec<_> = {
            let tree = self.workspaces.active_tree_mut();
            tree.tiles
                .iter()
                .filter_map(|(id, tile)| match tile {
                    egui_tiles::Tile::Pane(pane)
                        if pane.kind == crate::workspace::PaneKind::Chart
                            && tree.tiles.is_visible(*id) =>
                    {
                        Some((
                            pane.symbol
                                .clone()
                                .unwrap_or_else(|| fallback_symbol.clone()),
                            pane.interval,
                            pane.settings_key(),
                            pane.instance,
                        ))
                    }
                    _ => None,
                })
                .collect()
        };
        let chart_keys = active_charts
            .iter()
            .map(|(_, _, key, _)| key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        self.model.local_markets.retain_chart_keys(&chart_keys);
        if self.market_server != self.model.preferences.market_server {
            self.market_client.take();
            self.market_server = self.model.preferences.market_server;
            self.market_generation = self.model.market_generation;
            self.workspaces.reset_chart_viewports();
            self.market_client = match LocalMarketClient::start_with_context(
                self.market_server,
                Some(context.clone()),
            ) {
                Ok(client) => Some(client),
                Err(error) => {
                    self.model.market_worker_failed = true;
                    self.model.local_catalog_error =
                        Some(format!("Market worker unavailable: {error}"));
                    None
                }
            };
        }
        if self.model.market_worker_failed || self.market_client.is_none() {
            self.market_client.take();
            let _ = self.model.local_markets.replace([]);
            return;
        }
        let mut source_demands = std::collections::BTreeMap::new();
        let selections =
            active_charts
                .into_iter()
                .filter_map(|(symbol, interval, key, pane_instance)| {
                    match MarketSelection::for_server(self.market_server, &symbol, interval) {
                        Ok(selection) => {
                            let settings = self
                                .model
                                .preferences
                                .chart_overrides
                                .get(&key)
                                .unwrap_or(&self.model.preferences.chart);
                            let has_anchor =
                                self.model
                                    .preferences
                                    .analysis_anchors
                                    .iter()
                                    .any(|anchor| {
                                        anchor.pane_instance == pane_instance
                                            && anchor.binding == selection.binding
                                    });
                            let demand = crate::market_client::SharedSourceDemand::for_chart(
                                settings, has_anchor,
                            );
                            source_demands
                                .entry(selection.binding.clone())
                                .or_insert_with(crate::market_client::SharedSourceDemand::default)
                                .merge(demand);
                            Some(selection)
                        }
                        Err(error) => {
                            self.model.notice(format!(
                                "Local Binance selection rejected for {symbol}: {error}"
                            ));
                            None
                        }
                    }
                })
                .collect::<Vec<_>>();
        let generation = match self.model.local_markets.replace(selections) {
            Ok(generation) => generation,
            Err(error) => {
                self.model
                    .notice(format!("Local Binance subscription rejected: {error}"));
                return;
            }
        };
        let demanded = source_demands
            .iter()
            .flat_map(|(binding, demand)| {
                [
                    crate::chart::ChartInterval::OneMinute,
                    crate::chart::ChartInterval::OneDay,
                ]
                .into_iter()
                .filter(move |interval| demand.allows(*interval))
                .map(move |interval| (binding.clone(), interval))
            })
            .collect::<std::collections::BTreeSet<_>>();
        self.model
            .local_markets
            .retain_shared_history_demands(&demanded);
        if let Some(client) = self.market_client.as_ref() {
            client.update_source_demands(source_demands);
        }
        let (Some(generation), Some(client)) = (generation, self.market_client.as_ref()) else {
            return;
        };
        let mut selections: Vec<_> = self.model.local_markets.selections().cloned().collect();
        selections.sort_by_key(|selection| {
            selection.binding.symbol.to_string() != self.model.preferences.selected_symbol
        });
        if let Err(error) = client.replace_subscriptions(generation, selections) {
            self.model
                .notice(format!("Local Binance subscription unavailable: {error}"));
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn drain_local_markets(&mut self, context: &egui::Context) {
        let Some(client) = self.market_client.as_ref() else {
            return;
        };
        let started = std::time::Instant::now();
        for _ in 0..512 {
            if started.elapsed() >= Duration::from_millis(3) {
                break;
            }
            let Some(event) = client.next_event() else {
                break;
            };
            market_events::apply(
                &mut self.model,
                &mut self.workspaces,
                self.market_server,
                self.market_generation,
                event,
                context,
            );
        }
        if client.has_events() {
            context.request_repaint();
        }

        for request in self.model.history_requests.drain(..) {
            if let Err(error) = client.load_older(request.clone()) {
                let _ = self
                    .model
                    .local_markets
                    .finish_history(&request, Err(error.to_string()));
            }
        }
        for request in self.model.shared_history_requests.drain(..) {
            if !client.source_requested(&request) {
                self.model.local_markets.cancel_shared_history(&request);
                continue;
            }
            if let Err(error) = client.load_shared(request.clone()) {
                let _ = self
                    .model
                    .local_markets
                    .finish_shared_history(&request, Err(error.to_string()));
            }
        }
        // Expired synchronization must mark every old public view stale.
        let market_now = venue_gateway_api::display::received_ms().unwrap_or(u64::MAX);
        self.model
            .local_markets
            .refresh_staleness(market_now, 5_000);
    }

    fn drain_client(&mut self, context: &egui::Context) {
        #[cfg(not(target_arch = "wasm32"))]
        let started = std::time::Instant::now();
        for _ in 0..256 {
            #[cfg(not(target_arch = "wasm32"))]
            if started.elapsed() >= Duration::from_millis(2) {
                break;
            }
            let Some(event) = self.client.drain().next() else {
                break;
            };
            let event = match event {
                ClientEvent::AccountScoped { scope, event } => {
                    if self.model.apply_account_event(&scope, *event) {
                        ClientEvent::SessionExpired
                    } else {
                        continue;
                    }
                }
                event => event,
            };
            match event {
                ClientEvent::AccountScoped { .. }
                | ClientEvent::AccountClock(_)
                | ClientEvent::TerminalAccountProjection { .. }
                | ClientEvent::TerminalAccountSharedProjection { .. }
                | ClientEvent::TerminalAccountUnavailable { .. }
                | ClientEvent::TerminalExecutions(_)
                | ClientEvent::TerminalExecutionUpdated(_)
                | ClientEvent::TerminalExecutionsUnavailable(_)
                | ClientEvent::TerminalSubmissionUnavailable { .. } => continue,
                ClientEvent::SnapshotConnected => self.model.snapshot_connected(),
                ClientEvent::GridInstances(instances) => {
                    self.model.execution.grid.apply_instances(instances)
                }
                ClientEvent::LeaderBotAccess(access) => {
                    self.model.execution.leader_bot.access = Some(access);
                    self.model.execution.leader_bot.fresh = true;
                    if self.model.execution.leader_bot.pending.is_none() {
                        self.model.execution.leader_bot.error = None;
                    }
                }
                ClientEvent::LeaderBotMutationApplied(access) => {
                    self.model.execution.leader_bot.access = Some(access);
                    self.model.execution.leader_bot.fresh = true;
                    self.model.execution.leader_bot.pending = None;
                    self.model.execution.leader_bot.error = None;
                }
                ClientEvent::LeaderBotUnavailable {
                    mutation,
                    definitive,
                    message,
                } => {
                    self.model.execution.leader_bot.error = Some(message);
                    self.model.execution.leader_bot.fresh = false;
                    if mutation && definitive {
                        self.model.execution.leader_bot.pending = None;
                    }
                }
                ClientEvent::SupportMartingaleInstances(instances) => self
                    .model
                    .execution
                    .support_martingale
                    .apply_instances(instances),
                ClientEvent::InventoryMm(event) => self.model.execution.inventory_mm.apply(event),
                ClientEvent::SupportMartingaleMutationApplied(summary) => self
                    .model
                    .execution
                    .support_martingale
                    .apply_summary(*summary),
                ClientEvent::SupportMartingalePreflightApplied(result) => self
                    .model
                    .execution
                    .support_martingale
                    .apply_preflight(*result),
                ClientEvent::SupportMartingaleUnavailable(message) => self
                    .model
                    .execution
                    .support_martingale
                    .unavailable(message, false),
                ClientEvent::SupportMartingaleMutationUnavailable(message) => self
                    .model
                    .execution
                    .support_martingale
                    .unavailable(message, true),
                ClientEvent::GridMutationApplied(summary) => {
                    self.model.execution.grid.apply_summary(*summary)
                }
                ClientEvent::GridUnavailable(message) => {
                    self.model.execution.grid.list_unavailable(message)
                }
                ClientEvent::GridMutationUnavailable(message) => {
                    self.model.execution.grid.mutation_unavailable(message)
                }
                ClientEvent::SessionExpired => {
                    // An unauthenticated bootstrap response must not invalidate
                    // a vaulted session that the account endpoint is validating.
                    if self.account_center.session.is_some() {
                        self.account_center.session_expired(&mut self.model);
                        self.reconnect = true;
                    }
                    break;
                }
                ClientEvent::SnapshotUnavailable(message) => {
                    self.model.snapshot_unavailable(message);
                }
                ClientEvent::StreamConnected { resumed_after } => {
                    self.model.stream_connected(resumed_after);
                }
                ClientEvent::StreamUnavailable(message) => {
                    self.model.stream_unavailable(message);
                }
                ClientEvent::CommandUnavailable(message) => {
                    self.model.last_error = Some(message.clone());
                    self.model.notice(message);
                }
                ClientEvent::CopyRelationUnavailable(message) => {
                    self.model.last_error = Some(message.clone());
                    self.model.notice(message);
                }
                ClientEvent::EventCursor(event_id) => self.model.observe_event_id(event_id),
                ClientEvent::Snapshot(snapshot) => self.model.apply_snapshot(snapshot),
                ClientEvent::Receipt(receipt) => {
                    if self.model.apply_receipt(receipt.clone()) {
                        self.model.notice(format!(
                            "Control receipt {} is {:?}: {}",
                            receipt.receipt_id, receipt.state, receipt.detail
                        ));
                    }
                }
                ClientEvent::CopyRelationConfigs(configs) => {
                    self.model.apply_copy_relation_configs(configs);
                }
                ClientEvent::CopyRelationReceipt(receipt) => {
                    self.model.notice(format!(
                        "Copy relation {} revision {} is {:?}",
                        receipt.relation_id, receipt.revision, receipt.state
                    ));
                }
            }
        }
        if self.client.has_events() {
            context.request_repaint();
        }
    }

    fn reconnect_if_requested(&mut self, context: &egui::Context) {
        if !self.reconnect {
            return;
        }
        self.reconnect = false;
        if self.connected_endpoint != self.model.preferences.endpoint {
            self.model.clear_account_session();
            self.account_center =
                crate::account_center::AccountCenter::new(&self.model.preferences.endpoint);
            self.connected_endpoint = self.model.preferences.endpoint.clone();
        }
        self.model.reconnecting();
        self.client = ControlClient::connect_authenticated(
            self.model.preferences.endpoint.clone(),
            context.clone(),
            self.account_center
                .session
                .as_ref()
                .map(|s| s.token.clone()),
        );
        self.model.notice("Reconnecting to the Control API");
    }

    fn synchronize_private_projection(&self) {
        self.client
            .select_execution_scope(self.model.selected_execution_credential().and_then(
                |credential| {
                    credential
                        .trading_account_id
                        .clone()
                        .map(
                            |trading_account_id| venue_control_protocol::UiAccountScope {
                                venue: credential.venue,
                                mode: venue_control_protocol::GatewayMode::Live,
                                trading_account_id,
                            },
                        )
                },
            ));
        let Some(scope) = self.model.confirmed_account_scope() else {
            self.client.clear_terminal_subscription();
            return;
        };
        let credential_id = scope.credential_id.clone();
        let mut symbols = std::iter::once(&self.model.preferences.selected_symbol)
            .chain(self.model.preferences.favorite_symbols.iter())
            .filter_map(|symbol| symbol.parse().ok())
            .fold(Vec::new(), |mut values, symbol| {
                if !values.contains(&symbol)
                    && values.len() < venue_control_protocol::kol::MAX_ALLOWED_SYMBOLS
                {
                    values.push(symbol);
                }
                values
            });
        // The account subscription is a set. Chart focus must not reconnect its SSE.
        symbols.sort_unstable();
        if symbols.is_empty() {
            return;
        }
        self.client
            .subscribe_terminal(crate::account_scope::Scoped {
                scope,
                value: venue_control_protocol::kol::TerminalProjectionRequest {
                    schema_version: venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
                    credential_id,
                    symbols,
                },
            });
    }
}

impl eframe::App for VenueFlowApp {
    fn logic(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        if self.connected_endpoint != self.model.preferences.endpoint {
            self.reconnect = true;
            self.reconnect_if_requested(context);
        }
        let zoom = self.model.preferences.ui_scale.clamp(0.85, 1.35);
        if (context.zoom_factor() - zoom).abs() > 0.001 {
            context.set_zoom_factor(zoom);
        }
        self.drain_client(context);
        #[cfg(all(target_arch = "wasm32", feature = "preview"))]
        self.model.browser_market.poll(
            self.workspaces
                .active_chart_requests(&self.model.preferences.selected_symbol),
            context,
        );
        if self.account_center.poll(&mut self.model, context) {
            self.reconnect = true;
        }
        self.reconnect_if_requested(context);
        self.synchronize_private_projection();
        if std::mem::take(&mut self.model.follow_latest_requested) {
            self.workspaces.follow_dynamic_charts_latest();
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.synchronize_local_markets(context);
            self.drain_local_markets(context);
        }
        let display = &self.model.preferences.trading;
        context.request_repaint_after(Duration::from_millis(
            display
                .book_cadence
                .millis()
                .min(display.tape_cadence.millis())
                .min(display.chart_cadence.millis())
                .min(250),
        ));
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        #[cfg(not(target_arch = "wasm32"))]
        self.frame_telemetry.record(_frame.info().cpu_usage);
        #[cfg(not(target_arch = "wasm32"))]
        crate::latency_evidence::begin_pass(ui.ctx(), self.model.confirmed_account_scope());
        crate::chart_trading::poll(&mut self.model);
        crate::chart_trading::notification(ui.ctx(), &mut self.model);
        ui.painter()
            .rect_filled(ui.max_rect(), 0.0, theme::BG_PRIMARY);
        ui.spacing_mut().item_spacing = egui::Vec2::ZERO;
        self.model.synchronize_trading_scope();
        self.model.refresh_trading_price(ui.ctx());
        #[cfg(all(target_arch = "wasm32", feature = "preview"))]
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 12.0;
            ui.colored_label(theme::BRAND, "浏览器终端预览 · 只读");
            ui.label("Binance 实时公开行情 · 账户及交易未接通");
            ui.weak(&self.model.browser_market.status);
        });
        ui::show_top_bar(
            ui,
            &mut self.model,
            &mut self.workspaces,
            &mut self.show_modules,
            &mut self.show_trading_settings,
            &mut self.show_execution_account,
            &mut self.show_symbol_picker,
        );
        self.synchronize_private_projection();
        #[cfg(not(target_arch = "wasm32"))]
        self.synchronize_local_markets(ui.ctx());
        self.model
            .execution
            .begin_frame(ui.ctx().cumulative_frame_nr());
        let accepts_trading_input = self.workspaces.active == crate::model::WorkspaceKind::Trading
            && !ui.ctx().egui_wants_keyboard_input()
            && !egui::Popup::is_any_open(ui.ctx())
            && !self.show_modules
            && !self.show_settings
            && !self.show_trading_settings
            && !self.show_execution_account
            && !self.show_symbol_picker
            && self.model.pending_confirmation.is_none();
        if accepts_trading_input {
            let actions = ui.input(|input| {
                input
                    .events
                    .iter()
                    .filter_map(|event| {
                        crate::trading::hotkey_action(event, &self.model.preferences.trading)
                    })
                    .collect::<Vec<_>>()
            });
            for action in actions {
                crate::trade_dock::apply_action(&mut self.model, &self.client, action, ui.ctx());
            }
        }
        if std::mem::take(&mut self.model.general_settings_requested) {
            self.settings_state.focus_general();
            self.show_settings = true;
        }

        let status_height = if self.model.preferences.show_status_bar {
            26.0
        } else {
            0.0
        };
        let available = egui::vec2(
            ui.available_width(),
            (ui.available_height() - status_height).max(0.0),
        );
        ui.allocate_ui(available, |ui| {
            let tree = self.workspaces.active_tree_mut();
            let mut behavior = ui::PaneBehavior {
                model: &mut self.model,
                client: &self.client,
            };
            tree.ui(&mut behavior, ui);
        });
        crate::chart_view::evict_inactive_indicator_caches(ui.ctx());
        #[cfg(not(target_arch = "wasm32"))]
        if self.frame_telemetry.memory_sample_due() {
            let source_bytes = self.model.local_markets.retained_study_source_bytes();
            let chart_cache_bytes = crate::chart_view::retained_indicator_cache_bytes(ui.ctx());
            let async_reserved_bytes = crate::chart_view::pending_indicator_input_bytes();
            self.frame_telemetry.record_memory(
                source_bytes,
                chart_cache_bytes,
                async_reserved_bytes,
            );
        }
        crate::chart_trading::apply_interaction(&mut self.model, &self.client, ui.ctx());
        crate::execution_view::show_position_confirmation(ui, &mut self.model, &self.client, None);
        if std::mem::take(&mut self.model.indicator_settings_requested) {
            self.show_settings = true;
            self.settings_state
                .focus_indicators(self.model.indicator_target.take());
        }
        if std::mem::take(&mut self.model.trading_settings_requested) {
            self.show_trading_settings = true;
        }
        if self.model.preferences.show_status_bar {
            #[cfg(not(all(target_arch = "wasm32", feature = "preview")))]
            ui::show_status_bar(ui, &self.model);
            #[cfg(all(target_arch = "wasm32", feature = "preview"))]
            ui.horizontal(|ui| {
                ui.spacing_mut().item_spacing.x = 12.0;
                ui.colored_label(theme::BRAND, "只读行情");
                ui.weak("本机行情桥接 · 每 500ms 读取 · 未连接交易账户");
            });
        }

        let context = ui.ctx().clone();
        ui::show_confirmation(&context, &mut self.model, &self.client);
        settings_panel::show(
            &context,
            &mut self.show_settings,
            &mut self.settings_state,
            &mut self.model,
            &mut self.reconnect,
        );
        // Drop the old session before account UI in this same frame can send to
        // the destination just edited in settings.
        self.reconnect_if_requested(&context);
        crate::trading::show_settings(&context, &mut self.show_trading_settings, &mut self.model);
        crate::account_center::show(
            &context,
            &mut self.show_execution_account,
            &mut self.account_center,
            &mut self.model,
        );
        ui::show_modules(
            &context,
            &mut self.show_modules,
            &mut self.workspaces,
            self.model.preferences.language,
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::latency_evidence::show(&context);
            crate::latency_evidence::end_pass(&context);
        }
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        let persisted = PersistedState {
            schema_version: PERSISTED_SCHEMA_VERSION,
            preferences: self.model.preferences.clone(),
            workspaces: self.workspaces.clone(),
        };
        if persistence::save(storage, &persisted).is_err() {
            self.model.last_error = Some(
                match self.model.preferences.language {
                    crate::i18n::Language::SimplifiedChinese => "布局保存失败，上次配置已保留。",
                    crate::i18n::Language::English => {
                        "Layout save failed; the previous configuration was retained."
                    }
                }
                .into(),
            );
        }
    }

    fn auto_save_interval(&self) -> Duration {
        Duration::from_secs(15)
    }

    fn persist_egui_memory(&self) -> bool {
        true
    }

    fn clear_color(&self, _visuals: &egui::Visuals) -> [f32; 4] {
        theme::BG_PRIMARY.to_normalized_gamma_f32()
    }

    fn on_exit(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        if !self.frame_telemetry.reported_full_window {
            self.frame_telemetry.report("partial");
        }
    }
}

fn migrate_persisted_state(mut state: PersistedState) -> PersistedState {
    if (2..=8).contains(&state.schema_version) {
        let migrate_order_flow = |chart: &mut crate::chart_settings::ChartDisplaySettings| {
            chart.session.pdh = false;
            chart.session.pdl = false;
            chart.session.sr_current = false;
            chart.microstructure.show_delta =
                chart.microstructure.order_flow && !chart.microstructure.cumulative;
            chart.microstructure.show_cvd =
                chart.microstructure.order_flow && chart.microstructure.cumulative;
            chart.microstructure.cvd_reset_mode =
                venue_indicators::chart::CvdResetMode::LoadedContinuous;
        };
        migrate_order_flow(&mut state.preferences.chart);
        for chart in state.preferences.chart_overrides.values_mut() {
            migrate_order_flow(chart);
        }
    }
    if (2..=7).contains(&state.schema_version) {
        // New readout defaults apply only to a fresh install; older saved charts keep their
        // sparse layout even when serde fills newly introduced fields.
        state.preferences.chart.atr_value_readout = false;
        state.preferences.chart.atr_percent_readout = false;
        for chart in state.preferences.chart_overrides.values_mut() {
            chart.atr_value_readout = false;
            chart.atr_percent_readout = false;
        }
    }
    // Old installs inherited the development tunnel. Resolve the new startup default
    // before opening the endpoint-scoped vault; never move saved credentials across origins.
    if cfg!(not(target_arch = "wasm32"))
        && (2..=6).contains(&state.schema_version)
        && state.preferences.endpoint.trim().trim_end_matches('/') == "http://127.0.0.1:39180"
    {
        state.preferences.endpoint.clear();
    }
    match state.schema_version {
        PERSISTED_SCHEMA_VERSION => state,
        6 | 7 | 8 => {
            state.schema_version = PERSISTED_SCHEMA_VERSION;
            state
        }
        5 => {
            // Schema 6 changes the product default from crossing GTC to maker-only. Apply the
            // new default once; subsequent user changes are preserved under schema 6.
            state.schema_version = PERSISTED_SCHEMA_VERSION;
            state.preferences.trading.post_only = true;
            state
        }
        2..=4 => PersistedState {
            schema_version: PERSISTED_SCHEMA_VERSION,
            preferences: {
                state.preferences.trading.post_only = true;
                state.preferences
            },
            workspaces: Workspaces::default(),
        },
        _ => PersistedState::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PERSISTED_SCHEMA_VERSION, PersistedState, migrate_persisted_state};

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn frame_percentiles_use_nearest_rank() {
        let samples: Vec<f32> = (1..=20).map(|value| value as f32).collect();
        assert_eq!(super::percentile(&samples, 50), 10.0);
        assert_eq!(super::percentile(&samples, 95), 19.0);
        assert_eq!(super::percentile(&samples, 99), 20.0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn sampled_indicator_memory_includes_worker_reservations() {
        let mut telemetry = super::FrameTelemetry::default();
        telemetry.record_memory(10, 20, 30);
        assert_eq!(telemetry.estimated_indicator_last_bytes, 60);
        assert_eq!(telemetry.estimated_indicator_peak_bytes, 60);
        assert_eq!(telemetry.async_reserved_peak_bytes, 30);
        telemetry.record_memory(5, 5, 0);
        assert_eq!(telemetry.estimated_indicator_last_bytes, 10);
        assert_eq!(telemetry.estimated_indicator_peak_bytes, 60);
        assert_eq!(telemetry.async_reserved_last_bytes, 0);
    }

    #[test]
    fn old_charts_keep_sparse_readouts_but_new_installs_show_atr_percent() {
        let mut old = PersistedState {
            schema_version: 7,
            ..Default::default()
        };
        old.preferences
            .chart_overrides
            .insert("chart-1".into(), old.preferences.chart.clone());
        let migrated = migrate_persisted_state(old);
        assert!(!migrated.preferences.chart.atr_percent_readout);
        assert!(!migrated.preferences.chart_overrides["chart-1"].atr_percent_readout);
        assert!(
            PersistedState::default()
                .preferences
                .chart
                .atr_percent_readout
        );
    }

    #[test]
    fn old_order_flow_choice_migrates_without_enabling_both_panes() {
        let mut old = PersistedState {
            schema_version: 8,
            ..Default::default()
        };
        old.preferences.chart.microstructure.order_flow = true;
        old.preferences.chart.microstructure.cumulative = true;
        old.preferences.chart.atr_percent_readout = true;
        let migrated = migrate_persisted_state(old);
        assert!(!migrated.preferences.chart.microstructure.show_delta);
        assert!(migrated.preferences.chart.microstructure.show_cvd);
        assert!(migrated.preferences.chart.atr_percent_readout);
        assert!(
            PersistedState::default()
                .preferences
                .chart
                .microstructure
                .heatmap
        );
        assert!(!PersistedState::default().preferences.chart.macd.enabled);
    }

    #[test]
    fn sparse_saved_chart_json_keeps_explicit_heatmap_and_flow_choices()
    -> Result<(), serde_json::Error> {
        let saved = r#"{
            "schema_version": 7,
            "preferences": {
                "chart": {"microstructure": {
                    "heatmap": false, "order_flow": true, "cumulative": false
                }},
                "chart_overrides": {"pane-1": {"microstructure": {
                    "heatmap": true, "order_flow": true, "cumulative": true
                }}}
            }
        }"#;
        let migrated = migrate_persisted_state(serde_json::from_str(saved)?);
        assert_eq!(migrated.schema_version, PERSISTED_SCHEMA_VERSION);
        assert!(!migrated.preferences.chart.microstructure.heatmap);
        assert!(migrated.preferences.chart.microstructure.show_delta);
        assert!(!migrated.preferences.chart.microstructure.show_cvd);
        assert!(!migrated.preferences.chart.session.pdh);
        assert!(!migrated.preferences.chart.atr_percent_readout);
        let pane = &migrated.preferences.chart_overrides["pane-1"];
        assert!(pane.microstructure.heatmap);
        assert!(!pane.microstructure.show_delta);
        assert!(pane.microstructure.show_cvd);
        assert!(!pane.session.sr_current);
        Ok(())
    }

    #[test]
    fn legacy_tunnel_default_migrates_once_but_custom_servers_are_preserved() {
        let mut old = PersistedState {
            schema_version: 6,
            ..Default::default()
        };
        old.preferences.endpoint = "http://127.0.0.1:39180/".into();
        old.preferences.trading.post_only = false;
        let migrated = migrate_persisted_state(old);
        if cfg!(not(target_arch = "wasm32")) {
            assert!(migrated.preferences.endpoint.is_empty());
        }
        assert!(!migrated.preferences.trading.post_only);

        for (version, endpoint) in [
            (6, "https://custom.example.com"),
            (6, "http://127.0.0.1:39181"),
            (PERSISTED_SCHEMA_VERSION, "http://127.0.0.1:39180"),
        ] {
            let mut state = PersistedState {
                schema_version: version,
                ..Default::default()
            };
            state.preferences.endpoint = endpoint.into();
            assert_eq!(
                migrate_persisted_state(state).preferences.endpoint,
                endpoint
            );
        }
    }

    #[test]
    fn persisted_state_contains_only_ui_preferences_and_layout() {
        let value = serde_json::to_value(PersistedState::default()).unwrap_or_default();
        assert_eq!(
            value
                .get("schema_version")
                .and_then(serde_json::Value::as_u64),
            Some(u64::from(PERSISTED_SCHEMA_VERSION))
        );
        for forbidden in [
            "credentials",
            "wal",
            "orders",
            "positions",
            "snapshot",
            "commands",
            "receipts",
        ] {
            assert!(value.get(forbidden).is_none());
        }
    }

    #[test]
    fn schema_five_migrates_once_to_maker_only_without_overriding_future_choices() {
        let mut old = PersistedState {
            schema_version: 5,
            ..Default::default()
        };
        old.preferences.trading.post_only = false;
        let migrated = migrate_persisted_state(old);
        assert_eq!(migrated.schema_version, PERSISTED_SCHEMA_VERSION);
        assert!(migrated.preferences.trading.post_only);

        let mut current = migrated;
        current.preferences.trading.post_only = false;
        assert!(
            !migrate_persisted_state(current)
                .preferences
                .trading
                .post_only
        );
    }
}
