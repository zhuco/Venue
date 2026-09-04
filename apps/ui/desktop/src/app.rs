use std::time::Duration;

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

const STORAGE_KEY: &str = "venueflow-state-v1";
const PERSISTED_SCHEMA_VERSION: u16 = 7;

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
    show_modules: bool,
    show_settings: bool,
    show_trading_settings: bool,
    show_execution_account: bool,
    settings_state: SettingsPanelState,
    show_symbol_picker: bool,
    reconnect: bool,
}

impl VenueFlowApp {
    pub fn new(creation_context: &eframe::CreationContext<'_>, default_endpoint: String) -> Self {
        theme::apply(&creation_context.egui_ctx);
        let mut persisted = load(creation_context.storage);
        persisted.workspaces.upgrade_trading_tables();
        if persisted.preferences.endpoint.trim().is_empty() {
            persisted.preferences.endpoint = default_endpoint;
        }
        let model = AppModel::new(persisted.preferences);
        let client = ControlClient::connect(
            model.preferences.endpoint.clone(),
            creation_context.egui_ctx.clone(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        let (model, market_client) = match LocalMarketClient::start() {
            Ok(client) => (model, Some(client)),
            Err(error) => {
                let mut model = model;
                model.notice(format!("Local Binance market worker unavailable: {error}"));
                (model, None)
            }
        };
        Self {
            connected_endpoint: model.preferences.endpoint.clone(),
            account_center: crate::account_center::AccountCenter::new(&model.preferences.endpoint),
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
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn synchronize_local_markets(&mut self) {
        let selections = self
            .workspaces
            .active_chart_requests(&self.model.preferences.selected_symbol)
            .into_iter()
            .filter_map(|(symbol, interval)| {
                match MarketSelection::binance_usd_m(&symbol, interval) {
                    Ok(selection) => Some(selection),
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
                None
            }
        };
        let (Some(generation), Some(client)) = (generation, self.market_client.as_ref()) else {
            return;
        };
        let selections = self.model.local_markets.selections().cloned().collect();
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
        for event in client.drain(10_000) {
            match event {
                LocalMarketClientEvent::History { request, result } => {
                    match self.model.local_markets.finish_history(&request, result) {
                        Ok(added) => self.workspaces.history_prepended(
                            &request.selection,
                            added,
                            &self.model.preferences.selected_symbol,
                        ),
                        Err(error) => {
                            let _ = self
                                .model
                                .local_markets
                                .finish_history(&request, Err(error.to_string()));
                            self.model.notice(format!("History page rejected: {error}"));
                        }
                    }
                }
                LocalMarketClientEvent::Market(envelope) => {
                    if let Err(error) = self.model.local_markets.apply(*envelope) {
                        self.model
                            .notice(format!("Ignored invalid local market event: {error}"));
                    }
                }
                LocalMarketClientEvent::Catalog(symbols) => {
                    self.model.apply_local_catalog(symbols);
                }
                LocalMarketClientEvent::Quotes(quotes) => {
                    self.model.apply_local_quotes(quotes);
                }
                LocalMarketClientEvent::QuotesUnavailable(error) => {
                    self.model
                        .notice(format!("Local Binance 24h quotes unavailable: {error}"));
                }
                LocalMarketClientEvent::CatalogUnavailable(error) => {
                    self.model.local_catalog_error = Some(error.clone());
                    self.model
                        .notice(format!("Local Binance symbol catalog unavailable: {error}"));
                }
                LocalMarketClientEvent::ProxyDetected(detected) => {
                    self.model.local_proxy_detected = detected;
                }
                LocalMarketClientEvent::RepaintRequested => context.request_repaint(),
                LocalMarketClientEvent::WorkerFailed(error) => {
                    self.model
                        .notice(format!("Local Binance market worker stopped: {error}"));
                }
            }
        }
        for request in self.model.history_requests.drain(..) {
            if let Err(error) = client.load_older(request.clone()) {
                let _ = self
                    .model
                    .local_markets
                    .finish_history(&request, Err(error.to_string()));
            }
        }
        self.model
            .local_markets
            .refresh_staleness(unix_now_ms(), 5_000);
    }

    fn drain_client(&mut self) {
        let events = self.client.drain().take(5_000).collect::<Vec<_>>();
        for event in events {
            match event {
                ClientEvent::SnapshotConnected => {
                    self.model.snapshot_connected();
                }
                ClientEvent::TerminalAccountProjection {
                    credential_id,
                    projection,
                } => {
                    if self
                        .model
                        .account_overview
                        .as_ref()
                        .and_then(|overview| overview.selected_credential_id.as_deref())
                        == Some(credential_id.as_str())
                    {
                        self.model
                            .execution
                            .apply_private(projection, &mut self.model.trade_dock);
                    }
                }
                ClientEvent::TerminalExecutions(executions) => {
                    self.model.execution.apply_terminal_executions(executions)
                }
                ClientEvent::TerminalExecutionUpdated(summary) => {
                    self.model.execution.apply_terminal_execution(summary)
                }
                ClientEvent::TerminalExecutionsUnavailable(message) => {
                    self.model.execution.terminal_executions_error = Some(message)
                }
                ClientEvent::TerminalSubmissionUnavailable {
                    request_id,
                    message,
                    definitely_not_submitted,
                } => {
                    self.model
                        .execution
                        .position_submission_failed(&request_id, definitely_not_submitted);
                    if self.model.execution.terminal_request_id.as_deref()
                        == Some(request_id.as_str())
                    {
                        self.model.execution.terminal_submission_error = Some(message.clone());
                        self.model.notice(message);
                    }
                }
                ClientEvent::TerminalAccountUnavailable {
                    credential_id,
                    message,
                } => {
                    if self
                        .model
                        .account_overview
                        .as_ref()
                        .is_some_and(|overview| {
                            overview.selected_credential_id.as_ref() == Some(&credential_id)
                        })
                    {
                        self.model.execution.private_error = Some(message);
                    }
                }
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
                    self.model.execution.leader_bot.create_credential_id = None;
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
                        self.account_center.clear(&mut self.model);
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
        let Some(credential_id) = self
            .model
            .account_overview
            .as_ref()
            .and_then(|overview| overview.selected_credential_id.clone())
        else {
            return;
        };
        let symbols = std::iter::once(&self.model.preferences.selected_symbol)
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
        if symbols.is_empty() {
            return;
        }
        self.client
            .subscribe_terminal(venue_control_protocol::kol::TerminalProjectionRequest {
                schema_version: venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
                credential_id,
                symbols,
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
        self.drain_client();
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
            self.synchronize_local_markets();
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
        crate::chart_trading::poll(&mut self.model);
        crate::chart_trading::notification(ui.ctx(), &mut self.model);
        ui.painter()
            .rect_filled(ui.max_rect(), 0.0, theme::BG_PRIMARY);
        ui.spacing_mut().item_spacing = egui::Vec2::ZERO;
        self.model.synchronize_trading_scope();
        self.model.refresh_trading_price(ui.ctx());
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
        ui::show_top_bar(
            ui,
            &mut self.model,
            &mut self.workspaces,
            &mut self.show_modules,
            &mut self.show_trading_settings,
            &mut self.show_execution_account,
            &mut self.show_symbol_picker,
        );

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
        crate::chart_trading::apply_interaction(&mut self.model, &self.client, ui.ctx());
        if std::mem::take(&mut self.model.indicator_settings_requested) {
            self.show_settings = true;
            self.settings_state
                .focus_indicators(self.model.indicator_target.take());
        }
        if std::mem::take(&mut self.model.trading_settings_requested) {
            self.show_trading_settings = true;
        }
        if self.model.preferences.show_status_bar {
            ui::show_status_bar(ui, &self.model);
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
    }

    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        let persisted = PersistedState {
            schema_version: PERSISTED_SCHEMA_VERSION,
            preferences: self.model.preferences.clone(),
            workspaces: self.workspaces.clone(),
        };
        if let Ok(encoded) = serde_json::to_string(&persisted) {
            storage.set_string(STORAGE_KEY, encoded);
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
}

#[cfg(not(target_arch = "wasm32"))]
fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn load(storage: Option<&dyn eframe::Storage>) -> PersistedState {
    let Some(encoded) = storage.and_then(|storage| storage.get_string(STORAGE_KEY)) else {
        return PersistedState::default();
    };
    serde_json::from_str::<PersistedState>(&encoded)
        .map(migrate_persisted_state)
        .unwrap_or_default()
}

fn migrate_persisted_state(mut state: PersistedState) -> PersistedState {
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
        6 => {
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

    #[test]
    fn legacy_tunnel_default_migrates_once_but_custom_servers_are_preserved() {
        let mut old = PersistedState::default();
        old.schema_version = 6;
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
            let mut state = PersistedState::default();
            state.schema_version = version;
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
        let mut old = PersistedState::default();
        old.schema_version = 5;
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
