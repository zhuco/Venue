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
const PERSISTED_SCHEMA_VERSION: u16 = 4;

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
    #[cfg(not(target_arch = "wasm32"))]
    market_client: Option<LocalMarketClient>,
    show_modules: bool,
    show_settings: bool,
    settings_state: SettingsPanelState,
    show_symbol_picker: bool,
    reconnect: bool,
}

impl VenueFlowApp {
    pub fn new(creation_context: &eframe::CreationContext<'_>, default_endpoint: String) -> Self {
        theme::apply(&creation_context.egui_ctx);
        let mut persisted = load(creation_context.storage);
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
            model,
            workspaces: persisted.workspaces,
            client,
            #[cfg(not(target_arch = "wasm32"))]
            market_client,
            show_modules: false,
            show_settings: false,
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
        self.model.reconnecting();
        self.client =
            ControlClient::connect(self.model.preferences.endpoint.clone(), context.clone());
        self.model.notice("Reconnecting to the Control API");
    }
}

impl eframe::App for VenueFlowApp {
    fn logic(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        let zoom = self.model.preferences.ui_scale.clamp(0.85, 1.35);
        if (context.zoom_factor() - zoom).abs() > 0.001 {
            context.set_zoom_factor(zoom);
        }
        self.drain_client();
        self.reconnect_if_requested(context);
        if std::mem::take(&mut self.model.follow_latest_requested) {
            self.workspaces.follow_dynamic_charts_latest();
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.synchronize_local_markets();
            self.drain_local_markets(context);
        }
        context.request_repaint_after(Duration::from_millis(if cfg!(target_arch = "wasm32") {
            500
        } else {
            250
        }));
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        ui.painter()
            .rect_filled(ui.max_rect(), 0.0, theme::BG_PRIMARY);
        ui.spacing_mut().item_spacing = egui::Vec2::ZERO;
        ui::show_top_bar(
            ui,
            &mut self.model,
            &mut self.workspaces,
            &mut self.show_modules,
            &mut self.show_settings,
            &mut self.show_symbol_picker,
        );

        let status_height = if self.model.preferences.show_status_bar {
            28.0
        } else {
            0.0
        };
        let available = egui::vec2(
            ui.available_width(),
            (ui.available_height() - status_height).max(240.0),
        );
        ui.allocate_ui(available, |ui| {
            let tree = self.workspaces.active_tree_mut();
            let mut behavior = ui::PaneBehavior {
                model: &mut self.model,
                client: &self.client,
            };
            tree.ui(&mut behavior, ui);
        });
        if std::mem::take(&mut self.model.indicator_settings_requested) {
            self.show_settings = true;
            self.settings_state.focus_indicators();
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
    match serde_json::from_str::<PersistedState>(&encoded) {
        Ok(state) if state.schema_version == PERSISTED_SCHEMA_VERSION => state,
        Ok(state) if matches!(state.schema_version, 2 | 3) => PersistedState {
            schema_version: PERSISTED_SCHEMA_VERSION,
            preferences: state.preferences,
            workspaces: Workspaces::default(),
        },
        Ok(_) | Err(_) => PersistedState::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PERSISTED_SCHEMA_VERSION, PersistedState};

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
}
