use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState, ConnectionState, ControlAction,
    ControlCommandRequest, ControlSnapshot, CopyLifecyclePolicy, CopyRelationBinding,
    CopyRelationConfig, CopyRelationRecord, CopyRelationUpsertRequest, CopyRiskPolicy, GatewayMode,
    StrategySummary, VenueId,
};

use crate::i18n::{Language, TextKey, text};

const MAX_NOTICES: usize = 8;
const MAX_RECEIPT_IDS: usize = 256;
const MAX_COMMANDS: usize = 32;
pub const DEFAULT_FAVORITE_SYMBOLS: [&str; 4] = ["BTC/USDC", "ETH/USDC", "SOL/USDC", "BNB/USDC"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketQuote {
    pub symbol: String,
    pub last: Decimal,
    pub change_percent_24h: Decimal,
    pub quote_volume_24h: Decimal,
    pub exchange_time_ms: u64,
    pub received_ms: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketInstrument {
    pub symbol: String,
    pub price_scale: u32,
    pub quantity_scale: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SymbolGroup {
    Favorites,
    Usdc,
    Usdt,
    #[default]
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkspaceKind {
    Trading,
    Operations,
    MultiChart,
}

impl WorkspaceKind {
    pub const ALL: [Self; 3] = [Self::Trading, Self::Operations, Self::MultiChart];

    pub const fn label(self, language: Language) -> &'static str {
        match self {
            Self::Trading => text(language, TextKey::Trading),
            Self::Operations => text(language, TextKey::Operations),
            Self::MultiChart => text(language, TextKey::MultiChart),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Preferences {
    pub endpoint: String,
    pub selected_symbol: String,
    pub selected_instance: Option<String>,
    /// Selection is a view preference only. Relation configuration always remains in Control.
    pub selected_copy_relation: Option<String>,
    pub ui_scale: f32,
    pub show_status_bar: bool,
    pub language: Language,
    pub favorite_symbols: Vec<String>,
    pub chart: crate::chart_settings::ChartDisplaySettings,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            selected_symbol: "BTC/USDC".to_owned(),
            selected_instance: None,
            selected_copy_relation: None,
            ui_scale: 1.0,
            show_status_bar: true,
            language: default_language(),
            favorite_symbols: DEFAULT_FAVORITE_SYMBOLS.map(str::to_owned).to_vec(),
            chart: crate::chart_settings::ChartDisplaySettings::default(),
        }
    }
}

const fn default_language() -> Language {
    if cfg!(target_arch = "wasm32") {
        Language::English
    } else {
        Language::SimplifiedChinese
    }
}

#[derive(Clone, Debug)]
pub struct PendingConfirmation {
    pub request: ControlCommandRequest,
    pub typed: String,
}

#[derive(Clone, Debug)]
pub struct CopyRelationDraft {
    pub relation_id: String,
    pub expected_revision: Option<u64>,
    pub leader_venue: VenueId,
    pub leader_account_id: String,
    pub leader_instance_id: String,
    pub leader_symbol: String,
    pub follower_venue: VenueId,
    pub follower_account_id: String,
    pub follower_instance_id: String,
    pub follower_symbol: String,
    pub allocated_capital: String,
    pub multiplier: String,
    pub safety_reserve_rate: String,
    pub max_total_notional: String,
    pub max_order_notional: String,
    pub max_leverage: String,
    pub lifecycle: CopyLifecyclePolicy,
}

impl CopyRelationDraft {
    pub fn new() -> Self {
        Self {
            relation_id: String::new(),
            expected_revision: None,
            leader_venue: VenueId::Binance,
            leader_account_id: String::new(),
            leader_instance_id: String::new(),
            leader_symbol: String::new(),
            follower_venue: VenueId::Binance,
            follower_account_id: String::new(),
            follower_instance_id: String::new(),
            follower_symbol: String::new(),
            allocated_capital: "100".to_owned(),
            multiplier: "1".to_owned(),
            safety_reserve_rate: "0".to_owned(),
            max_total_notional: "100".to_owned(),
            max_order_notional: "100".to_owned(),
            max_leverage: "1".to_owned(),
            lifecycle: CopyLifecyclePolicy::Active,
        }
    }

    pub fn from_config(config: &CopyRelationConfig, revision: u64) -> Self {
        Self {
            relation_id: config.relation_id.clone(),
            expected_revision: Some(revision),
            leader_venue: config.leader.venue,
            leader_account_id: config.leader.trading_account_id.clone(),
            leader_instance_id: config.leader.instance_id.clone(),
            leader_symbol: config.leader.symbol.to_string(),
            follower_venue: config.follower.venue,
            follower_account_id: config.follower.trading_account_id.clone(),
            follower_instance_id: config.follower.instance_id.clone(),
            follower_symbol: config.follower.symbol.to_string(),
            allocated_capital: config.allocated_capital.normalize().to_string(),
            multiplier: config.multiplier.normalize().to_string(),
            safety_reserve_rate: config.safety_reserve_rate.normalize().to_string(),
            max_total_notional: config.risk.max_total_notional.normalize().to_string(),
            max_order_notional: config.risk.max_order_notional.normalize().to_string(),
            max_leverage: config.risk.max_leverage.normalize().to_string(),
            lifecycle: config.lifecycle,
        }
    }

    pub fn to_request(&self) -> Result<CopyRelationUpsertRequest, String> {
        let relation = CopyRelationConfig {
            relation_id: self.relation_id.trim().to_owned(),
            leader: binding(
                self.leader_venue,
                &self.leader_account_id,
                &self.leader_instance_id,
                &self.leader_symbol,
            )?,
            follower: binding(
                self.follower_venue,
                &self.follower_account_id,
                &self.follower_instance_id,
                &self.follower_symbol,
            )?,
            allocated_capital: decimal(&self.allocated_capital, "allocated capital")?,
            multiplier: decimal(&self.multiplier, "multiplier")?,
            safety_reserve_rate: decimal(&self.safety_reserve_rate, "safety reserve rate")?,
            risk: CopyRiskPolicy {
                max_total_notional: decimal(&self.max_total_notional, "maximum total notional")?,
                max_order_notional: decimal(&self.max_order_notional, "maximum order notional")?,
                max_leverage: decimal(&self.max_leverage, "maximum leverage")?,
            },
            lifecycle: self.lifecycle,
        };
        let request = CopyRelationUpsertRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            relation,
            expected_revision: self.expected_revision,
        };
        request
            .validate()
            .map_err(|error| format!("invalid copy relation: {error}"))?;
        Ok(request)
    }
}

fn binding(
    venue: VenueId,
    account_id: &str,
    instance_id: &str,
    symbol: &str,
) -> Result<CopyRelationBinding, String> {
    Ok(CopyRelationBinding {
        venue,
        mode: GatewayMode::Live,
        trading_account_id: account_id.trim().to_owned(),
        instance_id: instance_id.trim().to_owned(),
        symbol: symbol
            .trim()
            .parse()
            .map_err(|_| "symbol must use canonical BASE/QUOTE form".to_owned())?,
    })
}

fn decimal(value: &str, label: &str) -> Result<Decimal, String> {
    Decimal::from_str(value.trim()).map_err(|_| format!("{label} must be a decimal number"))
}

impl PendingConfirmation {
    pub fn new(request: ControlCommandRequest) -> Self {
        Self {
            request,
            typed: String::new(),
        }
    }

    pub fn confirmed_request(&self) -> Option<ControlCommandRequest> {
        if self.typed != self.request.expected_confirmation() {
            return None;
        }
        let mut request = self.request.clone();
        if request.action.requires_confirmation() {
            request.confirmation = Some(self.typed.clone());
        }
        Some(request)
    }
}

#[derive(Clone, Debug)]
pub struct CommandProgress {
    pub request: ControlCommandRequest,
    pub latest_receipt: Option<CommandReceipt>,
    pub terminal_receipt: Option<CommandReceipt>,
}

#[derive(Debug)]
pub struct AppModel {
    pub preferences: Preferences,
    /// Connectivity of this secret-free Control API client, not the account runtime projection.
    pub connection: ConnectionState,
    /// The account-runtime state reported by the most recent validated snapshot.
    pub control_connection: Option<ConnectionState>,
    pub snapshot_online: bool,
    pub event_stream_online: bool,
    pub last_event_id: Option<i64>,
    pub last_error: Option<String>,
    pub snapshot: Option<ControlSnapshot>,
    /// Configurations returned by the dedicated secret-free Control relation endpoint.
    pub copy_relation_configs: Vec<CopyRelationRecord>,
    pub copy_relation_draft: Option<CopyRelationDraft>,
    pub pending_confirmation: Option<PendingConfirmation>,
    pub last_receipt: Option<CommandReceipt>,
    pub commands: VecDeque<CommandProgress>,
    receipt_ids: VecDeque<String>,
    pub notices: VecDeque<String>,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_markets: crate::market::LocalMarketStore,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_symbols: Vec<String>,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_precisions: BTreeMap<String, (u32, u32)>,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_catalog_error: Option<String>,
    #[cfg(not(target_arch = "wasm32"))]
    pub local_proxy_detected: bool,
    pub local_quotes: BTreeMap<String, MarketQuote>,
    pub symbol_filter: String,
    pub symbol_group: SymbolGroup,
    pub follow_latest_requested: bool,
    pub indicator_settings_requested: bool,
    request_sequence: u64,
}

impl AppModel {
    pub fn new(mut preferences: Preferences) -> Self {
        if preferences.chart.validate().is_err() {
            preferences.chart = crate::chart_settings::ChartDisplaySettings::default();
        }
        #[cfg(not(target_arch = "wasm32"))]
        let local_markets = {
            let mut store = crate::market::LocalMarketStore::default();
            let _configuration_is_valid = store
                .reconfigure_studies(preferences.chart.engine_config())
                .is_ok();
            store
        };
        Self {
            preferences,
            connection: ConnectionState::Connecting,
            control_connection: None,
            snapshot_online: false,
            event_stream_online: false,
            last_event_id: None,
            last_error: None,
            snapshot: None,
            copy_relation_configs: Vec::new(),
            copy_relation_draft: None,
            pending_confirmation: None,
            last_receipt: None,
            commands: VecDeque::new(),
            receipt_ids: VecDeque::new(),
            notices: VecDeque::new(),
            #[cfg(not(target_arch = "wasm32"))]
            local_markets,
            #[cfg(not(target_arch = "wasm32"))]
            local_symbols: Vec::new(),
            #[cfg(not(target_arch = "wasm32"))]
            local_precisions: BTreeMap::new(),
            #[cfg(not(target_arch = "wasm32"))]
            local_catalog_error: None,
            #[cfg(not(target_arch = "wasm32"))]
            local_proxy_detected: false,
            local_quotes: BTreeMap::new(),
            symbol_filter: String::new(),
            symbol_group: SymbolGroup::All,
            follow_latest_requested: false,
            indicator_settings_requested: false,
            request_sequence: 0,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn apply_local_catalog(&mut self, instruments: Vec<MarketInstrument>) {
        self.local_precisions = instruments
            .iter()
            .map(|instrument| {
                (
                    instrument.symbol.clone(),
                    (instrument.price_scale, instrument.quantity_scale),
                )
            })
            .collect();
        let mut symbols = instruments
            .into_iter()
            .map(|instrument| instrument.symbol)
            .collect::<Vec<_>>();
        symbols.sort_by(|left, right| {
            preferred_symbol_rank(left)
                .cmp(&preferred_symbol_rank(right))
                .then_with(|| left.cmp(right))
        });
        symbols.dedup();
        self.local_symbols = symbols;
        self.local_catalog_error = None;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn apply_local_quotes(&mut self, quotes: Vec<MarketQuote>) {
        for quote in quotes {
            let replace = self
                .local_quotes
                .get(&quote.symbol)
                .is_none_or(|current| quote.exchange_time_ms >= current.exchange_time_ms);
            if replace {
                self.local_quotes.insert(quote.symbol.clone(), quote);
            }
        }
    }

    pub fn format_market_price(&self, _symbol: &str, value: Decimal) -> String {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((scale, _)) = self.local_precisions.get(_symbol) {
            return format_decimal(value, *scale as usize);
        }
        format_decimal(value, 8)
    }

    pub fn format_market_quantity(&self, _symbol: &str, value: Decimal) -> String {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((_, scale)) = self.local_precisions.get(_symbol) {
            return format_decimal(value, *scale as usize);
        }
        format_decimal(value, 8)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn market_scales(&self, symbol: &str) -> (usize, usize) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((price, quantity)) = self.local_precisions.get(symbol) {
            return (*price as usize, *quantity as usize);
        }
        (8, 8)
    }

    pub fn apply_snapshot(&mut self, snapshot: ControlSnapshot) {
        self.control_connection = Some(snapshot.connection);
        if self.preferences.selected_symbol.trim().is_empty()
            && let Some(first) = snapshot.markets.first()
        {
            self.preferences.selected_symbol = first.symbol.to_string();
        }
        self.snapshot = Some(snapshot);
    }

    pub fn apply_copy_relation_configs(&mut self, configs: Vec<CopyRelationRecord>) {
        self.copy_relation_configs = configs;
    }

    pub fn snapshot_connected(&mut self) {
        self.snapshot_online = true;
        self.refresh_client_connection();
    }

    pub fn reconnecting(&mut self) {
        self.snapshot_online = false;
        self.event_stream_online = false;
        self.connection = ConnectionState::Connecting;
        self.last_error = None;
    }

    pub fn snapshot_unavailable(&mut self, message: String) {
        self.snapshot_online = false;
        self.last_error = Some(message.clone());
        self.notice(message);
        self.refresh_client_connection();
    }

    pub fn stream_connected(&mut self, resumed_after: Option<i64>) {
        self.event_stream_online = true;
        if let Some(cursor) = resumed_after {
            self.last_event_id = Some(cursor);
        }
        self.last_error = None;
        self.refresh_client_connection();
    }

    pub fn stream_unavailable(&mut self, message: String) {
        self.event_stream_online = false;
        self.last_error = Some(message.clone());
        self.notice(message);
        self.refresh_client_connection();
    }

    pub fn observe_event_id(&mut self, event_id: i64) {
        self.last_event_id = Some(event_id);
    }

    fn refresh_client_connection(&mut self) {
        self.connection = if self.event_stream_online {
            ConnectionState::Live
        } else if self.snapshot_online || self.snapshot.is_some() {
            ConnectionState::Degraded
        } else {
            ConnectionState::Offline
        };
    }

    pub fn record_submission(&mut self, request: ControlCommandRequest) {
        if self
            .commands
            .iter()
            .any(|command| command.request.request_id == request.request_id)
        {
            return;
        }
        self.commands.push_front(CommandProgress {
            request,
            latest_receipt: None,
            terminal_receipt: None,
        });
        self.commands.truncate(MAX_COMMANDS);
    }

    /// Returns false for an already-rendered receipt. SSE replay is expected after reconnects.
    pub fn apply_receipt(&mut self, receipt: CommandReceipt) -> bool {
        if self.receipt_ids.iter().any(|id| id == &receipt.receipt_id) {
            return false;
        }
        self.receipt_ids.push_back(receipt.receipt_id.clone());
        while self.receipt_ids.len() > MAX_RECEIPT_IDS {
            self.receipt_ids.pop_front();
        }
        if let Some(command) = self
            .commands
            .iter_mut()
            .find(|command| command.request.request_id == receipt.request_id)
        {
            command.latest_receipt = Some(receipt.clone());
            if is_terminal(receipt.state) {
                command.terminal_receipt = Some(receipt.clone());
            }
        }
        self.last_receipt = Some(receipt);
        true
    }

    pub fn last_terminal_receipt(&self) -> Option<&CommandReceipt> {
        self.commands
            .iter()
            .filter_map(|command| command.terminal_receipt.as_ref())
            .max_by_key(|receipt| receipt.observed_ms)
            .or_else(|| {
                self.last_receipt
                    .as_ref()
                    .filter(|receipt| is_terminal(receipt.state))
            })
    }

    pub fn begin_command(
        &mut self,
        strategy: &StrategySummary,
        action: ControlAction,
        now_ms: u64,
    ) -> ControlCommandRequest {
        self.request_sequence = self.request_sequence.saturating_add(1);
        ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: format!("venueflow-{now_ms}-{}", self.request_sequence),
            venue: strategy.venue,
            mode: strategy.mode,
            trading_account_id: strategy.trading_account_id.clone(),
            instance_id: strategy.instance_id.clone(),
            symbol: strategy.symbol.clone(),
            action,
            expected_config_epoch: strategy.config_epoch,
            confirmation: None,
        }
    }

    pub fn select_copy_relation(
        &mut self,
        relation_id: &str,
        follower_instance_id: &str,
        symbol: &str,
    ) {
        self.preferences.selected_copy_relation = Some(relation_id.to_owned());
        self.preferences.selected_instance = Some(follower_instance_id.to_owned());
        self.preferences.selected_symbol = symbol.to_owned();
    }

    pub fn notice(&mut self, message: impl Into<String>) {
        self.notices.push_front(message.into());
        self.notices.truncate(MAX_NOTICES);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn preferred_symbol_rank(symbol: &str) -> usize {
    DEFAULT_FAVORITE_SYMBOLS
        .iter()
        .position(|preferred| *preferred == symbol)
        .unwrap_or(DEFAULT_FAVORITE_SYMBOLS.len())
}

pub const fn requires_operator_confirmation(action: ControlAction) -> bool {
    matches!(
        action,
        ControlAction::Pause | ControlAction::Resume | ControlAction::Stop | ControlAction::Flatten
    )
}

const fn is_terminal(state: CommandState) -> bool {
    !matches!(state, CommandState::Accepted)
}

pub fn freshness_age_ms(generated_ms: u64, observed_ms: u64) -> Option<u64> {
    (observed_ms != 0)
        .then(|| generated_ms.checked_sub(observed_ms))
        .flatten()
}

pub fn decimal_to_f64(value: Decimal) -> f64 {
    value.to_string().parse::<f64>().unwrap_or_default()
}

pub fn format_decimal(value: Decimal, precision: usize) -> String {
    value
        .round_dp(precision.min(28) as u32)
        .normalize()
        .to_string()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_control_protocol::{
        CONTROL_SCHEMA_VERSION, CommandReceipt, ConnectionState, ControlAction,
        ControlCommandRequest, ControlSnapshot, GatewayMode, StrategyKind, StrategyLifecycle,
        StrategySummary, VenueId,
    };

    use super::{
        AppModel, DEFAULT_FAVORITE_SYMBOLS, PendingConfirmation, Preferences, freshness_age_ms,
        requires_operator_confirmation,
    };

    #[test]
    fn defaults_select_and_pin_the_four_usdc_markets() {
        let preferences = Preferences::default();
        assert_eq!(preferences.selected_symbol, "BTC/USDC");
        assert_eq!(
            preferences.favorite_symbols,
            DEFAULT_FAVORITE_SYMBOLS.map(str::to_owned)
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn public_catalog_keeps_favorites_first_without_hiding_other_symbols() {
        let mut model = AppModel::new(Preferences::default());
        model.apply_local_catalog(
            ["XRP/USDT", "BNB/USDC", "BTC/USDC", "ETH/USDC", "SOL/USDC"]
                .map(|symbol| super::MarketInstrument {
                    symbol: symbol.to_owned(),
                    price_scale: 2,
                    quantity_scale: 3,
                })
                .to_vec(),
        );
        assert_eq!(
            &model.local_symbols[..4],
            &DEFAULT_FAVORITE_SYMBOLS.map(str::to_owned)
        );
        assert_eq!(model.local_symbols[4], "XRP/USDT");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn exchange_precision_is_used_without_padding_trailing_zeroes() {
        let mut model = AppModel::new(Preferences::default());
        model.apply_local_catalog(vec![super::MarketInstrument {
            symbol: "SOL/USDC".to_owned(),
            price_scale: 4,
            quantity_scale: 3,
        }]);

        assert_eq!(
            model.format_market_price("SOL/USDC", Decimal::new(104_900, 3)),
            "104.9"
        );
        assert_eq!(
            model.format_market_quantity("SOL/USDC", Decimal::new(12_340, 3)),
            "12.34"
        );
    }

    #[test]
    fn a_snapshot_is_query_state_not_persisted_authority() {
        let mut model = AppModel::new(Preferences::default());
        model.apply_snapshot(ControlSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: 1,
            connection: ConnectionState::Live,
            accounts: Vec::new(),
            strategies: Vec::new(),
            copy_relations: Vec::new(),
            markets: Vec::new(),
            ledger: Vec::new(),
        });
        assert_eq!(model.connection, ConnectionState::Connecting);
        assert_eq!(model.control_connection, Some(ConnectionState::Live));
        assert!(model.snapshot.is_some());
    }

    #[test]
    fn snapshot_polling_does_not_hide_a_disconnected_event_stream() {
        let mut model = AppModel::new(Preferences::default());
        model.snapshot_connected();
        assert_eq!(model.connection, ConnectionState::Degraded);
        model.stream_connected(Some(7));
        assert_eq!(model.connection, ConnectionState::Live);
        model.stream_unavailable("stream lost".to_owned());
        assert_eq!(model.connection, ConnectionState::Degraded);
        assert_eq!(model.last_event_id, Some(7));
    }

    #[test]
    fn selecting_a_copy_relation_updates_only_local_view_selection() {
        let mut model = AppModel::new(Preferences::default());
        model.select_copy_relation(
            "00000000-0000-4000-8000-000000000003",
            "copy-btc",
            "BTC/USDT",
        );

        assert_eq!(
            model.preferences.selected_copy_relation,
            Some("00000000-0000-4000-8000-000000000003".to_owned())
        );
        assert_eq!(
            model.preferences.selected_instance.as_deref(),
            Some("copy-btc")
        );
        assert_eq!(model.preferences.selected_symbol, "BTC/USDT");
    }

    #[test]
    fn copy_relation_draft_requires_valid_live_bindings_and_risk_limits() {
        let mut draft = super::CopyRelationDraft::new();
        draft.relation_id = "00000000-0000-4000-8000-000000000003".to_owned();
        draft.leader_account_id = "00000000-0000-4000-8000-000000000001".to_owned();
        draft.leader_instance_id = "leader-btc".to_owned();
        draft.leader_symbol = "BTC/USDT".to_owned();
        draft.follower_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        draft.follower_instance_id = "copy-btc".to_owned();
        draft.follower_symbol = "BTC/USDT".to_owned();
        draft.max_total_notional = "100".to_owned();
        draft.max_order_notional = "50".to_owned();

        assert!(draft.to_request().is_ok());

        draft.max_order_notional = "101".to_owned();
        assert!(draft.to_request().is_err());
    }

    #[test]
    fn command_scope_preserves_the_live_strategy_gateway_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = AppModel::new(Preferences::default());
        let strategy = StrategySummary {
            instance_id: "grid-btc".to_owned(),
            kind: StrategyKind::Grid,
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            lifecycle: StrategyLifecycle::Running,
            config_epoch: 7,
            open_orders: 0,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_receipt_ms: 1,
            attention: None,
        };

        let request = model.begin_command(&strategy, ControlAction::Stop, 10);

        assert_eq!(request.mode, GatewayMode::Live);
        assert!(request.expected_confirmation().contains("mode=LIVE"));
        Ok(())
    }

    #[test]
    fn all_lifecycle_actions_require_exact_operator_visible_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = AppModel::new(Preferences::default());
        let strategy = StrategySummary {
            instance_id: "grid-btc".to_owned(),
            kind: StrategyKind::Grid,
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            lifecycle: StrategyLifecycle::Running,
            config_epoch: 9,
            open_orders: 0,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_receipt_ms: 1,
            attention: None,
        };
        for action in [
            ControlAction::Pause,
            ControlAction::Resume,
            ControlAction::Stop,
            ControlAction::Flatten,
        ] {
            assert!(requires_operator_confirmation(action));
            let request = model.begin_command(&strategy, action, 20);
            let expected = request.expected_confirmation();
            for required in [
                "venue=binance",
                "mode=LIVE",
                "trading_account_id=",
                "symbol=BTC/USDT",
                "instance_id(8)=grid-btc",
                "expected_config_epoch=9",
            ] {
                assert!(
                    expected.contains(required),
                    "missing {required} in {expected}"
                );
            }
            let mut pending = PendingConfirmation::new(request);
            assert!(pending.confirmed_request().is_none());
            pending.typed = expected;
            let confirmed = pending
                .confirmed_request()
                .ok_or("confirmation did not arm")?;
            confirmed.validate()?;
        }
        Ok(())
    }

    #[test]
    fn replayed_receipts_are_idempotent_but_new_terminal_receipts_remain_visible()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = AppModel::new(Preferences::default());
        model.record_submission(ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instance_id: "grid-btc".to_owned(),
            symbol: "BTC/USDT".parse()?,
            action: ControlAction::Pause,
            expected_config_epoch: 1,
            confirmation: None,
        });
        let accepted = CommandReceipt {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            state: venue_control_protocol::CommandState::Accepted,
            receipt_id: "receipt-accepted".to_owned(),
            observed_ms: 1,
            detail: String::new(),
        };
        assert!(model.apply_receipt(accepted.clone()));
        assert!(!model.apply_receipt(accepted));
        assert!(model.apply_receipt(CommandReceipt {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            receipt_id: "receipt-applied".to_owned(),
            state: venue_control_protocol::CommandState::Applied,
            observed_ms: 2,
            detail: String::new(),
        }));
        assert_eq!(
            model.last_terminal_receipt().map(|receipt| receipt.state),
            Some(venue_control_protocol::CommandState::Applied)
        );
        assert!(model.commands[0].terminal_receipt.is_some());
        Ok(())
    }

    #[test]
    fn freshness_never_invents_an_age_for_missing_or_future_evidence() {
        assert_eq!(freshness_age_ms(100, 80), Some(20));
        assert_eq!(freshness_age_ms(100, 0), None);
        assert_eq!(freshness_age_ms(100, 101), None);
    }
}
