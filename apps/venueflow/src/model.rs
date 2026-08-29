use std::collections::VecDeque;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState, ConnectionState, ControlAction,
    ControlCommandRequest, ControlSnapshot, StrategySummary,
};

const MAX_NOTICES: usize = 8;
const MAX_RECEIPT_IDS: usize = 256;
const MAX_COMMANDS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkspaceKind {
    Trading,
    Operations,
    MultiChart,
}

impl WorkspaceKind {
    pub const ALL: [Self; 3] = [Self::Trading, Self::Operations, Self::MultiChart];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Trading => "Trading",
            Self::Operations => "Operations",
            Self::MultiChart => "Multi-chart",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Preferences {
    pub endpoint: String,
    pub selected_symbol: String,
    pub selected_instance: Option<String>,
    pub ui_scale: f32,
    pub show_status_bar: bool,
}

impl Default for Preferences {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            selected_symbol: "BTC/USDT".to_owned(),
            selected_instance: None,
            ui_scale: 1.0,
            show_status_bar: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PendingConfirmation {
    pub request: ControlCommandRequest,
    pub typed: String,
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
    pub pending_confirmation: Option<PendingConfirmation>,
    pub last_receipt: Option<CommandReceipt>,
    pub commands: VecDeque<CommandProgress>,
    receipt_ids: VecDeque<String>,
    pub notices: VecDeque<String>,
    request_sequence: u64,
}

impl AppModel {
    pub fn new(preferences: Preferences) -> Self {
        Self {
            preferences,
            connection: ConnectionState::Connecting,
            control_connection: None,
            snapshot_online: false,
            event_stream_online: false,
            last_event_id: None,
            last_error: None,
            snapshot: None,
            pending_confirmation: None,
            last_receipt: None,
            commands: VecDeque::new(),
            receipt_ids: VecDeque::new(),
            notices: VecDeque::new(),
            request_sequence: 0,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: ControlSnapshot) {
        self.control_connection = Some(snapshot.connection);
        if !snapshot
            .markets
            .iter()
            .any(|market| market.symbol.to_string() == self.preferences.selected_symbol)
            && let Some(first) = snapshot.markets.first()
        {
            self.preferences.selected_symbol = first.symbol.to_string();
        }
        self.snapshot = Some(snapshot);
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

    pub fn notice(&mut self, message: impl Into<String>) {
        self.notices.push_front(message.into());
        self.notices.truncate(MAX_NOTICES);
    }
}

pub const fn requires_operator_confirmation(action: ControlAction) -> bool {
    matches!(
        action,
        ControlAction::Pause | ControlAction::Stop | ControlAction::Flatten
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
    format!("{:.*}", precision, decimal_to_f64(value))
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
        AppModel, PendingConfirmation, Preferences, freshness_age_ms,
        requires_operator_confirmation,
    };

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
    fn command_scope_preserves_the_strategy_gateway_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut model = AppModel::new(Preferences::default());
        let strategy = StrategySummary {
            instance_id: "grid-btc".to_owned(),
            kind: StrategyKind::Grid,
            venue: VenueId::Binance,
            mode: GatewayMode::Test,
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

        assert_eq!(request.mode, GatewayMode::Test);
        assert!(request.expected_confirmation().contains("mode=TEST"));
        Ok(())
    }

    #[test]
    fn pause_stop_and_flatten_require_exact_operator_visible_scope()
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
        assert!(!requires_operator_confirmation(ControlAction::Resume));
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
            mode: GatewayMode::Test,
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
