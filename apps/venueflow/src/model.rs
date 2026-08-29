use std::collections::VecDeque;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, CommandReceipt, ConnectionState, ControlAction, ControlCommandRequest,
    ControlSnapshot, StrategySummary,
};

const MAX_NOTICES: usize = 8;

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

#[derive(Debug)]
pub struct AppModel {
    pub preferences: Preferences,
    /// Connectivity of this secret-free Control API client, not the account runtime projection.
    pub connection: ConnectionState,
    /// The account-runtime state reported by the most recent validated snapshot.
    pub control_connection: Option<ConnectionState>,
    pub last_error: Option<String>,
    pub snapshot: Option<ControlSnapshot>,
    pub pending_confirmation: Option<PendingConfirmation>,
    pub last_receipt: Option<CommandReceipt>,
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
            last_error: None,
            snapshot: None,
            pending_confirmation: None,
            last_receipt: None,
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

    /// Returns false for an already-rendered receipt. SSE replay is expected after reconnects.
    pub fn apply_receipt(&mut self, receipt: CommandReceipt) -> bool {
        if self.receipt_ids.iter().any(|id| id == &receipt.receipt_id) {
            return false;
        }
        self.receipt_ids.push_back(receipt.receipt_id.clone());
        while self.receipt_ids.len() > MAX_NOTICES {
            self.receipt_ids.pop_front();
        }
        self.last_receipt = Some(receipt);
        true
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
        CONTROL_SCHEMA_VERSION, CommandReceipt, ConnectionState, ControlAction, ControlSnapshot,
        GatewayMode, StrategyKind, StrategyLifecycle, StrategySummary, VenueId,
    };

    use super::{AppModel, Preferences};

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
    fn replayed_receipts_are_idempotent_but_new_terminal_receipts_remain_visible() {
        let mut model = AppModel::new(Preferences::default());
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
    }
}
