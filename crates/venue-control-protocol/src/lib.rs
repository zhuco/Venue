//! Versioned, secret-free DTOs shared by Venue control services and UI clients.
//!
//! These types are query projections and semantic control requests. They never grant physical
//! mutation authority; an account node must independently validate every accepted request.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::Symbol;
use venue_gateway_api::{GatewayMode, VenueId};

pub const CONTROL_SCHEMA_VERSION: u16 = 1;
pub const SNAPSHOT_PATH: &str = "/v1/ui/snapshot";
pub const EVENT_STREAM_PATH: &str = "/v1/ui/events";
pub const COMMAND_PATH: &str = "/v1/control/commands";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connecting,
    Live,
    Degraded,
    Offline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Recovering,
    NeedsAttention,
    Stopped,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    Grid,
    Scalping,
    Copy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyLifecycle {
    Starting,
    Running,
    Paused,
    Rebuilding,
    Stopping,
    Stopped,
    NeedsAttention,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountSummary {
    pub venue: VenueId,
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub health: HealthState,
    pub equity: Decimal,
    pub available_margin: Decimal,
    pub unrealized_pnl: Decimal,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub last_reconciled_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategySummary {
    pub instance_id: String,
    pub kind: StrategyKind,
    pub venue: VenueId,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub lifecycle: StrategyLifecycle,
    pub config_epoch: u64,
    pub open_orders: u32,
    pub long_quantity: Decimal,
    pub short_quantity: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub last_receipt_ms: u64,
    pub attention: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyStatus {
    Planning,
    Tracking,
    Drifting,
    Paused,
    NeedsAttention,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationSummary {
    pub leader_id: String,
    pub follower_instance_id: String,
    pub symbol: Symbol,
    pub target_exposure: Decimal,
    pub actual_exposure: Decimal,
    pub drift: Decimal,
    pub status: CopyStatus,
    pub last_applied_job: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UiBar {
    pub open_time_ms: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UiBookLevel {
    pub price: Decimal,
    pub quantity: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UiTrade {
    pub trade_id: String,
    pub occurred_ms: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub aggressor: AggressorSide,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndicatorValue {
    pub name: String,
    pub value: Decimal,
    pub observed_ms: u64,
    pub source_version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MarketSummary {
    pub symbol: Symbol,
    pub last: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub change_percent_24h: Decimal,
    pub bars: Vec<UiBar>,
    pub bids: Vec<UiBookLevel>,
    pub asks: Vec<UiBookLevel>,
    pub trades: Vec<UiTrade>,
    /// Values are computed by Venue indicators and merely rendered by the UI.
    pub indicators: Vec<IndicatorValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub receipt_id: String,
    pub instance_id: String,
    pub occurred_ms: u64,
    pub action: String,
    pub state: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ControlSnapshot {
    pub schema_version: u16,
    pub generated_ms: u64,
    pub connection: ConnectionState,
    pub accounts: Vec<AccountSummary>,
    pub strategies: Vec<StrategySummary>,
    pub copy_relations: Vec<CopyRelationSummary>,
    pub markets: Vec<MarketSummary>,
    pub ledger: Vec<LedgerEntry>,
}

impl ControlSnapshot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        if self.generated_ms == 0 {
            return Err(ProtocolError::GeneratedTime);
        }
        for account in &self.accounts {
            if !venue_domain::is_canonical_trading_account_id(&account.trading_account_id) {
                return Err(ProtocolError::AccountId);
            }
        }
        for strategy in &self.strategies {
            if strategy.instance_id.trim().is_empty()
                || !venue_domain::is_canonical_trading_account_id(&strategy.trading_account_id)
            {
                return Err(ProtocolError::StrategyIdentity);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Pause,
    Resume,
    Stop,
    Flatten,
}

impl ControlAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "PAUSE",
            Self::Resume => "RESUME",
            Self::Stop => "STOP",
            Self::Flatten => "FLATTEN",
        }
    }

    #[must_use]
    pub const fn requires_confirmation(self) -> bool {
        matches!(self, Self::Stop | Self::Flatten)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlCommandRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub venue: VenueId,
    pub trading_account_id: String,
    pub instance_id: String,
    pub symbol: Symbol,
    pub action: ControlAction,
    pub expected_config_epoch: u64,
    pub confirmation: Option<String>,
}

impl ControlCommandRequest {
    #[must_use]
    pub fn expected_confirmation(&self) -> String {
        format!(
            "{} {} {} {}",
            self.action.as_str(),
            self.venue,
            self.trading_account_id,
            self.symbol
        )
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        if self.request_id.trim().is_empty() || self.instance_id.trim().is_empty() {
            return Err(ProtocolError::RequestIdentity);
        }
        if !venue_domain::is_canonical_trading_account_id(&self.trading_account_id) {
            return Err(ProtocolError::AccountId);
        }
        if self.expected_config_epoch == 0 {
            return Err(ProtocolError::ConfigEpoch);
        }
        if self.action.requires_confirmation()
            && self.confirmation.as_deref() != Some(self.expected_confirmation().as_str())
        {
            return Err(ProtocolError::Confirmation);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Accepted,
    Applied,
    Rejected,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub schema_version: u16,
    pub request_id: String,
    pub state: CommandState,
    pub receipt_id: String,
    pub observed_ms: u64,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ControlEvent {
    Snapshot(ControlSnapshot),
    CommandReceipt(CommandReceipt),
    Notice { observed_ms: u64, message: String },
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported control protocol schema version")]
    SchemaVersion,
    #[error("control snapshot generated time is missing")]
    GeneratedTime,
    #[error("trading account id is not canonical")]
    AccountId,
    #[error("strategy identity is missing or invalid")]
    StrategyIdentity,
    #[error("control request identity is missing")]
    RequestIdentity,
    #[error("control request config epoch must be positive")]
    ConfigEpoch,
    #[error("high-risk control confirmation does not match the exact scope")]
    Confirmation,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(action: ControlAction) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
        Ok(ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            venue: VenueId::Binance,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instance_id: "grid-btc".to_owned(),
            symbol: "BTC/USDT".parse()?,
            action,
            expected_config_epoch: 7,
            confirmation: None,
        })
    }

    #[test]
    fn pause_is_semantic_and_never_needs_a_physical_mutation_token()
    -> Result<(), Box<dyn std::error::Error>> {
        let pause = request(ControlAction::Pause)?;
        assert_eq!(pause.validate(), Ok(()));
        let encoded = serde_json::to_string(&pause)?;
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("api_key"));
        assert!(!encoded.contains("writer"));
        Ok(())
    }

    #[test]
    fn stop_and_flatten_require_exact_human_visible_scope() -> Result<(), Box<dyn std::error::Error>>
    {
        for action in [ControlAction::Stop, ControlAction::Flatten] {
            let mut command = request(action)?;
            assert_eq!(command.validate(), Err(ProtocolError::Confirmation));
            command.confirmation = Some(command.expected_confirmation());
            assert_eq!(command.validate(), Ok(()));
            command.confirmation = Some("FLATTEN another-account BTC/USDT".to_owned());
            assert_eq!(command.validate(), Err(ProtocolError::Confirmation));
        }
        Ok(())
    }

    #[test]
    fn snapshot_rejects_invalid_schema_and_account_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut snapshot = ControlSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: 1,
            connection: ConnectionState::Live,
            accounts: Vec::new(),
            strategies: Vec::new(),
            copy_relations: Vec::new(),
            markets: Vec::new(),
            ledger: Vec::new(),
        };
        assert_eq!(snapshot.validate(), Ok(()));
        snapshot.schema_version += 1;
        assert_eq!(snapshot.validate(), Err(ProtocolError::SchemaVersion));
        snapshot.schema_version = CONTROL_SCHEMA_VERSION;
        snapshot.accounts.push(AccountSummary {
            venue: VenueId::Binance,
            mode: GatewayMode::Test,
            trading_account_id: "not-canonical".to_owned(),
            health: HealthState::Unknown,
            equity: Decimal::ZERO,
            available_margin: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            private_generation: 0,
            writer_generation: 0,
            last_reconciled_ms: 0,
        });
        assert_eq!(snapshot.validate(), Err(ProtocolError::AccountId));
        Ok(())
    }
}
