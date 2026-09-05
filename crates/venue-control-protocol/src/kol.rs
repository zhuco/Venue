//! Secret-free KOL, follow and terminal contracts for the Binance-only MVP.

use std::collections::BTreeSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{OrderSide, PositionSide, Symbol, is_canonical_trading_account_id};

pub const KOL_SCHEMA_VERSION: u16 = 1;
/// Terminal order wire v2 replaces the ambiguous legacy limit-GTC shape with explicit
/// post-only limits and confirmed close-only market orders.
pub const TERMINAL_SCHEMA_VERSION: u16 = 2;
pub const KOL_INVITE_RESOLVE_PATH: &str = "/v2/public/kol/invites";
pub const KOL_PROFILE_PATH: &str = "/v2/kol/profile";
pub const KOL_FOLLOW_SETTINGS_PATH: &str = "/v2/kol/follow/settings";
pub const KOL_FOLLOW_LIFECYCLE_PATH: &str = "/v2/kol/follow/lifecycle";
pub const KOL_TERMINAL_ORDER_PATH: &str = "/v2/kol/terminal/orders";
pub const KOL_TERMINAL_CANCEL_PATH: &str = "/v2/kol/terminal/orders/cancel";
pub const KOL_EXECUTION_STATUS_PATH: &str = "/v2/kol/executions";
pub const KOL_TERMINAL_ACCOUNT_PATH: &str = "/v2/kol/terminal/account";

pub const MAX_KOL_NAME_CHARS: usize = 40;
pub const MAX_KOL_TITLE_CHARS: usize = 80;
pub const MAX_KOL_DESCRIPTION_CHARS: usize = 2_000;
pub const MAX_ALLOWED_SYMBOLS: usize = 32;
pub const MAX_DEVIATION_BPS: u32 = 5_000;
pub const TERMINAL_PROJECTION_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalPositionMode {
    Hedge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOrderState {
    New,
    PartiallyFilled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalProjectionRequest {
    pub schema_version: u16,
    pub credential_id: String,
    pub symbols: Vec<Symbol>,
}

impl TerminalProjectionRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        let symbols = self.symbols.iter().collect::<BTreeSet<_>>();
        if self.schema_version != TERMINAL_PROJECTION_SCHEMA_VERSION
            || !canonical_id(&self.credential_id)
            || self.symbols.is_empty()
            || self.symbols.len() > MAX_ALLOWED_SYMBOLS
            || symbols.len() != self.symbols.len()
        {
            return Err(KolProtocolError::TerminalProjection);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalPosition {
    pub symbol: Symbol,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub entry_price: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub mark_price: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalPositionHistoryEntry {
    pub observed_ms: u64,
    pub position: TerminalPosition,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOpenOrder {
    pub client_order_id: String,
    pub native_order_id: Option<String>,
    pub symbol: Symbol,
    pub order_side: OrderSide,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub filled_quantity: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub limit_price: Option<Decimal>,
    #[serde(default)]
    pub time_in_force: Option<venue_domain::LimitTimeInForce>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub state: TerminalOrderState,
    pub created_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalFill {
    pub native_trade_id: String,
    pub native_order_id: String,
    pub symbol: Symbol,
    pub order_side: OrderSide,
    pub position_side: PositionSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub maker: Option<bool>,
    pub occurred_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalAsset {
    pub asset: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub equity: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub available_margin: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalAccountProjection {
    pub schema_version: u16,
    pub credential_id: String,
    pub trading_account_id: String,
    pub observed_ms: u64,
    pub persisted_ms: u64,
    pub private_generation: u64,
    pub position_mode: TerminalPositionMode,
    pub positions: Vec<TerminalPosition>,
    #[serde(default)]
    pub position_history: Vec<TerminalPositionHistoryEntry>,
    pub open_orders: Vec<TerminalOpenOrder>,
    pub fills: Vec<TerminalFill>,
    pub assets: Vec<TerminalAsset>,
}

impl TerminalAccountProjection {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != TERMINAL_PROJECTION_SCHEMA_VERSION
            || !canonical_id(&self.credential_id)
            || !is_canonical_trading_account_id(&self.trading_account_id)
            || self.observed_ms == 0
            || self.persisted_ms < self.observed_ms
            || self.private_generation == 0
            || self.positions.iter().any(|position| {
                position.position_side == PositionSide::Net
                    || position.quantity == Decimal::MAX
                    || position.quantity == Decimal::MIN
            })
            || self
                .position_history
                .iter()
                .any(|entry| entry.observed_ms == 0)
            || self.open_orders.iter().any(|order| {
                order.client_order_id.trim().is_empty()
                    || order.position_side == PositionSide::Net
                    || !positive(order.quantity)
            })
            || self.fills.iter().any(|fill| {
                fill.native_trade_id.trim().is_empty()
                    || fill.native_order_id.trim().is_empty()
                    || fill.position_side == PositionSide::Net
                    || !positive(fill.quantity)
                    || !positive(fill.price)
            })
            || self.assets.iter().any(|asset| {
                asset.asset.trim().is_empty()
                    || asset.equity == Decimal::MAX
                    || asset.equity == Decimal::MIN
            })
        {
            return Err(KolProtocolError::TerminalProjection);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KolProfileState {
    Draft,
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KolPublicProfile {
    pub kol_id: String,
    pub name: String,
    pub title: String,
    pub description: String,
    pub state: KolProfileState,
    pub revision: u64,
}

impl KolPublicProfile {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if !canonical_id(&self.kol_id) || self.revision == 0 {
            return Err(KolProtocolError::Identity);
        }
        validate_profile_text(&self.name, &self.title, &self.description)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InviteResolution {
    pub schema_version: u16,
    pub profile: KolPublicProfile,
}

impl InviteResolution {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != KOL_SCHEMA_VERSION
            || self.profile.state != KolProfileState::Enabled
        {
            return Err(KolProtocolError::SchemaOrState);
        }
        self.profile.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KolProfileUpdateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub name: String,
    pub title: String,
    pub description: String,
    pub expected_revision: u64,
}

impl KolProfileUpdateRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != KOL_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || self.expected_revision == 0
        {
            return Err(KolProtocolError::Identity);
        }
        validate_profile_text(&self.name, &self.title, &self.description)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FollowLifecycleState {
    Paused,
    Active,
    NeedsAttention,
    Disabled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowRiskSettings {
    pub credential_id: String,
    #[serde(default)]
    pub sizing: crate::follow_sizing::FollowSizing,
    #[serde(with = "rust_decimal::serde::str")]
    pub allocated_capital: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_total_notional: Decimal,
    pub max_deviation_bps: u32,
    pub allowed_symbols: Vec<Symbol>,
}

impl FollowRiskSettings {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        let unique_symbols = self
            .allowed_symbols
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if !canonical_id(&self.credential_id)
            || !self.sizing.valid_for(self.max_order_notional)
            || !positive(self.allocated_capital)
            || !positive(self.multiplier)
            || !positive(self.max_order_notional)
            || !positive(self.max_total_notional)
            || self.max_order_notional > self.max_total_notional
            || self.max_deviation_bps > MAX_DEVIATION_BPS
            || self.allowed_symbols.is_empty()
            || self.allowed_symbols.len() > MAX_ALLOWED_SYMBOLS
            || unique_symbols.len() != self.allowed_symbols.len()
        {
            return Err(KolProtocolError::RiskSettings);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowSettingsUpsertRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub settings: FollowRiskSettings,
    pub expected_revision: Option<u64>,
}

impl FollowSettingsUpsertRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != KOL_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || self.expected_revision == Some(0)
        {
            return Err(KolProtocolError::Identity);
        }
        self.settings.validate()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FollowLifecycleAction {
    Activate,
    Pause,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowLifecycleRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub relation_id: String,
    pub expected_revision: u64,
    pub action: FollowLifecycleAction,
    pub risk_confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowRelationSummary {
    pub relation_id: String,
    pub state: FollowLifecycleState,
    pub revision: u64,
    pub settings: FollowRiskSettings,
    pub activation_requested: bool,
}

impl FollowRelationSummary {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if !canonical_id(&self.relation_id) || self.revision == 0 {
            return Err(KolProtocolError::Identity);
        }
        self.settings.validate()
    }
}

impl FollowLifecycleRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != KOL_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.relation_id)
            || self.expected_revision == 0
            || (self.action == FollowLifecycleAction::Activate && !self.risk_confirmed)
            || (self.action == FollowLifecycleAction::Pause && self.risk_confirmed)
        {
            return Err(KolProtocolError::Lifecycle);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalAction {
    OpenLong,
    CloseLong,
    OpenShort,
    CloseShort,
}

impl TerminalAction {
    #[must_use]
    pub const fn is_close(self) -> bool {
        matches!(self, Self::CloseLong | Self::CloseShort)
    }

    #[must_use]
    pub const fn position_side(self) -> PositionSide {
        match self {
            Self::OpenLong | Self::CloseLong => PositionSide::Long,
            Self::OpenShort | Self::CloseShort => PositionSide::Short,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOrderKind {
    Market,
    LimitPostOnly,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalOrderRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub action: TerminalAction,
    pub order_kind: TerminalOrderKind,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_notional: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub limit_price: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub close_quantity_cap: Option<Decimal>,
    pub market_risk_confirmed: bool,
}

impl TerminalOrderRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != TERMINAL_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.credential_id)
        {
            return Err(KolProtocolError::TerminalOrder);
        }
        match self.order_kind {
            TerminalOrderKind::Market => {
                if !self.action.is_close()
                    || self.limit_price.is_some()
                    || !self.market_risk_confirmed
                    || self.quote_notional != Decimal::ZERO
                {
                    return Err(KolProtocolError::TerminalOrder);
                }
            }
            TerminalOrderKind::LimitPostOnly => {
                if !positive(self.quote_notional)
                    || self.limit_price.is_none_or(|price| !positive(price))
                    || self.market_risk_confirmed
                {
                    return Err(KolProtocolError::TerminalOrder);
                }
            }
        }
        if self.action.is_close() != self.close_quantity_cap.is_some_and(positive) {
            return Err(KolProtocolError::TerminalOrder);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalCancelRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub symbol: Symbol,
    pub native_order_id: String,
}

impl TerminalCancelRequest {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        if self.schema_version != TERMINAL_SCHEMA_VERSION
            || !canonical_id(&self.request_id)
            || !canonical_id(&self.credential_id)
            || !bounded_plain(&self.native_order_id, 1, 128)
        {
            return Err(KolProtocolError::TerminalOrder);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorCommandOrigin {
    Copy,
    Terminal,
    Grid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorCommandPhase {
    Open,
    Close,
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorOrderKind {
    Market,
    LimitPostOnly,
    LimitGtc,
    CancelExact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorCommandState {
    Pending,
    Sending,
    Accepted,
    Rejected,
    ReconcileRequired,
    Reconciled,
    Cancelled,
}

impl ExecutorCommandState {
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Sending | Self::Cancelled)
                | (
                    Self::Sending,
                    Self::Accepted | Self::Rejected | Self::ReconcileRequired
                )
                | (Self::Accepted, Self::Reconciled | Self::ReconcileRequired)
                | (Self::ReconcileRequired, Self::Reconciled)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorCommandSummary {
    pub command_id: String,
    pub request_id: Option<String>,
    pub origin: ExecutorCommandOrigin,
    pub phase: ExecutorCommandPhase,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub position_side: Option<PositionSide>,
    pub order_side: Option<OrderSide>,
    pub order_kind: ExecutorOrderKind,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub requested_quantity: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub limit_price: Option<Decimal>,
    pub state: ExecutorCommandState,
    pub native_order_id: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub sanitized_error_code: Option<String>,
}

impl ExecutorCommandSummary {
    pub fn validate(&self) -> Result<(), KolProtocolError> {
        // Grid stores bounded opaque ledger IDs, not account UUIDs. Keep the stricter
        // identity contract for terminal/copy commands and every account/request ID.
        let valid_command_id = match self.origin {
            ExecutorCommandOrigin::Grid => bounded_plain(&self.command_id, 1, 128),
            ExecutorCommandOrigin::Copy | ExecutorCommandOrigin::Terminal => {
                canonical_id(&self.command_id)
            }
        };
        if !valid_command_id
            || !is_canonical_trading_account_id(&self.trading_account_id)
            || self
                .request_id
                .as_deref()
                .is_some_and(|id| !canonical_id(id))
            || self.created_ms == 0
            || self.updated_ms < self.created_ms
            || (self.phase == ExecutorCommandPhase::Cancel) != self.position_side.is_none()
            || (self.phase == ExecutorCommandPhase::Cancel) != self.order_side.is_none()
            || (self.phase == ExecutorCommandPhase::Cancel)
                != (self.order_kind == ExecutorOrderKind::CancelExact)
            || matches!(
                self.order_kind,
                ExecutorOrderKind::LimitPostOnly | ExecutorOrderKind::LimitGtc
            ) != self.limit_price.is_some()
            || (self.order_kind == ExecutorOrderKind::LimitGtc
                && self.origin != ExecutorCommandOrigin::Copy)
            || self.limit_price.is_some_and(|price| !positive(price))
            || (self.phase != ExecutorCommandPhase::Cancel)
                != self.requested_quantity.is_some_and(positive)
            || self
                .position_side
                .is_some_and(|side| side == PositionSide::Net)
            || self
                .native_order_id
                .as_deref()
                .is_some_and(|id| !bounded_plain(id, 1, 128))
            || self
                .sanitized_error_code
                .as_deref()
                .is_some_and(|code| !bounded_plain(code, 1, 64))
        {
            return Err(KolProtocolError::CommandSummary);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum KolProtocolError {
    #[error("KOL identity or revision is invalid")]
    Identity,
    #[error("KOL schema version or public state is invalid")]
    SchemaOrState,
    #[error("KOL profile text is invalid")]
    ProfileText,
    #[error("follow risk settings are invalid")]
    RiskSettings,
    #[error("follow lifecycle transition is invalid")]
    Lifecycle,
    #[error("terminal order is invalid")]
    TerminalOrder,
    #[error("executor command summary is invalid")]
    CommandSummary,
    #[error("terminal account projection is invalid")]
    TerminalProjection,
}

fn validate_profile_text(
    name: &str,
    title: &str,
    description: &str,
) -> Result<(), KolProtocolError> {
    if !bounded_plain(name, 1, MAX_KOL_NAME_CHARS)
        || !bounded_plain(title, 1, MAX_KOL_TITLE_CHARS)
        || description.chars().count() > MAX_KOL_DESCRIPTION_CHARS
        || description
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\r' | '\n'))
    {
        return Err(KolProtocolError::ProfileText);
    }
    Ok(())
}

fn bounded_plain(value: &str, minimum: usize, maximum: usize) -> bool {
    let trimmed = value.trim();
    (minimum..=maximum).contains(&trimmed.chars().count()) && !value.chars().any(char::is_control)
}

fn canonical_id(value: &str) -> bool {
    is_canonical_trading_account_id(value)
}

fn positive(value: Decimal) -> bool {
    value.is_sign_positive() && !value.is_zero()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID_1: &str = "00000000-0000-4000-8000-000000000001";
    const ID_2: &str = "00000000-0000-4000-8000-000000000002";

    fn settings() -> Result<FollowRiskSettings, Box<dyn std::error::Error>> {
        Ok(FollowRiskSettings {
            credential_id: ID_1.into(),
            sizing: Default::default(),
            allocated_capital: Decimal::new(1_000, 0),
            multiplier: Decimal::ONE,
            max_order_notional: Decimal::new(100, 0),
            max_total_notional: Decimal::new(1_000, 0),
            max_deviation_bps: 100,
            allowed_symbols: vec!["BTC/USDT".parse()?, "ETH/USDT".parse()?],
        })
    }

    #[test]
    fn profile_is_bounded_and_rejects_unknown_wire_fields() {
        let profile = KolPublicProfile {
            kol_id: ID_1.into(),
            name: "示例 KOL".into(),
            title: "双向合约交易".into(),
            description: "只展示可编辑说明；固定风险提示由平台提供。".into(),
            state: KolProfileState::Enabled,
            revision: 1,
        };
        assert_eq!(profile.validate(), Ok(()));
        let raw = format!(
            r#"{{"kol_id":"{ID_1}","name":"KOL","title":"title","description":"","state":"enabled","revision":1,"html":"<script>"}}"#
        );
        assert!(serde_json::from_str::<KolPublicProfile>(&raw).is_err());
    }

    #[test]
    fn follow_settings_require_unique_symbols_and_bounded_risk()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut valid = settings()?;
        assert_eq!(valid.validate(), Ok(()));
        valid.allowed_symbols.push("BTC/USDT".parse()?);
        assert_eq!(valid.validate(), Err(KolProtocolError::RiskSettings));
        valid = settings()?;
        valid.max_order_notional = valid.max_total_notional + Decimal::ONE;
        assert_eq!(valid.validate(), Err(KolProtocolError::RiskSettings));
        Ok(())
    }

    #[test]
    fn terminal_market_and_close_semantics_are_explicit() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut order = TerminalOrderRequest {
            schema_version: TERMINAL_SCHEMA_VERSION,
            request_id: ID_1.into(),
            credential_id: ID_2.into(),
            symbol: "BTC/USDT".parse()?,
            action: TerminalAction::OpenLong,
            order_kind: TerminalOrderKind::Market,
            quote_notional: Decimal::ZERO,
            limit_price: None,
            close_quantity_cap: None,
            market_risk_confirmed: true,
        };
        assert_eq!(order.validate(), Err(KolProtocolError::TerminalOrder));
        order.action = TerminalAction::CloseShort;
        assert_eq!(order.validate(), Err(KolProtocolError::TerminalOrder));
        order.close_quantity_cap = Some(Decimal::new(2, 3));
        assert_eq!(order.validate(), Ok(()));
        order.action = TerminalAction::OpenLong;
        order.close_quantity_cap = None;
        order.order_kind = TerminalOrderKind::LimitPostOnly;
        order.quote_notional = Decimal::new(10, 0);
        order.market_risk_confirmed = false;
        order.limit_price = Some(Decimal::new(67_000, 0));
        assert_eq!(order.validate(), Ok(()));
        Ok(())
    }

    #[test]
    fn command_state_machine_has_no_implicit_retry_path() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(ExecutorCommandState::Pending.can_transition_to(ExecutorCommandState::Sending));
        assert!(ExecutorCommandState::Pending.can_transition_to(ExecutorCommandState::Cancelled));
        assert!(
            ExecutorCommandState::Sending
                .can_transition_to(ExecutorCommandState::ReconcileRequired)
        );
        assert!(ExecutorCommandState::Accepted.can_transition_to(ExecutorCommandState::Reconciled));
        assert!(!ExecutorCommandState::Sending.can_transition_to(ExecutorCommandState::Pending));
        assert!(!ExecutorCommandState::Rejected.can_transition_to(ExecutorCommandState::Sending));
        assert_eq!(
            serde_json::to_string(&ExecutorCommandState::ReconcileRequired)?,
            r#""reconcile_required""#
        );
        Ok(())
    }

    #[test]
    fn grid_command_history_preserves_opaque_ledger_ids() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut summary = ExecutorCommandSummary {
            command_id: format!("gp-{}", "a".repeat(55)),
            request_id: None,
            origin: ExecutorCommandOrigin::Grid,
            phase: ExecutorCommandPhase::Open,
            trading_account_id: ID_1.into(),
            symbol: "SOL/USDC".parse()?,
            position_side: Some(PositionSide::Long),
            order_side: Some(OrderSide::Buy),
            order_kind: ExecutorOrderKind::LimitPostOnly,
            requested_quantity: Some(Decimal::new(5, 2)),
            limit_price: Some(Decimal::from(80)),
            state: ExecutorCommandState::Reconciled,
            native_order_id: Some("123456789".into()),
            created_ms: 10,
            updated_ms: 20,
            sanitized_error_code: None,
        };
        for phase in [ExecutorCommandPhase::Open, ExecutorCommandPhase::Close] {
            summary.phase = phase;
            assert_eq!(summary.validate(), Ok(()));
            let decoded: ExecutorCommandSummary =
                serde_json::from_str(&serde_json::to_string(&summary)?)?;
            assert_eq!(decoded, summary);
            decoded.validate()?;
        }
        for origin in [ExecutorCommandOrigin::Terminal, ExecutorCommandOrigin::Copy] {
            summary.origin = origin;
            assert_eq!(summary.validate(), Err(KolProtocolError::CommandSummary));
            let mut canonical = summary.clone();
            canonical.command_id = ID_2.into();
            assert_eq!(canonical.validate(), Ok(()));
        }
        summary.origin = ExecutorCommandOrigin::Grid;
        for invalid in [
            String::new(),
            " ".into(),
            "gp-\ninvalid".into(),
            "a".repeat(129),
        ] {
            let mut invalid_summary = summary.clone();
            invalid_summary.command_id = invalid;
            assert_eq!(
                invalid_summary.validate(),
                Err(KolProtocolError::CommandSummary)
            );
        }
        let mut invalid_account = summary.clone();
        invalid_account.trading_account_id = summary.command_id.clone();
        assert_eq!(
            invalid_account.validate(),
            Err(KolProtocolError::CommandSummary)
        );
        let mut invalid_request = summary.clone();
        invalid_request.request_id = Some(summary.command_id.clone());
        assert_eq!(
            invalid_request.validate(),
            Err(KolProtocolError::CommandSummary)
        );
        summary.command_id = format!("gc-{}", "b".repeat(55));
        summary.phase = ExecutorCommandPhase::Cancel;
        summary.order_kind = ExecutorOrderKind::CancelExact;
        summary.position_side = None;
        summary.order_side = None;
        summary.requested_quantity = None;
        summary.limit_price = None;
        assert_eq!(summary.validate(), Ok(()));
        Ok(())
    }

    #[test]
    fn copy_gtc_history_preserves_limit_price_without_admitting_gtc_for_other_origins()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut summary = ExecutorCommandSummary {
            command_id: ID_1.into(),
            request_id: None,
            origin: ExecutorCommandOrigin::Copy,
            phase: ExecutorCommandPhase::Open,
            trading_account_id: ID_2.into(),
            symbol: "SOL/USDC".parse()?,
            position_side: Some(PositionSide::Long),
            order_side: Some(OrderSide::Buy),
            order_kind: ExecutorOrderKind::LimitGtc,
            requested_quantity: Some(Decimal::ONE),
            limit_price: Some(Decimal::from(80)),
            state: ExecutorCommandState::Pending,
            native_order_id: None,
            created_ms: 1,
            updated_ms: 1,
            sanitized_error_code: None,
        };
        let wire = serde_json::to_string(&summary)?;
        serde_json::from_str::<ExecutorCommandSummary>(&wire)?.validate()?;
        for price in [None, Some(Decimal::ZERO), Some(Decimal::NEGATIVE_ONE)] {
            summary.limit_price = price;
            assert_eq!(summary.validate(), Err(KolProtocolError::CommandSummary));
        }
        summary.limit_price = Some(Decimal::from(80));
        for origin in [ExecutorCommandOrigin::Terminal, ExecutorCommandOrigin::Grid] {
            summary.origin = origin;
            assert_eq!(summary.validate(), Err(KolProtocolError::CommandSummary));
        }
        Ok(())
    }
}
