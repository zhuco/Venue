//! Versioned DTOs shared by Venue control services and UI clients. Trading projections
//! are secret-free; `accounts` alone carries redacted, transient authentication inputs.
//!
//! These types are query projections and semantic control requests. They never grant physical
//! mutation authority; an account node must independently validate every accepted request.
pub mod accounts;
mod trade;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
pub use trade::{TradeIntent, TradingAction, TradingOrderType, TradingTimeInForce};
use venue_domain::{Asset, Symbol};
pub use venue_gateway_api::{GatewayMode, VenueId};
mod ui_event;
pub use ui_event::{UiAccountScope, UiEventEnvelope, UiEventKind, UiEventNotification};
fn deserialize_live_mode<'de, D>(deserializer: D) -> Result<GatewayMode, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mode = GatewayMode::deserialize(deserializer)?;
    if mode == GatewayMode::Live {
        Ok(mode)
    } else {
        Err(serde::de::Error::custom("mode must be exactly LIVE"))
    }
}
pub const CONTROL_SCHEMA_VERSION: u16 = 2;
pub const SNAPSHOT_PATH: &str = "/v2/ui/snapshot";
pub const EVENT_STREAM_PATH: &str = "/v2/ui/events";
pub const INDICATOR_SNAPSHOT_PATH: &str = "/v2/indicators/snapshot";
pub const INDICATOR_EVENT_STREAM_PATH: &str = "/v2/indicators/events";
pub const COMMAND_PATH: &str = "/v2/control/commands";
pub const ACCOUNT_DELIVERY_SCHEMA_VERSION: u16 = 2;
pub const ACCOUNT_DELIVERY_CLAIM_PATH: &str = "/v2/account-node/deliveries/claim";
pub const ACCOUNT_DELIVERY_ACK_PATH: &str = "/v2/account-node/deliveries/ack";
pub const ACCOUNT_DELIVERY_RECEIPT_PATH: &str = "/v2/account-node/deliveries/receipts";
/// Node-to-Control read-model upload. This is exact loopback transport only and conveys no
/// writer lease, capability, WAL authority, or dispatch permit.
pub const ACCOUNT_NODE_PROJECTION_PATH: &str = "/v2/account-node/projection";
pub const COPY_RELATION_PATH: &str = "/v2/copy/relations";
pub const COPY_RELATION_CANDIDATES_PATH: &str = "/v2/copy/relation-candidates";
pub const EXECUTION_FACTS_PATH: &str = "/v2/ui/execution-facts";
/// Exact account-node scope for durable semantic delivery. It is deliberately unrelated to a
/// gateway capability, writer generation, WAL position, or physical dispatch permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryBinding {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub instance_id: String,
    pub config_epoch: u64,
}
impl AccountDeliveryBinding {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.mode != GatewayMode::Live {
            return Err(ProtocolError::Mode);
        }
        if !venue_domain::is_canonical_trading_account_id(&self.trading_account_id) {
            return Err(ProtocolError::AccountId);
        }
        if self.instance_id.trim().is_empty() {
            return Err(ProtocolError::StrategyIdentity);
        }
        if self.config_epoch == 0 {
            return Err(ProtocolError::ConfigEpoch);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountDeliveryKind {
    ControlCommand,
    CopySemanticJob,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopySemanticJobDelivery {
    pub job_id: String,
    pub job_digest: [u8; 32],
    pub symbol: Symbol,
    pub manifest: serde_json::Value,
    pub semantic_job: serde_json::Value,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
}
impl CopySemanticJobDelivery {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.job_id.trim().is_empty() || self.job_digest == [0; 32] {
            return Err(ProtocolError::DeliveryIdentity);
        }
        if self.manifest.is_null() || self.semantic_job.is_null() {
            return Err(ProtocolError::DeliveryPayload);
        }
        if self.created_at_ms == 0 || self.expires_at_ms <= self.created_at_ms {
            return Err(ProtocolError::DeliveryTime);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum AccountDeliveryPayload {
    ControlCommand(ControlCommandRequest),
    CopySemanticJob(CopySemanticJobDelivery),
}
impl AccountDeliveryPayload {
    #[must_use]
    pub const fn kind(&self) -> AccountDeliveryKind {
        match self {
            Self::ControlCommand(_) => AccountDeliveryKind::ControlCommand,
            Self::CopySemanticJob(_) => AccountDeliveryKind::CopySemanticJob,
        }
    }
    fn validate_against(&self, binding: &AccountDeliveryBinding) -> Result<(), ProtocolError> {
        match self {
            Self::ControlCommand(command) => {
                command.validate()?;
                if command.venue != binding.venue
                    || command.mode != binding.mode
                    || command.trading_account_id != binding.trading_account_id
                    || command.symbol != binding.symbol
                    || command.instance_id != binding.instance_id
                    || command.expected_config_epoch != binding.config_epoch
                {
                    return Err(ProtocolError::DeliveryBinding);
                }
            }
            Self::CopySemanticJob(job) => {
                job.validate()?;
                if binding.mode != GatewayMode::Live {
                    return Err(ProtocolError::Mode);
                }
                if job.symbol != binding.symbol {
                    return Err(ProtocolError::DeliveryBinding);
                }
            }
        }
        Ok(())
    }
    /// Revalidates a decoded database payload against the exact durable account binding.
    pub fn validate_for_account_delivery(
        &self,
        binding: &AccountDeliveryBinding,
    ) -> Result<(), ProtocolError> {
        binding.validate()?;
        self.validate_against(binding)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountDeliveryPurpose {
    Install,
    ReconcileOnly,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryClaimRequest {
    pub schema_version: u16,
    pub binding: AccountDeliveryBinding,
    pub node_id: String,
    pub lease_duration_ms: u64,
    pub limit: u32,
}
impl AccountDeliveryClaimRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != ACCOUNT_DELIVERY_SCHEMA_VERSION {
            return Err(ProtocolError::DeliverySchemaVersion);
        }
        self.binding.validate()?;
        if self.node_id.trim().is_empty() {
            return Err(ProtocolError::DeliveryIdentity);
        }
        if self.lease_duration_ms == 0 || self.limit == 0 {
            return Err(ProtocolError::DeliveryLease);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryLease {
    pub schema_version: u16,
    pub delivery_id: String,
    pub binding: AccountDeliveryBinding,
    pub node_id: String,
    pub lease_epoch: u64,
    pub leased_at_ms: u64,
    pub expires_at_ms: u64,
    pub purpose: AccountDeliveryPurpose,
}
impl AccountDeliveryLease {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != ACCOUNT_DELIVERY_SCHEMA_VERSION {
            return Err(ProtocolError::DeliverySchemaVersion);
        }
        self.binding.validate()?;
        if self.delivery_id.trim().is_empty()
            || self.node_id.trim().is_empty()
            || self.lease_epoch == 0
        {
            return Err(ProtocolError::DeliveryIdentity);
        }
        if self.leased_at_ms == 0 || self.expires_at_ms <= self.leased_at_ms {
            return Err(ProtocolError::DeliveryLease);
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryClaim {
    pub lease: AccountDeliveryLease,
    pub payload: AccountDeliveryPayload,
}
impl AccountDeliveryClaim {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.lease.validate()?;
        self.payload.validate_against(&self.lease.binding)?;
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryAck {
    pub schema_version: u16,
    pub lease: AccountDeliveryLease,
    pub acknowledged_ms: u64,
    pub durable_inbox_digest: [u8; 32],
}
impl AccountDeliveryAck {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != ACCOUNT_DELIVERY_SCHEMA_VERSION {
            return Err(ProtocolError::DeliverySchemaVersion);
        }
        self.lease.validate()?;
        if self.lease.purpose != AccountDeliveryPurpose::Install
            || self.acknowledged_ms < self.lease.leased_at_ms
            || self.durable_inbox_digest == [0; 32]
        {
            return Err(ProtocolError::DeliveryAck);
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountDeliveryReceiptState {
    Applied,
    Rejected,
    Unknown,
    Reconciled,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountDeliveryReceipt {
    pub schema_version: u16,
    pub lease: AccountDeliveryLease,
    pub receipt_id: String,
    pub state: AccountDeliveryReceiptState,
    pub observed_ms: u64,
    pub account_fact_digest: [u8; 32],
    pub detail: String,
}
impl AccountDeliveryReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != ACCOUNT_DELIVERY_SCHEMA_VERSION {
            return Err(ProtocolError::DeliverySchemaVersion);
        }
        self.lease.validate()?;
        if self.receipt_id.trim().is_empty() || self.observed_ms < self.lease.leased_at_ms {
            return Err(ProtocolError::ReceiptIdentity);
        }
        if matches!(
            self.state,
            AccountDeliveryReceiptState::Unknown | AccountDeliveryReceiptState::Rejected
        ) && self.detail.trim().is_empty()
        {
            return Err(ProtocolError::ReceiptDetail);
        }
        if self.state == AccountDeliveryReceiptState::Reconciled
            && (self.lease.purpose != AccountDeliveryPurpose::ReconcileOnly
                || self.account_fact_digest == [0; 32])
        {
            return Err(ProtocolError::DeliveryReceipt);
        }
        if self.lease.purpose == AccountDeliveryPurpose::ReconcileOnly
            && self.state != AccountDeliveryReceiptState::Reconciled
        {
            return Err(ProtocolError::DeliveryReceipt);
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
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
pub struct AccountBalanceSummary {
    pub asset: Asset,
    pub equity: Decimal,
    pub available_margin: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountSummary {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub health: HealthState,
    /// No scalar aggregate is emitted across assets. `None` means the source supplied balances
    /// in more than one currency or omitted an account-wide value.
    pub equity: Option<Decimal>,
    pub available_margin: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    #[serde(default)]
    pub balances: Vec<AccountBalanceSummary>,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub last_reconciled_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StrategySummary {
    pub instance_id: String,
    pub kind: StrategyKind,
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub lifecycle: StrategyLifecycle,
    pub config_epoch: u64,
    pub open_orders: u32,
    pub long_quantity: Decimal,
    pub short_quantity: Decimal,
    /// Strategy-level PnL is omitted until an adapter supplies a signed value.
    pub realized_pnl: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
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
    pub relation_id: String,
    pub revision: u64,
    pub leader_id: String,
    pub follower_instance_id: String,
    pub symbol: Symbol,
    pub target_exposure: Decimal,
    pub actual_exposure: Decimal,
    pub drift: Decimal,
    pub status: CopyStatus,
    pub last_applied_job: Option<String>,
}
/// Exact LIVE identity of one endpoint in a Copy relation. This remains a control-plane
/// declaration; it never represents an account writer, credential, or dispatch authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationBinding {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub instance_id: String,
    pub symbol: Symbol,
}
impl CopyRelationBinding {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.mode != GatewayMode::Live {
            return Err(ProtocolError::Mode);
        }
        if !venue_domain::is_canonical_trading_account_id(&self.trading_account_id) {
            return Err(ProtocolError::AccountId);
        }
        if self.instance_id.trim().is_empty() {
            return Err(ProtocolError::StrategyIdentity);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRiskPolicy {
    pub max_total_notional: Decimal,
    pub max_order_notional: Decimal,
    pub max_leverage: Decimal,
}
impl CopyRiskPolicy {
    fn validate(&self) -> Result<(), ProtocolError> {
        if !positive(self.max_total_notional)
            || !positive(self.max_order_notional)
            || self.max_order_notional > self.max_total_notional
            || !positive(self.max_leverage)
        {
            return Err(ProtocolError::CopyRelationPolicy);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyLifecyclePolicy {
    Active,
    Paused,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationConfig {
    pub relation_id: String,
    pub leader: CopyRelationBinding,
    pub follower: CopyRelationBinding,
    pub allocated_capital: Decimal,
    pub multiplier: Decimal,
    pub safety_reserve_rate: Decimal,
    pub risk: CopyRiskPolicy,
    pub lifecycle: CopyLifecyclePolicy,
}
impl CopyRelationConfig {
    /// Stable commitment carried by every Copy snapshot and child job.  It deliberately covers
    /// both endpoints and every policy input, so a row revision alone cannot be replayed under a
    /// changed risk policy.
    #[must_use]
    pub fn policy_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        let leader_symbol = self.leader.symbol.to_string();
        let follower_symbol = self.follower.symbol.to_string();
        let allocated_capital = self.allocated_capital.to_string();
        let multiplier = self.multiplier.to_string();
        let safety_reserve_rate = self.safety_reserve_rate.to_string();
        let max_total_notional = self.risk.max_total_notional.to_string();
        let max_order_notional = self.risk.max_order_notional.to_string();
        let max_leverage = self.risk.max_leverage.to_string();
        for value in [
            self.relation_id.as_str(),
            self.leader.venue.as_str(),
            self.leader.trading_account_id.as_str(),
            self.leader.instance_id.as_str(),
            &leader_symbol,
            self.follower.venue.as_str(),
            self.follower.trading_account_id.as_str(),
            self.follower.instance_id.as_str(),
            &follower_symbol,
            &allocated_capital,
            &multiplier,
            &safety_reserve_rate,
            &max_total_notional,
            &max_order_notional,
            &max_leverage,
            match self.lifecycle {
                CopyLifecyclePolicy::Active => "active",
                CopyLifecyclePolicy::Paused => "paused",
            },
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
        digest.finalize().into()
    }
}
/// Versioned query projection for one durable Copy relation configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationRecord {
    pub relation: CopyRelationConfig,
    pub revision: u64,
}
/// A server-derived, credential-free endpoint that may be selected in a Copy relation form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationCandidate {
    pub binding: CopyRelationBinding,
    pub lifecycle: StrategyLifecycle,
    pub config_epoch: u64,
}
impl CopyRelationCandidate {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.binding.validate()?;
        if self.config_epoch == 0 {
            return Err(ProtocolError::CopyRelationRevision);
        }
        Ok(())
    }
}
impl CopyRelationRecord {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.relation.validate()?;
        if self.revision == 0 {
            return Err(ProtocolError::CopyRelationRevision);
        }
        Ok(())
    }
}
impl CopyRelationConfig {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if !is_uuid(&self.relation_id) {
            return Err(ProtocolError::CopyRelationIdentity);
        }
        self.leader.validate()?;
        self.follower.validate()?;
        if self.leader == self.follower
            || !positive(self.allocated_capital)
            || !positive(self.multiplier)
            || self.safety_reserve_rate.is_sign_negative()
            || self.safety_reserve_rate >= Decimal::ONE
        {
            return Err(ProtocolError::CopyRelationPolicy);
        }
        self.risk.validate()
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationUpsertRequest {
    pub schema_version: u16,
    /// Client-generated UUID that is held stable until the relation mutation has a terminal receipt.
    pub request_id: String,
    pub relation: CopyRelationConfig,
    /// `0` creates a relation. A positive revision is required for every edit to make retries and
    /// concurrent operators fail closed instead of silently overwriting risk policy.
    pub expected_revision: Option<u64>,
}
impl CopyRelationUpsertRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        self.relation.validate()?;
        if !is_uuid(&self.request_id) {
            return Err(ProtocolError::CopyRelationIdentity);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyRelationReceiptState {
    Created,
    Updated,
    Existing,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CopyRelationReceipt {
    pub schema_version: u16,
    pub relation_id: String,
    pub revision: u64,
    pub state: CopyRelationReceiptState,
    pub observed_ms: u64,
}
impl CopyRelationReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        if !is_uuid(&self.relation_id) || self.revision == 0 || self.observed_ms == 0 {
            return Err(ProtocolError::CopyRelationIdentity);
        }
        Ok(())
    }
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
/// One exact LIVE account/symbol scope for a read-only derived market projection. This scope is
/// intentionally not an execution identity and carries no writer, WAL, credential, or permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndicatorBinding {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub symbol: Symbol,
}
impl IndicatorBinding {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.mode != GatewayMode::Live {
            return Err(ProtocolError::Mode);
        }
        if !venue_domain::is_canonical_trading_account_id(&self.trading_account_id) {
            return Err(ProtocolError::AccountId);
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
/// The public-evidence cursor for one input family. `age_ms` is not caller interpretation: it
/// must exactly equal the containing frame observation time minus this event time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndicatorProvenance {
    pub source: String,
    pub generation: u64,
    pub sequence: u64,
    pub event_time_ms: u64,
    pub age_ms: u64,
    pub feature_version: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndicatorFeatureValues {
    pub mid_price: Decimal,
    pub fair_price: Decimal,
    pub spread_bps: Decimal,
    pub depth_quote: Decimal,
    pub book_imbalance: Decimal,
    pub trade_imbalance: Decimal,
    pub short_return_bps: Decimal,
    pub trend_efficiency: Decimal,
    pub bandwidth_expansion: Decimal,
    pub expected_move_bps: Decimal,
    pub toxicity: Decimal,
}
/// A fully-ready, bounded-age FeatureFrame rendered as a secret-free Control projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndicatorFrameProjection {
    pub schema_version: u16,
    pub binding: IndicatorBinding,
    pub generation: u64,
    pub watermark_ms: u64,
    pub observed_ms: u64,
    pub maximum_age_ms: u64,
    pub provenance: Vec<IndicatorProvenance>,
    pub values: IndicatorFeatureValues,
}
impl IndicatorFrameProjection {
    pub fn validate_at(&self, snapshot_generated_ms: u64) -> Result<(), ProtocolError> {
        self.binding.validate()?;
        if self.schema_version != CONTROL_SCHEMA_VERSION
            || self.generation == 0
            || self.watermark_ms == 0
            || self.observed_ms == 0
            || self.maximum_age_ms == 0
            || self.observed_ms > snapshot_generated_ms
            || self.watermark_ms > self.observed_ms
        {
            return Err(ProtocolError::IndicatorIdentity);
        }
        let mut sources = BTreeSet::new();
        for provenance in &self.provenance {
            if provenance.source.trim().is_empty()
                || provenance.generation != self.generation
                || provenance.sequence == 0
                || provenance.event_time_ms == 0
                || provenance.event_time_ms > self.observed_ms
                || provenance.age_ms != self.observed_ms - provenance.event_time_ms
                || provenance.age_ms > self.maximum_age_ms
                || snapshot_generated_ms - provenance.event_time_ms > self.maximum_age_ms
                || provenance.feature_version.trim().is_empty()
                || !sources.insert(provenance.source.as_str())
            {
                return Err(ProtocolError::IndicatorProvenance);
            }
        }
        if !["book", "trades", "bars"]
            .into_iter()
            .all(|source| sources.contains(source))
        {
            return Err(ProtocolError::IndicatorProvenance);
        }
        let values = &self.values;
        if !positive(values.mid_price)
            || !positive(values.fair_price)
            || values.spread_bps.is_sign_negative()
            || values.depth_quote.is_sign_negative()
            || values.book_imbalance < -Decimal::ONE
            || values.book_imbalance > Decimal::ONE
            || values.trade_imbalance < -Decimal::ONE
            || values.trade_imbalance > Decimal::ONE
            || values.trend_efficiency < -Decimal::ONE
            || values.trend_efficiency > Decimal::ONE
            || values.expected_move_bps.is_sign_negative()
            || values.toxicity < Decimal::ZERO
            || values.toxicity > Decimal::ONE
        {
            return Err(ProtocolError::IndicatorValues);
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndicatorSnapshot {
    pub schema_version: u16,
    pub generated_ms: u64,
    pub frames: Vec<IndicatorFrameProjection>,
}
impl IndicatorSnapshot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION || self.generated_ms == 0 {
            return Err(ProtocolError::IndicatorIdentity);
        }
        let mut identities = BTreeSet::new();
        for frame in &self.frames {
            frame.validate_at(self.generated_ms)?;
            let binding = &frame.binding;
            if !identities.insert((
                binding.venue,
                binding.mode,
                binding.trading_account_id.as_str(),
                &binding.symbol,
            )) {
                return Err(ProtocolError::DuplicateIdentity);
            }
        }
        Ok(())
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
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
mod copy_planning;
mod execution_facts;
mod node_projection;
pub use copy_planning::{
    CopyPlanningFact, CopyPlanningFactRole, MAX_COPY_PLANNING_FACT_TTL_MS, MAX_COPY_PLANNING_FACTS,
};
pub use execution_facts::{
    AccountHealthFact, AccountRiskFact, CopyDriftFact, CopyExecutionEvidence,
    CopyExecutionEvidenceEncoding, CopyExecutionFact, CopyExecutionPhaseProjection,
    CopyExecutionStateProjection, CopyLedgerFact, ExecutionFactBinding, ExecutionFactsSnapshot,
    ReconciliationFact, SignedFillFact, SignedOrderFact, SignedPositionFact,
};
pub use node_projection::NodeProjectionEnvelope;

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
        let mut account_identities = BTreeSet::new();
        for account in &self.accounts {
            if account.mode != GatewayMode::Live {
                return Err(ProtocolError::Mode);
            }
            if !venue_domain::is_canonical_trading_account_id(&account.trading_account_id) {
                return Err(ProtocolError::AccountId);
            }
            if account.last_reconciled_ms > self.generated_ms
                || (account.last_reconciled_ms != 0 && account.private_generation == 0)
            {
                return Err(ProtocolError::SnapshotTime);
            }
            let mut assets = BTreeSet::new();
            if account
                .balances
                .iter()
                .any(|balance| !assets.insert(balance.asset.as_str()))
            {
                return Err(ProtocolError::DuplicateIdentity);
            }
            if !account_identities.insert((
                account.venue,
                account.mode,
                account.trading_account_id.as_str(),
            )) {
                return Err(ProtocolError::DuplicateIdentity);
            }
        }
        let mut strategy_identities = BTreeSet::new();
        for strategy in &self.strategies {
            if strategy.mode != GatewayMode::Live {
                return Err(ProtocolError::Mode);
            }
            if strategy.instance_id.trim().is_empty()
                || !venue_domain::is_canonical_trading_account_id(&strategy.trading_account_id)
            {
                return Err(ProtocolError::StrategyIdentity);
            }
            if strategy.config_epoch == 0
                || strategy.long_quantity.is_sign_negative()
                || strategy.short_quantity.is_sign_negative()
            {
                return Err(ProtocolError::SnapshotValue);
            }
            if strategy.last_receipt_ms > self.generated_ms {
                return Err(ProtocolError::SnapshotTime);
            }
            if strategy
                .attention
                .as_ref()
                .is_some_and(|attention| attention.trim().is_empty())
                || (strategy.lifecycle == StrategyLifecycle::NeedsAttention
                    && strategy.attention.is_none())
            {
                return Err(ProtocolError::SnapshotContent);
            }
            if !strategy_identities.insert(strategy.instance_id.as_str()) {
                return Err(ProtocolError::DuplicateIdentity);
            }
            if !account_identities.iter().any(|(venue, mode, account_id)| {
                *venue == strategy.venue
                    && *mode == strategy.mode
                    && *account_id == strategy.trading_account_id
            }) {
                return Err(ProtocolError::StrategyIdentity);
            }
        }
        validate_copy_relations(&self.copy_relations, &strategy_identities)?;
        validate_markets(&self.markets, self.generated_ms)?;
        validate_ledger(&self.ledger, self.generated_ms)?;
        Ok(())
    }
}
fn validate_copy_relations(
    relations: &[CopyRelationSummary],
    strategy_identities: &BTreeSet<&str>,
) -> Result<(), ProtocolError> {
    let mut identities = BTreeSet::new();
    for relation in relations {
        if !is_uuid(&relation.relation_id)
            || relation.revision == 0
            || relation.leader_id.trim().is_empty()
            || relation.follower_instance_id.trim().is_empty()
            || relation.leader_id == relation.follower_instance_id
            || !strategy_identities.contains(relation.follower_instance_id.as_str())
            || relation
                .last_applied_job
                .as_ref()
                .is_some_and(|job| job.trim().is_empty())
        {
            return Err(ProtocolError::SnapshotContent);
        }
        if !identities.insert((
            relation.relation_id.as_str(),
            relation.leader_id.as_str(),
            relation.follower_instance_id.as_str(),
            &relation.symbol,
        )) {
            return Err(ProtocolError::DuplicateIdentity);
        }
    }
    Ok(())
}
fn is_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}
fn validate_markets(markets: &[MarketSummary], generated_ms: u64) -> Result<(), ProtocolError> {
    let mut market_identities = BTreeSet::new();
    for market in markets {
        if !market_identities.insert(&market.symbol) {
            return Err(ProtocolError::DuplicateIdentity);
        }
        if !positive(market.last)
            || !positive(market.bid)
            || !positive(market.ask)
            || market.bid > market.ask
        {
            return Err(ProtocolError::SnapshotValue);
        }
        validate_bars(&market.bars, generated_ms)?;
        validate_book(&market.bids)?;
        validate_book(&market.asks)?;
        validate_trades(&market.trades, generated_ms)?;
        validate_indicators(&market.indicators, generated_ms)?;
    }
    Ok(())
}
fn validate_bars(bars: &[UiBar], generated_ms: u64) -> Result<(), ProtocolError> {
    let mut previous_open_time = None;
    for bar in bars {
        if bar.open_time_ms == 0 || bar.open_time_ms > generated_ms {
            return Err(ProtocolError::SnapshotTime);
        }
        if previous_open_time.is_some_and(|previous| bar.open_time_ms <= previous) {
            return Err(ProtocolError::SnapshotContent);
        }
        if !positive(bar.open)
            || !positive(bar.high)
            || !positive(bar.low)
            || !positive(bar.close)
            || bar.volume.is_sign_negative()
            || bar.low > bar.open.min(bar.close)
            || bar.high < bar.open.max(bar.close)
            || bar.low > bar.high
        {
            return Err(ProtocolError::SnapshotValue);
        }
        previous_open_time = Some(bar.open_time_ms);
    }
    Ok(())
}
fn validate_book(levels: &[UiBookLevel]) -> Result<(), ProtocolError> {
    let mut prices = BTreeSet::new();
    for level in levels {
        if !positive(level.price) || !positive(level.quantity) {
            return Err(ProtocolError::SnapshotValue);
        }
        if !prices.insert(level.price) {
            return Err(ProtocolError::DuplicateIdentity);
        }
    }
    Ok(())
}
fn validate_trades(trades: &[UiTrade], generated_ms: u64) -> Result<(), ProtocolError> {
    let mut identities = BTreeSet::new();
    for trade in trades {
        if trade.trade_id.trim().is_empty() {
            return Err(ProtocolError::SnapshotContent);
        }
        if !identities.insert(trade.trade_id.as_str()) {
            return Err(ProtocolError::DuplicateIdentity);
        }
        if trade.occurred_ms == 0 || trade.occurred_ms > generated_ms {
            return Err(ProtocolError::SnapshotTime);
        }
        if !positive(trade.price) || !positive(trade.quantity) {
            return Err(ProtocolError::SnapshotValue);
        }
    }
    Ok(())
}
fn validate_indicators(
    indicators: &[IndicatorValue],
    generated_ms: u64,
) -> Result<(), ProtocolError> {
    let mut identities = BTreeSet::new();
    for indicator in indicators {
        if indicator.name.trim().is_empty() || indicator.source_version.trim().is_empty() {
            return Err(ProtocolError::SnapshotContent);
        }
        if !identities.insert(indicator.name.as_str()) {
            return Err(ProtocolError::DuplicateIdentity);
        }
        if indicator.observed_ms == 0 || indicator.observed_ms > generated_ms {
            return Err(ProtocolError::SnapshotTime);
        }
    }
    Ok(())
}
fn validate_ledger(ledger: &[LedgerEntry], generated_ms: u64) -> Result<(), ProtocolError> {
    let mut receipt_identities = BTreeSet::new();
    for entry in ledger {
        if entry.receipt_id.trim().is_empty()
            || entry.instance_id.trim().is_empty()
            || entry.action.trim().is_empty()
            || entry.state.trim().is_empty()
        {
            return Err(ProtocolError::SnapshotContent);
        }
        if entry.occurred_ms == 0 || entry.occurred_ms > generated_ms {
            return Err(ProtocolError::SnapshotTime);
        }
        if !receipt_identities.insert(entry.receipt_id.as_str()) {
            return Err(ProtocolError::DuplicateIdentity);
        }
    }
    Ok(())
}
fn positive(value: Decimal) -> bool {
    value.is_sign_positive() && !value.is_zero()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Pause,
    Resume,
    Stop,
    Flatten,
    Trade,
}
impl ControlAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "PAUSE",
            Self::Resume => "RESUME",
            Self::Stop => "STOP",
            Self::Flatten => "FLATTEN",
            Self::Trade => "TRADE",
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
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub instance_id: String,
    pub symbol: Symbol,
    pub action: ControlAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade: Option<TradeIntent>,
    pub expected_config_epoch: u64,
    pub confirmation: Option<String>,
}
impl ControlCommandRequest {
    #[must_use]
    pub fn expected_confirmation(&self) -> String {
        format!(
            "{} venue={} mode={} trading_account_id={} symbol={} instance_id({})={} expected_config_epoch={}",
            self.action.as_str(),
            self.venue,
            self.mode,
            self.trading_account_id,
            self.symbol,
            self.instance_id.len(),
            self.instance_id,
            self.expected_config_epoch,
        )
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        if self.mode != GatewayMode::Live {
            return Err(ProtocolError::Mode);
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
        match (self.action, self.trade.as_ref()) {
            (ControlAction::Trade, Some(trade)) => {
                trade.validate()?;
                if trade.quote_asset != self.symbol.quote() {
                    return Err(ProtocolError::TradeIntent);
                }
            }
            (ControlAction::Trade, None) | (_, Some(_)) => {
                return Err(ProtocolError::TradeIntent);
            }
            (_, None) => {}
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
impl CommandReceipt {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION {
            return Err(ProtocolError::SchemaVersion);
        }
        if self.request_id.trim().is_empty() || self.receipt_id.trim().is_empty() {
            return Err(ProtocolError::ReceiptIdentity);
        }
        if self.observed_ms == 0 {
            return Err(ProtocolError::ReceiptTime);
        }
        if matches!(self.state, CommandState::Rejected | CommandState::Unknown)
            && self.detail.trim().is_empty()
        {
            return Err(ProtocolError::ReceiptDetail);
        }
        Ok(())
    }
}

/// Separate from UI control events so legacy Control consumers never need to decode market
/// projections they did not request. The indicator SSE stream uses this event envelope only.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum IndicatorEvent {
    Snapshot(IndicatorSnapshot),
}
impl IndicatorEvent {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Snapshot(snapshot) => snapshot.validate(),
        }
    }
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProtocolError {
    #[error("unsupported account delivery protocol schema version")]
    DeliverySchemaVersion,
    #[error("unsupported control protocol schema version")]
    SchemaVersion,
    #[error("control and account delivery mode must be exactly LIVE")]
    Mode,
    #[error("control snapshot generated time is missing")]
    GeneratedTime,
    #[error("trading account id is not canonical")]
    AccountId,
    #[error("strategy identity is missing or invalid")]
    StrategyIdentity,
    #[error("copy relation identity is missing or invalid")]
    CopyRelationIdentity,
    #[error("copy relation revision is invalid or stale")]
    CopyRelationRevision,
    #[error("copy relation capital, multiplier, lifecycle, or risk policy is invalid")]
    CopyRelationPolicy,
    #[error("control request identity is missing")]
    RequestIdentity,
    #[error("control request config epoch must be positive")]
    ConfigEpoch,
    #[error("high-risk control confirmation does not match the exact scope")]
    Confirmation,
    #[error("manual trading intent is malformed or contains a UI-only action")]
    TradeIntent,
    #[error("control snapshot contains a duplicate identity")]
    DuplicateIdentity,
    #[error("control snapshot contains an invalid time or generation")]
    SnapshotTime,
    #[error("control snapshot contains an invalid numeric value")]
    SnapshotValue,
    #[error("control snapshot contains invalid nested content")]
    SnapshotContent,
    #[error("indicator projection identity, scope, or freshness window is invalid")]
    IndicatorIdentity,
    #[error("indicator projection provenance is missing, duplicated, stale, or cross-generation")]
    IndicatorProvenance,
    #[error("indicator projection values are outside their normalized range")]
    IndicatorValues,
    #[error("command receipt identity is missing")]
    ReceiptIdentity,
    #[error("command receipt observed time is missing")]
    ReceiptTime,
    #[error("rejected or unknown command receipt detail is missing")]
    ReceiptDetail,
    #[error("UI event scope is not an exact LIVE account binding")]
    EventScope,
    #[error("UI event cursor chain is invalid")]
    EventCursor,
    #[error("account delivery identity is missing")]
    DeliveryIdentity,
    #[error("account delivery payload is missing or malformed")]
    DeliveryPayload,
    #[error("account delivery time window is malformed")]
    DeliveryTime,
    #[error("account delivery payload does not match its exact binding")]
    DeliveryBinding,
    #[error("account delivery lease is malformed")]
    DeliveryLease,
    #[error("account delivery acknowledgement is malformed")]
    DeliveryAck,
    #[error("account delivery receipt transition is malformed")]
    DeliveryReceipt,
}
#[cfg(test)]
mod tests {
    use super::*;
    fn request(action: ControlAction) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
        Ok(ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instance_id: "grid-btc".to_owned(),
            symbol: "BTC/USDT".parse()?,
            action,
            trade: None,
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
    fn schema_v2_paths_and_mode_are_wire_required() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(CONTROL_SCHEMA_VERSION, 2);
        assert_eq!(SNAPSHOT_PATH, "/v2/ui/snapshot");
        assert_eq!(EVENT_STREAM_PATH, "/v2/ui/events");
        assert_eq!(COMMAND_PATH, "/v2/control/commands");
        assert_eq!(ACCOUNT_DELIVERY_SCHEMA_VERSION, 2);
        assert_eq!(
            ACCOUNT_DELIVERY_CLAIM_PATH,
            "/v2/account-node/deliveries/claim"
        );
        assert_eq!(ACCOUNT_DELIVERY_ACK_PATH, "/v2/account-node/deliveries/ack");
        assert_eq!(
            ACCOUNT_DELIVERY_RECEIPT_PATH,
            "/v2/account-node/deliveries/receipts"
        );
        let mut encoded = serde_json::to_value(request(ControlAction::Pause)?)?;
        let object = encoded
            .as_object_mut()
            .ok_or("control request must encode as an object")?;
        object.remove("mode");
        assert!(serde_json::from_value::<ControlCommandRequest>(encoded).is_err());
        let mut encoded = serde_json::to_value(request(ControlAction::Pause)?)?;
        encoded["mode"] = serde_json::json!("TEST");
        assert!(serde_json::from_value::<ControlCommandRequest>(encoded).is_err());
        Ok(())
    }
    #[test]
    fn account_delivery_is_exact_versioned_and_non_authoritative()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = AccountDeliveryBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            instance_id: "copy-btc".to_owned(),
            config_epoch: 7,
        };
        let lease = AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: "copy:job-1".to_owned(),
            binding: binding.clone(),
            node_id: "node-a".to_owned(),
            lease_epoch: 1,
            leased_at_ms: 100,
            expires_at_ms: 200,
            purpose: AccountDeliveryPurpose::Install,
        };
        let claim = AccountDeliveryClaim {
            lease: lease.clone(),
            payload: AccountDeliveryPayload::CopySemanticJob(CopySemanticJobDelivery {
                job_id: "job-1".to_owned(),
                job_digest: [1; 32],
                symbol: binding.symbol.clone(),
                manifest: serde_json::json!({"immutable": true}),
                semantic_job: serde_json::json!({"target": "1"}),
                created_at_ms: 90,
                expires_at_ms: 300,
            }),
        };
        assert_eq!(claim.validate(), Ok(()));
        assert!(!claim.grants_mutation_authority());
        assert!(!lease.grants_mutation_authority());
        let ack = AccountDeliveryAck {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: lease.clone(),
            acknowledged_ms: 110,
            durable_inbox_digest: [2; 32],
        };
        assert_eq!(ack.validate(), Ok(()));
        assert!(!ack.grants_mutation_authority());
        let mut encoded = serde_json::to_value(&claim)?;
        encoded["lease"]["binding"]["mode"] = serde_json::json!("TEST");
        assert!(serde_json::from_value::<AccountDeliveryClaim>(encoded).is_err());
        let mut wrong_symbol = claim;
        wrong_symbol.lease.binding.symbol = "ETH/USDT".parse()?;
        assert_eq!(wrong_symbol.validate(), Err(ProtocolError::DeliveryBinding));
        Ok(())
    }
    #[test]
    fn reconciliation_receipt_requires_reconcile_only_lease_and_account_fact()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut lease = AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: "command:request-1".to_owned(),
            binding: AccountDeliveryBinding {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                instance_id: "grid-btc".to_owned(),
                config_epoch: 7,
            },
            node_id: "node-a".to_owned(),
            lease_epoch: 2,
            leased_at_ms: 200,
            expires_at_ms: 300,
            purpose: AccountDeliveryPurpose::Install,
        };
        let mut receipt = AccountDeliveryReceipt {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            lease: lease.clone(),
            receipt_id: "receipt-2".to_owned(),
            state: AccountDeliveryReceiptState::Reconciled,
            observed_ms: 210,
            account_fact_digest: [3; 32],
            detail: "read back from durable account facts".to_owned(),
        };
        assert_eq!(receipt.validate(), Err(ProtocolError::DeliveryReceipt));
        lease.purpose = AccountDeliveryPurpose::ReconcileOnly;
        receipt.lease = lease;
        assert_eq!(receipt.validate(), Ok(()));
        assert!(!receipt.grants_mutation_authority());
        receipt.account_fact_digest = [0; 32];
        assert_eq!(receipt.validate(), Err(ProtocolError::DeliveryReceipt));
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
    fn high_risk_confirmation_cannot_be_replayed_across_any_scope_field()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut command = request(ControlAction::Stop)?;
        command.confirmation = Some(command.expected_confirmation());
        assert_eq!(command.validate(), Ok(()));
        let mut changed = command.clone();
        changed.action = ControlAction::Flatten;
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
        let mut changed = command.clone();
        changed.venue = VenueId::Okx;
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
        let mut changed = command.clone();
        changed.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
        let mut changed = command.clone();
        changed.symbol = "ETH/USDT".parse()?;
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
        let mut changed = command.clone();
        changed.instance_id = "grid-eth".to_owned();
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
        let mut changed = command;
        changed.expected_config_epoch += 1;
        assert_eq!(changed.validate(), Err(ProtocolError::Confirmation));
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
            mode: GatewayMode::Live,
            trading_account_id: "not-canonical".to_owned(),
            health: HealthState::Unknown,
            equity: None,
            available_margin: None,
            unrealized_pnl: None,
            balances: Vec::new(),
            private_generation: 0,
            writer_generation: 0,
            last_reconciled_ms: 0,
        });
        assert_eq!(snapshot.validate(), Err(ProtocolError::AccountId));
        Ok(())
    }
    #[test]
    fn command_receipt_validates_identity_time_detail_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let valid_receipt = receipt(CommandState::Applied, "");
        assert_eq!(valid_receipt.validate(), Ok(()));
        let encoded = serde_json::to_string(&valid_receipt)?;
        assert_eq!(
            serde_json::from_str::<CommandReceipt>(&encoded)?,
            valid_receipt
        );
        let mut invalid = valid_receipt.clone();
        invalid.schema_version += 1;
        assert_eq!(invalid.validate(), Err(ProtocolError::SchemaVersion));
        let mut invalid = valid_receipt.clone();
        invalid.request_id = " ".to_owned();
        assert_eq!(invalid.validate(), Err(ProtocolError::ReceiptIdentity));
        let mut invalid = valid_receipt.clone();
        invalid.receipt_id.clear();
        assert_eq!(invalid.validate(), Err(ProtocolError::ReceiptIdentity));
        let mut invalid = valid_receipt;
        invalid.observed_ms = 0;
        assert_eq!(invalid.validate(), Err(ProtocolError::ReceiptTime));
        for state in [CommandState::Rejected, CommandState::Unknown] {
            assert_eq!(
                receipt(state, " ").validate(),
                Err(ProtocolError::ReceiptDetail)
            );
            assert_eq!(receipt(state, "verified reason").validate(), Ok(()));
        }
        Ok(())
    }
    #[test]
    fn scoped_ui_event_has_no_facts_and_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let notification = UiEventNotification {
            schema_version: CONTROL_SCHEMA_VERSION,
            event_type: UiEventKind::Snapshot,
            scope: UiAccountScope {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            },
            observed_ms: 100,
        };
        let event = UiEventEnvelope::from_notification(notification, 7, 6)?;
        assert_eq!(event.validate(), Ok(()));
        let encoded = serde_json::to_string(&event)?;
        assert_eq!(serde_json::from_str::<UiEventEnvelope>(&encoded)?, event);
        assert!(!encoded.contains("order"));
        assert!(!encoded.contains("fill"));
        assert!(
            UiEventEnvelope::from_notification(
                UiEventNotification {
                    schema_version: CONTROL_SCHEMA_VERSION,
                    event_type: UiEventKind::Command,
                    scope: event.scope.clone(),
                    observed_ms: 100,
                },
                7,
                7,
            )
            .is_err()
        );
        Ok(())
    }
    #[test]
    fn snapshot_rejects_duplicate_nested_identities() -> Result<(), Box<dyn std::error::Error>> {
        let original = snapshot()?;
        let mut duplicate = original.clone();
        duplicate.accounts.push(duplicate.accounts[0].clone());
        assert_eq!(duplicate.validate(), Err(ProtocolError::DuplicateIdentity));
        let mut duplicate = original.clone();
        duplicate.strategies.push(duplicate.strategies[0].clone());
        assert_eq!(duplicate.validate(), Err(ProtocolError::DuplicateIdentity));
        let mut duplicate = original.clone();
        duplicate
            .copy_relations
            .push(duplicate.copy_relations[0].clone());
        assert_eq!(duplicate.validate(), Err(ProtocolError::DuplicateIdentity));
        let mut duplicate = original.clone();
        duplicate.markets.push(duplicate.markets[0].clone());
        assert_eq!(duplicate.validate(), Err(ProtocolError::DuplicateIdentity));
        let mut duplicate = original;
        duplicate.ledger.push(duplicate.ledger[0].clone());
        assert_eq!(duplicate.validate(), Err(ProtocolError::DuplicateIdentity));
        Ok(())
    }
    #[test]
    fn snapshot_rejects_invalid_nested_values_times_and_references()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = snapshot()?;
        let mut invalid = original.clone();
        invalid.strategies[0].long_quantity = Decimal::NEGATIVE_ONE;
        assert_eq!(invalid.validate(), Err(ProtocolError::SnapshotValue));
        let mut invalid = original.clone();
        invalid.markets[0].bars[0].high = Decimal::new(9, 0);
        assert_eq!(invalid.validate(), Err(ProtocolError::SnapshotValue));
        let mut invalid = original.clone();
        invalid.markets[0].trades[0].occurred_ms = invalid.generated_ms + 1;
        assert_eq!(invalid.validate(), Err(ProtocolError::SnapshotTime));
        let mut invalid = original.clone();
        invalid.accounts[0].private_generation = 0;
        assert_eq!(invalid.validate(), Err(ProtocolError::SnapshotTime));
        let mut invalid = original.clone();
        invalid.strategies[0].trading_account_id =
            "00000000-0000-4000-8000-000000000002".to_owned();
        assert_eq!(invalid.validate(), Err(ProtocolError::StrategyIdentity));
        let mut invalid = original;
        invalid.copy_relations[0].follower_instance_id = "missing".to_owned();
        assert_eq!(invalid.validate(), Err(ProtocolError::SnapshotContent));
        Ok(())
    }
    #[test]
    fn manual_close_is_reduce_only_and_requires_a_positive_position_cap()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut command = request(ControlAction::Trade)?;
        command.trade = Some(TradeIntent {
            action: TradingAction::CloseLong,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: true,
            selected_price: Some(Decimal::new(67_4285, 1)),
            quote_notional: Some(Decimal::new(100, 0)),
            close_quantity_cap: Some(Decimal::new(148, 5)),
            selected_order_id: None,
        });
        command.validate()?;
        assert!(command.trade.as_ref().is_some_and(TradeIntent::reduce_only));
        let mut invalid = command;
        if let Some(trade) = invalid.trade.as_mut() {
            trade.close_quantity_cap = None;
        }
        assert_eq!(invalid.validate(), Err(ProtocolError::TradeIntent));
        Ok(())
    }

    #[test]
    fn ui_only_trading_actions_never_cross_the_control_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut command = request(ControlAction::Trade)?;
        command.trade = Some(TradeIntent {
            action: TradingAction::CenterMarket,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            selected_price: None,
            quote_notional: None,
            close_quantity_cap: None,
            selected_order_id: None,
        });
        assert_eq!(command.validate(), Err(ProtocolError::TradeIntent));
        Ok(())
    }

    #[test]
    fn snapshot_wire_rejects_non_live_nested_modes() -> Result<(), Box<dyn std::error::Error>> {
        let encoded = serde_json::to_value(snapshot()?)?;
        for path in [["accounts", "0", "mode"], ["strategies", "0", "mode"]] {
            for raw in ["TEST", "live", " LIVE", "LIVE "] {
                let mut rejected = encoded.clone();
                rejected[path[0]][path[1].parse::<usize>()?][path[2]] = serde_json::json!(raw);
                assert!(
                    serde_json::from_value::<ControlSnapshot>(rejected).is_err(),
                    "accepted {raw:?} at {}",
                    path[0]
                );
            }
        }
        Ok(())
    }
    fn receipt(state: CommandState, detail: &str) -> CommandReceipt {
        CommandReceipt {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            state,
            receipt_id: "receipt-1".to_owned(),
            observed_ms: 100,
            detail: detail.to_owned(),
        }
    }
    fn snapshot() -> Result<ControlSnapshot, Box<dyn std::error::Error>> {
        let account_id = "00000000-0000-4000-8000-000000000001".to_owned();
        let symbol: Symbol = "BTC/USDT".parse()?;
        Ok(ControlSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: 100,
            connection: ConnectionState::Live,
            accounts: vec![AccountSummary {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: account_id.clone(),
                health: HealthState::Healthy,
                equity: Some(Decimal::new(10_000, 0)),
                available_margin: Some(Decimal::new(8_000, 0)),
                unrealized_pnl: Some(Decimal::new(50, 0)),
                balances: vec![AccountBalanceSummary {
                    asset: Asset::new("USDT")?,
                    equity: Decimal::new(10_000, 0),
                    available_margin: Some(Decimal::new(8_000, 0)),
                }],
                private_generation: 4,
                writer_generation: 2,
                last_reconciled_ms: 90,
            }],
            strategies: vec![StrategySummary {
                instance_id: "copy-btc".to_owned(),
                kind: StrategyKind::Copy,
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: account_id,
                symbol: symbol.clone(),
                lifecycle: StrategyLifecycle::Running,
                config_epoch: 7,
                open_orders: 1,
                long_quantity: Decimal::ONE,
                short_quantity: Decimal::ZERO,
                realized_pnl: Some(Decimal::new(10, 0)),
                unrealized_pnl: Some(Decimal::new(5, 0)),
                last_receipt_ms: 95,
                attention: None,
            }],
            copy_relations: vec![CopyRelationSummary {
                relation_id: "00000000-0000-4000-8000-000000000010".to_owned(),
                revision: 1,
                leader_id: "leader-btc".to_owned(),
                follower_instance_id: "copy-btc".to_owned(),
                symbol: symbol.clone(),
                target_exposure: Decimal::new(100, 0),
                actual_exposure: Decimal::new(99, 0),
                drift: Decimal::NEGATIVE_ONE,
                status: CopyStatus::Tracking,
                last_applied_job: Some("job-1".to_owned()),
            }],
            markets: vec![MarketSummary {
                symbol,
                last: Decimal::new(100, 0),
                bid: Decimal::new(99, 0),
                ask: Decimal::new(101, 0),
                change_percent_24h: Decimal::new(5, 1),
                bars: vec![UiBar {
                    open_time_ms: 50,
                    open: Decimal::new(98, 0),
                    high: Decimal::new(102, 0),
                    low: Decimal::new(97, 0),
                    close: Decimal::new(100, 0),
                    volume: Decimal::new(200, 0),
                }],
                bids: vec![UiBookLevel {
                    price: Decimal::new(99, 0),
                    quantity: Decimal::ONE,
                }],
                asks: vec![UiBookLevel {
                    price: Decimal::new(101, 0),
                    quantity: Decimal::ONE,
                }],
                trades: vec![UiTrade {
                    trade_id: "trade-1".to_owned(),
                    occurred_ms: 96,
                    price: Decimal::new(100, 0),
                    quantity: Decimal::ONE,
                    aggressor: AggressorSide::Buy,
                }],
                indicators: vec![IndicatorValue {
                    name: "rsi".to_owned(),
                    value: Decimal::new(55, 0),
                    observed_ms: 95,
                    source_version: "v1".to_owned(),
                }],
            }],
            ledger: vec![LedgerEntry {
                receipt_id: "receipt-1".to_owned(),
                instance_id: "copy-btc".to_owned(),
                occurred_ms: 97,
                action: "resume".to_owned(),
                state: "applied".to_owned(),
                detail: String::new(),
            }],
        })
    }
    #[test]
    fn copy_relation_config_requires_exact_live_bindings_and_safe_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "BTC/USDT".parse()?;
        let binding = |instance_id: &str| CopyRelationBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instance_id: instance_id.to_owned(),
            symbol: symbol.clone(),
        };
        let request = CopyRelationUpsertRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "00000000-0000-4000-8000-000000000011".to_owned(),
            relation: CopyRelationConfig {
                relation_id: "00000000-0000-4000-8000-000000000010".to_owned(),
                leader: binding("leader-btc"),
                follower: binding("copy-btc"),
                allocated_capital: Decimal::new(500, 0),
                multiplier: Decimal::ONE,
                safety_reserve_rate: Decimal::new(1, 1),
                risk: CopyRiskPolicy {
                    max_total_notional: Decimal::new(1_000, 0),
                    max_order_notional: Decimal::new(100, 0),
                    max_leverage: Decimal::new(3, 0),
                },
                lifecycle: CopyLifecyclePolicy::Paused,
            },
            expected_revision: None,
        };
        assert_eq!(request.validate(), Ok(()));
        let digest = request.relation.policy_digest();
        let mut changed_policy = request.relation.clone();
        changed_policy.risk.max_order_notional = Decimal::new(99, 0);
        assert_ne!(digest, changed_policy.policy_digest());
        let mut changed_binding = request.relation.clone();
        changed_binding.follower.instance_id = "copy-btc-2".to_owned();
        assert_ne!(digest, changed_binding.policy_digest());
        let mut invalid = request.clone();
        invalid.relation.safety_reserve_rate = Decimal::ONE;
        assert_eq!(invalid.validate(), Err(ProtocolError::CopyRelationPolicy));
        let mut invalid = request;
        invalid.relation.follower = invalid.relation.leader.clone();
        assert_eq!(invalid.validate(), Err(ProtocolError::CopyRelationPolicy));
        Ok(())
    }
    #[test]
    fn execution_facts_are_an_explicit_empty_read_model_until_node_signed_evidence_arrives()
    -> Result<(), Box<dyn std::error::Error>> {
        let facts = ExecutionFactsSnapshot {
            schema_version: CONTROL_SCHEMA_VERSION,
            generated_ms: 100,
            orders: Vec::new(),
            positions: Vec::new(),
            fills: Vec::new(),
            reconciliation: Vec::new(),
            copy_ledger: Vec::new(),
            drift: Vec::new(),
            execution: Vec::new(),
            risk: Vec::new(),
            health: Vec::new(),
        };
        assert_eq!(facts.validate(), Ok(()));
        assert_eq!(
            serde_json::from_value::<ExecutionFactsSnapshot>(serde_json::to_value(&facts)?)?,
            facts
        );
        Ok(())
    }
}
