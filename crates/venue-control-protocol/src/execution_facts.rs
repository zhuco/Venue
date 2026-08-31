//! Signed execution and copy read-model facts.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::{OrderSide, OrderState, PositionSide, Symbol};

use crate::{
    AccountDeliveryBinding, CONTROL_SCHEMA_VERSION, GatewayMode, HealthState, ProtocolError,
    VenueId, deserialize_live_mode, is_uuid, positive,
};
/// Read-only evidence uploaded by an account node after its adapter has validated a private
/// response. Control stores and renders this material but never uses it as an execution permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFactBinding {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub instance_id: String,
    pub config_epoch: u64,
}
impl ExecutionFactBinding {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        AccountDeliveryBinding {
            venue: self.venue,
            mode: self.mode,
            trading_account_id: self.trading_account_id.clone(),
            symbol: self.symbol.clone(),
            instance_id: self.instance_id.clone(),
            config_epoch: self.config_epoch,
        }
        .validate()
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignedOrderFact {
    pub binding: ExecutionFactBinding,
    pub order_id: String,
    pub client_order_id: Option<String>,
    /// Some signed open-order endpoints omit status or cumulative fills. The row remains
    /// visible; absence must not turn into a fabricated New/zero or hide an existing order.
    #[serde(default)]
    pub state: Option<OrderState>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    #[serde(default)]
    pub filled_quantity: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub reduce_only: bool,
    pub signed_generation: u64,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignedPositionFact {
    pub binding: ExecutionFactBinding,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub entry_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub signed_generation: u64,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignedFillFact {
    pub binding: ExecutionFactBinding,
    pub fill_id: String,
    pub order_id: String,
    pub side: OrderSide,
    /// Absent when the venue does not expose a signed leg classification.
    pub position_side: Option<PositionSide>,
    pub quantity: Decimal,
    pub price: Decimal,
    /// Venue-native monotonic cursor when supplied. Hash IDs and timestamps are never invented
    /// as a cursor.
    pub execution_sequence: Option<u64>,
    pub occurred_ms: u64,
    pub signed_generation: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationFact {
    pub binding: ExecutionFactBinding,
    pub signed_generation: u64,
    pub reconciled_ms: u64,
    pub complete_order_families: bool,
    pub complete_position_legs: bool,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyLedgerFact {
    pub relation_id: String,
    pub relation_revision: u64,
    pub job_id: String,
    pub binding: ExecutionFactBinding,
    /// Venue/API sequence when the authoritative source exposes one. Copy reconciliation uses
    /// normalized fills and signed position facts; it must not manufacture a sequence from an
    /// order id or timestamp when the venue does not provide an ordered ledger cursor.
    pub ledger_sequence: Option<u64>,
    pub managed_exposure: Decimal,
    pub signed_generation: u64,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyDriftFact {
    pub relation_id: String,
    pub relation_revision: u64,
    pub job_id: String,
    pub binding: ExecutionFactBinding,
    pub target_exposure: Decimal,
    pub actual_exposure: Decimal,
    pub repair_pending: bool,
    pub signed_generation: u64,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyExecutionStateProjection {
    SemanticApplied,
    Prepared,
    Submitted,
    Accepted,
    Rejected,
    Unknown,
    Reconciled,
}

/// The immutable Copy phase selected before a physical child can cross the account boundary.
/// This is metadata for a fixed opaque result encoding, not a second Copy planner contract.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyExecutionPhaseProjection {
    ReduceToZero,
    Adjust,
}

/// A bounded, fixed-format copy result carried alongside the account projection. `result_bytes`
/// is exactly UTF-8 JSON encoding of `venue_copy::CopyExecutionResult` v1; Control owns the only
/// decoder and checks every outer field against it before recording a read-only result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyExecutionEvidence {
    pub encoding: CopyExecutionEvidenceEncoding,
    pub relation_id: String,
    pub relation_revision: u64,
    pub job_id: String,
    pub binding: ExecutionFactBinding,
    pub phase: CopyExecutionPhaseProjection,
    pub state: CopyExecutionStateProjection,
    pub command_id: Option<String>,
    pub observed_ms: u64,
    pub result_fact_digest: [u8; 32],
    pub result_sha256: [u8; 32],
    /// Bounded UTF-8 JSON bytes represented as a string to avoid a lossy/expansive byte-array
    /// transport. This field is never interpreted as a generic JSON proxy by the protocol.
    pub result_bytes: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyExecutionEvidenceEncoding {
    VenueCopyExecutionResultV1,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopyExecutionFact {
    pub relation_id: String,
    pub relation_revision: u64,
    pub job_id: String,
    pub binding: ExecutionFactBinding,
    pub state: CopyExecutionStateProjection,
    pub command_id: Option<String>,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountRiskFact {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub observed_ms: u64,
    pub signed_generation: u64,
    pub absolute_position_notional: Decimal,
    pub open_entry_notional: Decimal,
    pub reserved_entry_notional: Decimal,
    pub max_total_notional: Decimal,
    pub accepts_new_risk: bool,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountHealthFact {
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
    pub health: HealthState,
    pub private_generation: u64,
    pub last_reconciled_ms: u64,
    pub observed_ms: u64,
    pub fact_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionFactsSnapshot {
    pub schema_version: u16,
    pub generated_ms: u64,
    pub orders: Vec<SignedOrderFact>,
    pub positions: Vec<SignedPositionFact>,
    pub fills: Vec<SignedFillFact>,
    pub reconciliation: Vec<ReconciliationFact>,
    pub copy_ledger: Vec<CopyLedgerFact>,
    pub drift: Vec<CopyDriftFact>,
    pub execution: Vec<CopyExecutionFact>,
    pub risk: Vec<AccountRiskFact>,
    pub health: Vec<AccountHealthFact>,
}
impl ExecutionFactsSnapshot {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema_version != CONTROL_SCHEMA_VERSION || self.generated_ms == 0 {
            return Err(ProtocolError::SchemaVersion);
        }
        for binding in self
            .orders
            .iter()
            .map(|fact| &fact.binding)
            .chain(self.positions.iter().map(|fact| &fact.binding))
            .chain(self.fills.iter().map(|fact| &fact.binding))
            .chain(self.reconciliation.iter().map(|fact| &fact.binding))
            .chain(self.copy_ledger.iter().map(|fact| &fact.binding))
            .chain(self.drift.iter().map(|fact| &fact.binding))
            .chain(self.execution.iter().map(|fact| &fact.binding))
        {
            binding.validate()?;
        }
        for time in self
            .orders
            .iter()
            .map(|fact| fact.observed_ms)
            .chain(self.positions.iter().map(|fact| fact.observed_ms))
            .chain(self.fills.iter().map(|fact| fact.occurred_ms))
            .chain(self.reconciliation.iter().map(|fact| fact.reconciled_ms))
            .chain(self.copy_ledger.iter().map(|fact| fact.observed_ms))
            .chain(self.drift.iter().map(|fact| fact.observed_ms))
            .chain(self.execution.iter().map(|fact| fact.observed_ms))
            .chain(self.risk.iter().map(|fact| fact.observed_ms))
            .chain(self.health.iter().map(|fact| fact.observed_ms))
        {
            if time == 0 || time > self.generated_ms {
                return Err(ProtocolError::SnapshotTime);
            }
        }
        if self.orders.iter().any(|fact| {
            fact.order_id.trim().is_empty()
                || fact.signed_generation == 0
                || fact.fact_digest == [0; 32]
                || !positive(fact.quantity)
                || fact
                    .filled_quantity
                    .is_some_and(|filled| filled < Decimal::ZERO || filled > fact.quantity)
        }) || self.positions.iter().any(|fact| {
            fact.signed_generation == 0
                || fact.fact_digest == [0; 32]
                || fact.quantity == Decimal::MAX
                || fact.quantity == Decimal::MIN
        }) || self.fills.iter().any(|fact| {
            fact.fill_id.trim().is_empty()
                || fact.order_id.trim().is_empty()
                || fact
                    .execution_sequence
                    .is_some_and(|sequence| sequence == 0)
                || fact.signed_generation == 0
                || fact.fact_digest == [0; 32]
                || !fact.quantity.is_sign_positive()
                || !positive(fact.price)
        }) || self
            .reconciliation
            .iter()
            .any(|fact| fact.signed_generation == 0 || fact.fact_digest == [0; 32])
            || self.copy_ledger.iter().any(|fact| {
                !is_uuid(&fact.relation_id)
                    || fact.relation_revision == 0
                    || fact.job_id.trim().is_empty()
                    || fact.ledger_sequence.is_some_and(|sequence| sequence == 0)
                    || fact.signed_generation == 0
                    || fact.fact_digest == [0; 32]
            })
            || self.drift.iter().any(|fact| {
                !is_uuid(&fact.relation_id)
                    || fact.relation_revision == 0
                    || fact.job_id.trim().is_empty()
                    || fact.signed_generation == 0
                    || fact.fact_digest == [0; 32]
            })
            || self.execution.iter().any(|fact| {
                !is_uuid(&fact.relation_id)
                    || fact.relation_revision == 0
                    || fact.job_id.trim().is_empty()
                    || fact.fact_digest == [0; 32]
            })
            || self.risk.iter().any(|fact| {
                fact.mode != GatewayMode::Live
                    || !venue_domain::is_canonical_trading_account_id(&fact.trading_account_id)
                    || fact.signed_generation == 0
                    || fact.fact_digest == [0; 32]
                    || fact.absolute_position_notional.is_sign_negative()
                    || fact.open_entry_notional.is_sign_negative()
                    || fact.reserved_entry_notional.is_sign_negative()
                    || !positive(fact.max_total_notional)
            })
            || self.health.iter().any(|fact| {
                fact.mode != GatewayMode::Live
                    || !venue_domain::is_canonical_trading_account_id(&fact.trading_account_id)
                    || fact.fact_digest == [0; 32]
                    || fact.last_reconciled_ms > fact.observed_ms
            })
        {
            return Err(ProtocolError::SnapshotContent);
        }
        Ok(())
    }
}
