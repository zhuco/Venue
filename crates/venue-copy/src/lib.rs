//! Deterministic copy-planning reducers.
//!
//! This crate has no storage, network, runtime, credential, or mutation authority. Callers must
//! freeze and persist the inputs before invoking a reducer, then submit any resulting semantic
//! command through the account runtime and execution lane.

mod capital;
mod delivery;
mod drift;
mod execution;
mod identity;
mod ledger;
mod limit;
mod sizing;

pub use capital::{
    CapitalSnapshot, TargetExposureError, TargetExposurePlan, TargetExposureRequest,
    reduce_target_exposure, reduce_target_exposure_with_multiplier,
};
pub use delivery::{
    DeliveryBinding, DeliveryError, DeliveryReceiptStatus, DeliveryState, DeliveryTracker,
    FollowerDeliveryManifest, MAX_DELIVERY_TTL_MS, PersistedDeliveryReceipt, ReceiptApply,
    RelationCommitment,
};
pub use drift::{
    AuthoritativePositionSnapshot, DriftRepairError, DriftRepairPlanRequest, DriftRepairRequest,
    MAX_POSITION_SNAPSHOT_TTL_MS, MAX_REPAIR_TTL_MS, plan_drift_repair,
};
pub use execution::{
    CopyExecutionError, CopyExecutionPhase, CopyExecutionRequest, CopyExecutionResult,
    CopyExecutionState, plan_copy_execution,
};
pub use identity::{
    CopyAction, CopyId, CopyIdentityError, CopyIdentityInput, CopyIdentitySet,
    CopyTargetObservationIdentity, IdempotencyKey, derive_copy_identities,
    derive_repair_identities, derive_target_observation_identity,
};
pub use ledger::{CopyLedger, LedgerApply, LedgerAttribution, LedgerEntry, LedgerError};
pub use limit::{
    CrossVenueLimitPlan, CrossVenueLimitRequest, LimitPriceError, convert_cross_venue_limit,
    normalize_limit_price,
};
pub use sizing::{
    ReferencePriceSnapshot, SemanticSizingPlan, SemanticSizingRequest, SizedQuantity, SizingError,
    plan_semantic_size,
};
