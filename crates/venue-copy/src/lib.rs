//! Deterministic copy-planning reducers.
//!
//! This crate has no storage, network, runtime, credential, or mutation authority. Callers must
//! freeze and persist the inputs before invoking a reducer, then submit any resulting semantic
//! command through the account runtime and execution lane.

mod capital;
mod identity;
mod limit;
mod sizing;

pub use capital::{
    CapitalSnapshot, TargetExposureError, TargetExposurePlan, TargetExposureRequest,
    reduce_target_exposure,
};
pub use identity::{
    CopyAction, CopyId, CopyIdentityError, CopyIdentityInput, CopyIdentitySet, IdempotencyKey,
    derive_copy_identities,
};
pub use limit::{
    CrossVenueLimitPlan, CrossVenueLimitRequest, LimitPriceError, convert_cross_venue_limit,
    normalize_limit_price,
};
pub use sizing::{
    ReferencePriceSnapshot, SemanticSizingPlan, SemanticSizingRequest, SizedQuantity, SizingError,
    plan_semantic_size,
};
