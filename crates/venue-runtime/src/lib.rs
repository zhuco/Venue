#![allow(
    dead_code,
    reason = "runtime authority constructors stay sealed until their durable adapters move"
)]

pub mod account;
pub mod account_lane;
mod authority;
pub mod shared;
pub mod strategy;

pub use authority::{
    AccountKey, AccountModelError, AccountOrderCapabilityEvidence, AppliedStrategyTurnReceipt,
    StrategyBinding, StrategyInstanceKey, StrategyKind, StrategyTurnToken,
};
pub use venue_gateway_api::VenueId as ExchangeId;

pub(crate) mod domain {
    pub(crate) use crate::authority::validate_config_digest;
    pub use crate::authority::{
        AccountKey, AccountOrderCapabilityEvidence, AppliedStrategyTurnReceipt, StrategyBinding,
        StrategyInstanceKey, StrategyKind, StrategyTurnToken,
    };
    pub use venue_domain::domain::*;
    pub use venue_gateway_api::VenueId as ExchangeId;
}

#[allow(unused_imports)]
pub(crate) mod execution {
    pub(crate) use crate::account_lane::AccountExecutionLane;
    pub use crate::account_lane::{
        AccountDispatchDecision, AccountDispatchPermit, AccountExecutionIntent,
        AccountExecutionRequest, AccountLaneError, AccountLaneFollowUp, AccountLanePriority,
        AccountMutationOutcome, AccountReplanReason, AccountWalPreparedFence,
        AccountWriterCapability, CommandIdentityReceipt, ExposureEffect,
        PersistedMutationOutcomeReceipt, PersistedWalPreparedReceipt, PersistedWriterLeaseReceipt,
        PreWalCandidate, UnknownReadbackProof, UnknownResolution, WalNotPreparedReceipt,
    };
}

pub(crate) mod runtime {
    pub use crate::{account, strategy};
}

pub(crate) mod storage {
    pub use venue_storage::*;
}
