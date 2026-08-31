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
/// Node and adapter composition obtain gateway DTOs through Runtime.  In particular this list
/// deliberately excludes `AccountMutationHost` and `HostPreparedCommand`: only the resident
/// Runtime Host can prepare or dispatch a physical mutation.
pub use venue_execution::{
    AccountCanonicalRootError, AccountCanonicalRootGuard, AccountCommandStatus,
    AccountDispatchOutcome, AccountDispatchPermit, AccountGatewayResult, AccountHostError,
    AccountHostValidationError, AccountInstrumentIdentity, AccountLimitNormalizationIntent,
    AccountOwnerRouteScope, AccountPhysicalGateway, AccountQuoteToUsdtRate, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest, AccountRiskAmount, AccountRiskEvidence,
    AccountRiskSummary, AccountSymbolSet, CommandJournal, CommandJournalError, CommandState,
    DispatchGuard, DurableOwnerRoutes, LegacyV1WriterPredecessor, OwnerRouteFence,
    OwnerRoutesError, RuntimeBootstrapReceipt, SignedAccountBalance, SignedAccountOrderFact,
    SignedAccountPositionFact, SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact,
    SignedUnknownResult, WriterLeaseAuthority, WriterLeaseError, WriterScope, WriterSession,
    acquire_account_canonical_root,
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
        AccountWriterCapability, CommandIdentityReceipt, DurableCommandIdentityAllocation,
        ExposureEffect, PersistedMutationOutcomeReceipt, PersistedWalPreparedReceipt,
        PersistedWriterLeaseReceipt, PreWalCandidate, UnknownReadbackProof, UnknownResolution,
        WalNotPreparedReceipt,
    };
}

pub(crate) mod runtime {
    pub use crate::{account, strategy};
}

pub(crate) mod storage {
    pub use venue_storage::*;
}
