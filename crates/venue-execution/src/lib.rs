mod account_host;
mod canonical_root;
mod journal;
mod owner_routes;
mod writer_lease;

use sha2::{Digest, Sha256};
use venue_domain::domain::ExecutionCommand;

pub use account_host::{
    AccountCommandStatus, AccountDispatchOutcome, AccountDispatchPermit, AccountGatewayResult,
    AccountHostError, AccountHostValidationError, AccountInstrumentIdentity,
    AccountLimitNormalizationIntent, AccountMutationHost, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountQuoteToUsdtRate, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest, AccountRecoveryState, AccountRiskAmount,
    AccountRiskEvidence, AccountRiskSummary, AccountSymbolSet, COMMAND_JOURNAL_HARD_LIMIT_BYTES,
    COMMAND_JOURNAL_ROTATE_BYTES, HostPreparedCommand, LegacyV1CustodyRoute,
    ManagedGridRollingDispatchPermit, ManagedGridSurfaceReceipt, RuntimeBootstrapReceipt,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
    command_matches_readback_order,
};
pub use canonical_root::{
    AccountCanonicalRootError, AccountCanonicalRootGuard, LegacyV1WriterGuard,
    LegacyV1WriterPredecessor, acquire_account_canonical_root,
};
pub use journal::{
    CommandJournal, CommandJournalError, CommandReceipt, CommandState, OrderReadbackIdentity,
};
pub use owner_routes::{
    AccountOwnerRouteScope, DurableOwnerRoutes, ExactCancelRoute, NativeOrderRoute,
    NativeOrderRouteKey, OwnerRouteFence, OwnerRouteProjection, OwnerRoutesError,
};
pub use writer_lease::{
    DispatchGuard, ExecutableHandoffReceipt, FlatReceipt, ProtectedReceipt, WRITER_LEASE_TTL_MS,
    WriterLeaseAuthority, WriterLeaseError, WriterScope, WriterSession,
};

pub(crate) mod domain {
    pub use venue_domain::domain::*;
}

/// Shared byte commitment used by the command WAL and runtime admission receipts.
#[doc(hidden)]
pub fn execution_command_sha256(command: &ExecutionCommand) -> Result<[u8; 32], serde_json::Error> {
    serde_json::to_vec(command).map(|encoded| Sha256::digest(encoded).into())
}
