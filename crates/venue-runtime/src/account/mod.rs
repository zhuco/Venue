mod copy_actor;
mod market_hub;
mod model;
mod physical_recovery;
mod private_router;
mod reconciler;
mod recovery;
mod recovery_session;
mod registry;
mod runtime;
mod runtime_error;

pub use crate::{
    AccountKey, AccountModelError, AccountOrderCapabilityEvidence, ExchangeId, StrategyBinding,
    StrategyInstanceKey, StrategyKind,
};
pub use copy_actor::{CopyActorAppliedArtifacts, CopyActorAppliedReceipt, CopyActorCommitment};
pub use market_hub::{BestBidOffer, MarketHub, MarketHubError, MarketPublish};
pub use model::{AccountFault, AccountHealth, InstanceLifecycle};
pub use physical_recovery::{
    PhysicalReadbackCoverage, PhysicalReadbackReceipt, PhysicalReadbackSurface,
    PhysicalRecoveryAuthorityRoots, PhysicalRecoveryManifestError,
    PhysicalRecoveryReadbackManifest, PhysicalRecoveryScope, PhysicalRecoveryUniverseEntry,
};
pub(crate) use private_router::PrivateRouter;
pub use private_router::{
    PrivateDelivery, PrivateReconcileRequest, PrivateRouteReport, PrivateRouterError,
    ReconcileReason, ReconcileScope,
};
pub(crate) use reconciler::reconcile_open_orders;
pub use reconciler::{
    AccountPositionMode, AccountReconcilerError, AccountReconciliationReport,
    DesiredCheckpointFingerprint, DesiredOrder, DesiredOrderSets, InstanceReconciliation,
    OrderFamilySemanticFingerprint, RecoveredDesiredOrdersReceipt, SignedOpenOrders,
    SignedOrderFamilySnapshot, SignedPositionSnapshot, UnresolvedSignedOrder,
    UnsupportedOrderFamilyCapabilityReceipt,
};
pub use recovery::{
    AccountRecoverySnapshot, PersistedOrderRouteAppendReceipt, RecoveredActorInboxEntry,
    RecoveredOrderRoute, RecoveredPrivateBatch, RecoveredPrivateCursor, RecoveredShutdownMode,
    RecoveredShutdownState, RecoveredStrategyState, RecoveryJournalBoundary, RecoveryJournalRoots,
    RecoveryManifestCommitment, RecoverySnapshotError,
};
pub use recovery_session::{
    PhysicalRecoveryDurableRoots, PhysicalRecoveryRootRefresh, PhysicalRecoverySession,
    PhysicalRecoverySessionError,
};
pub use registry::{FlattenPlan, RegistryError, StopPlan, StrategyRegistration};
pub(crate) use registry::{SignedStopProof, StrategyRegistry};
pub use runtime::{AccountRuntime, PersistedPrivateDispatchReceipt, PrivateRoutePlan};
pub use runtime_error::AccountRuntimeError;

#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;
