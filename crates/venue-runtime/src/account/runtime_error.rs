use crate::{
    execution::AccountLaneError,
    runtime::{
        account::{
            AccountPrivateIngressError, AccountReconcilerError, MarketHubError, PrivateRouterError,
            RegistryError,
        },
        strategy::StrategyHostError,
    },
};
use venue_storage::ActorAppliedError;

#[derive(Debug, thiserror::Error)]
pub enum AccountRuntimeError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    PrivateRouter(#[from] PrivateRouterError),
    #[error(transparent)]
    MarketHub(#[from] MarketHubError),
    #[error(transparent)]
    StrategyHost(#[from] StrategyHostError),
    #[error(transparent)]
    Reconciler(#[from] AccountReconcilerError),
    #[error(transparent)]
    ExecutionLane(#[from] AccountLaneError),
    #[error(transparent)]
    ActorApplied(#[from] ActorAppliedError),
    #[error(transparent)]
    PrivateIngress(#[from] AccountPrivateIngressError),
    #[error("account is not ready on the requested private generation")]
    AccountUnavailable,
    #[error("the account private facts journal has not been attached")]
    PrivateIngressUnavailable,
    #[error("the account private facts journal is already attached")]
    PrivateIngressAttached,
    #[error("strategy actor host is missing")]
    ActorMissing,
    #[error("signed reconciliation generation is stale or duplicated")]
    StaleReconciliation,
    #[error("risk-increasing execution is fenced for this account or instance")]
    RiskFenced,
    #[error("native order family is unsupported by this account capability evidence")]
    UnsupportedOrderFamily,
    #[error("execution request is not bound to a current signed private generation")]
    StaleExecutionAuthority,
    #[error("Stop and Flatten lifecycle modes cannot be confused or downgraded")]
    ShutdownMode,
    #[error("Stop or Flatten cannot complete while its actor has an active or unapplied turn")]
    ShutdownActorStatePending,
    #[error("Flatten requires a same-generation signed zero-position proof")]
    FlattenNotProven,
    #[error(
        "Stop preserves a residual position, so the instance retains symbol custody until flat"
    )]
    ResidualPositionCustody,
    #[error("durable account recovery must be installed before private connectivity becomes ready")]
    DurableRecoveryRequired,
    #[error(
        "production physical recovery integration is unavailable; caller manifests cannot authorize connectivity or actor turns"
    )]
    PhysicalRecoveryIntegrationUnavailable,
    #[error("a complete physical recovery readback manifest is required for the next connection")]
    PhysicalRecoveryRequired,
    #[error("a physical recovery manifest is already staged for the next connection")]
    PhysicalRecoveryAlreadyInstalled,
    #[error("physical recovery binding, generation, configuration, or authority roots drifted")]
    PhysicalRecoveryScopeMismatch,
    #[error("a physical recovery session is already active for this account")]
    PhysicalRecoverySessionActive,
    #[error("physical recovery requires a complete durable checkpoint and five-journal root set")]
    PhysicalRecoveryDurableRootsRequired,
    #[error("physical recovery requires at least one completely configured account symbol")]
    PhysicalRecoveryUniverseIncomplete,
    #[error("physical recovery session is forged, revoked, stale, or belongs to another attempt")]
    PhysicalRecoverySessionInvalid,
    #[error("physical recovery session lease expired")]
    PhysicalRecoverySessionExpired,
    #[error("physical recovery manifest requires a post-await durable refresh session epoch")]
    PhysicalRecoveryPostAwaitRefreshRequired,
    #[error("physical recovery durable roots or account authority drifted during collection")]
    PhysicalRecoveryDurableRootDrift,
    #[error("physical recovery durable-root refresh is incomplete, regressive, or cross-attempt")]
    PhysicalRecoveryDurableRootRegression,
    #[error("durable account recovery was already installed or startup already advanced")]
    DurableRecoveryAlreadyInstalled,
    #[error("durable recovery snapshot belongs to another exchange account")]
    RecoveryAccountMismatch,
    #[error("durable recovery does not exactly cover configured strategies and mutation epochs")]
    RecoveryStateMismatch,
    #[error("reconciliation or execution authority belongs to a stale configuration epoch")]
    StaleConfiguration,
    #[error("configuration cannot change while this instance has an in-flight or UNKNOWN mutation")]
    ParameterChangeBusy,
    #[error("an in-flight mutation must be classified as UNKNOWN before reconnect becomes ready")]
    ReconnectWithInFlight,
    #[error("durable actor inbox turns must be applied or recovered before reconnect advances")]
    ReconnectWithUnappliedActorState,
    #[error("connection generation counter is exhausted")]
    ConnectionGenerationExhausted,
    #[error("a strategy instance already has an unacknowledged actor turn")]
    StrategyTurnActive,
    #[error("strategy turn receipt is stale, unpersisted, or belongs to another authority epoch")]
    StrategyTurnAuthority,
    #[error("the exact Actor-applied durable store or WAL head is unavailable")]
    ActorAppliedUnavailable,
    #[error("resident Host supplied a regressive or inconsistent command WAL head")]
    ResidentWalHead,
    #[error("Actor-applied storage cannot be replaced after registration or use")]
    ActorAppliedStore,
    #[error("private route plan is stale relative to the committed durable inbox revision")]
    StalePrivateRoutePlan,
    #[error("order route append receipt does not extend the installed durable owner-index head")]
    OrderRouteReceipt,
    #[error("private actor application cursor or delivery acknowledgement is inconsistent")]
    PrivateApplicationState,
    #[error("strategy registry/lifecycle/config state revision counter is exhausted")]
    StrategyStateRevisionExhausted,
    #[error("actor authority revision counter is exhausted")]
    ActorAuthorityRevisionExhausted,
    #[error("execution dispatch authority revision counter is exhausted")]
    DispatchRevisionExhausted,
}
