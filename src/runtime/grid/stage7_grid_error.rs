use super::*;

#[derive(Debug, thiserror::Error)]
pub enum Stage7GridError {
    #[error("stage-7 grid mainnet mutations require explicit confirmation")]
    Confirmation,
    #[error("stage-7 exact external Algo cancellation requires explicit confirmation")]
    ExternalAlgoConfirmation,
    #[error("stage-7 external Algo target is absent, duplicated, changed, or mismatched")]
    ExternalAlgoTarget,
    #[error("stage-7 external Algo cancellation remains unresolved after signed readback")]
    ExternalAlgoUnresolved,
    #[error("stage-7 non-flat executable handoff requires explicit confirmation")]
    ExecutableHandoffConfirmation,
    #[error("stage-7 executable handoff proof is missing, expired, or inconsistent")]
    ExecutableHandoff,
    #[error("stage-7 grid artifact root must be absolute")]
    ArtifactsRoot,
    #[error("stage-7 grid binding is invalid for the selected exchange")]
    Binding,
    #[error("stage-7 grid requires [hedged_grid].grid_count")]
    GridConfig,
    #[error("stage-7 grid_count is outside the fixed release range")]
    GridCount,
    #[error("stage-7 grid control is invalid for this binding")]
    Control,
    #[error("stage-7 grid was stopped while venue startup was waiting for recovery")]
    StartupStopped,
    #[error("stage-7 grid checkpoint is incompatible")]
    Checkpoint,
    #[error("stage-7 grid parameter change requires --reset-on-start")]
    ParameterChange,
    #[error("stage-7 grid private facts are incomplete or unsafe")]
    Inventory,
    #[error("stage-7 grid signed order-family coverage is incomplete or unsupported by evidence")]
    OrderFamily,
    #[error("stage-7 grid signed available margin is insufficient for new risk")]
    InsufficientMargin,
    #[error("stage-7 grid found foreign or unowned orders")]
    ForeignOrders,
    #[error("stage-7 grid mutation is unresolved")]
    Unresolved,
    #[error("stage-7 grid mutation was rejected")]
    Rejected,
    #[error("stage-7 post-only grid order was reported as a taker fill")]
    PostOnlyFillBecameTaker,
    #[error("stage-7 owned grid fill has no authoritative maker/taker evidence")]
    FillLiquidityUnknown,
    #[error("stage-7 grid command journal is outside its bound scope")]
    JournalScope,
    #[error("stage-7 grid command is invalid")]
    Command,
    #[error("stage-7 grid actual order notional exceeds its fixed low-balance cap")]
    Notional,
    #[error(
        "stage-7 grid order notional rejected quantity={quantity} price={price} value={value} minimum={minimum} maximum={maximum}"
    )]
    OrderNotional {
        quantity: Decimal,
        price: Decimal,
        value: Decimal,
        minimum: Decimal,
        maximum: Decimal,
    },
    #[error(
        "stage-7 grid replenishment notional rejected quantity={quantity} price={price} value={value} minimum={minimum} maximum=18"
    )]
    MarketNotional {
        quantity: Decimal,
        price: Decimal,
        value: Decimal,
        minimum: Decimal,
    },
    #[error("stage-7 grid private evidence is incomplete")]
    PrivateEvidence,
    #[error("stage-7 private-evidence forensic recovery failed closed: {reason}")]
    PrivateEvidenceRecovery { reason: String },
    #[error("stage-7 public-evidence forensic recovery failed closed: {reason}")]
    PublicEvidenceRecovery { reason: String },
    #[error("stage-7 grid public market is unavailable, stale, or not durably captured")]
    PublicMarket,
    #[error(
        "stage-7 grid requires current successful place/cancel and hedge/reduce canary evidence"
    )]
    CanaryEvidence,
    #[error("stage-7 grid requires current successful three-by-three lifecycle Canary evidence")]
    GridCanaryEvidence,
    #[error("stage-7 grid canary did not prove the required bounded order semantics")]
    Canary,
    #[error("stage-7 grid canary failed and automatic cancel/flatten could not prove a flat state")]
    CanaryCleanup,
    #[error("stage-7 live grid flatten could not prove owned-order-free, WAL-resolved signed flat")]
    Flatten,
    #[error("stage-7 grid periodic order health check found an inconsistent live grid")]
    OrderHealth,
    #[error(
        "stage-7 grid is fenced by a prior unhealthy order health report; run grid-restart, then start from the same root after signed recovery"
    )]
    OrderHealthFenced,
    #[error("stage-7 grid clock is unavailable")]
    Clock,
    #[error("stage-7 grid writer is unavailable")]
    Writer,
    #[error("stage-7 canonical writer root registry rejected this entry: {reason}")]
    WriterRegistry { reason: String },
    #[error("stage-7 grid dispatch worker terminated unexpectedly")]
    Dispatch,
    #[error("stage-7 grid I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Strategy(#[from] HedgedGridError),
    #[error(transparent)]
    ExposureGuard(#[from] crate::strategy::hedged_grid::ExposureGuardError),
    #[error("stage-7 exposure runtime failed closed: {reason}")]
    ExposureRuntime { reason: String },
    #[error("stage-7 grid venue operation failed: {reason}")]
    Venue { reason: String },
    #[error(transparent)]
    Journal(#[from] CommandJournalError),
    #[error(transparent)]
    ExternalAlgoCleanup(#[from] crate::execution::ExternalAlgoCleanupError),
    #[error(transparent)]
    RecoveryWriter(#[from] crate::execution::RecoveryWriterError),
    #[error(transparent)]
    Lease(#[from] WriterLeaseError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Evidence(#[from] PrivateEvidenceError),
    #[error(transparent)]
    PublicJournal(#[from] crate::runtime::stage7_public_journal::Stage7PublicJournalError),
    #[error(transparent)]
    InventoryRecoveryEvidence(
        #[from] super::stage7_inventory_recovery_evidence::InventoryRecoveryEvidenceError,
    ),
    #[error(transparent)]
    CapabilityEvidence(#[from] CapabilityEvidenceError),
    #[error(transparent)]
    Legacy(#[from] HedgedGridLiveError),
}

impl From<GridVenueError> for Stage7GridError {
    fn from(error: GridVenueError) -> Self {
        Self::Venue {
            reason: error.to_string(),
        }
    }
}

impl From<crate::runtime::hedged_grid::RiskSnapshotRuntimeError> for Stage7GridError {
    fn from(error: crate::runtime::hedged_grid::RiskSnapshotRuntimeError) -> Self {
        Self::ExposureRuntime {
            reason: error.to_string(),
        }
    }
}
