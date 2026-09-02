//! Transport-neutral Venue control-plane core.
//!
//! The crate validates schema-v2 projections and semantic commands, then persists them through a
//! repository boundary. Account administration encrypts credentials and uses read-only
//! adapter verification; it has no trading writer, command WAL, or artifact access.

mod account_delivery_postgres;
mod account_delivery_repository;
mod account_node_poll;
pub mod accounts;
mod copy_ledger_read_model;
mod copy_model;
mod copy_postgres;
mod copy_relation_postgres;
mod copy_relation_repository;
mod copy_repository;
mod copy_worker;
mod http;
mod indicator_projection;
pub mod kol_mvp;
mod model;
mod node_projection_postgres;
mod postgres;
mod repository;
mod service;

pub use account_delivery_postgres::{
    MAX_ACCOUNT_DELIVERY_CLAIM, MAX_ACCOUNT_DELIVERY_LEASE_MS, MIGRATION_0004,
};
pub use account_delivery_repository::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, DeliveryStoreResult,
};
pub use account_node_poll::{
    MAX_ACCOUNT_NODE_HTTP_BODY_BYTES, MAX_ACCOUNT_NODE_HTTP_RESPONSE_BYTES,
    MAX_ACCOUNT_NODE_HTTP_TIMEOUT,
};
pub use copy_model::{
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyDriftProjection,
    CopyExecutionProjectionInput, CopyJob, CopyLeaderEnvelope, CopyLeaderIntent,
    CopyLeaderSnapshot, CopyLedgerProjectionInput, CopyObserverLease, CopyObserverScope,
    CopyReplayDeliveryState, CopyReplayJob, CopyStoreResult, MAX_COPY_DELIVERY_CLAIM_MS,
    MAX_COPY_OBSERVER_LEASE_MS, MAX_COPY_SNAPSHOT_TTL_MS, ObservedCopyIntent,
    ScopedCopyDeliveryReceipt,
};
pub use copy_postgres::{
    MIGRATION_0002, MIGRATION_0007, MIGRATION_0008, MIGRATION_0013, MIGRATION_0016,
};
pub use copy_relation_postgres::{MIGRATION_0006, MIGRATION_0010};
pub use copy_relation_repository::{CopyRelationRepository, CopyRelationRepositoryError};
pub use copy_repository::{CopyRepository, CopyRepositoryError};
pub use copy_worker::{
    CopyPlanningSnapshot, CopySemanticJob, CopyWorker, CopyWorkerConfig, CopyWorkerError,
    FrozenCapitalSnapshot, MIGRATION_0003, PlannedCopyJob, relation_commitment,
};
pub use http::{
    ControlHttpConfig, HttpServerError, control_shutdown_channel, serve_local,
    serve_local_with_accounts, serve_local_with_indicators,
};
pub use indicator_projection::{
    IndicatorProjectionError, IndicatorProjectionStore, IndicatorProjector,
    MAX_INDICATOR_EVENT_PAGE, StoredIndicatorEvent,
};
pub use kol_mvp::{
    BINANCE_EXECUTOR_ADVISORY_LOCK, BinanceExecutorSingleton, ExecutorSingletonError,
    MIGRATION_0017,
};
pub use model::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};
pub use postgres::{
    MIGRATION_0001, MIGRATION_0005, MIGRATION_0009, MIGRATION_0011, MIGRATION_0012, MIGRATION_0014,
    PgControlRepository,
};
pub use repository::{
    CommandEnqueueResult, CommandSettleResult, ControlRepository, RepositoryError,
    SnapshotStoreResult,
};
pub use service::{ControlService, ServiceError};
