//! Transport-neutral Venue control-plane core.
//!
//! The crate validates schema-v2 projections and semantic commands, then persists them through a
//! repository boundary. It has no exchange adapter, credential, writer, WAL, or artifact access.

mod account_delivery_postgres;
mod account_delivery_repository;
mod account_node_poll;
mod copy_model;
mod copy_postgres;
mod copy_relation_postgres;
mod copy_relation_repository;
mod copy_repository;
mod copy_worker;
mod http;
mod indicator_projection;
mod model;
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
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyDriftProjection, CopyJob,
    CopyLeaderEnvelope, CopyLeaderIntent, CopyLeaderSnapshot, CopyLedgerProjectionInput,
    CopyObserverLease, CopyObserverScope, CopyReplayDeliveryState, CopyReplayJob, CopyStoreResult,
    MAX_COPY_DELIVERY_CLAIM_MS, MAX_COPY_OBSERVER_LEASE_MS, MAX_COPY_SNAPSHOT_TTL_MS,
    ObservedCopyIntent, ScopedCopyDeliveryReceipt,
};
pub use copy_postgres::MIGRATION_0002;
pub use copy_relation_postgres::MIGRATION_0006;
pub use copy_relation_repository::{CopyRelationRepository, CopyRelationRepositoryError};
pub use copy_repository::{CopyRepository, CopyRepositoryError};
pub use copy_worker::{
    CopyPlanningSnapshot, CopySemanticJob, CopyWorker, CopyWorkerConfig, CopyWorkerError,
    FrozenCapitalSnapshot, MIGRATION_0003, PlannedCopyJob,
};
pub use http::{
    ControlHttpConfig, HttpServerError, control_shutdown_channel, serve_local,
    serve_local_with_indicators,
};
pub use indicator_projection::{
    IndicatorProjectionError, IndicatorProjectionStore, IndicatorProjector,
    MAX_INDICATOR_EVENT_PAGE, StoredIndicatorEvent,
};
pub use model::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};
pub use postgres::{MIGRATION_0001, MIGRATION_0005, PgControlRepository};
pub use repository::{
    CommandEnqueueResult, CommandSettleResult, ControlRepository, RepositoryError,
    SnapshotStoreResult,
};
pub use service::{ControlService, ServiceError};
