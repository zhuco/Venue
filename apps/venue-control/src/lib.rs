//! Transport-neutral Venue control-plane core.
//!
//! The crate validates schema-v2 projections and semantic commands, then persists them through a
//! repository boundary. It has no exchange adapter, credential, writer, WAL, or artifact access.

mod copy_model;
mod copy_postgres;
mod copy_repository;
mod http;
mod model;
mod postgres;
mod repository;
mod service;

pub use copy_model::{
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyDriftProjection, CopyLeaderEnvelope,
    CopyLeaderIntent, CopyLeaderSnapshot, CopyLedgerProjectionInput, CopyObserverLease,
    CopyObserverScope, CopyReplayDeliveryState, CopyReplayJob, CopyStoreResult, CopyTestJob,
    MAX_COPY_DELIVERY_CLAIM_MS, MAX_COPY_OBSERVER_LEASE_MS, MAX_COPY_SNAPSHOT_TTL_MS,
    ObservedCopyIntent, ScopedCopyDeliveryReceipt,
};
pub use copy_postgres::MIGRATION_0002;
pub use copy_repository::{CopyRepository, CopyRepositoryError};
pub use http::{ControlHttpConfig, HttpServerError, control_shutdown_channel, serve_local};
pub use model::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};
pub use postgres::{MIGRATION_0001, PgControlRepository};
pub use repository::{
    CommandEnqueueResult, CommandSettleResult, ControlRepository, RepositoryError,
    SnapshotStoreResult,
};
pub use service::{ControlService, ServiceError};
