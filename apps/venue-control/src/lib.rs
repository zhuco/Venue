//! Transport-neutral Venue control-plane core.
//!
//! The crate validates schema-v2 projections and semantic commands, then persists them through a
//! repository boundary. It has no exchange adapter, credential, writer, WAL, or artifact access.

mod model;
mod postgres;
mod repository;
mod service;

pub use model::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};
pub use postgres::{MIGRATION_0001, PgControlRepository};
pub use repository::{
    CommandEnqueueResult, CommandSettleResult, ControlRepository, RepositoryError,
    SnapshotStoreResult,
};
pub use service::{ControlService, ServiceError};
