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
pub mod executor_config;
pub mod executor_exchange;
pub mod executor_runtime;
pub mod executor_secret;
pub mod executor_store;
mod grid_hot_dispatch;
pub mod grid_runtime;
pub mod grid_store;
mod http;
mod indicator_projection;
pub mod kol_executor;
pub mod kol_mvp;
pub mod kol_private_source;
pub mod leader_bot_admin;
mod model;
mod node_projection_postgres;
pub mod order_mirror;
mod postgres;
pub mod private_projection;
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
pub use executor_store::{
    ActiveKolPrivateSource, MIGRATION_0022, PlannedCopyCommand, RecoverableBinanceCommand,
};
pub use grid_hot_dispatch::{GridHotDispatchCache, GridHotDispatchToken};
pub use grid_runtime::{
    BinanceGridRuntime, BinanceGridRuntimeError, GRID_PRIVATE_STREAM_CHANNEL_CAPACITY,
    GridPrivateStreamSignal,
};
pub use grid_store::{
    BinanceGridStore, GridCommandIntent, GridCommandStatus, GridConvergenceUpdate,
    GridDesiredOrder, GridDesiredSurface, GridFillAllocation, GridLedgerCommand,
    GridLedgerCommandRecord, GridOrderOwnership, GridOwnedOrderState, GridRuntimeRecord,
    GridStoreError, MAX_GRID_DESIRED_ORDERS, MIGRATION_0021,
};
pub use http::{
    ControlHttpConfig, HttpServerError, control_shutdown_channel, serve_local,
    serve_local_with_accounts, serve_local_with_indicators,
};
pub use indicator_projection::{
    IndicatorProjectionError, IndicatorProjectionStore, IndicatorProjector,
    MAX_INDICATOR_EVENT_PAGE, StoredIndicatorEvent,
};
pub use kol_executor::{
    AccountSerialScheduler, BinanceCommandLedger, BinanceCommandLedgerError, ClaimedBinanceCommand,
    KolSourceFill, MAX_ENABLED_FOLLOWERS, MAX_ENABLED_KOLS,
};
pub use kol_mvp::{
    BINANCE_EXECUTOR_ADVISORY_LOCK, BinanceExecutorSingleton, ExecutorSingletonError,
    MIGRATION_0017, MIGRATION_0018,
};
pub use model::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};
pub use postgres::{
    MIGRATION_0001, MIGRATION_0005, MIGRATION_0009, MIGRATION_0011, MIGRATION_0012, MIGRATION_0014,
    PgControlRepository,
};
pub use private_projection::{MIGRATION_0019, MIGRATION_0020};
pub use repository::{
    CommandEnqueueResult, CommandSettleResult, ControlRepository, RepositoryError,
    SnapshotStoreResult,
};
pub use service::{ControlService, ServiceError};

pub const MIGRATION_0023: &str = include_str!("../migrations/0023_binance_grid_hot_batch.sql");
pub const MIGRATION_0024: &str = include_str!("../migrations/0024_binance_grid_batch_chain.sql");
pub const MIGRATION_0027: &str = include_str!("../migrations/0027_terminal_position_actions.sql");
pub const MIGRATION_0025: &str = include_str!("../migrations/0025_kol_market_convergence.sql");
pub const MIGRATION_0026: &str = include_str!("../migrations/0026_kol_copy_risk.sql");
pub const MIGRATION_0028: &str = include_str!("../migrations/0028_leader_order_mirror.sql");
pub const MIGRATION_0031: &str = include_str!("../migrations/0031_managed_credential_store.sql");
pub const MIGRATION_0032: &str = include_str!("../migrations/0032_follow_sizing.sql");
pub const MIGRATION_0033: &str = include_str!("../migrations/0033_managed_follow_binding.sql");
pub const MIGRATION_0034: &str = include_str!("../migrations/0034_leader_bot_catalog.sql");
pub const MIGRATION_0030: &str = include_str!("../migrations/0030_managed_followers.sql");
pub const MIGRATION_0029: &str = include_str!("../migrations/0029_mirror_gtc.sql");
mod schema;
pub use schema::{SchemaError, install_control_schema};
