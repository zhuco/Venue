use std::future::Future;

use thiserror::Error;
use venue_control_protocol::{
    CommandReceipt, ControlCommandRequest, ControlSnapshot, GatewayMode, VenueId,
};

use crate::{AccountNodeBinding, ClaimedCommand, ScopedCommandReceipt, StoredEvent};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotStoreResult {
    Inserted { event_sequence: i64 },
    Unchanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandEnqueueResult {
    Inserted(CommandReceipt),
    Existing(CommandReceipt),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandSettleResult {
    Stored(CommandReceipt),
    Existing(CommandReceipt),
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum RepositoryError {
    #[error("control repository is unavailable")]
    Database,
    #[error("control repository contains invalid encoded data")]
    CorruptData,
    #[error("control repository value exceeds PostgreSQL integer range")]
    NumericRange,
    #[error("snapshot generation is stale or conflicts with durable state")]
    SnapshotConflict,
    #[error("request id was replayed with a different command scope or payload")]
    ReplayConflict,
    #[error("command scope or config epoch is no longer current")]
    StaleScope,
    #[error("command delivery or receipt does not match durable custody")]
    DeliveryConflict,
}

pub trait ControlRepository: Send + Sync {
    fn load_snapshot(
        &self,
    ) -> impl Future<Output = Result<Option<ControlSnapshot>, RepositoryError>> + Send;

    fn store_snapshot(
        &self,
        snapshot: &ControlSnapshot,
    ) -> impl Future<Output = Result<SnapshotStoreResult, RepositoryError>> + Send;

    fn enqueue_command(
        &self,
        command: &ControlCommandRequest,
        accepted: &CommandReceipt,
    ) -> impl Future<Output = Result<CommandEnqueueResult, RepositoryError>> + Send;

    fn claim_commands(
        &self,
        binding: &AccountNodeBinding,
        consumer_id: &str,
        claimed_ms: u64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<ClaimedCommand>, RepositoryError>> + Send;

    fn settle_command(
        &self,
        scoped: &ScopedCommandReceipt,
    ) -> impl Future<Output = Result<CommandSettleResult, RepositoryError>> + Send;

    fn list_events(
        &self,
        after_sequence: i64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<StoredEvent>, RepositoryError>> + Send;

    fn has_current_strategy_scope(
        &self,
        command: &ControlCommandRequest,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send;

    fn has_current_account_scope(
        &self,
        venue: VenueId,
        mode: GatewayMode,
        trading_account_id: &str,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send;
}
