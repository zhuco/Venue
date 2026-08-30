use std::future::Future;

use thiserror::Error;

use crate::{
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyJob, CopyLeaderEnvelope,
    CopyLedgerProjectionInput, CopyObserverLease, CopyObserverScope, CopyStoreResult,
    ObservedCopyIntent, ScopedCopyDeliveryReceipt,
};

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CopyRepositoryError {
    #[error("copy repository input is invalid")]
    InvalidData,
    #[error("copy repository is unavailable")]
    Database,
    #[error("copy repository contains invalid encoded data")]
    CorruptData,
    #[error("copy repository value exceeds PostgreSQL integer range")]
    NumericRange,
    #[error("copy replay changed an immutable intent, snapshot, job, or receipt")]
    ReplayConflict,
    #[error("copy observer lease is held by another coordinator")]
    LeaseUnavailable,
    #[error("copy observer lease is stale, expired, or does not match its fencing epoch")]
    LeaseConflict,
    #[error("copy observer cursor is stale or skipped an unconsumed event")]
    CursorConflict,
    #[error("copy delivery claim or receipt does not match durable custody")]
    DeliveryConflict,
    #[error("copy ledger or drift projection conflicts with durable account facts")]
    ProjectionConflict,
}

/// Durable LIVE Copy coordination. Implementations may fence planner/observer work, but these
/// methods never acquire, renew, revoke, or impersonate an account mutation writer.
pub trait CopyRepository: Send + Sync {
    fn store_leader_envelope(
        &self,
        envelope: &CopyLeaderEnvelope,
        stored_at_ms: u64,
    ) -> impl Future<Output = Result<CopyStoreResult, CopyRepositoryError>> + Send;

    fn acquire_observer_lease(
        &self,
        scope: &CopyObserverScope,
        holder_id: &str,
        acquired_at_ms: u64,
        expires_at_ms: u64,
    ) -> impl Future<Output = Result<CopyObserverLease, CopyRepositoryError>> + Send;

    fn observe_leader_intents(
        &self,
        lease: &CopyObserverLease,
        observed_at_ms: u64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<ObservedCopyIntent>, CopyRepositoryError>> + Send;

    fn commit_copy_job(
        &self,
        lease: &CopyObserverLease,
        observed: &ObservedCopyIntent,
        job: &CopyJob,
        committed_at_ms: u64,
    ) -> impl Future<Output = Result<CopyApplyResult, CopyRepositoryError>> + Send;

    fn claim_copy_jobs(
        &self,
        scope: &CopyObserverScope,
        consumer_id: &str,
        claimed_at_ms: u64,
        expires_at_ms: u64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<CopyDeliveryClaim>, CopyRepositoryError>> + Send;

    fn record_copy_receipt(
        &self,
        scoped: &ScopedCopyDeliveryReceipt,
    ) -> impl Future<Output = Result<CopyApplyResult, CopyRepositoryError>> + Send;

    fn project_copy_ledger(
        &self,
        input: &CopyLedgerProjectionInput,
    ) -> impl Future<Output = Result<CopyApplyResult, CopyRepositoryError>> + Send;

    fn load_copy_replay(
        &self,
        scope: &CopyObserverScope,
        replayed_at_ms: u64,
    ) -> impl Future<Output = Result<CopyCrashReplay, CopyRepositoryError>> + Send;
}
