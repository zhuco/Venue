use std::future::Future;

use thiserror::Error;
use venue_control_protocol::{
    CopyRelationCandidate, CopyRelationReceipt, CopyRelationRecord, CopyRelationUpsertRequest,
};

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CopyRelationRepositoryError {
    #[error("copy relation input is invalid")]
    InvalidData,
    #[error("copy relation repository is unavailable")]
    Database,
    #[error("copy relation repository contains invalid encoded data")]
    CorruptData,
    #[error("copy relation revision, identity, or follower binding conflicts with durable state")]
    Conflict,
}

/// Stores product configuration only. It does not grant an account writer, WAL ownership, or
/// permission to dispatch a semantic job.
pub trait CopyRelationRepository: Send + Sync {
    fn upsert_copy_relation(
        &self,
        request: &CopyRelationUpsertRequest,
        observed_ms: u64,
    ) -> impl Future<Output = Result<CopyRelationReceipt, CopyRelationRepositoryError>> + Send;

    fn list_copy_relations(
        &self,
    ) -> impl Future<Output = Result<Vec<CopyRelationRecord>, CopyRelationRepositoryError>> + Send;

    fn list_copy_relation_candidates(
        &self,
    ) -> impl Future<Output = Result<Vec<CopyRelationCandidate>, CopyRelationRepositoryError>> + Send
    {
        async { Ok(Vec::new()) }
    }
}
