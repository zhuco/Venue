use super::*;

impl CopyWorker {
    /// Settles one already durable Node rejection into Copy's read-only receipt history. This is
    /// deliberately separate from planning and cannot produce a replacement child.
    pub async fn project_next_rejected_delivery(
        &self,
    ) -> Result<Option<CopyApplyResult>, CopyWorkerError> {
        self.repository
            .project_next_rejected_copy_delivery(&self.config.scope)
            .await
            .map_err(Into::into)
    }

    /// Projects one terminal Node Copy result into the immutable ledger/drift read model. This
    /// is deliberately separate from planning: it can account for a paused or expired delivery,
    /// but it never creates a new delivery or execution request.
    pub async fn project_next_reconciled_ledger(
        &self,
        projected_at_ms: u64,
    ) -> Result<Option<CopyApplyResult>, CopyWorkerError> {
        self.repository
            .project_next_reconciled_copy_ledger(&self.config.scope, projected_at_ms)
            .await
            .map_err(Into::into)
    }
}
