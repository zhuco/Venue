use venue_control_protocol::{
    AccountDeliveryAck, AccountDeliveryClaim, AccountDeliveryClaimRequest, AccountDeliveryReceipt,
    CONTROL_SCHEMA_VERSION, CommandReceipt, CommandState, ControlCommandRequest, ControlSnapshot,
    ExecutionFactsSnapshot, NodeProjectionEnvelope, ProtocolError, UiAccountScope,
};

use crate::{
    AccountNodeBinding, ClaimedCommand, CommandEnqueueResult, CommandSettleResult,
    ControlRepository, RepositoryError, ScopedCommandReceipt, SnapshotStoreResult, StoredEvent,
};

const MAX_EVENT_PAGE: u32 = 1_000;
const MAX_COMMAND_CLAIM: u32 = 256;

impl<R> ControlService<R>
where
    R: crate::CopyRelationRepository,
{
    pub async fn upsert_copy_relation(
        &self,
        request: &venue_control_protocol::CopyRelationUpsertRequest,
        observed_ms: u64,
    ) -> Result<venue_control_protocol::CopyRelationReceipt, ServiceError> {
        request.validate()?;
        if observed_ms == 0 {
            return Err(ServiceError::InvalidObservedTime);
        }
        Ok(self
            .repository
            .upsert_copy_relation(request, observed_ms)
            .await?)
    }

    pub async fn copy_relations(
        &self,
    ) -> Result<Vec<venue_control_protocol::CopyRelationRecord>, ServiceError> {
        Ok(self.repository.list_copy_relations().await?)
    }

    pub async fn copy_relation_candidates(
        &self,
    ) -> Result<Vec<venue_control_protocol::CopyRelationCandidate>, ServiceError> {
        let candidates = self.repository.list_copy_relation_candidates().await?;
        for candidate in &candidates {
            candidate.validate()?;
        }
        Ok(candidates)
    }
}

impl<R> ControlService<R>
where
    R: crate::AccountDeliveryRepository,
{
    pub async fn claim_account_deliveries(
        &self,
        request: &AccountDeliveryClaimRequest,
        leased_at_ms: u64,
    ) -> Result<Vec<AccountDeliveryClaim>, ServiceError> {
        request.validate()?;
        if request.lease_duration_ms > crate::MAX_ACCOUNT_DELIVERY_LEASE_MS
            || request.limit > crate::MAX_ACCOUNT_DELIVERY_CLAIM
            || leased_at_ms == 0
        {
            return Err(ServiceError::InvalidDelivery(
                "account delivery lease window or limit is invalid",
            ));
        }
        let expires_at_ms = leased_at_ms
            .checked_add(request.lease_duration_ms)
            .ok_or(ServiceError::InvalidObservedTime)?;
        let claims = self
            .repository
            .claim_account_deliveries(
                &request.binding,
                &request.node_id,
                leased_at_ms,
                expires_at_ms,
                request.limit,
            )
            .await?;
        for claim in &claims {
            claim.validate()?;
        }
        Ok(claims)
    }

    pub async fn acknowledge_account_delivery(
        &self,
        ack: &AccountDeliveryAck,
    ) -> Result<crate::DeliveryStoreResult, ServiceError> {
        ack.validate()?;
        Ok(self.repository.acknowledge_account_delivery(ack).await?)
    }

    pub async fn record_account_delivery_receipt(
        &self,
        receipt: &AccountDeliveryReceipt,
    ) -> Result<crate::DeliveryStoreResult, ServiceError> {
        receipt.validate()?;
        Ok(self
            .repository
            .record_account_delivery_receipt(receipt)
            .await?)
    }
}

pub struct ControlService<R> {
    repository: R,
}

impl<R> ControlService<R>
where
    R: ControlRepository,
{
    pub const fn new(repository: R) -> Self {
        Self { repository }
    }

    pub fn repository(&self) -> &R {
        &self.repository
    }

    pub async fn snapshot(&self) -> Result<ControlSnapshot, ServiceError> {
        let snapshot = self
            .repository
            .load_snapshot()
            .await?
            .ok_or(ServiceError::SnapshotUnavailable)?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub async fn publish_snapshot(
        &self,
        snapshot: &ControlSnapshot,
    ) -> Result<SnapshotStoreResult, ServiceError> {
        snapshot.validate()?;
        Ok(self.repository.store_snapshot(snapshot).await?)
    }

    pub async fn execution_facts(&self) -> Result<ExecutionFactsSnapshot, ServiceError> {
        let facts = self
            .repository
            .load_execution_facts()
            .await?
            .ok_or(ServiceError::SnapshotUnavailable)?;
        facts.validate()?;
        Ok(facts)
    }

    /// Signed exchange facts remain a node-owned observation; Control only stores this read model.
    pub async fn publish_execution_facts(
        &self,
        facts: &ExecutionFactsSnapshot,
    ) -> Result<SnapshotStoreResult, ServiceError> {
        facts.validate()?;
        Ok(self.repository.store_execution_facts(facts).await?)
    }

    pub async fn merge_node_projection(
        &self,
        projection: &NodeProjectionEnvelope,
    ) -> Result<SnapshotStoreResult, ServiceError> {
        projection.validate()?;
        Ok(self.repository.merge_node_projection(projection).await?)
    }

    pub async fn submit_command(
        &self,
        command: &ControlCommandRequest,
        observed_ms: u64,
    ) -> Result<CommandReceipt, ServiceError> {
        command.validate()?;
        if observed_ms == 0 {
            return Err(ServiceError::InvalidObservedTime);
        }
        let snapshot = self.snapshot().await?;
        if snapshot.generated_ms > observed_ms {
            return Err(ServiceError::InvalidObservedTime);
        }
        let strategy = snapshot
            .strategies
            .iter()
            .find(|strategy| {
                strategy.venue == command.venue
                    && strategy.mode == command.mode
                    && strategy.trading_account_id == command.trading_account_id
                    && strategy.instance_id == command.instance_id
                    && strategy.symbol == command.symbol
                    && strategy.config_epoch == command.expected_config_epoch
            })
            .ok_or(ServiceError::StaleOrMismatchedScope)?;
        if command.action == venue_control_protocol::ControlAction::Trade
            && strategy.kind != venue_control_protocol::StrategyKind::Manual
        {
            return Err(ServiceError::InvalidDelivery(
                "Trade commands require an exact Manual strategy binding",
            ));
        }
        if !self.repository.has_current_strategy_scope(command).await? {
            return Err(ServiceError::StaleOrMismatchedScope);
        }

        let accepted = CommandReceipt {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: command.request_id.clone(),
            state: CommandState::Accepted,
            receipt_id: format!("control-accepted:{}", command.request_id),
            observed_ms,
            detail: "durably queued for the bound account node".to_owned(),
        };
        accepted.validate()?;
        let enqueue = match self.repository.enqueue_command(command, &accepted).await {
            Err(RepositoryError::StaleScope) => {
                return Err(ServiceError::StaleOrMismatchedScope);
            }
            result => result?,
        };
        match enqueue {
            CommandEnqueueResult::Inserted(receipt) | CommandEnqueueResult::Existing(receipt) => {
                Ok(receipt)
            }
        }
    }

    pub async fn claim_commands(
        &self,
        binding: &AccountNodeBinding,
        consumer_id: &str,
        claimed_ms: u64,
        limit: u32,
    ) -> Result<Vec<ClaimedCommand>, ServiceError> {
        binding.validate().map_err(ServiceError::InvalidDelivery)?;
        if consumer_id.trim().is_empty() || claimed_ms == 0 {
            return Err(ServiceError::InvalidDelivery(
                "command claim identity or time is missing",
            ));
        }
        if !(1..=MAX_COMMAND_CLAIM).contains(&limit) {
            return Err(ServiceError::InvalidLimit);
        }
        if !self
            .repository
            .has_current_account_scope(binding.venue, binding.mode, &binding.trading_account_id)
            .await?
        {
            return Err(ServiceError::StaleOrMismatchedScope);
        }
        Ok(self
            .repository
            .claim_commands(binding, consumer_id, claimed_ms, limit)
            .await?)
    }

    pub async fn record_receipt(
        &self,
        scoped: &ScopedCommandReceipt,
    ) -> Result<CommandReceipt, ServiceError> {
        scoped.validate().map_err(ServiceError::InvalidDelivery)?;
        match self.repository.settle_command(scoped).await? {
            CommandSettleResult::Stored(receipt) | CommandSettleResult::Existing(receipt) => {
                Ok(receipt)
            }
        }
    }

    pub async fn events(
        &self,
        scope: &UiAccountScope,
        after_sequence: i64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, ServiceError> {
        if after_sequence < 0 || !(1..=MAX_EVENT_PAGE).contains(&limit) {
            return Err(ServiceError::InvalidLimit);
        }
        scope.validate()?;
        let events = self
            .repository
            .list_events(scope, after_sequence, limit)
            .await?;
        for stored in &events {
            if stored.sequence <= after_sequence {
                return Err(ServiceError::Repository(RepositoryError::CorruptData));
            }
            stored.event.validate()?;
        }
        Ok(events)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ServiceError {
    #[error("control protocol validation failed: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("no durable control snapshot is available")]
    SnapshotUnavailable,
    #[error("command scope or config epoch is stale or mismatched")]
    StaleOrMismatchedScope,
    #[error("control timestamp is missing or precedes the current snapshot")]
    InvalidObservedTime,
    #[error("invalid control page or claim limit")]
    InvalidLimit,
    #[error("invalid command delivery: {0}")]
    InvalidDelivery(&'static str),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    AccountDeliveryRepository(#[from] crate::AccountDeliveryRepositoryError),
    #[error(transparent)]
    CopyRelationRepository(#[from] crate::CopyRelationRepositoryError),
}

#[cfg(test)]
mod tests;
