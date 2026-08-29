use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use venue_control_protocol::GatewayMode;
use venue_copy::{
    CapitalSnapshot, DeliveryBinding, FollowerDeliveryManifest, TargetExposureError,
    TargetExposurePlan, TargetExposureRequest, derive_copy_identities, reduce_target_exposure,
};
use venue_domain::domain::Amount;

use crate::{
    CopyApplyResult, CopyCrashReplay, CopyDeliveryClaim, CopyLedgerProjectionInput,
    CopyObserverScope, CopyRepository, CopyRepositoryError, CopyTestJob, ObservedCopyIntent,
    PgControlRepository, ScopedCopyDeliveryReceipt,
};

pub const MIGRATION_0003: &str = include_str!("../migrations/0003_copy_planner_worker.sql");

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrozenCapitalSnapshot {
    pub generation: u64,
    pub observed_ms: u64,
    pub expires_ms: u64,
    pub leader_strategy_capital: Amount,
    pub leader_target_exposure: Amount,
    pub follower_configured_capital: Amount,
    pub follower_allocated_capital: Amount,
    pub follower_available_margin: Amount,
    pub follower_managed_exposure: Amount,
    pub margin_safety_reserve_rate: Decimal,
}

impl FrozenCapitalSnapshot {
    fn as_planner_input(&self) -> CapitalSnapshot {
        CapitalSnapshot {
            generation: self.generation,
            observed_ms: self.observed_ms,
            expires_ms: self.expires_ms,
            leader_strategy_capital: self.leader_strategy_capital.clone(),
            leader_target_exposure: self.leader_target_exposure.clone(),
            follower_configured_capital: self.follower_configured_capital.clone(),
            follower_allocated_capital: self.follower_allocated_capital.clone(),
            follower_available_margin: self.follower_available_margin.clone(),
            follower_managed_exposure: self.follower_managed_exposure.clone(),
            margin_safety_reserve_rate: self.margin_safety_reserve_rate,
        }
    }
}

/// Typed contents of `CopyLeaderSnapshot::snapshot_payload` consumed by the TEST planner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyPlanningSnapshot {
    pub capital: FrozenCapitalSnapshot,
    pub binding: DeliveryBinding,
    pub instrument_generation: u64,
    pub delivery_expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopySemanticJob {
    pub target: TargetExposurePlan,
    pub leader_intent: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedCopyJob {
    pub observed: ObservedCopyIntent,
    pub frozen_capital: FrozenCapitalSnapshot,
    pub target: TargetExposurePlan,
    pub job: CopyTestJob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyTestWorkerConfig {
    pub mode: GatewayMode,
    pub scope: CopyObserverScope,
    pub worker_id: String,
    pub observer_lease_ms: u64,
    pub delivery_claim_ms: u64,
}

impl CopyTestWorkerConfig {
    fn validate(&self) -> Result<(), CopyWorkerError> {
        if self.mode != GatewayMode::Test {
            return Err(CopyWorkerError::LiveDisabled);
        }
        self.scope
            .validate()
            .map_err(|_| CopyWorkerError::InvalidConfig)?;
        if self.worker_id.trim().is_empty()
            || self.observer_lease_ms == 0
            || self.observer_lease_ms > crate::MAX_COPY_OBSERVER_LEASE_MS
            || self.delivery_claim_ms == 0
            || self.delivery_claim_ms > crate::MAX_COPY_DELIVERY_CLAIM_MS
        {
            return Err(CopyWorkerError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CopyTestWorker {
    repository: PgControlRepository,
    config: CopyTestWorkerConfig,
}

impl CopyTestWorker {
    pub fn new(
        repository: PgControlRepository,
        config: CopyTestWorkerConfig,
    ) -> Result<Self, CopyWorkerError> {
        config.validate()?;
        Ok(Self { repository, config })
    }

    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn config(&self) -> &CopyTestWorkerConfig {
        &self.config
    }

    /// Replays the durable cursor, receipts, delivery fences, ledger, and drift projections before
    /// new planning work is accepted after process restart.
    pub async fn recover(&self, now_ms: u64) -> Result<CopyCrashReplay, CopyWorkerError> {
        self.repository
            .load_copy_worker_replay(&self.config.scope, now_ms)
            .await
            .map_err(Into::into)
    }

    /// Claims and plans exactly the next leader event. The PostgreSQL implementation locks the
    /// durable cursor and persists frozen capital, target, manifest, job, and cursor atomically.
    pub async fn plan_next(&self, now_ms: u64) -> Result<Option<PlannedCopyJob>, CopyWorkerError> {
        let expires_at_ms = now_ms
            .checked_add(self.config.observer_lease_ms)
            .ok_or(CopyWorkerError::InvalidTime)?;
        let lease = self
            .repository
            .acquire_observer_lease(
                &self.config.scope,
                &self.config.worker_id,
                now_ms,
                expires_at_ms,
            )
            .await?;
        self.repository
            .plan_next_copy_job_atomic(&lease, now_ms)
            .await
            .map_err(Into::into)
    }

    pub async fn claim_deliveries(
        &self,
        consumer_id: &str,
        now_ms: u64,
        limit: u32,
    ) -> Result<Vec<CopyDeliveryClaim>, CopyWorkerError> {
        let expires_at_ms = now_ms
            .checked_add(self.config.delivery_claim_ms)
            .ok_or(CopyWorkerError::InvalidTime)?;
        self.repository
            .claim_copy_jobs(
                &self.config.scope,
                consumer_id,
                now_ms,
                expires_at_ms,
                limit,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn record_receipt(
        &self,
        receipt: &ScopedCopyDeliveryReceipt,
    ) -> Result<CopyApplyResult, CopyWorkerError> {
        self.repository
            .record_copy_receipt(receipt)
            .await
            .map_err(Into::into)
    }

    pub async fn project_ledger(
        &self,
        projection: &CopyLedgerProjectionInput,
    ) -> Result<CopyApplyResult, CopyWorkerError> {
        self.repository
            .project_copy_ledger(projection)
            .await
            .map_err(Into::into)
    }
}

pub(crate) fn plan_observed_copy_job(
    observed: ObservedCopyIntent,
    planned_at_ms: u64,
) -> Result<PlannedCopyJob, CopyWorkerError> {
    observed
        .envelope
        .validate(planned_at_ms)
        .map_err(|_| CopyWorkerError::InvalidPlanningInput)?;
    let snapshot: CopyPlanningSnapshot =
        serde_json::from_value(observed.envelope.snapshot.snapshot_payload.clone())
            .map_err(|_| CopyWorkerError::InvalidPlanningInput)?;
    if snapshot.capital.generation != observed.envelope.snapshot.generation
        || snapshot.binding.account_id != observed.envelope.scope.trading_account_id
        || snapshot.instrument_generation == 0
        || snapshot.delivery_expires_at_ms > observed.envelope.snapshot.expires_at_ms
    {
        return Err(CopyWorkerError::InvalidPlanningInput);
    }
    let target = reduce_target_exposure(&TargetExposureRequest {
        expected_generation: observed.envelope.snapshot.generation,
        now_ms: planned_at_ms,
        snapshot: snapshot.capital.as_planner_input(),
    })?;
    let identities = derive_copy_identities(&observed.envelope.intent.identity_input)
        .map_err(|_| CopyWorkerError::InvalidPlanningInput)?;
    let semantic = CopySemanticJob {
        target: target.clone(),
        leader_intent: observed.envelope.intent.intent_payload.clone(),
    };
    // The immutable outbox digest is the producer commitment to this exact intent/snapshot event.
    // Recovery re-runs the reducer and compares the complete capital, target, manifest, and job.
    let plan_digest = observed.event_digest;
    let manifest = FollowerDeliveryManifest {
        identities,
        binding: snapshot.binding,
        plan_digest,
        snapshot_generation: observed.envelope.snapshot.generation,
        instrument_generation: snapshot.instrument_generation,
        issued_at_ms: planned_at_ms,
        expires_at_ms: snapshot.delivery_expires_at_ms,
    };
    manifest
        .validate(planned_at_ms)
        .map_err(|_| CopyWorkerError::InvalidPlanningInput)?;
    let job = CopyTestJob {
        scope: observed.envelope.scope.clone(),
        source_event_sequence: observed.event_sequence,
        intent_id: observed.envelope.intent.intent_id,
        identities,
        manifest,
        job_digest: plan_digest,
        semantic_job: serde_json::to_value(semantic)
            .map_err(|_| CopyWorkerError::InvalidPlanningInput)?,
        created_at_ms: planned_at_ms,
    };
    job.validate_against(&observed)
        .map_err(|_| CopyWorkerError::InvalidPlanningInput)?;
    Ok(PlannedCopyJob {
        observed,
        frozen_capital: snapshot.capital,
        target,
        job,
    })
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CopyWorkerError {
    #[error("LIVE copy control worker is disabled")]
    LiveDisabled,
    #[error("copy worker configuration is invalid")]
    InvalidConfig,
    #[error("copy worker clock overflowed")]
    InvalidTime,
    #[error("frozen copy planning input is invalid, stale, or inconsistent")]
    InvalidPlanningInput,
    #[error("pure target exposure planning failed: {0}")]
    Target(#[from] TargetExposureError),
    #[error("copy PostgreSQL repository failed: {0}")]
    Repository(#[from] CopyRepositoryError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::VenueId;
    use venue_copy::{CopyAction, CopyIdentityInput};
    use venue_domain::domain::{Asset, InstrumentIdentity, MarketKind, Symbol};

    #[tokio::test]
    async fn live_worker_is_fail_closed_before_database_access() {
        let pool = sqlx::PgPool::connect_lazy("postgres://unused:unused@127.0.0.1/unused")
            .map(PgControlRepository::new);
        let Ok(repository) = pool else {
            return;
        };
        let result = CopyTestWorker::new(
            repository,
            CopyTestWorkerConfig {
                mode: GatewayMode::Live,
                scope: CopyObserverScope {
                    observer_id: "leader-a".to_owned(),
                    venue: venue_control_protocol::VenueId::Binance,
                    trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                },
                worker_id: "planner-a".to_owned(),
                observer_lease_ms: 1_000,
                delivery_claim_ms: 1_000,
            },
        );
        assert_eq!(result.map(|_| ()), Err(CopyWorkerError::LiveDisabled));
    }

    #[test]
    fn migration_keeps_planner_evidence_test_only_and_non_authoritative() {
        assert!(MIGRATION_0003.contains("CHECK (mode = 'TEST')"));
        assert!(!MIGRATION_0003.contains("writer_generation"));
        assert!(!MIGRATION_0003.contains("dispatch_permit"));
        assert!(!MIGRATION_0003.contains("mutation_authority BOOLEAN"));
    }

    #[test]
    fn frozen_capital_target_and_manifest_are_deterministic()
    -> Result<(), Box<dyn std::error::Error>> {
        let observed = observed(100, 0)?;
        let first = plan_observed_copy_job(observed.clone(), 101)?;
        let second = plan_observed_copy_job(observed, 101)?;
        assert_eq!(first, second);
        assert_eq!(first.target.snapshot_generation, 1);
        assert_eq!(first.target.target_exposure.value, Decimal::from(80));
        assert_eq!(first.job.job_digest, first.job.manifest.plan_digest);
        assert!(
            !first
                .job
                .manifest
                .delivery_digest()
                .iter()
                .all(|byte| *byte == 0)
        );
        Ok(())
    }

    #[test]
    fn stale_capital_and_cross_zero_target_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let stale = observed(100, 0)?;
        assert_eq!(
            plan_observed_copy_job(stale, 301),
            Err(CopyWorkerError::InvalidPlanningInput)
        );
        let crossing = observed(100, 20)?;
        assert_eq!(
            plan_observed_copy_job(crossing, 101),
            Err(CopyWorkerError::Target(
                TargetExposureError::DirectionFlipRequiresSplit
            ))
        );
        Ok(())
    }

    fn observed(
        now_ms: u64,
        managed_exposure: i64,
    ) -> Result<ObservedCopyIntent, Box<dyn std::error::Error>> {
        let identity_input = CopyIdentityInput {
            event_id: [1; 16],
            source_event_id: [2; 16],
            follower_account_id: [3; 16],
            follower_binding_id: [4; 16],
            leader_order_id: [5; 16],
            revision: 1,
            action: CopyAction::New,
        };
        let identities = derive_copy_identities(&identity_input)?;
        let related = derive_copy_identities(&CopyIdentityInput {
            event_id: [11; 16],
            source_event_id: [12; 16],
            follower_account_id: [13; 16],
            follower_binding_id: [14; 16],
            leader_order_id: [15; 16],
            revision: 1,
            action: CopyAction::New,
        })?;
        let account_id = "00000000-0000-4000-8000-000000000001".to_owned();
        let quote = Asset::new("USDT")?;
        let amount = |value| Amount::new(quote.clone(), Decimal::from(value));
        let planning = CopyPlanningSnapshot {
            capital: FrozenCapitalSnapshot {
                generation: 1,
                observed_ms: now_ms,
                expires_ms: now_ms + 200,
                leader_strategy_capital: amount(1_000),
                leader_target_exposure: amount(if managed_exposure == 0 { 200 } else { -200 }),
                follower_configured_capital: amount(500),
                follower_allocated_capital: amount(400),
                follower_available_margin: amount(450),
                follower_managed_exposure: amount(managed_exposure),
                margin_safety_reserve_rate: Decimal::new(1, 1),
            },
            binding: DeliveryBinding {
                leader_id: related.job_id,
                follower_id: related.planning_snapshot_id,
                follower_binding_id: related.child_order_id,
                account_id: account_id.clone(),
                instrument: InstrumentIdentity {
                    symbol: "BTC/USDT".parse::<Symbol>()?,
                    market: MarketKind::LinearPerpetual,
                    settlement_asset: Some(quote),
                },
                policy_id: identities.job_id,
            },
            instrument_generation: 1,
            delivery_expires_at_ms: now_ms + 200,
        };
        let envelope = crate::CopyLeaderEnvelope {
            scope: CopyObserverScope {
                observer_id: "leader-a".to_owned(),
                venue: VenueId::Binance,
                trading_account_id: account_id,
            },
            intent: crate::CopyLeaderIntent {
                intent_id: identities.child_order_id,
                snapshot_id: identities.planning_snapshot_id,
                identity_input,
                intent_digest: [7; 32],
                intent_payload: serde_json::json!({"semantic_action": "FOLLOW_TARGET"}),
                observed_at_ms: now_ms,
            },
            snapshot: crate::CopyLeaderSnapshot {
                snapshot_id: identities.planning_snapshot_id,
                generation: 1,
                observed_at_ms: now_ms,
                expires_at_ms: now_ms + 200,
                snapshot_digest: [8; 32],
                snapshot_payload: serde_json::to_value(planning)?,
            },
            outbox_digest: [9; 32],
        };
        Ok(ObservedCopyIntent {
            event_sequence: 1,
            event_digest: envelope.outbox_digest,
            envelope,
        })
    }
}
