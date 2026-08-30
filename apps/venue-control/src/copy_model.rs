use serde::{Deserialize, Serialize};
use serde_json::Value;
use venue_control_protocol::{GatewayMode, VenueId};
use venue_copy::{
    AuthoritativePositionSnapshot, CopyId, CopyIdentityInput, CopyIdentitySet, DriftRepairError,
    DriftRepairPlanRequest, DriftRepairRequest, FollowerDeliveryManifest, LedgerEntry,
    PersistedDeliveryReceipt, TargetExposurePlan, derive_copy_identities, plan_drift_repair,
};
use venue_domain::domain::is_canonical_trading_account_id;

fn deserialize_live_mode<'de, D>(deserializer: D) -> Result<GatewayMode, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let mode = GatewayMode::deserialize(deserializer)?;
    if mode == GatewayMode::Live {
        Ok(mode)
    } else {
        Err(serde::de::Error::custom("copy mode must be exactly LIVE"))
    }
}

pub const MAX_COPY_OBSERVER_LEASE_MS: u64 = 60_000;
pub const MAX_COPY_DELIVERY_CLAIM_MS: u64 = 60_000;
pub const MAX_COPY_SNAPSHOT_TTL_MS: u64 = 5 * 60_000;

/// A PostgreSQL coordination scope for semantic Copy planning. It is bound to LIVE while remaining
/// unrelated to a gateway capability, writer generation, WAL position, or dispatch permit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyObserverScope {
    pub observer_id: String,
    pub venue: VenueId,
    #[serde(deserialize_with = "deserialize_live_mode")]
    pub mode: GatewayMode,
    pub trading_account_id: String,
}

impl CopyObserverScope {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.mode != GatewayMode::Live
            || self.observer_id.trim().is_empty()
            || !is_canonical_trading_account_id(&self.trading_account_id)
        {
            return Err("copy observer scope is incomplete");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyLeaderSnapshot {
    pub snapshot_id: CopyId,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub snapshot_digest: [u8; 32],
    pub snapshot_payload: Value,
}

impl CopyLeaderSnapshot {
    pub(crate) fn validate(&self, now_ms: u64) -> Result<(), &'static str> {
        let ttl = self
            .expires_at_ms
            .checked_sub(self.observed_at_ms)
            .ok_or("copy snapshot window is malformed")?;
        if self.snapshot_id.is_nil()
            || self.generation == 0
            || self.observed_at_ms == 0
            || ttl == 0
            || ttl > MAX_COPY_SNAPSHOT_TTL_MS
            || now_ms < self.observed_at_ms
            || now_ms >= self.expires_at_ms
            || self.snapshot_digest == [0; 32]
            || self.snapshot_payload.is_null()
        {
            return Err("copy snapshot is invalid or stale");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyLeaderIntent {
    pub intent_id: CopyId,
    pub snapshot_id: CopyId,
    pub identity_input: CopyIdentityInput,
    pub intent_digest: [u8; 32],
    pub intent_payload: Value,
    pub observed_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyLeaderEnvelope {
    pub scope: CopyObserverScope,
    pub intent: CopyLeaderIntent,
    pub snapshot: CopyLeaderSnapshot,
    pub outbox_digest: [u8; 32],
}

impl CopyLeaderEnvelope {
    pub(crate) fn validate(&self, now_ms: u64) -> Result<(), &'static str> {
        self.scope.validate()?;
        self.snapshot.validate(now_ms)?;
        derive_copy_identities(&self.intent.identity_input)
            .map_err(|_| "copy identity input is invalid")?;
        if self.intent.intent_id.is_nil()
            || self.intent.snapshot_id != self.snapshot.snapshot_id
            || self.intent.intent_digest == [0; 32]
            || self.outbox_digest == [0; 32]
            || self.intent.intent_payload.is_null()
            || self.intent.observed_at_ms == 0
            || self.intent.observed_at_ms > now_ms
            || self.intent.observed_at_ms > self.snapshot.observed_at_ms
        {
            return Err("copy leader envelope is inconsistent");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObservedCopyIntent {
    pub event_sequence: i64,
    pub event_digest: [u8; 32],
    pub envelope: CopyLeaderEnvelope,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyJob {
    pub scope: CopyObserverScope,
    pub source_event_sequence: i64,
    pub intent_id: CopyId,
    pub identities: CopyIdentitySet,
    pub manifest: FollowerDeliveryManifest,
    pub job_digest: [u8; 32],
    pub semantic_job: Value,
    pub created_at_ms: u64,
}

impl CopyJob {
    pub(crate) fn validate_against(
        &self,
        observed: &ObservedCopyIntent,
    ) -> Result<(), &'static str> {
        self.scope.validate()?;
        let expected = derive_copy_identities(&observed.envelope.intent.identity_input)
            .map_err(|_| "copy identity input is invalid")?;
        self.manifest
            .validate(self.created_at_ms)
            .map_err(|_| "copy delivery manifest is invalid")?;
        if self.scope != observed.envelope.scope
            || self.source_event_sequence != observed.event_sequence
            || self.intent_id != observed.envelope.intent.intent_id
            || self.identities != expected
            || self.manifest.identities != expected
            || self.identities.planning_snapshot_id != observed.envelope.snapshot.snapshot_id
            || self.manifest.snapshot_generation != observed.envelope.snapshot.generation
            || self.manifest.binding.account_id != self.scope.trading_account_id
            || self.job_digest == [0; 32]
            || self.job_digest != self.manifest.plan_digest
            || self.semantic_job.is_null()
        {
            return Err("deterministic copy job does not match its immutable leader input");
        }
        Ok(())
    }
}

/// Coordination-only lease. `grants_mutation_authority` is intentionally a constant false.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyObserverLease {
    pub scope: CopyObserverScope,
    pub holder_id: String,
    pub lease_epoch: u64,
    pub acquired_at_ms: u64,
    pub expires_at_ms: u64,
}

impl CopyObserverLease {
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }

    pub(crate) fn validate(&self, now_ms: u64) -> Result<(), &'static str> {
        self.scope.validate()?;
        let ttl = self
            .expires_at_ms
            .checked_sub(self.acquired_at_ms)
            .ok_or("copy observer lease window is malformed")?;
        if self.holder_id.trim().is_empty()
            || self.lease_epoch == 0
            || self.acquired_at_ms == 0
            || ttl == 0
            || ttl > MAX_COPY_OBSERVER_LEASE_MS
            || now_ms < self.acquired_at_ms
            || now_ms >= self.expires_at_ms
        {
            return Err("copy observer lease is invalid or expired");
        }
        Ok(())
    }
}

/// At-least-once database delivery custody. The account node must still durably install the job in
/// its Actor inbox and pass Execution/Risk/Owner/WAL/Reconciliation before any mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyDeliveryClaim {
    pub job: CopyJob,
    pub consumer_id: String,
    pub claim_epoch: u64,
    pub claimed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl CopyDeliveryClaim {
    #[must_use]
    pub const fn grants_mutation_authority(&self) -> bool {
        false
    }
}

/// A receipt already persisted by the account node. PostgreSQL records and projects it but cannot
/// turn it into, or substitute it for, a local dispatch permit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedCopyDeliveryReceipt {
    pub claim: CopyDeliveryClaim,
    pub receipt: PersistedDeliveryReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyStoreResult {
    Inserted { sequence: i64 },
    Existing { sequence: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyApplyResult {
    Stored,
    Existing,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyLedgerProjectionInput {
    pub job_id: CopyId,
    pub receipt_sequence: u64,
    pub projection_digest: [u8; 32],
    pub ledger_entry: LedgerEntry,
    pub position: AuthoritativePositionSnapshot,
    pub target: TargetExposurePlan,
    pub repair_identities: CopyIdentitySet,
    pub projected_at_ms: u64,
    pub repair_expires_at_ms: u64,
}

impl CopyLedgerProjectionInput {
    pub(crate) fn plan_repair(&self) -> Result<Option<DriftRepairRequest>, DriftRepairError> {
        if self.ledger_entry.binding != self.position.binding
            || self.ledger_entry.generation != self.position.generation
            || self.ledger_entry.fact_digest != self.position.fact_digest
            || self.ledger_entry.managed_exposure != self.position.exposure
            || self.projection_digest == [0; 32]
            || self.receipt_sequence == 0
        {
            return Err(DriftRepairError::Binding);
        }
        plan_drift_repair(&DriftRepairPlanRequest {
            source_job_id: self.job_id,
            repair_identities: self.repair_identities,
            binding: self.position.binding.clone(),
            expected_position_generation: self.position.generation,
            expected_target_generation: self.target.snapshot_generation,
            position: self.position.clone(),
            target: self.target.clone(),
            now_ms: self.projected_at_ms,
            repair_expires_at_ms: self.repair_expires_at_ms,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyDriftProjection {
    pub source_job_id: CopyId,
    pub receipt_sequence: u64,
    pub position: AuthoritativePositionSnapshot,
    pub target: TargetExposurePlan,
    pub repair: Option<DriftRepairRequest>,
    pub projected_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyReplayDeliveryState {
    Redeliverable,
    Claimed,
    Expired,
    ReconciliationRequired,
    Settled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyReplayJob {
    pub job: CopyJob,
    pub delivery_state: CopyReplayDeliveryState,
    pub receipts: Vec<PersistedDeliveryReceipt>,
    pub projection_pending: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyCrashReplay {
    pub observer_cursor: i64,
    pub jobs: Vec<CopyReplayJob>,
    pub ledger_entries: Vec<LedgerEntry>,
    pub drift_projections: Vec<CopyDriftProjection>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_copy::{CopyAction, CopyIdentityInput};

    fn identity_input() -> CopyIdentityInput {
        CopyIdentityInput {
            event_id: [1; 16],
            source_event_id: [2; 16],
            follower_account_id: [3; 16],
            follower_binding_id: [4; 16],
            leader_order_id: [5; 16],
            revision: 1,
            action: CopyAction::New,
        }
    }

    #[test]
    fn database_coordination_tokens_never_grant_mutation_authority() {
        let lease = CopyObserverLease {
            scope: CopyObserverScope {
                observer_id: "leader-a".to_owned(),
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            },
            holder_id: "planner-1".to_owned(),
            lease_epoch: 1,
            acquired_at_ms: 100,
            expires_at_ms: 200,
        };
        assert!(!lease.grants_mutation_authority());
        assert!(lease.validate(150).is_ok());
        assert!(lease.validate(200).is_err());
    }

    #[test]
    fn copy_scope_wire_mode_accepts_only_live() -> Result<(), Box<dyn std::error::Error>> {
        let scope = CopyObserverScope {
            observer_id: "leader-a".to_owned(),
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        };
        let encoded = serde_json::to_value(scope)?;
        assert!(serde_json::from_value::<CopyObserverScope>(encoded.clone()).is_ok());
        for raw in ["TEST", "live", " LIVE", "LIVE "] {
            let mut rejected = encoded.clone();
            rejected["mode"] = serde_json::json!(raw);
            assert!(
                serde_json::from_value::<CopyObserverScope>(rejected).is_err(),
                "accepted {raw:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn leader_envelope_requires_canonical_scope_and_half_open_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let identities = derive_copy_identities(&identity_input())?;
        let mut envelope = CopyLeaderEnvelope {
            scope: CopyObserverScope {
                observer_id: "leader-a".to_owned(),
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            },
            intent: CopyLeaderIntent {
                intent_id: identities.child_order_id,
                snapshot_id: identities.planning_snapshot_id,
                identity_input: identity_input(),
                intent_digest: [7; 32],
                intent_payload: serde_json::json!({"side": "BUY"}),
                observed_at_ms: 100,
            },
            snapshot: CopyLeaderSnapshot {
                snapshot_id: identities.planning_snapshot_id,
                generation: 1,
                observed_at_ms: 100,
                expires_at_ms: 200,
                snapshot_digest: [8; 32],
                snapshot_payload: serde_json::json!({"capital": "100"}),
            },
            outbox_digest: [9; 32],
        };
        assert!(envelope.validate(199).is_ok());
        assert!(envelope.validate(200).is_err());
        envelope.scope.trading_account_id = "not-an-account".to_owned();
        assert!(envelope.validate(150).is_err());
        Ok(())
    }

    #[test]
    fn durable_identity_encoding_round_trips_without_changing_job_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let identities = derive_copy_identities(&identity_input())?;
        let encoded = serde_json::to_value(identities)?;
        let decoded: CopyIdentitySet = serde_json::from_value(encoded)?;
        assert_eq!(decoded, identities);
        Ok(())
    }
}
