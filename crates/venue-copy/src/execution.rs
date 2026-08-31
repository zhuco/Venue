use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use venue_domain::domain::Amount;

use crate::{
    AuthoritativePositionSnapshot, CopyId, DeliveryBinding, FollowerDeliveryManifest,
    TargetExposurePlan,
};

/// A durable, venue-neutral request for the Account Runtime.  It intentionally carries exposure,
/// not an adapter order: the runtime alone converts it using fresh rules and routes it through its
/// lane, WAL and account writer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyExecutionRequest {
    pub job_id: CopyId,
    pub delivery_digest: [u8; 32],
    pub binding: DeliveryBinding,
    pub target_generation: u64,
    pub position_generation: u64,
    pub target_exposure: Amount,
    pub current_exposure: Amount,
    pub requested_delta_exposure: Amount,
    pub phase: CopyExecutionPhase,
}

impl CopyExecutionRequest {
    /// Checks immutable semantics, including historical results received after expiry. This
    /// does not refresh the manifest or grant permission to execute the request again.
    pub fn validate_against(
        &self,
        manifest: &FollowerDeliveryManifest,
        target: &TargetExposurePlan,
    ) -> Result<(), CopyExecutionError> {
        manifest.validate(manifest.issued_at_ms)?;
        if self.job_id != manifest.identities.job_id
            || self.delivery_digest != manifest.delivery_digest()
            || self.binding != manifest.binding
            || self.target_generation != manifest.snapshot_generation
            || self.target_generation != target.snapshot_generation
            || self.position_generation == 0
            || self.target_exposure != target.target_exposure
            || self.current_exposure.asset != self.target_exposure.asset
            || self.requested_delta_exposure.asset != self.target_exposure.asset
        {
            return Err(CopyExecutionError::Binding);
        }
        let (phase, delta) =
            execution_delta(self.current_exposure.value, self.target_exposure.value)?;
        if self.phase != phase || self.requested_delta_exposure.value != delta {
            return Err(CopyExecutionError::Binding);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyExecutionPhase {
    /// A cross-zero target must first reduce the signed follower position to zero.
    ReduceToZero,
    /// The signed current fact and target are on one side (or zero), so the runtime may decide
    /// whether the delta is a reduce or an allowed new-risk request.
    Adjust,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyExecutionState {
    Prepared,
    Submitted,
    Accepted,
    Rejected,
    Unknown,
    Reconciled,
}

/// Projection-only evidence emitted after the Account Runtime has handled a request.  `Unknown`
/// is terminal for this child until signed facts produce a distinct `Reconciled` result; it never
/// asks the caller to replay the request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyExecutionResult {
    pub request: CopyExecutionRequest,
    pub state: CopyExecutionState,
    pub command_id: Option<String>,
    pub fact_digest: [u8; 32],
    /// The signed position that closed this child. Its digest differs from `fact_digest`,
    /// which commits to the command and fills as well. Absent on non-reconciled WAL outcomes.
    #[serde(default)]
    pub reconciled_position: Option<AuthoritativePositionSnapshot>,
    pub observed_at_ms: u64,
}

pub fn plan_copy_execution(
    manifest: &FollowerDeliveryManifest,
    target: &TargetExposurePlan,
    position: &AuthoritativePositionSnapshot,
    now_ms: u64,
) -> Result<CopyExecutionRequest, CopyExecutionError> {
    manifest.validate(now_ms)?;
    if position.binding != manifest.binding
        || position.generation == 0
        || position.fact_digest == [0; 32]
        || position.observed_at_ms == 0
        || now_ms < position.observed_at_ms
        || now_ms >= position.expires_at_ms
        || target.snapshot_generation != manifest.snapshot_generation
        || target.target_exposure.asset != position.exposure.asset
        || target.delta_exposure.asset != position.exposure.asset
    {
        return Err(CopyExecutionError::Binding);
    }
    let (phase, delta) = execution_delta(position.exposure.value, target.target_exposure.value)?;
    Ok(CopyExecutionRequest {
        job_id: manifest.identities.job_id,
        delivery_digest: manifest.delivery_digest(),
        binding: manifest.binding.clone(),
        target_generation: target.snapshot_generation,
        position_generation: position.generation,
        target_exposure: target.target_exposure.clone(),
        current_exposure: position.exposure.clone(),
        requested_delta_exposure: Amount::new(position.exposure.asset.clone(), delta),
        phase,
    })
}

fn execution_delta(
    current: Decimal,
    desired: Decimal,
) -> Result<(CopyExecutionPhase, Decimal), CopyExecutionError> {
    if crosses_zero(current, desired) {
        Ok((CopyExecutionPhase::ReduceToZero, -current))
    } else {
        Ok((
            CopyExecutionPhase::Adjust,
            desired
                .checked_sub(current)
                .ok_or(CopyExecutionError::Arithmetic)?,
        ))
    }
}

const fn crosses_zero(current: Decimal, desired: Decimal) -> bool {
    (current.is_sign_positive() && !current.is_zero() && desired.is_sign_negative())
        || (current.is_sign_negative() && desired.is_sign_positive() && !desired.is_zero())
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CopyExecutionError {
    #[error("Copy execution request is not bound to fresh signed follower facts")]
    Binding,
    #[error("Copy execution exposure arithmetic overflowed")]
    Arithmetic,
    #[error("Copy delivery manifest is invalid")]
    Delivery(#[from] crate::DeliveryError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Asset, InstrumentIdentity, MarketKind, Symbol};

    use super::*;
    use crate::{CopyAction, CopyIdentityInput, RelationCommitment, derive_copy_identities};

    fn identities(seed: u8) -> Result<crate::CopyIdentitySet, crate::CopyIdentityError> {
        derive_copy_identities(&CopyIdentityInput {
            event_id: [seed; 16],
            source_event_id: [seed + 1; 16],
            follower_account_id: [seed + 2; 16],
            follower_binding_id: [seed + 3; 16],
            leader_order_id: [seed + 4; 16],
            revision: 1,
            action: CopyAction::New,
        })
    }

    fn fixture(
        current: i64,
        desired: i64,
    ) -> Result<
        (
            FollowerDeliveryManifest,
            TargetExposurePlan,
            AuthoritativePositionSnapshot,
        ),
        Box<dyn std::error::Error>,
    > {
        let ids = identities(1)?;
        let relation = identities(30)?;
        let binding = DeliveryBinding {
            relation: RelationCommitment {
                relation_id: relation.job_id,
                revision: 1,
                policy_digest: [8; 32],
            },
            leader_id: relation.planning_snapshot_id,
            follower_id: relation.child_order_id,
            follower_binding_id: identities(40)?.job_id,
            follower_instance_id: "copy-follower".to_owned(),
            account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instrument: InstrumentIdentity {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
            },
            policy_id: relation.job_id,
        };
        let asset = Asset::new("USDT")?;
        let amount = |value| Amount::new(asset.clone(), Decimal::from(value));
        let manifest = FollowerDeliveryManifest {
            identities: ids,
            binding: binding.clone(),
            plan_digest: [7; 32],
            snapshot_generation: 2,
            instrument_generation: 1,
            issued_at_ms: 100,
            expires_at_ms: 200,
        };
        let target = TargetExposurePlan {
            snapshot_generation: 2,
            exposure_ratio: Decimal::new(desired, 3),
            safe_available_margin: amount(1_000),
            effective_follower_capital: amount(1_000),
            target_exposure: amount(desired),
            delta_exposure: amount(desired - current),
        };
        let position = AuthoritativePositionSnapshot {
            binding,
            generation: 3,
            observed_at_ms: 100,
            expires_at_ms: 200,
            exposure: amount(current),
            fact_digest: [6; 32],
        };
        Ok((manifest, target, position))
    }

    #[test]
    fn cross_zero_requires_a_reduce_only_first_phase() -> Result<(), Box<dyn std::error::Error>> {
        let (manifest, target, position) = fixture(20, -10)?;
        let request = plan_copy_execution(&manifest, &target, &position, 150)?;
        assert_eq!(request.phase, CopyExecutionPhase::ReduceToZero);
        assert_eq!(request.requested_delta_exposure.value, Decimal::from(-20));
        Ok(())
    }

    #[test]
    fn same_side_uses_the_signed_fact_not_planner_delta() -> Result<(), Box<dyn std::error::Error>>
    {
        let (manifest, target, position) = fixture(20, 50)?;
        let request = plan_copy_execution(&manifest, &target, &position, 150)?;
        assert_eq!(request.phase, CopyExecutionPhase::Adjust);
        assert_eq!(request.requested_delta_exposure.value, Decimal::from(30));
        Ok(())
    }

    #[test]
    fn historical_request_cannot_change_target_asset_phase_or_delta()
    -> Result<(), Box<dyn std::error::Error>> {
        let (manifest, target, position) = fixture(20, -10)?;
        let request = plan_copy_execution(&manifest, &target, &position, 150)?;
        request.validate_against(&manifest, &target)?;
        let mut changed = request.clone();
        changed.target_exposure.value -= Decimal::ONE;
        assert!(changed.validate_against(&manifest, &target).is_err());
        changed = request.clone();
        changed.phase = CopyExecutionPhase::Adjust;
        changed.requested_delta_exposure.value = Decimal::from(-30);
        assert!(changed.validate_against(&manifest, &target).is_err());
        changed = request.clone();
        changed.current_exposure.asset = Asset::new("USDC")?;
        assert!(changed.validate_against(&manifest, &target).is_err());
        changed = request.clone();
        changed.requested_delta_exposure.value -= Decimal::ONE;
        assert!(changed.validate_against(&manifest, &target).is_err());
        changed = request;
        changed.position_generation = 0;
        assert!(changed.validate_against(&manifest, &target).is_err());
        Ok(())
    }
}
