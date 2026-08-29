use rust_decimal::Decimal;
use thiserror::Error;
use venue_domain::domain::Amount;

use crate::{CopyId, CopyIdentitySet, DeliveryBinding, TargetExposurePlan};

pub const MAX_REPAIR_TTL_MS: u64 = 5 * 60 * 1_000;
pub const MAX_POSITION_SNAPSHOT_TTL_MS: u64 = 60 * 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoritativePositionSnapshot {
    pub binding: DeliveryBinding,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub exposure: Amount,
    pub fact_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriftRepairPlanRequest {
    pub source_job_id: CopyId,
    pub repair_identities: CopyIdentitySet,
    pub binding: DeliveryBinding,
    pub expected_position_generation: u64,
    pub expected_target_generation: u64,
    pub position: AuthoritativePositionSnapshot,
    pub target: TargetExposurePlan,
    pub now_ms: u64,
    pub repair_expires_at_ms: u64,
}

/// A new semantic planning request. It carries no order shape, transport, or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriftRepairRequest {
    pub supersedes_job_id: CopyId,
    pub identities: CopyIdentitySet,
    pub binding: DeliveryBinding,
    pub position_generation: u64,
    pub target_generation: u64,
    pub authoritative_fact_digest: [u8; 32],
    pub authoritative_exposure: Amount,
    pub target_exposure: Amount,
    pub delta_exposure: Amount,
    pub requested_at_ms: u64,
    pub expires_at_ms: u64,
}

pub fn plan_drift_repair(
    request: &DriftRepairPlanRequest,
) -> Result<Option<DriftRepairRequest>, DriftRepairError> {
    validate_request(request)?;
    let current = request.position.exposure.value;
    let target = request.target.target_exposure.value;
    if current == target {
        return Ok(None);
    }
    if crosses_zero(current, target) {
        return Err(DriftRepairError::DirectionFlipRequiresSplit);
    }
    let delta = target
        .checked_sub(current)
        .ok_or(DriftRepairError::ArithmeticOverflow)?;

    Ok(Some(DriftRepairRequest {
        supersedes_job_id: request.source_job_id,
        identities: request.repair_identities,
        binding: request.binding.clone(),
        position_generation: request.position.generation,
        target_generation: request.target.snapshot_generation,
        authoritative_fact_digest: request.position.fact_digest,
        authoritative_exposure: request.position.exposure.clone(),
        target_exposure: request.target.target_exposure.clone(),
        delta_exposure: Amount::new(request.position.exposure.asset.clone(), delta),
        requested_at_ms: request.now_ms,
        expires_at_ms: request.repair_expires_at_ms,
    }))
}

fn validate_request(request: &DriftRepairPlanRequest) -> Result<(), DriftRepairError> {
    if request.position.binding != request.binding {
        return Err(DriftRepairError::Binding);
    }
    if request.source_job_id == request.repair_identities.job_id
        || request.repair_identities.job_id.is_nil()
        || request.repair_identities.idempotency_key.is_zero()
    {
        return Err(DriftRepairError::JobIdentity);
    }
    if request.expected_position_generation == 0
        || request.expected_target_generation == 0
        || request.position.generation != request.expected_position_generation
        || request.target.snapshot_generation != request.expected_target_generation
    {
        return Err(DriftRepairError::Generation);
    }
    if request.position.fact_digest == [0; 32] {
        return Err(DriftRepairError::FactDigest);
    }
    let position_window = request
        .position
        .expires_at_ms
        .checked_sub(request.position.observed_at_ms)
        .ok_or(DriftRepairError::PositionFreshness)?;
    if request.position.observed_at_ms == 0
        || position_window == 0
        || position_window > MAX_POSITION_SNAPSHOT_TTL_MS
        || request.now_ms < request.position.observed_at_ms
        || request.now_ms >= request.position.expires_at_ms
    {
        return Err(DriftRepairError::PositionFreshness);
    }
    let repair_ttl = request
        .repair_expires_at_ms
        .checked_sub(request.now_ms)
        .ok_or(DriftRepairError::RepairWindow)?;
    if request.now_ms == 0 || repair_ttl == 0 || repair_ttl > MAX_REPAIR_TTL_MS {
        return Err(DriftRepairError::RepairWindow);
    }
    let asset = &request.position.exposure.asset;
    if request.target.target_exposure.asset != *asset
        || request.target.delta_exposure.asset != *asset
        || request.target.safe_available_margin.asset != *asset
        || request.target.effective_follower_capital.asset != *asset
        || request.binding.instrument.symbol.quote() != asset.as_str()
    {
        return Err(DriftRepairError::Asset);
    }
    let expected_target = request
        .target
        .exposure_ratio
        .checked_mul(request.target.effective_follower_capital.value)
        .ok_or(DriftRepairError::ArithmeticOverflow)?;
    let prior_managed = request
        .target
        .target_exposure
        .value
        .checked_sub(request.target.delta_exposure.value)
        .ok_or(DriftRepairError::ArithmeticOverflow)?;
    if request.target.safe_available_margin.value < Decimal::ZERO
        || request.target.effective_follower_capital.value < Decimal::ZERO
        || request.target.effective_follower_capital.value
            > request.target.safe_available_margin.value
        || expected_target != request.target.target_exposure.value
        || crosses_zero(prior_managed, request.target.target_exposure.value)
    {
        return Err(DriftRepairError::Target);
    }
    Ok(())
}

const fn crosses_zero(current: Decimal, target: Decimal) -> bool {
    (current.is_sign_positive() && !current.is_zero() && target.is_sign_negative())
        || (current.is_sign_negative() && target.is_sign_positive() && !target.is_zero())
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DriftRepairError {
    #[error("authoritative position does not match the exact follower binding")]
    Binding,
    #[error("repair must use a new deterministic job identity")]
    JobIdentity,
    #[error("position and target generations must exactly match positive expectations")]
    Generation,
    #[error("authoritative position fact digest must be non-zero")]
    FactDigest,
    #[error("authoritative position is malformed, stale, or future-dated")]
    PositionFreshness,
    #[error("repair request freshness window is malformed or too long")]
    RepairWindow,
    #[error("position and target must use the instrument quote asset")]
    Asset,
    #[error("target exposure plan is malformed")]
    Target,
    #[error("cross-zero repair must close and reconcile before a new opposite-side job")]
    DirectionFlipRequiresSplit,
    #[error("drift repair arithmetic overflowed decimal precision")]
    ArithmeticOverflow,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{Asset, InstrumentIdentity, MarketKind, Symbol};

    use super::*;
    use crate::{CopyAction, CopyIdentityInput, derive_copy_identities};

    fn identities(seed: u8, revision: u32) -> Result<CopyIdentitySet, crate::CopyIdentityError> {
        derive_copy_identities(&CopyIdentityInput {
            event_id: [seed; 16],
            source_event_id: [seed + 1; 16],
            follower_account_id: [seed + 2; 16],
            follower_binding_id: [seed + 3; 16],
            leader_order_id: [seed + 4; 16],
            revision,
            action: CopyAction::New,
        })
    }

    fn binding() -> Result<DeliveryBinding, Box<dyn std::error::Error>> {
        let ids = identities(1, 1)?;
        Ok(DeliveryBinding {
            leader_id: ids.job_id,
            follower_id: ids.planning_snapshot_id,
            follower_binding_id: ids.child_order_id,
            account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            instrument: InstrumentIdentity {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
            },
            policy_id: identities(20, 1)?.job_id,
        })
    }

    fn amount(value: i64) -> Result<Amount, Box<dyn std::error::Error>> {
        Ok(Amount::new(Asset::new("USDT")?, Decimal::from(value)))
    }

    fn request(
        current: i64,
        target: i64,
    ) -> Result<DriftRepairPlanRequest, Box<dyn std::error::Error>> {
        let source = identities(40, 1)?;
        Ok(DriftRepairPlanRequest {
            source_job_id: source.job_id,
            repair_identities: identities(40, 2)?,
            binding: binding()?,
            expected_position_generation: 11,
            expected_target_generation: 12,
            position: AuthoritativePositionSnapshot {
                binding: binding()?,
                generation: 11,
                observed_at_ms: 100,
                expires_at_ms: 1_000,
                exposure: amount(current)?,
                fact_digest: [9; 32],
            },
            target: TargetExposurePlan {
                snapshot_generation: 12,
                exposure_ratio: Decimal::from(target) / Decimal::from(1_000),
                safe_available_margin: amount(1_000)?,
                effective_follower_capital: amount(1_000)?,
                target_exposure: amount(target)?,
                delta_exposure: amount(target)?,
            },
            now_ms: 500,
            repair_expires_at_ms: 800,
        })
    }

    #[test]
    fn authoritative_drift_yields_new_semantic_job_only() -> Result<(), Box<dyn std::error::Error>>
    {
        let input = request(20, 50)?;
        let Some(repair) = plan_drift_repair(&input)? else {
            return Err("fixture must contain drift".into());
        };
        assert_ne!(repair.identities.job_id, repair.supersedes_job_id);
        assert_eq!(repair.authoritative_exposure.value, Decimal::from(20));
        assert_eq!(repair.target_exposure.value, Decimal::from(50));
        assert_eq!(repair.delta_exposure.value, Decimal::from(30));
        assert_eq!(repair.position_generation, 11);
        assert_eq!(repair.target_generation, 12);
        Ok(())
    }

    #[test]
    fn exact_target_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(plan_drift_repair(&request(50, 50)?), Ok(None));
        Ok(())
    }

    #[test]
    fn stale_cross_generation_and_cross_binding_facts_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut input = request(20, 50)?;
        input.now_ms = input.position.expires_at_ms;
        assert_eq!(
            plan_drift_repair(&input),
            Err(DriftRepairError::PositionFreshness)
        );
        input = request(20, 50)?;
        input.expected_position_generation += 1;
        assert_eq!(plan_drift_repair(&input), Err(DriftRepairError::Generation));
        input = request(20, 50)?;
        input.position.binding.account_id = "00000000-0000-4000-8000-000000000002".to_owned();
        assert_eq!(plan_drift_repair(&input), Err(DriftRepairError::Binding));
        input = request(20, 50)?;
        input.position.expires_at_ms =
            input.position.observed_at_ms + MAX_POSITION_SNAPSHOT_TTL_MS + 1;
        assert_eq!(
            plan_drift_repair(&input),
            Err(DriftRepairError::PositionFreshness)
        );
        Ok(())
    }

    #[test]
    fn old_job_identity_and_unbounded_authorization_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut input = request(20, 50)?;
        input.repair_identities.job_id = input.source_job_id;
        assert_eq!(
            plan_drift_repair(&input),
            Err(DriftRepairError::JobIdentity)
        );
        input = request(20, 50)?;
        input.repair_expires_at_ms = input.now_ms + MAX_REPAIR_TTL_MS + 1;
        assert_eq!(
            plan_drift_repair(&input),
            Err(DriftRepairError::RepairWindow)
        );
        Ok(())
    }

    #[test]
    fn forged_target_capital_or_delta_relationship_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut input = request(20, 50)?;
        input.target.exposure_ratio = Decimal::ONE;
        assert_eq!(plan_drift_repair(&input), Err(DriftRepairError::Target));

        input = request(20, 50)?;
        input.target.effective_follower_capital.value = Decimal::from(1_001);
        assert_eq!(plan_drift_repair(&input), Err(DriftRepairError::Target));

        input = request(20, 50)?;
        input.target.delta_exposure.value = Decimal::from(60);
        assert_eq!(plan_drift_repair(&input), Err(DriftRepairError::Target));
        Ok(())
    }

    #[test]
    fn reversal_requires_close_and_new_authoritative_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            plan_drift_repair(&request(20, -10)?),
            Err(DriftRepairError::DirectionFlipRequiresSplit)
        );
        Ok(())
    }
}
