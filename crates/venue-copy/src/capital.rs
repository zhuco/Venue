use rust_decimal::Decimal;
use thiserror::Error;
use venue_domain::domain::{Amount, Asset};

/// One immutable, single-currency set of capital facts used to derive a follower target.
///
/// Exposures are signed quote notionals. Capital and margin values must be non-negative; signed
/// target and managed exposure are intentionally allowed so short positions remain representable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapitalSnapshot {
    pub generation: u64,
    pub observed_ms: u64,
    pub expires_ms: u64,
    pub leader_strategy_capital: Amount,
    pub leader_target_exposure: Amount,
    pub follower_configured_capital: Amount,
    pub follower_allocated_capital: Amount,
    pub follower_available_margin: Amount,
    pub follower_managed_exposure: Amount,
    /// Fraction in `[0, 1]`; `0.10` reserves ten percent of available margin.
    pub margin_safety_reserve_rate: Decimal,
}

/// Exact version and evaluation time requested by the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetExposureRequest {
    pub expected_generation: u64,
    pub now_ms: u64,
    pub snapshot: CapitalSnapshot,
}

/// Pure target-exposure result. It is not an order or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetExposurePlan {
    pub snapshot_generation: u64,
    pub exposure_ratio: Decimal,
    pub safe_available_margin: Amount,
    pub effective_follower_capital: Amount,
    pub target_exposure: Amount,
    pub delta_exposure: Amount,
}

/// Apply the frozen leader exposure ratio to the follower's lowest safe capital bound.
///
/// A sign flip is rejected even though its arithmetic delta is defined. The caller must first
/// plan a reduction to zero, wait for confirmed private facts, freeze a new snapshot, and only
/// then plan the opposite opening exposure.
pub fn reduce_target_exposure(
    request: &TargetExposureRequest,
) -> Result<TargetExposurePlan, TargetExposureError> {
    let snapshot = &request.snapshot;
    validate_snapshot(request)?;
    let valuation_asset = validate_assets(snapshot)?;

    if snapshot.leader_strategy_capital.value <= Decimal::ZERO {
        return Err(TargetExposureError::InvalidLeaderCapital);
    }
    if snapshot.follower_configured_capital.value < Decimal::ZERO
        || snapshot.follower_allocated_capital.value < Decimal::ZERO
        || snapshot.follower_available_margin.value < Decimal::ZERO
    {
        return Err(TargetExposureError::InvalidFollowerCapital);
    }
    if snapshot.margin_safety_reserve_rate < Decimal::ZERO
        || snapshot.margin_safety_reserve_rate > Decimal::ONE
    {
        return Err(TargetExposureError::InvalidSafetyReserve);
    }

    let usable_rate = Decimal::ONE
        .checked_sub(snapshot.margin_safety_reserve_rate)
        .ok_or(TargetExposureError::ArithmeticOverflow)?;
    let safe_available_margin = snapshot
        .follower_available_margin
        .value
        .checked_mul(usable_rate)
        .ok_or(TargetExposureError::ArithmeticOverflow)?;
    let effective_follower_capital = snapshot
        .follower_configured_capital
        .value
        .min(snapshot.follower_allocated_capital.value)
        .min(safe_available_margin);
    let exposure_ratio = snapshot
        .leader_target_exposure
        .value
        .checked_div(snapshot.leader_strategy_capital.value)
        .ok_or(TargetExposureError::ArithmeticOverflow)?;
    let target_exposure = exposure_ratio
        .checked_mul(effective_follower_capital)
        .ok_or(TargetExposureError::ArithmeticOverflow)?;

    if crosses_zero(snapshot.follower_managed_exposure.value, target_exposure) {
        return Err(TargetExposureError::DirectionFlipRequiresSplit);
    }

    let delta_exposure = target_exposure
        .checked_sub(snapshot.follower_managed_exposure.value)
        .ok_or(TargetExposureError::ArithmeticOverflow)?;

    Ok(TargetExposurePlan {
        snapshot_generation: snapshot.generation,
        exposure_ratio,
        safe_available_margin: Amount::new(valuation_asset.clone(), safe_available_margin),
        effective_follower_capital: Amount::new(
            valuation_asset.clone(),
            effective_follower_capital,
        ),
        target_exposure: Amount::new(valuation_asset.clone(), target_exposure),
        delta_exposure: Amount::new(valuation_asset.clone(), delta_exposure),
    })
}

fn validate_snapshot(request: &TargetExposureRequest) -> Result<(), TargetExposureError> {
    let snapshot = &request.snapshot;
    if snapshot.generation == 0 || request.expected_generation == 0 {
        return Err(TargetExposureError::InvalidGeneration);
    }
    if snapshot.generation != request.expected_generation {
        return Err(TargetExposureError::GenerationMismatch);
    }
    if snapshot.observed_ms == 0 || snapshot.expires_ms <= snapshot.observed_ms {
        return Err(TargetExposureError::InvalidWindow);
    }
    if request.now_ms < snapshot.observed_ms || request.now_ms >= snapshot.expires_ms {
        return Err(TargetExposureError::StaleSnapshot);
    }
    Ok(())
}

fn validate_assets(snapshot: &CapitalSnapshot) -> Result<&Asset, TargetExposureError> {
    let expected = &snapshot.leader_strategy_capital.asset;
    let values = [
        &snapshot.leader_target_exposure,
        &snapshot.follower_configured_capital,
        &snapshot.follower_allocated_capital,
        &snapshot.follower_available_margin,
        &snapshot.follower_managed_exposure,
    ];
    if values.iter().all(|amount| &amount.asset == expected) {
        Ok(expected)
    } else {
        Err(TargetExposureError::ValuationAssetMismatch)
    }
}

const fn crosses_zero(managed: Decimal, target: Decimal) -> bool {
    (managed.is_sign_positive() && !managed.is_zero() && target.is_sign_negative())
        || (managed.is_sign_negative() && target.is_sign_positive() && !target.is_zero())
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TargetExposureError {
    #[error("capital snapshot generation must be positive")]
    InvalidGeneration,
    #[error("capital snapshot generation does not match the requested generation")]
    GenerationMismatch,
    #[error("capital snapshot freshness window is malformed")]
    InvalidWindow,
    #[error("capital snapshot is stale or future-dated")]
    StaleSnapshot,
    #[error("all capital and exposure values must use one valuation asset")]
    ValuationAssetMismatch,
    #[error("leader strategy capital must be positive")]
    InvalidLeaderCapital,
    #[error("follower capital and available margin must not be negative")]
    InvalidFollowerCapital,
    #[error("margin safety reserve rate must be between zero and one")]
    InvalidSafetyReserve,
    #[error("target exposure arithmetic overflowed decimal precision")]
    ArithmeticOverflow,
    #[error("cross-zero reversal must close to zero and confirm before opening the opposite side")]
    DirectionFlipRequiresSplit,
}
