use rust_decimal::Decimal;
use venue_copy::{
    CapitalSnapshot, TargetExposureError, TargetExposureRequest, reduce_target_exposure,
};
use venue_domain::domain::{Amount, Asset};

fn amount(asset: &Asset, value: Decimal) -> Amount {
    Amount::new(asset.clone(), value)
}

fn base_request() -> Result<TargetExposureRequest, Box<dyn std::error::Error>> {
    let asset = Asset::new("USDT")?;
    Ok(TargetExposureRequest {
        expected_generation: 7,
        now_ms: 1_500,
        snapshot: CapitalSnapshot {
            generation: 7,
            observed_ms: 1_000,
            expires_ms: 2_000,
            leader_strategy_capital: amount(&asset, Decimal::new(100_000, 0)),
            leader_target_exposure: amount(&asset, Decimal::new(25_000, 0)),
            follower_configured_capital: amount(&asset, Decimal::new(20_000, 0)),
            follower_allocated_capital: amount(&asset, Decimal::new(15_000, 0)),
            follower_available_margin: amount(&asset, Decimal::new(15_000, 0)),
            follower_managed_exposure: amount(&asset, Decimal::new(1_000, 0)),
            margin_safety_reserve_rate: Decimal::new(20, 2),
        },
    })
}

#[test]
fn lowest_safe_capital_drives_target_and_managed_exposure_is_subtracted()
-> Result<(), Box<dyn std::error::Error>> {
    let request = base_request()?;
    let plan = reduce_target_exposure(&request)?;

    assert_eq!(plan.snapshot_generation, 7);
    assert_eq!(plan.exposure_ratio, Decimal::new(25, 2));
    assert_eq!(plan.safe_available_margin.value, Decimal::new(12_000, 0));
    assert_eq!(
        plan.effective_follower_capital.value,
        Decimal::new(12_000, 0)
    );
    assert_eq!(plan.target_exposure.value, Decimal::new(3_000, 0));
    assert_eq!(plan.delta_exposure.value, Decimal::new(2_000, 0));
    assert_eq!(plan.target_exposure.asset, Asset::new("USDT")?);
    Ok(())
}

#[test]
fn leader_leverage_is_not_multiplied_a_second_time() -> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    let asset = Asset::new("USDT")?;
    request.snapshot.leader_strategy_capital = amount(&asset, Decimal::new(100, 0));
    request.snapshot.leader_target_exposure = amount(&asset, Decimal::new(300, 0));
    request.snapshot.follower_configured_capital = amount(&asset, Decimal::new(50, 0));
    request.snapshot.follower_allocated_capital = amount(&asset, Decimal::new(50, 0));
    request.snapshot.follower_available_margin = amount(&asset, Decimal::new(50, 0));
    request.snapshot.follower_managed_exposure = amount(&asset, Decimal::ZERO);
    request.snapshot.margin_safety_reserve_rate = Decimal::ZERO;

    let plan = reduce_target_exposure(&request)?;
    assert_eq!(plan.exposure_ratio, Decimal::new(3, 0));
    assert_eq!(plan.target_exposure.value, Decimal::new(150, 0));
    Ok(())
}

#[test]
fn negative_capital_and_invalid_reserve_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    request.snapshot.follower_allocated_capital.value = Decimal::NEGATIVE_ONE;
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::InvalidFollowerCapital)
    );

    request = base_request()?;
    request.snapshot.margin_safety_reserve_rate = Decimal::new(101, 2);
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::InvalidSafetyReserve)
    );
    Ok(())
}

#[test]
fn stale_future_and_generation_mismatch_snapshots_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    request.now_ms = request.snapshot.expires_ms;
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::StaleSnapshot)
    );

    request = base_request()?;
    request.now_ms = request.snapshot.observed_ms - 1;
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::StaleSnapshot)
    );

    request = base_request()?;
    request.expected_generation += 1;
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::GenerationMismatch)
    );
    Ok(())
}

#[test]
fn mixed_valuation_assets_fail_before_arithmetic() -> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    request.snapshot.follower_available_margin.asset = Asset::new("USDC")?;
    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::ValuationAssetMismatch)
    );
    Ok(())
}

#[test]
fn cross_zero_semantic_target_preserves_both_original_endpoints()
-> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    let asset = Asset::new("USDT")?;
    request.snapshot.leader_strategy_capital = amount(&asset, Decimal::new(100, 0));
    request.snapshot.leader_target_exposure = amount(&asset, Decimal::new(-20, 0));
    request.snapshot.follower_configured_capital = amount(&asset, Decimal::new(100, 0));
    request.snapshot.follower_allocated_capital = amount(&asset, Decimal::new(100, 0));
    request.snapshot.follower_available_margin = amount(&asset, Decimal::new(100, 0));
    request.snapshot.follower_managed_exposure = amount(&asset, Decimal::new(10, 0));
    request.snapshot.margin_safety_reserve_rate = Decimal::ZERO;

    let plan = reduce_target_exposure(&request)?;
    assert_eq!(plan.target_exposure.value, Decimal::from(-20));
    assert_eq!(plan.delta_exposure.value, Decimal::from(-30));
    assert_eq!(
        plan.target_exposure.value - plan.delta_exposure.value,
        request.snapshot.follower_managed_exposure.value
    );
    Ok(())
}

#[test]
fn decimal_overflow_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let mut request = base_request()?;
    let asset = Asset::new("USDT")?;
    request.snapshot.leader_strategy_capital = amount(&asset, Decimal::ONE);
    request.snapshot.leader_target_exposure = amount(&asset, Decimal::MAX);
    request.snapshot.follower_configured_capital = amount(&asset, Decimal::MAX);
    request.snapshot.follower_allocated_capital = amount(&asset, Decimal::MAX);
    request.snapshot.follower_available_margin = amount(&asset, Decimal::MAX);
    request.snapshot.follower_managed_exposure = amount(&asset, Decimal::ZERO);
    request.snapshot.margin_safety_reserve_rate = Decimal::ZERO;

    assert_eq!(
        reduce_target_exposure(&request),
        Err(TargetExposureError::ArithmeticOverflow)
    );
    Ok(())
}
