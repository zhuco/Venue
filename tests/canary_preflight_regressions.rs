use rust_decimal::Decimal;
use venue::{
    domain::{
        Amount, Asset, FieldState, Instrument, MarketKind, OrderOwner, OrderPurpose, PositionSide,
        Price, Symbol,
    },
    execution::{
        CanaryBinding, CanaryPosition, CanaryPreflightError, CanaryPreflightInput, CanarySnapshot,
        authorize_canary_preflight,
    },
    risk,
};

fn binding() -> Result<CanaryBinding, Box<dyn std::error::Error>> {
    let symbol: Symbol = "DOGE/USDT".parse()?;
    Ok(CanaryBinding {
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: symbol.clone(),
        owner: OrderOwner {
            strategy_instance_id: "scalping_1".to_owned(),
            run_id: "g6_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol,
            purpose: OrderPurpose::Entry,
        },
        release_id: "release_1".to_owned(),
        position_side: PositionSide::Long,
    })
}

fn instrument(
    symbol: Symbol,
    price_tick: Decimal,
    minimum_notional: Decimal,
) -> Result<Instrument, Box<dyn std::error::Error>> {
    let asset: Asset = "USDT".parse()?;
    Ok(Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(asset.clone()),
        generation: 7,
        price_tick: Price::new(price_tick)?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: Amount::new(asset, minimum_notional),
    })
}

fn snapshot(
    binding: &CanaryBinding,
    generation: u64,
    observed_at_ms: u64,
) -> Result<CanarySnapshot, Box<dyn std::error::Error>> {
    Ok(CanarySnapshot {
        binding: binding.clone(),
        observed_at_ms,
        generation,
        instrument_generation: FieldState::Known(7),
        can_trade: FieldState::Known(true),
        hedge_mode: FieldState::Known(true),
        positions: vec![
            CanaryPosition {
                side: FieldState::Known(PositionSide::Long),
                quantity: FieldState::Known(Decimal::ZERO),
            },
            CanaryPosition {
                side: FieldState::Known(PositionSide::Short),
                quantity: FieldState::Known(Decimal::ZERO),
            },
        ],
        open_orders: FieldState::Known(0),
        available_margin: FieldState::Known(Amount::new("USDT".parse()?, Decimal::new(100, 0))),
        owner_conflict: FieldState::Known(false),
        execution_unknown: FieldState::Known(false),
    })
}

#[test]
fn stable_hedge_scope_sizes_the_minimum_rule_compliant_canary()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let instrument = instrument(
        binding.symbol.clone(),
        Decimal::new(1, 1),
        Decimal::new(5, 0),
    )?;
    let snapshots = [snapshot(&binding, 10, 900)?, snapshot(&binding, 11, 950)?];

    let approval = authorize_canary_preflight(CanaryPreflightInput {
        binding: &binding,
        snapshots: &snapshots,
        instrument: &instrument,
        reference_price: Price::new(Decimal::new(1, 1))?,
        now_ms: 1_000,
        maximum_evidence_age_ms: 200,
    })?;

    assert_eq!(approval.quantity, Decimal::new(50, 0));
    assert_eq!(approval.notional.value, Decimal::new(5, 0));
    assert_eq!(approval.final_generation, 11);
    assert_eq!(approval.valid_until_ms, 1_150);
    Ok(())
}

#[test]
fn btc_minimum_step_exceeds_the_ten_usdt_hard_cap() -> Result<(), Box<dyn std::error::Error>> {
    let mut binding = binding()?;
    binding.symbol = "BTC/USDT".parse()?;
    binding.owner.symbol = binding.symbol.clone();
    let instrument = instrument(
        binding.symbol.clone(),
        Decimal::new(1, 1),
        Decimal::new(5, 0),
    )?;
    let snapshots = [snapshot(&binding, 10, 900)?, snapshot(&binding, 11, 950)?];

    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &snapshots,
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(100_000, 0))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Risk(risk::RiskError::Limit))
    ));
    Ok(())
}

#[test]
fn sol_and_bnb_minimum_compliant_candidates_stay_within_ten_usdt()
-> Result<(), Box<dyn std::error::Error>> {
    for (native_symbol, reference_price, expected_notional) in [
        ("SOL/USDT", Decimal::new(7_341, 2), Decimal::new(51_387, 4)),
        ("BNB/USDT", Decimal::new(58_526, 2), Decimal::new(58_526, 4)),
    ] {
        let mut binding = binding()?;
        binding.symbol = native_symbol.parse()?;
        binding.owner.symbol = binding.symbol.clone();
        let instrument = instrument(
            binding.symbol.clone(),
            Decimal::new(1, 2),
            Decimal::new(5, 0),
        )?;
        let snapshots = [snapshot(&binding, 10, 900)?, snapshot(&binding, 11, 950)?];
        let approval = authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &snapshots,
            instrument: &instrument,
            reference_price: Price::new(reference_price)?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        })?;
        assert_eq!(approval.notional.value, expected_notional);
        assert!(approval.notional.value <= Decimal::new(10, 0));
    }
    Ok(())
}

#[test]
fn stale_single_unknown_or_net_evidence_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let instrument = instrument(
        binding.symbol.clone(),
        Decimal::new(1, 1),
        Decimal::new(5, 0),
    )?;
    let first = snapshot(&binding, 10, 900)?;
    let mut second = snapshot(&binding, 11, 950)?;
    second.can_trade = FieldState::Missing;
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[first.clone(), second],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Capability)
    ));
    let mut missing_margin = snapshot(&binding, 11, 950)?;
    missing_margin.available_margin = FieldState::Missing;
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[first.clone(), missing_margin],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Margin)
    ));
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: std::slice::from_ref(&first),
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Evidence)
    ));
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[first.clone(), snapshot(&binding, 11, 950)?],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_200,
            maximum_evidence_age_ms: 100,
        }),
        Err(CanaryPreflightError::Evidence)
    ));
    let mut net = snapshot(&binding, 11, 950)?;
    net.positions[1].side = FieldState::Known(PositionSide::Net);
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[first, net],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Position)
    ));
    Ok(())
}

#[test]
fn binding_generation_orders_and_execution_debt_cannot_be_ignored()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let instrument = instrument(
        binding.symbol.clone(),
        Decimal::new(1, 1),
        Decimal::new(5, 0),
    )?;
    let first = snapshot(&binding, 11, 900)?;
    let regressed = snapshot(&binding, 10, 950)?;
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[first, regressed],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::SnapshotSequence)
    ));
    let mut debt = snapshot(&binding, 12, 950)?;
    debt.execution_unknown = FieldState::Known(true);
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[snapshot(&binding, 11, 900)?, debt],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::ExecutionUnknown)
    ));
    let mut wrong_release = snapshot(&binding, 12, 950)?;
    wrong_release.binding.release_id = "release_2".to_owned();
    assert!(matches!(
        authorize_canary_preflight(CanaryPreflightInput {
            binding: &binding,
            snapshots: &[snapshot(&binding, 11, 900)?, wrong_release],
            instrument: &instrument,
            reference_price: Price::new(Decimal::new(1, 1))?,
            now_ms: 1_000,
            maximum_evidence_age_ms: 200,
        }),
        Err(CanaryPreflightError::Binding)
    ));
    Ok(())
}
