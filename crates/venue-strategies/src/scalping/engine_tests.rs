use std::collections::BTreeMap;

use rust_decimal::Decimal;
use venue_domain::domain::{Amount, Asset, Price};
use venue_indicators::{FeatureFrame, FeatureState, FeatureValues, SourceCursor};

use super::super::{BARS_SOURCE, BOOK_SOURCE, StrategyKind, TRADES_SOURCE};
use super::{
    BlockingReason, LifecycleAuthorization, NoopReason, ProtectionState, SafetyProjection,
    ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding,
};

#[derive(Clone)]
struct Authorization {
    allowed: bool,
    binding_digest: String,
    revision: u64,
    generation: u64,
}

impl LifecycleAuthorization for Authorization {
    fn is_allowed(&self) -> bool {
        self.allowed
    }

    fn matches_at(&self, binding: &StrategyBinding, _decision_at_ms: u64) -> bool {
        binding.digest() == self.binding_digest
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn authority_generation(&self) -> u64 {
        self.generation
    }
}

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "scalping_primary".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "scalping_primary:shadow_1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn strategy() -> Result<(ScalpingStrategy, Authorization), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let strategy = ScalpingStrategy::new(
        binding.clone(),
        ScalpingParams::shadow(binding.risk_budget.clone()),
    )?;
    let authorization = Authorization {
        allowed: true,
        binding_digest: binding.digest(),
        revision: 1,
        generation: 1,
    };
    Ok((strategy, authorization))
}

fn frame(watermark_ms: u64, generation: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation,
        watermark_ms,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation,
                        sequence: watermark_ms + index as u64,
                        event_time_ms: watermark_ms,
                        fresh: true,
                    },
                )
            })
            .collect(),
        feature_versions: BTreeMap::from([
            (BOOK_SOURCE.to_owned(), "book-v1".to_owned()),
            (TRADES_SOURCE.to_owned(), "trades-v1".to_owned()),
            (BARS_SOURCE.to_owned(), "bars-v1".to_owned()),
            (
                "_feature_profile".to_owned(),
                "scalping-shadow-v1".to_owned(),
            ),
            ("_feature_profile_digest".to_owned(), "0".repeat(64)),
        ]),
        values: FeatureValues {
            mid_price: Price::new(Decimal::new(99, 0))?,
            fair_price: Price::new(Decimal::new(100, 0))?,
            spread_bps: Decimal::new(5, 1),
            depth_quote: Decimal::new(1_000, 0),
            book_imbalance: Decimal::ONE,
            trade_imbalance: Decimal::ONE,
            short_return_bps: Decimal::ZERO,
            trend_efficiency: Decimal::ZERO,
            bandwidth_expansion: Decimal::ZERO,
            expected_move_bps: Decimal::ZERO,
            toxicity: Decimal::ZERO,
        },
        breakout: None,
    })
}

fn safe() -> SafetyProjection {
    SafetyProjection {
        private_snapshot_ready: true,
        exposure: super::ExposureState::Flat,
        execution_unknown: false,
        protection: ProtectionState::Complete,
        owner_conflict: false,
        risk_budget_available: true,
    }
}

#[test]
fn reducer_emits_only_a_semantic_intent_after_explicit_direct_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut strategy, authorization) = strategy()?;
    let prepared = strategy.evaluate(&frame(100, 1)?, &safe(), &authorization)?;
    assert!(matches!(prepared, ScalpingDecision::Prepared(_)));
    let intent = match strategy.admit_direct(100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("expected semantic intent, got {decision:?}").into()),
    };
    assert_eq!(intent.symbol.to_string(), "BTC/USDT");
    assert_eq!(intent.target_quote.value, Decimal::new(5, 0));
    assert!(matches!(
        strategy.evaluate(&frame(101, 1)?, &safe(), &authorization)?,
        ScalpingDecision::Noop(NoopReason::ActiveEpisode)
    ));
    Ok(())
}

#[test]
fn invalid_profile_or_symbol_cannot_advance_the_reducer() -> Result<(), Box<dyn std::error::Error>>
{
    let (mut strategy, authorization) = strategy()?;
    let mut wrong_profile = frame(100, 1)?;
    wrong_profile
        .feature_versions
        .insert("_feature_profile".to_owned(), "other".to_owned());
    assert!(matches!(
        strategy.evaluate(&wrong_profile, &safe(), &authorization),
        Err(super::ScalpingError::FeatureProfile)
    ));
    let mut wrong_symbol = frame(100, 1)?;
    wrong_symbol.symbol = "ETH/USDT".parse()?;
    assert!(matches!(
        strategy.evaluate(&wrong_symbol, &safe(), &authorization),
        Err(super::ScalpingError::Symbol)
    ));
    assert!(matches!(
        strategy.evaluate(&frame(100, 1)?, &safe(), &authorization)?,
        ScalpingDecision::Prepared(_)
    ));
    Ok(())
}

#[test]
fn unsafe_signed_projection_is_a_fail_closed_noop() -> Result<(), Box<dyn std::error::Error>> {
    let (mut strategy, authorization) = strategy()?;
    let mut unsafe_projection = safe();
    unsafe_projection.execution_unknown = true;
    assert_eq!(
        strategy.evaluate(&frame(100, 1)?, &unsafe_projection, &authorization)?,
        ScalpingDecision::Noop(NoopReason::Blocked(BlockingReason::ExecutionUnknown))
    );
    Ok(())
}

#[test]
fn restored_strategy_requires_a_new_authority_generation() -> Result<(), Box<dyn std::error::Error>>
{
    let (mut strategy, mut authorization) = strategy()?;
    let _ = strategy.evaluate(&frame(100, 1)?, &safe(), &authorization)?;
    let checkpoint = strategy.checkpoint();
    let binding = binding()?;
    let mut restored = ScalpingStrategy::restore(
        binding.clone(),
        ScalpingParams::shadow(binding.risk_budget.clone()),
        checkpoint,
    )?;
    assert_eq!(
        restored.evaluate(&frame(101, 2)?, &safe(), &authorization)?,
        ScalpingDecision::Noop(NoopReason::Blocked(BlockingReason::RecoveryAuthorization))
    );
    authorization.generation = 2;
    assert!(!matches!(
        restored.evaluate(&frame(102, 3)?, &safe(), &authorization)?,
        ScalpingDecision::Intent(_)
    ));
    Ok(())
}
