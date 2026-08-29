mod controller {
    pub use venue::controller::*;
}
mod domain {
    pub use venue::domain::*;
}
mod indicator {
    pub use venue::indicator::*;
}
mod strategy {
    pub use venue::strategy::*;
}
mod scalping_risk_producer {
    pub use venue::runtime::BoundRiskRevaluation;
}

// This fixture compiles the coordinator in isolation so it can assert its internal ordering.
// Public host-only methods are intentionally unused in this isolated copy.
#[allow(dead_code)]
#[path = "../src/runtime/scalping/scalping_coordinator.rs"]
mod coordinator;

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use venue::{
    controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    runtime::BoundRiskRevaluation,
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        CandidateCosts, CandidateEvidence, CandidatePreparation, ExposureState, FillSlice,
        NoopReason, OutcomeProbabilities, ProtectionState, RiskFact, RiskRevaluation, RiskUnit,
        SafetyProjection, ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding,
        StrategyKind,
    },
};

use coordinator::{
    CustodyStatus as CoordinatorCustodyStatus, PrivateFacts, ScalpingCoordinatorError,
    ScalpingInput, ScalpingShadowCoordinator, ShadowDisposition,
};

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

fn coordinator() -> Result<ScalpingShadowCoordinator, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let strategy = ScalpingStrategy::new(
        binding.clone(),
        ScalpingParams::shadow(binding.risk_budget.clone()),
    )?;
    Ok(ScalpingShadowCoordinator::new(strategy))
}

fn safety() -> SafetyProjection {
    SafetyProjection {
        private_snapshot_ready: true,
        exposure: ExposureState::Flat,
        execution_unknown: false,
        protection: ProtectionState::Complete,
        owner_conflict: false,
        risk_budget_available: true,
    }
}

fn private(generation: u64, safety: SafetyProjection) -> PrivateFacts {
    private_at(generation, generation.saturating_mul(100), safety)
}

fn private_at(generation: u64, observed_at_ms: u64, safety: SafetyProjection) -> PrivateFacts {
    PrivateFacts {
        generation,
        observed_at_ms,
        root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:0"),
        safety,
        custody: CoordinatorCustodyStatus::Complete,
    }
}

fn authorization(
    generation: u64,
) -> Result<venue::controller::EntryAuthorization, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let record = InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "shadow-control".to_owned(),
        idempotency_key: "shadow-control-1".to_owned(),
        safety_deadline_ms: None,
        revision: 1,
    };
    Ok(record.authorize(
        &ControlAuthority {
            generation,
            parameter_release_id: binding.parameter_release_id,
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        },
        generation.saturating_mul(100),
    ))
}

fn frame(generation: u64, watermark_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
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

fn evidence(preparation: &CandidatePreparation) -> CandidateEvidence {
    CandidateEvidence {
        candidate_id: preparation.candidates[0].intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        valid_until_ms: preparation.valid_until_ms,
        calibration_model_version: "scalping-shadow-calibration-v1".to_owned(),
        calibration_digest: "0".repeat(64),
        cost_digest: "b".repeat(64),
        risk_digest: "c".repeat(64),
        worst_loss: preparation.candidates[0].risk_plan.risk_per_episode.clone(),
        fill_probability: Decimal::ONE,
        fill_distribution: vec![FillSlice {
            fill_ratio: Decimal::ONE,
            probability: Decimal::ONE,
        }],
        outcomes: OutcomeProbabilities {
            target: Decimal::ONE,
            stop: Decimal::ZERO,
            other: Decimal::ZERO,
        },
        costs: CandidateCosts {
            entry_cost_bps: Decimal::ZERO,
            exit_cost_bps: Decimal::ZERO,
            funding_cost_bps: Decimal::ZERO,
            nonfill_cost_bps: Decimal::ZERO,
            opportunity_cost_bps: Decimal::ZERO,
        },
        target_pnl_bps: Decimal::ONE,
        stop_pnl_bps: -Decimal::ONE,
        other_pnl_bps: Decimal::ZERO,
        outcome_expected_value_bps: Decimal::ONE,
        net_expected_value_bps: Decimal::ONE,
        uncertainty_bps: Decimal::ZERO,
        admissible: true,
    }
}

fn market(
    frame_generation: u64,
    authority_generation: u64,
    watermark_ms: u64,
    evidence: Vec<CandidateEvidence>,
) -> Result<ScalpingInput, Box<dyn std::error::Error>> {
    Ok(ScalpingInput::Market {
        frame: Box::new(frame(frame_generation, watermark_ms)?),
        decision_at_ms: watermark_ms,
        authorization: authorization(authority_generation)?,
        evidence,
    })
}

fn risk_fact(
    fact_id: &str,
    event_time_ms: u64,
    valuation_generation: u64,
) -> Result<RiskFact, Box<dyn std::error::Error>> {
    Ok(RiskFact {
        fact_id: fact_id.to_owned(),
        event_time_ms,
        valuation_generation,
        risk_unit: RiskUnit::new("risk")?,
        realized_pnl: Decimal::ONE,
    })
}

fn bound_risk(
    proof: RiskRevaluation,
    cursor_sequence: u64,
) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    let binding = binding()?;
    Ok(BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange,
            account: binding.account,
            owner_scope: binding.owner_scope,
            strategy_instance_id: binding.strategy_instance_id,
            run_id: binding.run_id,
            parameter_release_id: binding.parameter_release_id,
            symbol: binding.symbol,
            risk_unit: proof.risk_unit.clone(),
            valuation_generation: proof.target_generation,
        },
        proof,
        cursor_sequence,
    })
}

#[test]
fn private_facts_are_processed_before_an_earlier_market_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let inputs = vec![
        market(7, 1, 100, Vec::new())?,
        ScalpingInput::Private(private(1, safety())),
    ];
    let outputs = coordinator.process(inputs)?;
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].disposition, ShadowDisposition::ShadowOnly);
    assert!(matches!(
        outputs[1].decision,
        Some(ScalpingDecision::Prepared(_))
    ));
    assert!(outputs[1].preparation.is_some());
    Ok(())
}

#[test]
fn resident_risk_facts_are_persisted_in_the_coordinator_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let output = coordinator
        .process(vec![ScalpingInput::RiskFact(risk_fact("risk-one", 1, 1)?)])?
        .pop()
        .ok_or("missing risk output")?;
    assert_eq!(output.checkpoint.strategy.risk.facts.len(), 1);
    assert_eq!(output.checkpoint.strategy.risk.facts[0].fact_id, "risk-one");
    assert_eq!(
        output.checkpoint.strategy.risk.valuation_generation,
        Some(1)
    );
    Ok(())
}

#[test]
fn explicit_risk_revaluation_requirement_is_checkpointed_and_fences_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let output = coordinator
        .process(vec![ScalpingInput::RequireRiskRevaluation {
            observed_at_ms: 1,
        }])?
        .pop()
        .ok_or("missing revaluation requirement output")?;
    assert!(output.checkpoint.strategy.risk.generation_mismatch);
    assert_eq!(output.checkpoint.strategy.risk.last_event_time_ms, Some(1));
    Ok(())
}

#[test]
fn generation_mismatch_fences_entry_until_a_complete_risk_revaluation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    coordinator.process(vec![
        ScalpingInput::Private(private(1, safety())),
        ScalpingInput::RiskFact(risk_fact("risk-one", 1, 1)?),
        ScalpingInput::RiskFact(risk_fact("risk-two", 2, 2)?),
    ])?;
    let blocked = coordinator
        .process(vec![market(7, 1, 100, Vec::new())?])?
        .pop()
        .ok_or("missing blocked output")?;
    assert!(matches!(
        blocked.decision,
        Some(ScalpingDecision::Noop(NoopReason::Blocked(
            venue::strategy::scalping::BlockingReason::StrategyRisk
        )))
    ));
    assert!(blocked.checkpoint.strategy.risk.generation_mismatch);

    let proof = RiskRevaluation {
        proof_id: "risk-proof-2".to_owned(),
        target_generation: 2,
        risk_unit: RiskUnit::new("risk")?,
        window_start_ms: 0,
        complete_through_ms: 3,
        source_fact_ids: vec!["risk-one".to_owned(), "risk-two".to_owned()],
        revalued_facts: vec![risk_fact("risk-one", 1, 2)?, risk_fact("risk-two", 2, 2)?],
    };
    let reopened = coordinator.apply_bound_risk(&bound_risk(proof, 1)?)?;
    assert_eq!(
        reopened.checkpoint.strategy.risk.valuation_generation,
        Some(2)
    );
    assert!(!reopened.checkpoint.strategy.risk.generation_mismatch);
    assert!(matches!(
        coordinator
            .process(vec![market(7, 1, 101, Vec::new())?])?
            .pop()
            .ok_or("missing reopened market output")?
            .decision,
        Some(ScalpingDecision::Prepared(_))
    ));
    Ok(())
}

#[test]
fn control_precedes_resident_risk_and_keeps_the_market_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let outputs = coordinator.process(vec![
        market(7, 1, 100, Vec::new())?,
        ScalpingInput::RiskFact(risk_fact("risk-one", 1, 1)?),
        ScalpingInput::Private(private(1, safety())),
        ScalpingInput::Control(ControlTarget::StopAndProtect),
    ])?;
    assert_eq!(outputs[0].disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(outputs[1].disposition, ShadowDisposition::RemainFenced);
    assert_eq!(outputs[2].disposition, ShadowDisposition::RemainFenced);
    assert_eq!(outputs[3].disposition, ShadowDisposition::RemainFenced);
    assert!(outputs[3].decision.is_none());
    assert_eq!(outputs[3].checkpoint.strategy.risk.facts.len(), 1);
    Ok(())
}

#[test]
fn unknown_or_incomplete_custody_only_requests_stop_and_protect()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let mut unsafe_safety = safety();
    unsafe_safety.private_snapshot_ready = false;
    let inputs = vec![
        market(7, 1, 100, Vec::new())?,
        ScalpingInput::Private(PrivateFacts {
            custody: CoordinatorCustodyStatus::Unknown,
            ..private(1, unsafe_safety)
        }),
    ];
    let outputs = coordinator.process(inputs)?;
    assert_eq!(outputs[0].disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(outputs[1].disposition, ShadowDisposition::StopAndProtect);
    assert!(outputs[1].decision.is_none());
    Ok(())
}

#[test]
fn control_stop_remains_fenced_even_after_later_private_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let inputs = vec![
        market(7, 1, 100, Vec::new())?,
        ScalpingInput::Private(private(1, safety())),
        ScalpingInput::Control(ControlTarget::StopAndProtect),
    ];
    let outputs = coordinator.process(inputs)?;
    assert_eq!(outputs[0].disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(outputs[1].disposition, ShadowDisposition::RemainFenced);
    assert!(outputs[2].decision.is_none());
    assert_eq!(outputs[2].disposition, ShadowDisposition::RemainFenced);
    Ok(())
}

#[test]
fn running_reuses_safe_private_facts_observed_after_the_stop_control()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let stopped = coordinator.process(vec![
        ScalpingInput::Private(private(1, safety())),
        ScalpingInput::Control(ControlTarget::StopAndProtect),
    ])?;
    assert_eq!(stopped[0].disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(stopped[1].disposition, ShadowDisposition::RemainFenced);

    let running = coordinator.process(vec![ScalpingInput::Control(ControlTarget::Running)])?;
    assert_eq!(running[0].disposition, ShadowDisposition::ShadowOnly);
    let without_private = coordinator.process(vec![market(7, 1, 100, Vec::new())?])?;
    assert_eq!(
        without_private[0].disposition,
        ShadowDisposition::ShadowOnly
    );

    let fresh = coordinator.process(vec![
        ScalpingInput::Private(private_at(1, 101, safety())),
        market(7, 1, 101, Vec::new())?,
    ])?;
    assert_eq!(fresh[0].disposition, ShadowDisposition::ShadowOnly);
    assert_eq!(fresh[1].disposition, ShadowDisposition::ShadowOnly);
    Ok(())
}

#[test]
fn repeated_running_control_preserves_current_safe_private_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let private_output = coordinator.process(vec![ScalpingInput::Private(private(1, safety()))])?;
    assert_eq!(private_output[0].disposition, ShadowDisposition::ShadowOnly);

    let running = coordinator.process(vec![ScalpingInput::Control(ControlTarget::Running)])?;
    assert_eq!(running[0].disposition, ShadowDisposition::ShadowOnly);
    let market = coordinator.process(vec![market(7, 1, 101, Vec::new())?])?;
    assert_eq!(market[0].disposition, ShadowDisposition::ShadowOnly);
    assert!(matches!(
        market[0].decision,
        Some(ScalpingDecision::Prepared(_))
    ));
    Ok(())
}

#[test]
fn authorization_generation_must_match_private_facts() -> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let inputs = vec![
        ScalpingInput::Private(private(2, safety())),
        market(7, 1, 200, Vec::new())?,
    ];
    assert!(matches!(
        coordinator.process(inputs),
        Err(ScalpingCoordinatorError::Generation)
    ));
    Ok(())
}

#[test]
fn private_generation_allows_forward_time_but_rejects_same_time_or_rollback()
-> Result<(), Box<dyn std::error::Error>> {
    let mut current = coordinator()?;
    current.process(vec![ScalpingInput::Private(private_at(3, 300, safety()))])?;
    assert!(matches!(
        current.process(vec![ScalpingInput::Private(private_at(3, 300, safety()))]),
        Err(ScalpingCoordinatorError::PrivateGeneration)
    ));
    let mut next = coordinator()?;
    next.process(vec![ScalpingInput::Private(private_at(3, 300, safety()))])?;
    let output = next.process(vec![ScalpingInput::Private(private_at(3, 301, safety()))])?;
    assert_eq!(output[0].checkpoint.last_private_observed_at_ms, Some(301));
    assert!(matches!(
        next.process(vec![ScalpingInput::Private(private_at(2, 400, safety()))]),
        Err(ScalpingCoordinatorError::PrivateGeneration)
    ));
    Ok(())
}

#[test]
fn restored_pending_episode_requires_a_new_private_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut original = coordinator()?;
    let first_inputs = vec![ScalpingInput::Private(private(1, safety()))];
    original.process(first_inputs)?;
    let evaluate_inputs = vec![market(7, 1, 100, Vec::new())?];
    let preparation = original
        .process(evaluate_inputs)?
        .pop()
        .and_then(|output| output.preparation)
        .ok_or("missing preparation")?;
    original.process(vec![ScalpingInput::Private(private_at(1, 101, safety()))])?;
    let admitted = original
        .process(vec![market(7, 1, 102, vec![evidence(&preparation)])?])?
        .pop()
        .ok_or("missing admission")?;
    assert!(matches!(
        admitted.decision,
        Some(ScalpingDecision::Intent(_))
    ));
    let checkpoint = original.checkpoint();
    assert!(checkpoint.strategy.episode.is_some());

    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut restored = ScalpingShadowCoordinator::restore(binding, params, checkpoint)?;
    let old_private = vec![ScalpingInput::Private(private_at(1, 102, safety()))];
    assert!(matches!(
        restored.process(old_private),
        Err(ScalpingCoordinatorError::RecoveryGeneration)
    ));
    let new_private = vec![ScalpingInput::Private(private_at(2, 200, safety()))];
    let output = restored
        .process(new_private)?
        .pop()
        .ok_or("missing output")?;
    assert_eq!(output.disposition, ShadowDisposition::ShadowOnly);
    assert_eq!(output.checkpoint.last_private_generation, Some(2));
    assert_eq!(output.checkpoint.last_private_observed_at_ms, Some(200));
    Ok(())
}

#[test]
fn evidence_admission_rejects_a_regressed_market_context() -> Result<(), Box<dyn std::error::Error>>
{
    let mut coordinator = coordinator()?;
    coordinator.process(vec![ScalpingInput::Private(private(1, safety()))])?;
    let preparation = coordinator
        .process(vec![market(7, 1, 100, Vec::new())?])?
        .pop()
        .and_then(|output| output.preparation)
        .ok_or("missing preparation")?;
    assert!(matches!(
        coordinator.process(vec![market(7, 1, 99, vec![evidence(&preparation)])?]),
        Err(ScalpingCoordinatorError::Strategy(
            venue::strategy::scalping::ScalpingError::FeatureProgress
        ))
    ));
    Ok(())
}

#[test]
fn decision_clock_rejects_a_stale_market_frame() -> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let mut stale = market(7, 1, 100, Vec::new())?;
    if let ScalpingInput::Market { decision_at_ms, .. } = &mut stale {
        *decision_at_ms = 351;
    }
    let outputs = coordinator.process(vec![stale, ScalpingInput::Private(private(1, safety()))])?;
    let market_output = outputs.last().ok_or("missing market output")?;
    assert!(matches!(
        market_output.decision,
        Some(ScalpingDecision::Noop(NoopReason::DecisionExpired))
    ));
    assert!(market_output.preparation.is_none());
    Ok(())
}

#[test]
fn checkpoint_rejects_a_generation_without_its_private_observed_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let mut coordinator = coordinator()?;
    let _ = coordinator.process(vec![ScalpingInput::Private(private(1, safety()))])?;
    let mut checkpoint = coordinator.checkpoint();
    checkpoint.last_private_observed_at_ms = None;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    assert!(matches!(
        ScalpingShadowCoordinator::restore(binding, params, checkpoint),
        Err(ScalpingCoordinatorError::Checkpoint)
    ));
    Ok(())
}
