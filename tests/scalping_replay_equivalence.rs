use std::collections::BTreeMap;

use rust_decimal::Decimal;
use venue::{
    controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    strategy::scalping::{
        CalibrationEvidence, CandidateEvidenceBundle, CostEvidence, DeadlineFired, Direction,
        EntryStyle, EpisodeAction, EpisodeExitReason, EpisodeFaultKind, EpisodeState,
        EvidenceIdentity, ExitTemplate, Expert, ExposureState, FaultRecoveryAuthorization,
        FaultScope, FillSlice, NoopReason, OutcomeProbabilities, ProtectionState, RiskEvidence,
        RiskFact, RiskGate, RiskRevaluation, RiskUnit, SafetyDeadline, SafetyProjection,
        ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding, StrategyKind,
        join_candidate_evidence,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "scalping_equivalence".to_owned(),
        run_id: "range_fade_fixture".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "scalping_equivalence:range_fade_fixture".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn identity(
    kind: &str,
    preparation: &venue::strategy::scalping::CandidatePreparation,
    candidate: &venue::strategy::scalping::SemanticIntent,
    release_digest: String,
) -> EvidenceIdentity {
    EvidenceIdentity {
        schema_version: 1,
        evidence_id: format!("{kind}-{}", candidate.intent_id),
        candidate_id: candidate.intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        producer_generation: 1,
        release_digest,
        valid_until_ms: preparation.valid_until_ms,
    }
}

fn legacy_evidence_bundle(
    preparation: &venue::strategy::scalping::CandidatePreparation,
    candidate: &venue::strategy::scalping::SemanticIntent,
) -> CandidateEvidenceBundle {
    CandidateEvidenceBundle {
        calibration: CalibrationEvidence {
            identity: identity("calibration", preparation, candidate, "0".repeat(64)),
            model_version: "scalping-shadow-calibration-v1".to_owned(),
            fill_distribution: vec![FillSlice {
                fill_ratio: Decimal::ONE,
                probability: Decimal::ONE,
            }],
            outcomes: OutcomeProbabilities {
                target: Decimal::new(8, 1),
                stop: Decimal::new(1, 1),
                other: Decimal::new(1, 1),
            },
            target_pnl_bps: Decimal::new(20, 0),
            stop_pnl_bps: Decimal::new(-5, 0),
            other_pnl_bps: Decimal::ZERO,
            uncertainty_bps: Decimal::ONE,
        },
        costs: CostEvidence {
            identity: identity("cost", preparation, candidate, "a".repeat(64)),
            entry_cost_bps: Decimal::new(2, 1),
            exit_cost_bps: Decimal::new(4, 1),
            funding_cost_bps: Decimal::ZERO,
            nonfill_cost_bps: Decimal::ZERO,
            opportunity_cost_bps: Decimal::ZERO,
        },
        risk: RiskEvidence {
            identity: identity("risk", preparation, candidate, "b".repeat(64)),
            policy_digest: "c".repeat(64),
            worst_loss: candidate.risk_plan.risk_per_episode.clone(),
            admissible: true,
        },
    }
}

/// Independently transcribed from the frozen multi-alpha-scalper RangeFade fixture. It is a new
/// fixture, not a runtime dependency on `bak`.
fn legacy_range_fade_frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation: 1,
        watermark_ms: 100,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .map(|source| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 1,
                        sequence: 1,
                        event_time_ms: 100,
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
            mid_price: Price::new(Decimal::new(100, 0))?,
            fair_price: Price::new(Decimal::new(101, 0))?,
            spread_bps: Decimal::new(1, 1),
            depth_quote: Decimal::new(1_000, 0),
            book_imbalance: Decimal::ONE,
            trade_imbalance: Decimal::ONE,
            short_return_bps: Decimal::ZERO,
            trend_efficiency: Decimal::ZERO,
            bandwidth_expansion: Decimal::ZERO,
            expected_move_bps: Decimal::new(20, 0),
            toxicity: Decimal::ZERO,
        },
        breakout: None,
    })
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

fn open_safety() -> SafetyProjection {
    SafetyProjection {
        exposure: ExposureState::Open,
        ..safety()
    }
}

fn range_frame_at(
    watermark_ms: u64,
    sequence: u64,
) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut frame = legacy_range_fade_frame()?;
    frame.watermark_ms = watermark_ms;
    for cursor in frame.cursors.values_mut() {
        cursor.sequence = sequence;
        cursor.event_time_ms = watermark_ms;
    }
    Ok(frame)
}

fn authorization(binding: &StrategyBinding) -> venue::controller::EntryAuthorization {
    authorization_generation(binding, 1)
}

fn authorization_generation(
    binding: &StrategyBinding,
    generation: u64,
) -> venue::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: venue::controller::CONTROL_SCHEMA_VERSION,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "range-fade-start".to_owned(),
        idempotency_key: "range-fade-start-1".to_owned(),
        safety_deadline_ms: None,
        revision: 1,
    }
    .authorize(
        &ControlAuthority {
            generation,
            parameter_release_id: binding.parameter_release_id.clone(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        },
        100,
    )
}

fn breakout_frame(
    watermark_ms: u64,
    sequence: u64,
) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut frame = legacy_range_fade_frame()?;
    frame.watermark_ms = watermark_ms;
    for cursor in frame.cursors.values_mut() {
        cursor.sequence = sequence;
        cursor.event_time_ms = watermark_ms;
    }
    frame.values.short_return_bps = Decimal::new(10, 0);
    frame.values.trend_efficiency = Decimal::ONE;
    frame.values.bandwidth_expansion = Decimal::ONE;
    frame.feature_versions.insert(
        "_breakout_opportunity".to_owned(),
        "pulse-breakout-opportunity-v1".to_owned(),
    );
    frame.breakout = Some(venue::indicator::BreakoutOpportunity {
        schema_version: 1,
        generation: frame.generation,
        feature_version: "pulse-breakout-opportunity-v1".to_owned(),
        direction: venue::indicator::BreakoutDirection::Long,
        boundary_id: "boundary-1".to_owned(),
        boundary_sequence: 1,
        compression_cycle_id: "compression-1".to_owned(),
        compression_cycle_sequence: 1,
        detected_at_ms: 100,
        valid_until_ms: 10_000,
    });
    Ok(frame)
}

fn breakout_frame_with_watermarks(
    watermark_ms: u64,
    sequence: u64,
    boundary_sequence: u64,
    compression_cycle_sequence: u64,
) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut frame = breakout_frame(watermark_ms, sequence)?;
    let opportunity = frame
        .breakout
        .as_mut()
        .ok_or("breakout fixture must contain an opportunity")?;
    opportunity.boundary_sequence = boundary_sequence;
    opportunity.boundary_id = format!("boundary-{boundary_sequence}");
    opportunity.compression_cycle_sequence = compression_cycle_sequence;
    opportunity.compression_cycle_id = format!("compression-{compression_cycle_sequence}");
    Ok(frame)
}

#[test]
fn frozen_range_fade_fixture_has_the_same_candidate_and_single_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let frame = legacy_range_fade_frame()?;
    let safety = safety();
    let authorization = authorization(&binding);
    assert_eq!(binding.strategy_kind(), StrategyKind::Scalping);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let preparation = match strategy.evaluate(&frame, &safety, &authorization)? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    assert_eq!(preparation.candidates.len(), 1);
    let candidate = &preparation.candidates[0];
    assert_eq!(candidate.direction, Direction::Long);
    assert_eq!(candidate.expert, Expert::RangeFade);
    assert_eq!(candidate.entry_style, EntryStyle::PassiveMaker);
    assert_eq!(candidate.exit_template, ExitTemplate::FairValue);
    assert_eq!(candidate.target_quote.value, Decimal::new(5, 0));

    // Legacy fixture: 14.9 bps after cost and 1 bps sigma, hence 13.9 bps robust EV.
    let evidence = join_candidate_evidence(
        &preparation,
        candidate,
        &legacy_evidence_bundle(&preparation, candidate),
        100,
    )?;
    assert_eq!(evidence.outcome_expected_value_bps, Decimal::new(155, 1));
    assert_eq!(evidence.net_expected_value_bps, Decimal::new(149, 1));
    let intent = match strategy.admit(&[evidence], 100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    assert_eq!(intent.intent_id, candidate.intent_id);
    assert!(matches!(
        strategy.admit(&[], 100)?,
        ScalpingDecision::Noop(NoopReason::EvidenceUnavailable)
    ));
    Ok(())
}

#[test]
fn frozen_partial_fill_distribution_uses_after_cost_robust_ev()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.min_net_ev_bps = Decimal::new(8_185, 3);
    let frame = legacy_range_fade_frame()?;
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let preparation = match strategy.evaluate(&frame, &safety(), &authorization)? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let candidate = &preparation.candidates[0];
    let mut bundle = legacy_evidence_bundle(&preparation, candidate);
    bundle.calibration.fill_distribution = vec![
        FillSlice {
            fill_ratio: Decimal::ZERO,
            probability: Decimal::new(2, 1),
        },
        FillSlice {
            fill_ratio: Decimal::new(5, 1),
            probability: Decimal::new(3, 1),
        },
        FillSlice {
            fill_ratio: Decimal::ONE,
            probability: Decimal::new(5, 1),
        },
    ];
    bundle.costs.nonfill_cost_bps = Decimal::new(2, 1);
    bundle.costs.opportunity_cost_bps = Decimal::new(3, 1);
    let evidence = join_candidate_evidence(&preparation, candidate, &bundle, 100)?;
    assert_eq!(evidence.fill_probability, Decimal::new(8, 1));
    assert_eq!(evidence.net_expected_value_bps, Decimal::new(9_185, 3));
    assert!(matches!(
        strategy.admit(&[evidence], 100)?,
        ScalpingDecision::Intent(_)
    ));
    Ok(())
}

#[test]
fn restored_breakout_memory_rewarms_and_rejects_the_same_opportunity()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let initial_authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    assert!(matches!(
        strategy.evaluate(&breakout_frame(100, 1)?, &safety(), &initial_authorization)?,
        ScalpingDecision::Noop(NoopReason::NoSignal)
    ));
    let preparation = match strategy.evaluate(
        &breakout_frame(2_100, 2)?,
        &safety(),
        &initial_authorization,
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let candidate = &preparation.candidates[0];
    let evidence = join_candidate_evidence(
        &preparation,
        candidate,
        &legacy_evidence_bundle(&preparation, candidate),
        2_100,
    )?;
    let intent = match strategy.admit(&[evidence], 2_100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    strategy.acknowledge_shadow_intent(&intent.intent_id, 2_100)?;
    let checkpoint = strategy.checkpoint();
    assert_eq!(checkpoint.strategy_kind, StrategyKind::Scalping);

    let mut restored = ScalpingStrategy::restore(binding.clone(), params, checkpoint)?;
    let recovered_authorization = authorization_generation(&binding, 2);
    assert!(matches!(
        restored.evaluate(
            &breakout_frame(3_100, 3)?,
            &safety(),
            &recovered_authorization,
        )?,
        ScalpingDecision::Noop(NoopReason::RecoveryWarmup)
    ));
    let repeated = match restored.evaluate(
        &breakout_frame(5_100, 4)?,
        &safety(),
        &recovered_authorization,
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let repeated_candidate = &repeated.candidates[0];
    let repeated_evidence = join_candidate_evidence(
        &repeated,
        repeated_candidate,
        &legacy_evidence_bundle(&repeated, repeated_candidate),
        5_100,
    )?;
    assert!(matches!(
        restored.admit(&[repeated_evidence], 5_100)?,
        ScalpingDecision::Noop(NoopReason::DuplicateOpportunity)
    ));
    Ok(())
}

#[test]
fn frozen_reprice_reuses_identity_and_rejects_fresh_cost_edge_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut strategy = ScalpingStrategy::new(binding.clone(), params)?;
    let preparation = match strategy.evaluate(
        &legacy_range_fade_frame()?,
        &safety(),
        &authorization(&binding),
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let candidate = &preparation.candidates[0];
    assert_eq!(candidate.max_reprices, 1);
    let frozen = join_candidate_evidence(
        &preparation,
        candidate,
        &legacy_evidence_bundle(&preparation, candidate),
        100,
    )?;
    assert!(matches!(
        strategy.admit(&[frozen], 100)?,
        ScalpingDecision::Intent(_)
    ));

    let mut fresh_bundle = legacy_evidence_bundle(&preparation, candidate);
    fresh_bundle.costs.identity.evidence_id = "fresh-reprice-quote".to_owned();
    fresh_bundle.costs.identity.producer_generation = 2;
    let fresh = join_candidate_evidence(&preparation, candidate, &fresh_bundle, 101)?;
    strategy.validate_reprice(&fresh, 101)?;

    fresh_bundle.costs.entry_cost_bps = Decimal::new(1_000, 0);
    let deteriorated = join_candidate_evidence(&preparation, candidate, &fresh_bundle, 101)?;
    assert!(strategy.validate_reprice(&deteriorated, 101).is_err());
    assert_eq!(strategy.checkpoint().candidate_memory.candidates.len(), 1);
    Ok(())
}

#[test]
fn frozen_episode_enforces_retry_attempt_cap_and_cooldown() -> Result<(), Box<dyn std::error::Error>>
{
    let binding = binding()?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.max_order_attempts = 4;
    params.entry_retry_cooldown_ms = 50;
    params.cooldown_ms = 100;
    params.candidate_ttl_ms = 500;
    let mut strategy = ScalpingStrategy::new(binding.clone(), params)?;
    let preparation = match strategy.evaluate(
        &legacy_range_fade_frame()?,
        &safety(),
        &authorization(&binding),
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let candidate = &preparation.candidates[0];
    assert_eq!(candidate.attempt_cap, 2);
    let evidence = join_candidate_evidence(
        &preparation,
        candidate,
        &legacy_evidence_bundle(&preparation, candidate),
        100,
    )?;
    let intent = match strategy.admit(&[evidence], 100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };

    assert!(strategy.retry_reserved_entry(&intent.intent_id, 100)?);
    assert_eq!(
        strategy.episode().map(|episode| episode.state),
        Some(EpisodeState::EntryRetryWait)
    );
    assert!(!strategy.advance_entry_retry_frame(&range_frame_at(149, 2)?)?);
    assert!(strategy.advance_entry_retry_frame(&range_frame_at(150, 3)?)?);
    assert_eq!(
        strategy.episode().map(|episode| episode.attempts_started),
        Some(2)
    );
    assert!(!strategy.retry_reserved_entry(&intent.intent_id, 150)?);
    assert!(matches!(
        strategy.evaluate(
            &range_frame_at(200, 4)?,
            &safety(),
            &authorization(&binding),
        )?,
        ScalpingDecision::Noop(NoopReason::Cooldown)
    ));
    assert!(matches!(
        strategy.evaluate(
            &range_frame_at(250, 5)?,
            &safety(),
            &authorization(&binding),
        )?,
        ScalpingDecision::Prepared(_)
    ));
    Ok(())
}

#[test]
fn trend_attempt_cap_uses_the_full_release_limit() -> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.max_order_attempts = 4;
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let mut first = range_frame_at(100, 1)?;
    first.values.fair_price = first.values.mid_price;
    first.values.short_return_bps = -Decimal::ONE;
    first.values.trend_efficiency = Decimal::ONE;
    assert!(matches!(
        strategy.evaluate(&first, &safety(), &authorization)?,
        ScalpingDecision::Noop(NoopReason::NoSignal)
    ));
    let mut matured = first;
    matured.watermark_ms = 2_100;
    for cursor in matured.cursors.values_mut() {
        cursor.sequence = 2;
        cursor.event_time_ms = 2_100;
    }
    let preparation = match strategy.evaluate(&matured, &safety(), &authorization)? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    assert_eq!(preparation.candidates.len(), 2);
    assert!(
        preparation
            .candidates
            .iter()
            .all(|candidate| candidate.attempt_cap == 4)
    );
    Ok(())
}

#[test]
fn breakout_requires_both_watermarks_and_uses_one_attempt() -> Result<(), Box<dyn std::error::Error>>
{
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    assert!(matches!(
        strategy.evaluate(&breakout_frame(100, 1)?, &safety(), &authorization)?,
        ScalpingDecision::Noop(NoopReason::NoSignal)
    ));
    let first = match strategy.evaluate(
        &breakout_frame_with_watermarks(2_100, 2, 1, 1)?,
        &safety(),
        &authorization,
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    assert_eq!(first.candidates[0].attempt_cap, 1);
    assert_eq!(first.candidates[0].max_reprices, 0);
    let evidence = join_candidate_evidence(
        &first,
        &first.candidates[0],
        &legacy_evidence_bundle(&first, &first.candidates[0]),
        2_100,
    )?;
    let intent = match strategy.admit(&[evidence], 2_100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    strategy.acknowledge_shadow_intent(&intent.intent_id, 2_100)?;

    for (watermark_ms, sequence, boundary, compression) in [(3_100, 3, 2, 1), (4_100, 4, 1, 2)] {
        let repeated = match strategy.evaluate(
            &breakout_frame_with_watermarks(watermark_ms, sequence, boundary, compression)?,
            &safety(),
            &authorization,
        )? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let evidence = join_candidate_evidence(
            &repeated,
            &repeated.candidates[0],
            &legacy_evidence_bundle(&repeated, &repeated.candidates[0]),
            watermark_ms,
        )?;
        assert!(matches!(
            strategy.admit(&[evidence], watermark_ms)?,
            ScalpingDecision::Noop(NoopReason::DuplicateOpportunity)
        ));
    }

    let advanced = match strategy.evaluate(
        &breakout_frame_with_watermarks(5_100, 5, 2, 2)?,
        &safety(),
        &authorization,
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let evidence = join_candidate_evidence(
        &advanced,
        &advanced.candidates[0],
        &legacy_evidence_bundle(&advanced, &advanced.candidates[0]),
        5_100,
    )?;
    let intent = match strategy.admit(&[evidence], 5_100)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    strategy.acknowledge_shadow_intent(&intent.intent_id, 5_100)?;

    let mut next_generation = breakout_frame_with_watermarks(6_100, 1, 1, 1)?;
    next_generation.generation = 2;
    for cursor in next_generation.cursors.values_mut() {
        cursor.generation = 2;
    }
    next_generation
        .breakout
        .as_mut()
        .ok_or("breakout fixture must contain an opportunity")?
        .generation = 2;
    let reset_watermarks = match strategy.evaluate(&next_generation, &safety(), &authorization)? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    let evidence = join_candidate_evidence(
        &reset_watermarks,
        &reset_watermarks.candidates[0],
        &legacy_evidence_bundle(&reset_watermarks, &reset_watermarks.candidates[0]),
        6_100,
    )?;
    assert!(matches!(
        strategy.admit(&[evidence], 6_100)?,
        ScalpingDecision::Intent(_)
    ));
    Ok(())
}

#[test]
fn episode_projects_price_stop_target_hold_and_control_stop_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    fn admitted(max_hold_ms: u64) -> Result<ScalpingStrategy, Box<dyn std::error::Error>> {
        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.max_hold_ms = max_hold_ms;
        params.max_unprotected_ms = params.max_unprotected_ms.min(max_hold_ms);
        let mut strategy = ScalpingStrategy::new(binding.clone(), params)?;
        let preparation = match strategy.evaluate(
            &legacy_range_fade_frame()?,
            &safety(),
            &authorization(&binding),
        )? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let evidence = join_candidate_evidence(
            &preparation,
            &preparation.candidates[0],
            &legacy_evidence_bundle(&preparation, &preparation.candidates[0]),
            100,
        )?;
        if !matches!(
            strategy.admit(&[evidence], 100)?,
            ScalpingDecision::Intent(_)
        ) {
            return Err("legacy fixture must reserve one episode".into());
        }
        Ok(strategy)
    }

    let mut target = admitted(1_000)?;
    assert!(
        target
            .project_episode(
                ControlTarget::Running,
                &open_safety(),
                Price::new(Decimal::new(100, 0))?,
                101,
                "open-target",
            )?
            .is_empty()
    );
    assert!(matches!(
        target
            .project_episode(
                ControlTarget::Running,
                &open_safety(),
                Price::new(Decimal::new(1_002, 1))?,
                102,
                "target-hit",
            )?
            .as_slice(),
        [EpisodeAction::Exit {
            reason: EpisodeExitReason::TargetReached,
            ..
        }]
    ));

    let mut stop = admitted(1_000)?;
    stop.project_episode(
        ControlTarget::Running,
        &open_safety(),
        Price::new(Decimal::new(100, 0))?,
        101,
        "open-stop",
    )?;
    assert!(matches!(
        stop.project_episode(
            ControlTarget::Running,
            &open_safety(),
            Price::new(Decimal::new(998, 1))?,
            102,
            "hard-stop",
        )?
        .as_slice(),
        [EpisodeAction::Exit {
            reason: EpisodeExitReason::HardStop,
            ..
        }]
    ));

    let mut hold = admitted(50)?;
    hold.project_episode(
        ControlTarget::Running,
        &open_safety(),
        Price::new(Decimal::new(100, 0))?,
        101,
        "open-hold",
    )?;
    assert!(matches!(
        hold.project_episode(
            ControlTarget::Running,
            &open_safety(),
            Price::new(Decimal::new(100, 0))?,
            151,
            "hold-expired",
        )?
        .as_slice(),
        [EpisodeAction::Exit {
            reason: EpisodeExitReason::MaxHoldElapsed,
            ..
        }]
    ));

    let mut protected = admitted(1_000)?;
    protected.project_episode(
        ControlTarget::Running,
        &open_safety(),
        Price::new(Decimal::new(100, 0))?,
        101,
        "open-protected",
    )?;
    assert!(matches!(
        protected
            .project_episode(
                ControlTarget::StopAndProtect,
                &open_safety(),
                Price::new(Decimal::new(100, 0))?,
                102,
                "stop-protected",
            )?
            .as_slice(),
        [EpisodeAction::MaintainProtection { .. }]
    ));
    assert_eq!(
        protected.episode().map(|episode| episode.state),
        Some(EpisodeState::StoppedProtected)
    );
    Ok(())
}

#[test]
fn unprotected_deadline_is_persisted_non_sliding_and_explicitly_recovered()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.max_unprotected_ms = 9;
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    let preparation = match strategy.evaluate(
        &legacy_range_fade_frame()?,
        &safety(),
        &authorization(&binding),
    )? {
        ScalpingDecision::Prepared(preparation) => preparation,
        decision => return Err(format!("unexpected decision: {decision:?}").into()),
    };
    assert_eq!(preparation.candidates[0].max_unprotected_ms, 9);
    let evidence = join_candidate_evidence(
        &preparation,
        &preparation.candidates[0],
        &legacy_evidence_bundle(&preparation, &preparation.candidates[0]),
        100,
    )?;
    assert!(matches!(
        strategy.admit(&[evidence], 100)?,
        ScalpingDecision::Intent(_)
    ));
    let mut gap = open_safety();
    gap.protection = ProtectionState::Gap;
    assert!(matches!(
        strategy
            .project_episode(
                ControlTarget::Running,
                &gap,
                Price::new(Decimal::new(100, 0))?,
                101,
                "protection-gap-1",
            )?
            .as_slice(),
        [EpisodeAction::ArmFaultDeadline {
            kind: EpisodeFaultKind::UnprotectedExposure,
            no_later_than_ms: 110,
        }]
    ));
    strategy.arm_episode_fault_deadline(
        EpisodeFaultKind::UnprotectedExposure,
        SafetyDeadline {
            deadline_id: "unprotected-1".to_owned(),
            generation: 7,
            armed_at_ms: 101,
            expires_at_ms: 110,
        },
    )?;
    assert!(
        strategy
            .project_episode(
                ControlTarget::Running,
                &gap,
                Price::new(Decimal::new(100, 0))?,
                105,
                "protection-gap-2",
            )?
            .is_empty()
    );
    assert_eq!(
        strategy
            .episode()
            .and_then(|episode| episode.episode_fault_deadline.as_ref())
            .map(|armed| armed.deadline.expires_at_ms),
        Some(110)
    );

    let checkpoint = strategy.checkpoint();
    let mut restored = ScalpingStrategy::restore(binding, params, checkpoint)?;
    let fired = DeadlineFired {
        deadline_id: "unprotected-1".to_owned(),
        generation: 7,
        fired_at_ms: 110,
        root_cause_fact_id: "protection-gap-fact".to_owned(),
    };
    assert!(restored.apply_fault_deadline(&fired)?);
    assert!(!restored.apply_fault_deadline(&fired)?);
    assert_eq!(
        restored.episode().map(|episode| episode.state),
        Some(EpisodeState::EpisodeFaulted)
    );
    let wrong = FaultRecoveryAuthorization {
        authorization_id: "recover-wrong".to_owned(),
        episode_id: restored
            .episode()
            .ok_or("restored episode missing")?
            .episode_id
            .clone(),
        scope: FaultScope::Episode(EpisodeFaultKind::UnprotectedExposure),
        fault_generation: 7,
        root_cause_fact_id: "other-root".to_owned(),
        valid_until_ms: 200,
    };
    assert!(
        restored
            .recover_episode_fault(&wrong, &open_safety(), 111, "protection-gap-fact")
            .is_err()
    );
    let recovery = FaultRecoveryAuthorization {
        authorization_id: "recover-unprotected-1".to_owned(),
        episode_id: restored
            .episode()
            .ok_or("restored episode missing")?
            .episode_id
            .clone(),
        scope: FaultScope::Episode(EpisodeFaultKind::UnprotectedExposure),
        fault_generation: 7,
        root_cause_fact_id: "protection-gap-fact".to_owned(),
        valid_until_ms: 200,
    };
    restored.recover_episode_fault(&recovery, &open_safety(), 111, "protection-gap-fact")?;
    assert_eq!(
        restored.episode().map(|episode| episode.state),
        Some(EpisodeState::Open)
    );

    restored.arm_control_fault_deadline(SafetyDeadline {
        deadline_id: "control-1".to_owned(),
        generation: 8,
        armed_at_ms: 111,
        expires_at_ms: 120,
    })?;
    let control_fired = DeadlineFired {
        deadline_id: "control-1".to_owned(),
        generation: 8,
        fired_at_ms: 120,
        root_cause_fact_id: "control-root".to_owned(),
    };
    assert!(restored.apply_fault_deadline(&control_fired)?);
    let control_recovery = FaultRecoveryAuthorization {
        authorization_id: "recover-control-1".to_owned(),
        episode_id: restored
            .episode()
            .ok_or("restored episode missing")?
            .episode_id
            .clone(),
        scope: FaultScope::Control,
        fault_generation: 8,
        root_cause_fact_id: "control-root".to_owned(),
        valid_until_ms: 200,
    };
    restored.recover_control_fault(
        &control_recovery,
        &open_safety(),
        ControlTarget::StopAndProtect,
        121,
        "control-root",
    )?;
    assert_eq!(
        restored.episode().map(|episode| episode.state),
        Some(EpisodeState::StoppedProtected)
    );
    Ok(())
}

#[test]
fn risk_generation_change_requires_complete_revaluation_after_restore()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let risk_unit = RiskUnit::new("risk")?;
    let fact = |fact_id: &str, event_time_ms: u64, generation: u64| RiskFact {
        fact_id: fact_id.to_owned(),
        event_time_ms,
        valuation_generation: generation,
        risk_unit: risk_unit.clone(),
        realized_pnl: Decimal::ONE,
    };
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    assert_eq!(
        strategy.record_risk(fact("one", 1, 1))?.gate,
        RiskGate::Open
    );
    assert_eq!(
        strategy.record_risk(fact("two", 2, 2))?.gate,
        RiskGate::GenerationMismatch
    );
    assert_eq!(
        strategy.record_risk(fact("three", 3, 2))?.gate,
        RiskGate::GenerationMismatch
    );
    let checkpoint = strategy.checkpoint();
    let mut restored = ScalpingStrategy::restore(binding, params, checkpoint)?;
    assert_eq!(restored.risk_snapshot(3).gate, RiskGate::GenerationMismatch);
    assert_eq!(restored.risk_snapshot(3).risk_unit, risk_unit);
    assert!(
        restored
            .apply_risk_revaluation(RiskRevaluation {
                proof_id: "wrong-unit".to_owned(),
                target_generation: 2,
                risk_unit: RiskUnit::new("another-risk")?,
                window_start_ms: 0,
                complete_through_ms: 3,
                source_fact_ids: vec!["one".to_owned(), "two".to_owned(), "three".to_owned()],
                revalued_facts: Vec::new(),
            })
            .is_err()
    );
    assert!(
        restored
            .apply_risk_revaluation(RiskRevaluation {
                proof_id: "partial".to_owned(),
                target_generation: 2,
                risk_unit: risk_unit.clone(),
                window_start_ms: 0,
                complete_through_ms: 3,
                source_fact_ids: vec!["one".to_owned()],
                revalued_facts: Vec::new(),
            })
            .is_err()
    );
    let proof = RiskRevaluation {
        proof_id: "revaluation-2".to_owned(),
        target_generation: 2,
        risk_unit: risk_unit.clone(),
        window_start_ms: 0,
        complete_through_ms: 3,
        source_fact_ids: vec!["one".to_owned(), "two".to_owned(), "three".to_owned()],
        revalued_facts: vec![fact("one", 1, 2), fact("two", 2, 2), fact("three", 3, 2)],
    };
    let reopened = restored.apply_risk_revaluation(proof.clone())?;
    assert_eq!(reopened.gate, RiskGate::Open);
    assert_eq!(reopened.valuation_generation, Some(2));
    assert_eq!(reopened.risk_unit, risk_unit);
    assert_eq!(restored.apply_risk_revaluation(proof)?, reopened);
    Ok(())
}

#[test]
fn risk_cooldown_deadline_does_not_slide_on_reads() -> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.loss_window_ms = 1;
    params.drawdown_window_ms = 1;
    params.max_loss_streak = 1;
    params.cooldown_ms = 100;
    params.loss_cooldown_ms = 100;
    params.loss_window_limit.value = Decimal::new(100, 0);
    params.drawdown_limit.value = Decimal::new(100, 0);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let closed = strategy.record_risk(RiskFact {
        fact_id: "loss-one".to_owned(),
        event_time_ms: 1,
        valuation_generation: 1,
        risk_unit: RiskUnit::new("risk")?,
        realized_pnl: Decimal::NEGATIVE_ONE,
    })?;
    assert_eq!(closed.gate, RiskGate::LossStreak);
    assert_eq!(closed.cooldown_until_ms, Some(101));
    let first = strategy.risk_snapshot(3);
    let second = strategy.risk_snapshot(4);
    assert_eq!(first.gate, RiskGate::Cooldown);
    assert_eq!(first.cooldown_until_ms, Some(101));
    assert_eq!(second.cooldown_until_ms, Some(101));
    Ok(())
}
