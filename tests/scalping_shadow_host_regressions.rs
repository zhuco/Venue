use std::collections::BTreeMap;

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    controller::{
        ControlAuthority, ControlTarget, InstanceControlRecord, InstanceControlStore,
        ScalpingControllerBlock, ScalpingControllerSource,
    },
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    runtime::{
        BoundRiskRevaluation, CustodyStatus, DeadlineTick, EntryDisposition,
        EpisodeDeadlineCompletion, EpisodeDeadlineOutcome, EpisodeObservation, LifecycleReport,
        PrivateEntryGateReport, PrivateFacts, SCALPING_COORDINATOR_SCHEMA_VERSION,
        ScalpingCoordinatorCheckpoint, ScalpingResidentCycle, ScalpingResidentRuntime,
        ScalpingShadowHost, ScalpingShadowHostError, ShadowDisposition,
        episode_observation_fact_id,
    },
    storage::{ProjectionStore, ScalpingRiskBinding},
    strategy::scalping::{
        CandidateCosts, CandidateEvidence, CandidatePreparation, Direction, EpisodeAction,
        EpisodeExitReason, EpisodeFaultKind, ExposureState, FillSlice, OutcomeProbabilities,
        ProtectionState, RiskFact, RiskRevaluation, RiskUnit, SafetyDeadline, SafetyProjection,
        ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding, StrategyKind,
    },
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

fn private(generation: u64, observed_at_ms: u64) -> PrivateFacts {
    PrivateFacts {
        generation,
        observed_at_ms,
        root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:0"),
        safety: safety(),
        custody: CustodyStatus::Complete,
    }
}

fn gate_report(
    private: Option<PrivateFacts>,
    control: Option<ControlTarget>,
) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: if control.is_some() {
                venue::runtime::ControlDisposition::StopAndProtect
            } else {
                venue::runtime::ControlDisposition::None
            },
        },
        entry_ready: false,
        forwarded_private: private,
        control,
    }
}

fn ready_gate(private: PrivateFacts) -> PrivateEntryGateReport {
    let mut report = gate_report(Some(private), None);
    report.entry_ready = true;
    report.lifecycle.entry = EntryDisposition::Armed;
    report
}

fn bound_risk(
    binding: &StrategyBinding,
    cursor_sequence: u64,
    proof_id: &str,
    complete_through_ms: u64,
) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    let risk_unit = RiskUnit::new("risk")?;
    Ok(BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: risk_unit.clone(),
            valuation_generation: 2,
        },
        proof: RiskRevaluation {
            proof_id: proof_id.to_owned(),
            target_generation: 2,
            risk_unit,
            window_start_ms: 0,
            complete_through_ms,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        },
        cursor_sequence,
    })
}

fn authorization(
    binding: &StrategyBinding,
    generation: u64,
) -> venue::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "shadow-control".to_owned(),
        idempotency_key: "shadow-control-1".to_owned(),
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

fn frame_at(watermark_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation: 7,
        watermark_ms,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 7,
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

fn frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    frame_at(100)
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

fn persisted_deadline(
    control_deadline: bool,
) -> Result<
    (
        StrategyBinding,
        ScalpingParams,
        ScalpingCoordinatorCheckpoint,
    ),
    Box<dyn std::error::Error>,
> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    let mut episode_frame = frame()?;
    episode_frame.values.expected_move_bps = Decimal::new(10, 0);
    let decision =
        strategy.evaluate_at(&episode_frame, &safety(), &authorization(&binding, 1), 100)?;
    let preparation = match decision {
        ScalpingDecision::Prepared(preparation) => preparation,
        _ => return Err("strategy did not prepare an episode".into()),
    };
    strategy.admit(&[evidence(&preparation)], 100)?;
    let deadline = SafetyDeadline {
        deadline_id: if control_deadline {
            "control-deadline"
        } else {
            "episode-deadline"
        }
        .to_owned(),
        generation: 7,
        armed_at_ms: 101,
        expires_at_ms: 110,
    };
    if control_deadline {
        strategy.arm_control_fault_deadline(deadline)?;
    } else {
        strategy.arm_episode_fault_deadline(EpisodeFaultKind::UnprotectedExposure, deadline)?;
    }
    Ok((
        binding,
        params,
        ScalpingCoordinatorCheckpoint {
            schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
            strategy: strategy.checkpoint(),
            last_private_generation: Some(1),
            last_private_observed_at_ms: Some(101),
            last_private_root_cause_fact_id: Some("private-readback:1:101:0".to_owned()),
            last_risk_cursor_sequence: None,
            last_risk_proof_id: None,
            risk_control_target: None,
            control_target: ControlTarget::Running,
            last_episode_projection: None,
            last_episode_deadline_completion: None,
            last_market_delivery: None,
            control_stopped: false,
        },
    ))
}

struct ActiveEpisodeFixture {
    binding: StrategyBinding,
    params: ScalpingParams,
    checkpoint: ScalpingCoordinatorCheckpoint,
    episode_id: String,
    direction: Direction,
    reference: Decimal,
    hard_stop_bps: Decimal,
    target_bps: Decimal,
    max_hold_ms: u64,
}

fn active_episode_fixture() -> Result<ActiveEpisodeFixture, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    let mut episode_frame = frame()?;
    episode_frame.values.expected_move_bps = Decimal::new(10, 0);
    let decision =
        strategy.evaluate_at(&episode_frame, &safety(), &authorization(&binding, 1), 100)?;
    let preparation = match decision {
        ScalpingDecision::Prepared(preparation) => preparation,
        _ => return Err("strategy did not prepare an episode".into()),
    };
    strategy.admit(&[evidence(&preparation)], 100)?;
    let episode = strategy
        .episode()
        .cloned()
        .ok_or("strategy did not admit an episode")?;
    let fixture = ActiveEpisodeFixture {
        binding,
        params,
        checkpoint: ScalpingCoordinatorCheckpoint {
            schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
            strategy: strategy.checkpoint(),
            last_private_generation: Some(1),
            last_private_observed_at_ms: Some(100),
            last_private_root_cause_fact_id: Some("private-readback:1:100:0".to_owned()),
            last_risk_cursor_sequence: None,
            last_risk_proof_id: None,
            risk_control_target: None,
            control_target: ControlTarget::Running,
            last_episode_projection: None,
            last_episode_deadline_completion: None,
            last_market_delivery: None,
            control_stopped: false,
        },
        episode_id: episode.episode_id.clone(),
        direction: episode.frozen_intent.direction,
        reference: episode.frozen_intent.reference_price.value(),
        hard_stop_bps: episode.frozen_intent.hard_stop_distance_bps,
        target_bps: episode.frozen_intent.target_distance_bps,
        max_hold_ms: episode.frozen_intent.max_hold_ms,
    };
    Ok(fixture)
}

fn episode_private(
    generation: u64,
    observed_at_ms: u64,
    protection: ProtectionState,
    custody: CustodyStatus,
) -> PrivateFacts {
    PrivateFacts {
        generation,
        observed_at_ms,
        root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:0"),
        safety: SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Open,
            execution_unknown: false,
            protection,
            owner_conflict: false,
            risk_budget_available: true,
        },
        custody,
    }
}

fn private_fact_id(generation: u64, observed_at_ms: u64) -> String {
    format!("private-readback:{generation}:{observed_at_ms}:0")
}

fn episode_observation(
    fixture: &ActiveEpisodeFixture,
    generation: u64,
    observed_at_ms: u64,
    mark: Decimal,
) -> Result<EpisodeObservation, Box<dyn std::error::Error>> {
    let mut observation = EpisodeObservation {
        binding_digest: fixture.checkpoint.strategy.binding_digest.clone(),
        episode_id: fixture.episode_id.clone(),
        generation,
        observed_at_ms,
        private_root_cause_fact_id: private_fact_id(generation, observed_at_ms),
        observation_fact_id: String::new(),
        mark_symbol: fixture.binding.symbol.clone(),
        mark_generation: 1,
        mark_received_at_ms: observed_at_ms,
        mark_exchange_time_ms: observed_at_ms,
        mark_price: Price::new(mark)?,
    };
    observation.observation_fact_id = episode_observation_fact_id(&observation)?;
    Ok(observation)
}

fn directional_mark(
    fixture: &ActiveEpisodeFixture,
    distance_bps: Decimal,
    favorable: bool,
) -> Decimal {
    let ratio = distance_bps / Decimal::new(10_000, 0);
    match (fixture.direction, favorable) {
        (Direction::Long, true) | (Direction::Short, false) => {
            fixture.reference * (Decimal::ONE + ratio)
        }
        (Direction::Long, false) | (Direction::Short, true) => {
            fixture.reference * (Decimal::ONE - ratio)
        }
    }
}

fn arm_gap_deadline(
    host: &mut ScalpingShadowHost,
    fixture: &ActiveEpisodeFixture,
    generation: u64,
    observed_at_ms: u64,
    deadline_id: &str,
) -> Result<EpisodeDeadlineCompletion, Box<dyn std::error::Error>> {
    host.on_private_gate(&gate_report(
        Some(episode_private(
            generation,
            observed_at_ms,
            ProtectionState::Gap,
            CustodyStatus::Incomplete,
        )),
        Some(ControlTarget::StopAndProtect),
    ))?;
    let observation = episode_observation(fixture, generation, observed_at_ms, fixture.reference)?;
    let projected = host.on_episode_observation(observation.clone())?;
    let expires_at_ms = projected
        .episode_actions
        .iter()
        .find_map(|action| match action {
            EpisodeAction::ArmFaultDeadline {
                no_later_than_ms, ..
            } => Some(*no_later_than_ms),
            _ => None,
        })
        .ok_or("missing deadline arm action")?;
    let completion = EpisodeDeadlineCompletion {
        episode_id: fixture.episode_id.clone(),
        observation_generation: generation,
        observation_observed_at_ms: observed_at_ms,
        private_root_cause_fact_id: private_fact_id(generation, observed_at_ms),
        observation_fact_id: observation.observation_fact_id,
        completion_fact_id: format!("{deadline_id}-complete"),
        completed_at_ms: observed_at_ms,
        outcome: EpisodeDeadlineOutcome::Armed {
            kind: EpisodeFaultKind::UnprotectedExposure,
            deadline: SafetyDeadline {
                deadline_id: deadline_id.to_owned(),
                generation,
                armed_at_ms: observed_at_ms,
                expires_at_ms,
            },
        },
    };
    host.on_episode_deadline_completion(completion.clone())?;
    Ok(completion)
}

#[test]
fn restored_host_requires_a_strictly_newer_private_generation_before_persisting()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(false)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params.clone())?;

    assert!(matches!(
        host.on_private_gate(&gate_report(Some(private(1, 102)), None)),
        Err(ScalpingShadowHostError::RecoveryGeneration)
    ));
    let report = host.on_private_gate(&gate_report(Some(private(2, 200)), None))?;
    assert_eq!(report.disposition, ShadowDisposition::ShadowOnly);
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert_eq!(
        persisted.and_then(|checkpoint| checkpoint.last_private_generation),
        Some(2)
    );
    Ok(())
}

#[test]
fn overdue_deadline_is_persisted_before_stop_and_protect_is_reported()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(false)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    assert!(matches!(
        host.tick(DeadlineTick {
            now_ms: 110,
            root_cause_fact_id: "private-fact-1".to_owned(),
        }),
        Err(ScalpingShadowHostError::RecoveryGeneration)
    ));
    host.on_private_gate(&gate_report(Some(private(2, 200)), None))?;

    assert!(matches!(
        host.tick(DeadlineTick {
            now_ms: 199,
            root_cause_fact_id: "private-fact-clock-regression".to_owned(),
        }),
        Err(ScalpingShadowHostError::ClockRegression)
    ));

    let report = host.tick(DeadlineTick {
        now_ms: 200,
        root_cause_fact_id: private_fact_id(2, 200),
    })?;
    assert_eq!(report.disposition, ShadowDisposition::StopAndProtect);
    assert!(report.deadline_fired);
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(
        persisted
            .and_then(|checkpoint| checkpoint.strategy.episode)
            .is_some_and(|episode| episode.fault.is_some())
    );

    let duplicate = host.tick(DeadlineTick {
        now_ms: 201,
        root_cause_fact_id: private_fact_id(2, 200),
    })?;
    assert_eq!(duplicate.disposition, ShadowDisposition::StopAndProtect);
    assert!(!duplicate.deadline_fired);
    Ok(())
}

#[test]
fn overdue_control_deadline_and_empty_root_fact_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(true)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    host.on_private_gate(&gate_report(Some(private(2, 200)), None))?;
    assert!(matches!(
        host.tick(DeadlineTick {
            now_ms: 110,
            root_cause_fact_id: " ".to_owned(),
        }),
        Err(ScalpingShadowHostError::Tick)
    ));
    let report = host.tick(DeadlineTick {
        now_ms: 200,
        root_cause_fact_id: private_fact_id(2, 200),
    })?;
    assert_eq!(report.disposition, ShadowDisposition::StopAndProtect);
    assert!(report.deadline_fired);
    Ok(())
}

#[test]
fn tick_without_an_overdue_deadline_reports_the_coordinator_disposition()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    host.on_private_gate(&gate_report(Some(private(1, 100)), None))?;

    let report = host.tick(DeadlineTick {
        now_ms: 101,
        root_cause_fact_id: private_fact_id(1, 100),
    })?;
    assert_eq!(report.disposition, ShadowDisposition::ShadowOnly);
    assert!(!report.deadline_fired);
    Ok(())
}

#[test]
fn gate_control_is_processed_before_private_and_cannot_clear_recovery_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(false)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;

    let stopped = host.on_private_gate(&gate_report(None, Some(ControlTarget::StopAndProtect)))?;
    assert_eq!(stopped.disposition, ShadowDisposition::StopAndProtect);
    assert!(matches!(
        host.tick(DeadlineTick {
            now_ms: 110,
            root_cause_fact_id: "private-fact".to_owned(),
        }),
        Err(ScalpingShadowHostError::RecoveryGeneration)
    ));
    let forwarded = host.on_private_gate(&gate_report(Some(private(2, 200)), None))?;
    assert_eq!(forwarded.disposition, ShadowDisposition::RemainFenced);
    Ok(())
}

#[test]
fn save_failure_poisoning_requires_reopen_before_any_later_input()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("missing").join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    assert!(matches!(
        host.on_private_gate(&gate_report(Some(private(1, 100)), None)),
        Err(ScalpingShadowHostError::Storage(_))
    ));
    assert!(matches!(
        host.tick(DeadlineTick {
            now_ms: 101,
            root_cause_fact_id: "private-fact".to_owned(),
        }),
        Err(ScalpingShadowHostError::Poisoned)
    ));
    Ok(())
}

#[test]
fn control_then_invalid_private_batch_error_poisoning_requires_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(false)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params.clone())?;
    assert!(matches!(
        host.on_private_gate(&gate_report(
            Some(private(2, 0)),
            Some(ControlTarget::StopAndProtect),
        )),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    assert!(matches!(
        host.on_private_gate(&gate_report(Some(private(3, 300)), None)),
        Err(ScalpingShadowHostError::Poisoned)
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(persisted.is_some_and(|checkpoint| checkpoint.control_stopped));
    drop(host);
    let mut reopened = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    let report = reopened.on_private_gate(&gate_report(Some(private(3, 300)), None))?;
    assert_eq!(report.disposition, ShadowDisposition::RemainFenced);
    Ok(())
}

#[test]
fn fresh_safe_gate_advances_preparation_and_admission_only_through_the_coordinator()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;

    let before_gate = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    assert_eq!(before_gate.disposition, ShadowDisposition::RemainFenced);
    assert!(before_gate.decision.is_none());
    host.on_private_gate(&ready_gate(private(1, 100)))?;

    let prepared = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    let preparation = prepared.preparation.ok_or("missing preparation")?;
    assert!(matches!(
        prepared.decision,
        Some(ScalpingDecision::Prepared(_))
    ));
    let admitted = host.on_market(
        frame_at(102)?,
        102,
        authorization(&binding, 1),
        vec![evidence(&preparation)],
    )?;
    assert!(matches!(
        admitted.decision,
        Some(ScalpingDecision::Intent(_))
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(
        persisted
            .and_then(|checkpoint| checkpoint.strategy.episode)
            .is_some()
    );
    Ok(())
}

#[test]
fn restored_host_keeps_market_fenced_until_a_new_gate_rearms_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let (binding, params, checkpoint) = persisted_deadline(false)?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    host.on_private_gate(&gate_report(Some(private(2, 200)), None))?;

    let fenced = host.on_market(frame()?, 200, authorization(&binding, 2), Vec::new())?;
    assert_eq!(fenced.disposition, ShadowDisposition::RemainFenced);
    assert!(fenced.decision.is_none());
    assert!(fenced.preparation.is_none());
    Ok(())
}

#[test]
fn bound_risk_revaluation_validates_identity_and_persists_cursor_and_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    let mut wrong = bound_risk(&binding, 1, "proof-1", 100)?;
    wrong.binding.account = "other".to_owned();
    assert!(matches!(
        host.on_bound_risk_revaluation(wrong),
        Err(ScalpingShadowHostError::RiskBinding)
    ));

    let applied = host.on_bound_risk_revaluation(bound_risk(&binding, 1, "proof-1", 100)?)?;
    assert_eq!(applied.checkpoint.last_risk_cursor_sequence, Some(1));
    assert_eq!(
        applied.checkpoint.last_risk_proof_id.as_deref(),
        Some("proof-1")
    );
    assert_eq!(
        applied
            .checkpoint
            .strategy
            .risk
            .last_revaluation_id
            .as_deref(),
        Some("proof-1")
    );
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    let persisted = persisted.ok_or("risk checkpoint missing")?;
    assert_eq!(persisted.last_risk_cursor_sequence, Some(1));
    assert_eq!(persisted.last_risk_proof_id.as_deref(), Some("proof-1"));
    Ok(())
}

#[test]
fn bound_risk_duplicate_is_idempotent_but_cursor_rollback_poisons_the_host()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    let proof = bound_risk(&binding, 2, "proof-2", 100)?;
    let first = host.on_bound_risk_revaluation(proof.clone())?;
    let duplicate = host.on_bound_risk_revaluation(proof)?;
    assert_eq!(duplicate.checkpoint, first.checkpoint);
    assert!(matches!(
        host.on_bound_risk_revaluation(bound_risk(&binding, 1, "proof-1", 100)?),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(
        persisted
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.strategy.risk.generation_mismatch)
    );
    assert_eq!(
        persisted.and_then(|checkpoint| checkpoint.last_risk_cursor_sequence),
        Some(2)
    );
    assert!(matches!(
        host.on_bound_risk_revaluation(bound_risk(&binding, 3, "proof-3", 100)?),
        Err(ScalpingShadowHostError::Poisoned)
    ));
    Ok(())
}

#[test]
fn malformed_bound_proof_poisons_and_successful_proof_requires_a_new_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    let mut malformed = bound_risk(&binding, 1, "proof-bad", 100)?;
    malformed.proof.source_fact_ids = vec![" ".to_owned()];
    assert!(matches!(
        host.on_bound_risk_revaluation(malformed),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    assert!(matches!(
        host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new()),
        Err(ScalpingShadowHostError::Poisoned)
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(
        persisted
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.strategy.risk.generation_mismatch)
    );
    assert_eq!(
        persisted
            .as_ref()
            .and_then(|checkpoint| checkpoint.last_risk_cursor_sequence),
        None
    );

    drop(host);
    let mut reopened = ScalpingShadowHost::open_or_restore(
        &path,
        binding.clone(),
        ScalpingParams::shadow(binding.risk_budget.clone()),
    )?;
    reopened.on_private_gate(&ready_gate(private(1, 101)))?;
    let still_fenced = reopened.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    assert!(matches!(
        still_fenced.decision,
        Some(ScalpingDecision::Noop(_))
    ));
    assert!(still_fenced.checkpoint.strategy.risk.generation_mismatch);

    let safe_path = directory.path().join("scalping-host-safe.json");
    let mut safe = ScalpingShadowHost::open_or_restore(
        &safe_path,
        binding.clone(),
        ScalpingParams::shadow(binding.risk_budget.clone()),
    )?;
    safe.on_private_gate(&ready_gate(private(1, 100)))?;
    safe.on_bound_risk_revaluation(bound_risk(&binding, 1, "proof-good", 100)?)?;
    let fenced = safe.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    assert_eq!(fenced.disposition, ShadowDisposition::RemainFenced);
    assert!(fenced.decision.is_none());
    Ok(())
}

#[test]
fn bound_risk_proof_cannot_predate_private_or_market_watermarks()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    host.on_private_gate(&ready_gate(private(1, 100)))?;
    let _ = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;

    assert!(matches!(
        host.on_bound_risk_revaluation(bound_risk(&binding, 1, "proof-old", 99)?),
        Err(ScalpingShadowHostError::RiskWatermark)
    ));
    Ok(())
}

#[test]
fn active_episode_loss_proof_persists_the_legacy_emergency_control()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params.clone())?;
    host.on_private_gate(&ready_gate(private(1, 100)))?;
    let prepared = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    let preparation = prepared.preparation.ok_or("missing preparation")?;
    host.on_market(
        frame_at(102)?,
        102,
        authorization(&binding, 1),
        vec![evidence(&preparation)],
    )?;

    let mut loss = bound_risk(&binding, 1, "loss-proof", 102)?;
    loss.proof.source_fact_ids = vec!["loss-fill".to_owned()];
    loss.proof.revalued_facts = vec![RiskFact {
        fact_id: "loss-fill".to_owned(),
        event_time_ms: 102,
        valuation_generation: 2,
        risk_unit: RiskUnit::new("risk")?,
        realized_pnl: Decimal::new(-2, 0),
    }];
    let report = host.on_bound_risk_revaluation(loss)?;
    assert_eq!(report.disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(report.requested_control, Some(ControlTarget::EmergencyStop));
    assert_eq!(
        report.checkpoint.risk_control_target,
        Some(ControlTarget::EmergencyStop)
    );
    assert!(report.checkpoint.control_stopped);

    drop(host);
    let mut reopened = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    let recovered = reopened.on_private_gate(&gate_report(Some(private(2, 200)), None))?;
    assert_eq!(recovered.disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(
        recovered.checkpoint.risk_control_target,
        Some(ControlTarget::EmergencyStop)
    );
    Ok(())
}

#[test]
fn active_episode_open_proof_does_not_publish_the_temporary_mismatch_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    host.on_private_gate(&ready_gate(private(1, 100)))?;
    let prepared = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    let preparation = prepared.preparation.ok_or("missing preparation")?;
    host.on_market(
        frame_at(102)?,
        102,
        authorization(&binding, 1),
        vec![evidence(&preparation)],
    )?;

    let report = host.on_bound_risk_revaluation(bound_risk(&binding, 1, "open-proof", 102)?)?;
    assert_eq!(report.disposition, ShadowDisposition::ShadowOnly);
    assert_eq!(report.requested_control, None);
    assert_eq!(report.checkpoint.risk_control_target, None);
    assert!(!report.checkpoint.control_stopped);
    assert!(!report.checkpoint.strategy.risk.generation_mismatch);
    Ok(())
}

#[test]
fn malformed_proof_during_active_episode_persists_stop_and_protect()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("scalping-host.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params.clone())?;
    host.on_private_gate(&ready_gate(private(1, 100)))?;
    let prepared = host.on_market(frame()?, 100, authorization(&binding, 1), Vec::new())?;
    let preparation = prepared.preparation.ok_or("missing preparation")?;
    host.on_market(
        frame_at(102)?,
        102,
        authorization(&binding, 1),
        vec![evidence(&preparation)],
    )?;

    let mut malformed = bound_risk(&binding, 1, "bad-active-proof", 102)?;
    malformed.proof.source_fact_ids = vec![" ".to_owned()];
    assert!(matches!(
        host.on_bound_risk_revaluation(malformed),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    let persisted = persisted.ok_or("missing failed-proof checkpoint")?;
    assert!(persisted.strategy.risk.generation_mismatch);
    assert_eq!(
        persisted.risk_control_target,
        Some(ControlTarget::StopAndProtect)
    );
    assert!(persisted.control_stopped);

    drop(host);
    let mut reopened = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    let recovered = reopened.on_private_gate(&gate_report(Some(private(2, 200)), None))?;
    assert_eq!(recovered.disposition, ShadowDisposition::StopAndProtect);
    assert_eq!(
        recovered.checkpoint.risk_control_target,
        Some(ControlTarget::StopAndProtect)
    );
    Ok(())
}

#[test]
fn active_episode_max_hold_projects_exit_after_private_reconcile()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            200,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    let opened =
        host.on_episode_observation(episode_observation(&fixture, 2, 200, fixture.reference)?)?;
    assert!(
        opened.episode_actions.is_empty(),
        "unexpected opening actions: {:?}; reference={}, target_bps={}, stop_bps={}",
        opened.episode_actions,
        fixture.reference,
        fixture.target_bps,
        fixture.hard_stop_bps
    );

    let elapsed = 200_u64.saturating_add(fixture.max_hold_ms);
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            elapsed,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    let exited = host.on_episode_observation(episode_observation(
        &fixture,
        2,
        elapsed,
        fixture.reference,
    )?)?;
    assert!(exited.episode_actions.iter().any(|action| matches!(
        action,
        EpisodeAction::Exit {
            reason: EpisodeExitReason::MaxHoldElapsed,
            ..
        }
    )));
    Ok(())
}

#[test]
fn active_episode_projects_legacy_stop_and_target_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    for (favorable, expected) in [
        (false, EpisodeExitReason::HardStop),
        (true, EpisodeExitReason::TargetReached),
    ] {
        let directory = tempdir()?;
        let path = directory.path().join("episode-host.json");
        let fixture = active_episode_fixture()?;
        ProjectionStore::new(&path).save(&fixture.checkpoint)?;
        let mut host = ScalpingShadowHost::open_or_restore(
            &path,
            fixture.binding.clone(),
            fixture.params.clone(),
        )?;
        host.on_private_gate(&gate_report(
            Some(episode_private(
                2,
                200,
                ProtectionState::Complete,
                CustodyStatus::Complete,
            )),
            None,
        ))?;
        let distance = if favorable {
            fixture.target_bps
        } else {
            fixture.hard_stop_bps
        };
        let projected = host.on_episode_observation(episode_observation(
            &fixture,
            2,
            200,
            directional_mark(&fixture, distance, favorable),
        )?)?;
        assert!(projected.episode_actions.iter().any(|action| matches!(
            action,
            EpisodeAction::Exit { reason, .. } if *reason == expected
        )));
    }
    Ok(())
}

#[test]
fn protection_gap_arm_and_cancel_require_exact_external_completions()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            200,
            ProtectionState::Gap,
            CustodyStatus::Incomplete,
        )),
        Some(ControlTarget::StopAndProtect),
    ))?;
    let observation = episode_observation(&fixture, 2, 200, fixture.reference)?;
    let projected = host.on_episode_observation(observation.clone())?;
    let no_later_than_ms = projected
        .episode_actions
        .iter()
        .find_map(|action| match action {
            EpisodeAction::ArmFaultDeadline {
                no_later_than_ms, ..
            } => Some(*no_later_than_ms),
            _ => None,
        })
        .ok_or("missing deadline arm action")?;
    let duplicate = host.on_episode_observation(observation)?;
    assert_eq!(duplicate.episode_actions, projected.episode_actions);

    let arm = EpisodeDeadlineCompletion {
        episode_id: fixture.episode_id.clone(),
        observation_generation: 2,
        observation_observed_at_ms: 200,
        private_root_cause_fact_id: private_fact_id(2, 200),
        observation_fact_id: episode_observation(&fixture, 2, 200, fixture.reference)?
            .observation_fact_id,
        completion_fact_id: "deadline-arm-complete".to_owned(),
        completed_at_ms: 200,
        outcome: EpisodeDeadlineOutcome::Armed {
            kind: EpisodeFaultKind::UnprotectedExposure,
            deadline: SafetyDeadline {
                deadline_id: "episode-deadline-2".to_owned(),
                generation: 2,
                armed_at_ms: 200,
                expires_at_ms: no_later_than_ms,
            },
        },
    };
    host.on_episode_deadline_completion(arm.clone())?;
    host.on_episode_deadline_completion(arm)?;
    assert!(
        host.checkpoint()
            .strategy
            .episode
            .as_ref()
            .is_some_and(|episode| episode.episode_fault_deadline.is_some())
    );

    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            201,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    let protected =
        host.on_episode_observation(episode_observation(&fixture, 2, 201, fixture.reference)?)?;
    assert!(protected.episode_actions.iter().any(|action| matches!(
        action,
        EpisodeAction::CancelFaultDeadline { deadline_id }
            if deadline_id == "episode-deadline-2"
    )));
    assert!(
        protected
            .episode_actions
            .iter()
            .any(|action| matches!(action, EpisodeAction::MaintainProtection { .. }))
    );
    host.on_episode_deadline_completion(EpisodeDeadlineCompletion {
        episode_id: fixture.episode_id.clone(),
        observation_generation: 2,
        observation_observed_at_ms: 201,
        private_root_cause_fact_id: private_fact_id(2, 201),
        observation_fact_id: episode_observation(&fixture, 2, 201, fixture.reference)?
            .observation_fact_id,
        completion_fact_id: "deadline-cancel-complete".to_owned(),
        completed_at_ms: 201,
        outcome: EpisodeDeadlineOutcome::Cancelled {
            deadline_id: "episode-deadline-2".to_owned(),
            deadline_generation: 2,
        },
    })?;
    assert!(
        host.checkpoint()
            .strategy
            .episode
            .as_ref()
            .is_some_and(|episode| episode.episode_fault_deadline.is_none())
    );
    Ok(())
}

#[test]
fn expired_controller_authorization_still_projects_existing_episode_safety()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller-source.json");
    InstanceControlStore::new(&control_path).save(
        &InstanceControlRecord {
            schema_version: 1,
            binding: fixture.binding.clone(),
            target: ControlTarget::Running,
            command_id: "expired-running".to_owned(),
            idempotency_key: "expired-running-key".to_owned(),
            safety_deadline_ms: Some(150),
            revision: 1,
        },
        None,
    )?;
    let mut source =
        ScalpingControllerSource::open(&control_path, &cursor_path, fixture.binding.clone())?;
    let expired = source.observe(
        Some(&ControlAuthority {
            generation: 2,
            parameter_release_id: fixture.binding.parameter_release_id.clone(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        }),
        200,
    )?;
    assert_eq!(expired.block(), Some(ScalpingControllerBlock::Deadline));
    let mut resident = ScalpingResidentRuntime::new(host);
    let report = resident.drive_cycle(ScalpingResidentCycle {
        controller: Some(expired),
        private_gate: Some(gate_report(
            Some(episode_private(
                2,
                200,
                ProtectionState::Complete,
                CustodyStatus::Complete,
            )),
            None,
        )),
        episode_observation: Some(episode_observation(&fixture, 2, 200, fixture.reference)?),
        ..ScalpingResidentCycle::default()
    })?;
    assert!(report.market.is_none());
    assert!(report.episode.is_some_and(|output| {
        output
            .episode_actions
            .iter()
            .any(|action| matches!(action, EpisodeAction::MaintainProtection { .. }))
    }));
    Ok(())
}

#[test]
fn pending_arm_is_reprojected_after_restart_and_armed_deadline_survives_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            200,
            ProtectionState::Gap,
            CustodyStatus::Incomplete,
        )),
        Some(ControlTarget::StopAndProtect),
    ))?;
    host.on_episode_observation(episode_observation(&fixture, 2, 200, fixture.reference)?)?;
    drop(host);

    let mut restored = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    restored.on_private_gate(&gate_report(
        Some(episode_private(
            3,
            300,
            ProtectionState::Gap,
            CustodyStatus::Incomplete,
        )),
        Some(ControlTarget::StopAndProtect),
    ))?;
    let reprojected = restored.on_episode_observation(episode_observation(
        &fixture,
        3,
        300,
        fixture.reference,
    )?)?;
    let expires_at_ms = reprojected
        .episode_actions
        .iter()
        .find_map(|action| match action {
            EpisodeAction::ArmFaultDeadline {
                no_later_than_ms, ..
            } => Some(*no_later_than_ms),
            _ => None,
        })
        .ok_or("restart did not reproject arm")?;
    restored.on_episode_deadline_completion(EpisodeDeadlineCompletion {
        episode_id: fixture.episode_id.clone(),
        observation_generation: 3,
        observation_observed_at_ms: 300,
        private_root_cause_fact_id: private_fact_id(3, 300),
        observation_fact_id: episode_observation(&fixture, 3, 300, fixture.reference)?
            .observation_fact_id,
        completion_fact_id: "restart-arm-complete".to_owned(),
        completed_at_ms: 300,
        outcome: EpisodeDeadlineOutcome::Armed {
            kind: EpisodeFaultKind::UnprotectedExposure,
            deadline: SafetyDeadline {
                deadline_id: "restart-deadline".to_owned(),
                generation: 3,
                armed_at_ms: 300,
                expires_at_ms,
            },
        },
    })?;
    drop(restored);

    let mut deadline_restored =
        ScalpingShadowHost::open_or_restore(&path, fixture.binding, fixture.params)?;
    deadline_restored.on_private_gate(&gate_report(
        Some(episode_private(
            4,
            expires_at_ms,
            ProtectionState::Gap,
            CustodyStatus::Incomplete,
        )),
        Some(ControlTarget::StopAndProtect),
    ))?;
    let fired = deadline_restored.tick(DeadlineTick {
        now_ms: expires_at_ms,
        root_cause_fact_id: private_fact_id(4, expires_at_ms),
    })?;
    assert!(fired.deadline_fired);
    assert_eq!(fired.disposition, ShadowDisposition::StopAndProtect);
    Ok(())
}

#[test]
fn episode_observation_uses_independent_mark_fact_from_private_root()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            200,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    let observation = episode_observation(&fixture, 2, 200, fixture.reference)?;
    assert_ne!(
        observation.private_root_cause_fact_id,
        observation.observation_fact_id
    );
    host.on_episode_observation(observation.clone())?;

    let mut forged = observation.clone();
    forged.private_root_cause_fact_id = "caller-invented-private-root".to_owned();
    assert!(matches!(
        host.on_episode_observation(forged),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    let mut forged = observation;
    forged.observation_fact_id = forged.private_root_cause_fact_id.clone();
    assert!(matches!(
        host.on_episode_observation(forged),
        Err(ScalpingShadowHostError::Poisoned)
    ));
    let persisted: Option<ScalpingCoordinatorCheckpoint> = ProjectionStore::new(&path).load()?;
    assert!(persisted.is_some_and(|checkpoint| checkpoint.last_episode_projection.is_some()));
    Ok(())
}

#[test]
fn exact_old_episode_receipt_cannot_replay_after_private_identity_advances()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            200,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    let old = episode_observation(&fixture, 2, 200, fixture.reference)?;
    host.on_episode_observation(old.clone())?;
    host.on_private_gate(&gate_report(
        Some(episode_private(
            3,
            300,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;

    assert!(matches!(
        host.on_episode_observation(old),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    Ok(())
}

#[test]
fn deadline_completion_checkpoint_and_late_identity_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let tampered_directory = tempdir()?;
    let tampered_path = tampered_directory.path().join("episode-host.json");
    let fixture = active_episode_fixture()?;
    ProjectionStore::new(&tampered_path).save(&fixture.checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(
        &tampered_path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    arm_gap_deadline(&mut host, &fixture, 2, 200, "tampered-deadline")?;
    let mut checkpoint = host.checkpoint();
    checkpoint
        .last_episode_deadline_completion
        .as_mut()
        .ok_or("missing deadline completion")?
        .observation_fact_id = "tampered-private-root".to_owned();
    drop(host);
    ProjectionStore::new(&tampered_path).save(&checkpoint)?;
    assert!(matches!(
        ScalpingShadowHost::open_or_restore(
            &tampered_path,
            fixture.binding.clone(),
            fixture.params.clone()
        ),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));

    let late_directory = tempdir()?;
    let late_path = late_directory.path().join("episode-host.json");
    let late_fixture = active_episode_fixture()?;
    ProjectionStore::new(&late_path).save(&late_fixture.checkpoint)?;
    let mut late_host = ScalpingShadowHost::open_or_restore(
        &late_path,
        late_fixture.binding.clone(),
        late_fixture.params.clone(),
    )?;
    let old_completion = arm_gap_deadline(&mut late_host, &late_fixture, 2, 200, "late-deadline")?;
    late_host.on_private_gate(&gate_report(
        Some(episode_private(
            2,
            201,
            ProtectionState::Complete,
            CustodyStatus::Complete,
        )),
        None,
    ))?;
    late_host.on_episode_observation(episode_observation(
        &late_fixture,
        2,
        201,
        late_fixture.reference,
    )?)?;
    assert!(
        late_host
            .checkpoint()
            .last_episode_deadline_completion
            .is_none()
    );
    assert!(matches!(
        late_host.on_episode_deadline_completion(old_completion),
        Err(ScalpingShadowHostError::Coordinator(_))
    ));
    Ok(())
}
