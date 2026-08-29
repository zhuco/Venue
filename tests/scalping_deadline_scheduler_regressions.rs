use std::collections::BTreeMap;

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    runtime::{
        CustodyStatus, DeadlineClockObservation, DeadlineSchedulerOutcome, EntryDisposition,
        LifecycleReport, PrivateEntryGateReport, PrivateFacts, SCALPING_COORDINATOR_SCHEMA_VERSION,
        ScalpingCoordinatorCheckpoint, ScalpingDeadlineScheduler, ScalpingDeadlineSchedulerError,
        ScalpingShadowHost, ShadowDisposition,
    },
    storage::ProjectionStore,
    strategy::scalping::{
        CandidateCosts, CandidateEvidence, CandidatePreparation, EpisodeFaultKind, ExposureState,
        FillSlice, OutcomeProbabilities, ProtectionState, SafetyDeadline, SafetyProjection,
        ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "deadline_scheduler".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "deadline_scheduler:shadow_1".to_owned(),
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

fn gate(private: PrivateFacts) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Armed,
            control: venue::runtime::ControlDisposition::None,
        },
        entry_ready: true,
        forwarded_private: Some(private),
        control: None,
    }
}

fn authorization(binding: &StrategyBinding) -> venue::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "deadline-control".to_owned(),
        idempotency_key: "deadline-control-1".to_owned(),
        safety_deadline_ms: None,
        revision: 1,
    }
    .authorize(
        &ControlAuthority {
            generation: 2,
            parameter_release_id: binding.parameter_release_id.clone(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        },
        100,
    )
}

fn frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation: 7,
        watermark_ms: 100,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 7,
                        sequence: 100 + index as u64,
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

fn advanced_frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut frame = frame()?;
    frame.generation = 8;
    frame.watermark_ms = 200;
    for cursor in frame.cursors.values_mut() {
        cursor.generation = 8;
        cursor.sequence += 100;
        cursor.event_time_ms = 200;
    }
    Ok(frame)
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

fn persisted_deadlines() -> Result<
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
    let decision = strategy.evaluate_at(&frame()?, &safety(), &authorization(&binding), 100)?;
    let preparation = match decision {
        ScalpingDecision::Prepared(preparation) => preparation,
        _ => return Err("strategy did not prepare an episode".into()),
    };
    strategy.admit(&[evidence(&preparation)], 100)?;
    strategy.arm_episode_fault_deadline(
        EpisodeFaultKind::UnprotectedExposure,
        SafetyDeadline {
            deadline_id: "episode-deadline".to_owned(),
            generation: 7,
            armed_at_ms: 101,
            expires_at_ms: 350,
        },
    )?;
    strategy.arm_control_fault_deadline(SafetyDeadline {
        deadline_id: "control-deadline".to_owned(),
        generation: 8,
        armed_at_ms: 101,
        expires_at_ms: 300,
    })?;
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
            control_target: venue::controller::ControlTarget::Running,
            last_episode_projection: None,
            last_episode_deadline_completion: None,
            last_market_delivery: None,
            control_stopped: false,
        },
    ))
}

#[test]
fn scheduler_reconciles_future_deadline_then_fires_earliest_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("shadow.json");
    let (binding, params, checkpoint) = persisted_deadlines()?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding.clone(), params)?;
    host.on_private_gate(&gate(private(2, 200)))?;
    let before = host.checkpoint();
    let mut scheduler = ScalpingDeadlineScheduler::new();

    let fenced = host.on_market(advanced_frame()?, 200, authorization(&binding), Vec::new())?;
    assert_eq!(fenced.disposition, ShadowDisposition::RemainFenced);
    assert_eq!(host.checkpoint(), before);

    assert_eq!(
        scheduler.observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 250,
                root_cause_fact_id: "private-readback:2:200:0".to_owned(),
            },
        )?,
        DeadlineSchedulerOutcome::Waiting(venue::runtime::ScheduledDeadline {
            deadline_id: "control-deadline".to_owned(),
            generation: 8,
            expires_at_ms: 300,
        })
    );
    assert_eq!(host.checkpoint(), before);

    let reconciled = host.on_market(advanced_frame()?, 200, authorization(&binding), Vec::new())?;
    assert_ne!(reconciled.disposition, ShadowDisposition::RemainFenced);

    let fired = scheduler.observe(
        &mut host,
        DeadlineClockObservation {
            now_ms: 300,
            root_cause_fact_id: "private-readback:2:200:0".to_owned(),
        },
    )?;
    assert!(matches!(
        fired,
        DeadlineSchedulerOutcome::Fired(ref report)
            if report.deadline_fired && report.disposition == ShadowDisposition::StopAndProtect
    ));
    Ok(())
}

#[test]
fn scheduler_rejects_repeated_regressing_and_empty_clock_observations()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("shadow.json");
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    let mut scheduler = ScalpingDeadlineScheduler::new();
    assert_eq!(
        scheduler.observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 100,
                root_cause_fact_id: "private-fact-100".to_owned(),
            },
        )?,
        DeadlineSchedulerOutcome::NoDeadline
    );
    assert!(matches!(
        scheduler.observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 100,
                root_cause_fact_id: "private-fact-duplicate".to_owned(),
            },
        ),
        Err(ScalpingDeadlineSchedulerError::ClockMonotonic)
    ));
    assert!(matches!(
        scheduler.observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 99,
                root_cause_fact_id: "private-fact-regression".to_owned(),
            },
        ),
        Err(ScalpingDeadlineSchedulerError::ClockMonotonic)
    ));
    assert!(matches!(
        ScalpingDeadlineScheduler::new().observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 101,
                root_cause_fact_id: " ".to_owned(),
            },
        ),
        Err(ScalpingDeadlineSchedulerError::Observation)
    ));
    Ok(())
}

#[test]
fn restored_host_without_new_private_generation_stays_fenced_at_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("shadow.json");
    let (binding, params, checkpoint) = persisted_deadlines()?;
    ProjectionStore::new(&path).save(&checkpoint)?;
    let mut host = ScalpingShadowHost::open_or_restore(&path, binding, params)?;
    let before = host.checkpoint();
    assert!(matches!(
        ScalpingDeadlineScheduler::new().observe(
            &mut host,
            DeadlineClockObservation {
                now_ms: 300,
                root_cause_fact_id: "private-fact-300".to_owned(),
            },
        ),
        Err(ScalpingDeadlineSchedulerError::RecoveryFenced)
    ));
    assert_eq!(host.checkpoint(), before);
    Ok(())
}
