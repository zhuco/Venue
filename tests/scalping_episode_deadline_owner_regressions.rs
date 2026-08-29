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
        ControlDisposition, CustodyStatus, DeadlineClockObservation, EntryDisposition,
        EpisodeDeadlineCompletion, EpisodeDeadlineOutcome, EpisodeDeadlineOwnerError,
        EpisodeDeadlineOwnerTurn, EpisodeObservation, EpisodeProjectionReceipt, LifecycleReport,
        PrivateEntryGateReport, PrivateFacts, SCALPING_COORDINATOR_SCHEMA_VERSION,
        ScalpingCoordinatorCheckpoint, ScalpingEpisodeDeadlineOwner, ScalpingShadowHost,
        ScalpingShadowHostError, ShadowDisposition, episode_observation_fact_id,
    },
    storage::ProjectionStore,
    strategy::scalping::{
        ArmedEpisodeFaultDeadline, CandidateCosts, CandidateEvidence, CandidatePreparation,
        EpisodeAction, EpisodeFaultKind, ExposureState, FillSlice, OutcomeProbabilities,
        ProtectionState, SafetyDeadline, SafetyProjection, ScalpingDecision, ScalpingParams,
        ScalpingStrategy, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "episode_deadline_owner".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "episode_deadline_owner:shadow_1".to_owned(),
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

fn authorization(binding: &StrategyBinding) -> venue::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "deadline-owner-control".to_owned(),
        idempotency_key: "deadline-owner-control-1".to_owned(),
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
            expected_move_bps: Decimal::new(10, 0),
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

struct Fixture {
    binding: StrategyBinding,
    params: ScalpingParams,
    host: ScalpingCoordinatorCheckpoint,
    episode_id: String,
}

fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    let decision = strategy.evaluate_at(&frame()?, &safety(), &authorization(&binding), 100)?;
    let preparation = match decision {
        ScalpingDecision::Prepared(preparation) => preparation,
        _ => return Err("strategy did not prepare an episode".into()),
    };
    strategy.admit(&[evidence(&preparation)], 100)?;
    let episode_id = strategy
        .episode()
        .ok_or("strategy did not admit an episode")?
        .episode_id
        .clone();
    Ok(Fixture {
        binding,
        params,
        host: ScalpingCoordinatorCheckpoint {
            schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
            strategy: strategy.checkpoint(),
            last_private_generation: Some(1),
            last_private_observed_at_ms: Some(130),
            last_private_root_cause_fact_id: Some("private-readback:1:130:9".to_owned()),
            last_risk_cursor_sequence: None,
            last_risk_proof_id: None,
            risk_control_target: None,
            control_target: ControlTarget::Running,
            last_episode_projection: None,
            last_episode_deadline_completion: None,
            last_market_delivery: None,
            control_stopped: false,
        },
        episode_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn receipt(
    fixture: &Fixture,
    generation: u64,
    observed_at_ms: u64,
    root: &str,
    mark_generation: u64,
    mark_received_at_ms: u64,
    mark_exchange_time_ms: u64,
    actions: Vec<EpisodeAction>,
) -> Result<EpisodeProjectionReceipt, Box<dyn std::error::Error>> {
    let mut observation = EpisodeObservation {
        binding_digest: fixture.host.strategy.binding_digest.clone(),
        episode_id: fixture.episode_id.clone(),
        generation,
        observed_at_ms,
        private_root_cause_fact_id: root.to_owned(),
        observation_fact_id: String::new(),
        mark_symbol: fixture.binding.symbol.clone(),
        mark_generation,
        mark_received_at_ms,
        mark_exchange_time_ms,
        mark_price: Price::new(Decimal::new(100, 0))?,
    };
    observation.observation_fact_id = episode_observation_fact_id(&observation)?;
    Ok(EpisodeProjectionReceipt {
        binding_digest: observation.binding_digest,
        episode_id: observation.episode_id,
        generation: observation.generation,
        observed_at_ms: observation.observed_at_ms,
        private_root_cause_fact_id: observation.private_root_cause_fact_id,
        observation_fact_id: observation.observation_fact_id,
        mark_symbol: observation.mark_symbol,
        mark_generation: observation.mark_generation,
        mark_received_at_ms: observation.mark_received_at_ms,
        mark_exchange_time_ms: observation.mark_exchange_time_ms,
        mark_price: observation.mark_price,
        actions,
    })
}

fn resign_receipt(
    receipt: &mut EpisodeProjectionReceipt,
) -> Result<(), Box<dyn std::error::Error>> {
    receipt.observation_fact_id.clear();
    receipt.observation_fact_id = episode_observation_fact_id(&EpisodeObservation {
        binding_digest: receipt.binding_digest.clone(),
        episode_id: receipt.episode_id.clone(),
        generation: receipt.generation,
        observed_at_ms: receipt.observed_at_ms,
        private_root_cause_fact_id: receipt.private_root_cause_fact_id.clone(),
        observation_fact_id: String::new(),
        mark_symbol: receipt.mark_symbol.clone(),
        mark_generation: receipt.mark_generation,
        mark_received_at_ms: receipt.mark_received_at_ms,
        mark_exchange_time_ms: receipt.mark_exchange_time_ms,
        mark_price: receipt.mark_price,
    })?;
    Ok(())
}

fn set_receipt(host: &mut ScalpingCoordinatorCheckpoint, receipt: EpisodeProjectionReceipt) {
    host.last_private_generation = Some(receipt.generation);
    host.last_private_observed_at_ms = Some(receipt.observed_at_ms);
    host.last_private_root_cause_fact_id = Some(receipt.private_root_cause_fact_id.clone());
    host.last_episode_projection = Some(receipt);
    host.last_episode_deadline_completion = None;
}

#[allow(clippy::too_many_arguments)]
fn install_receipt(
    fixture: &mut Fixture,
    generation: u64,
    observed_at_ms: u64,
    root: &str,
    mark_generation: u64,
    mark_received_at_ms: u64,
    mark_exchange_time_ms: u64,
    actions: Vec<EpisodeAction>,
) -> Result<(), Box<dyn std::error::Error>> {
    let value = receipt(
        fixture,
        generation,
        observed_at_ms,
        root,
        mark_generation,
        mark_received_at_ms,
        mark_exchange_time_ms,
        actions,
    )?;
    set_receipt(&mut fixture.host, value);
    Ok(())
}

fn clock(now_ms: u64, root: &str) -> DeadlineClockObservation {
    DeadlineClockObservation {
        now_ms,
        root_cause_fact_id: root.to_owned(),
    }
}

fn acknowledge(
    host: &mut ScalpingCoordinatorCheckpoint,
    turn: &EpisodeDeadlineOwnerTurn,
) -> Result<(), Box<dyn std::error::Error>> {
    let completion = match turn {
        EpisodeDeadlineOwnerTurn::Persisted(completion)
        | EpisodeDeadlineOwnerTurn::PendingReplay(completion) => completion.clone(),
        _ => return Err("turn does not contain a completion".into()),
    };
    let episode = host
        .strategy
        .episode
        .as_mut()
        .ok_or("fixture episode is absent")?;
    match &completion.outcome {
        EpisodeDeadlineOutcome::Armed { kind, deadline } => {
            episode.episode_fault_deadline = Some(ArmedEpisodeFaultDeadline {
                kind: *kind,
                deadline: deadline.clone(),
            });
        }
        EpisodeDeadlineOutcome::Cancelled { .. } => {
            episode.episode_fault_deadline = None;
            episode.last_observed_at_ms = completion.completed_at_ms;
        }
    }
    host.last_episode_projection
        .as_mut()
        .ok_or("fixture receipt is absent")?
        .actions
        .retain(|action| {
            !matches!(
                action,
                EpisodeAction::ArmFaultDeadline { .. } | EpisodeAction::CancelFaultDeadline { .. }
            )
        });
    host.last_episode_deadline_completion = Some(completion);
    Ok(())
}

#[test]
fn completion_is_durable_replayed_and_cleared_only_after_exact_host_ack()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("deadline-owner.json");
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        4,
        110,
        109,
        vec![EpisodeAction::ArmFaultDeadline {
            kind: EpisodeFaultKind::UnprotectedExposure,
            no_later_than_ms: 180,
        }],
    )?;
    let mut owner = ScalpingEpisodeDeadlineOwner::open_or_restore(&path, fixture.binding.clone())?;
    let persisted = owner.turn(&fixture.host, clock(140, root))?;
    let persisted_completion = match &persisted {
        EpisodeDeadlineOwnerTurn::Persisted(completion) => completion.clone(),
        _ => return Err("deadline was not persisted".into()),
    };
    assert!(
        ProjectionStore::new(&path)
            .load::<venue::runtime::EpisodeDeadlineOwnerCheckpoint>()?
            .and_then(|checkpoint| checkpoint.pending)
            .is_some()
    );

    let mut restored =
        ScalpingEpisodeDeadlineOwner::open_or_restore(&path, fixture.binding.clone())?;
    assert_eq!(
        restored.turn(&fixture.host, clock(141, root))?,
        EpisodeDeadlineOwnerTurn::PendingReplay(persisted_completion.clone())
    );
    acknowledge(&mut fixture.host, &persisted)?;
    assert_eq!(
        restored.turn(&fixture.host, clock(142, root))?,
        EpisodeDeadlineOwnerTurn::Acknowledged {
            completion_fact_id: persisted_completion.completion_fact_id,
        }
    );
    assert!(restored.checkpoint().pending.is_none());
    Ok(())
}

#[test]
fn same_private_newer_mark_can_advance_from_no_action_to_arm_then_cancel()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(&mut fixture, 1, 130, root, 4, 110, 109, Vec::new())?;
    let mut owner = ScalpingEpisodeDeadlineOwner::open_or_restore(
        directory.path().join("deadline-owner.json"),
        fixture.binding.clone(),
    )?;
    assert_eq!(
        owner.turn(&fixture.host, clock(131, root))?,
        EpisodeDeadlineOwnerTurn::NoDeadlineAction
    );

    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        5,
        120,
        119,
        vec![EpisodeAction::ArmFaultDeadline {
            kind: EpisodeFaultKind::UnprotectedExposure,
            no_later_than_ms: 180,
        }],
    )?;
    let armed = owner.turn(&fixture.host, clock(140, root))?;
    let deadline_id = match &armed {
        EpisodeDeadlineOwnerTurn::Persisted(completion) => match &completion.outcome {
            EpisodeDeadlineOutcome::Armed { deadline, .. } => deadline.deadline_id.clone(),
            _ => return Err("arm action produced a cancellation".into()),
        },
        _ => return Err("newer mark did not produce arm completion".into()),
    };
    acknowledge(&mut fixture.host, &armed)?;
    assert!(matches!(
        owner.turn(&fixture.host, clock(141, root))?,
        EpisodeDeadlineOwnerTurn::Acknowledged { .. }
    ));

    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        6,
        125,
        124,
        vec![EpisodeAction::CancelFaultDeadline { deadline_id }],
    )?;
    assert!(matches!(
        owner.turn(&fixture.host, clock(145, root))?,
        EpisodeDeadlineOwnerTurn::Persisted(completion)
            if matches!(completion.outcome, EpisodeDeadlineOutcome::Cancelled { .. })
    ));
    Ok(())
}

#[test]
fn same_private_and_mark_content_change_is_equivocation() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(&mut fixture, 1, 130, root, 4, 110, 109, Vec::new())?;
    let mut owner = ScalpingEpisodeDeadlineOwner::open_or_restore(
        directory.path().join("deadline-owner.json"),
        fixture.binding.clone(),
    )?;
    owner.turn(&fixture.host, clock(131, root))?;
    let mut changed = receipt(&fixture, 1, 130, root, 4, 110, 109, Vec::new())?;
    changed.mark_price = Price::new(Decimal::new(101, 0))?;
    resign_receipt(&mut changed)?;
    set_receipt(&mut fixture.host, changed);
    assert!(matches!(
        owner.turn(&fixture.host, clock(132, root)),
        Err(EpisodeDeadlineOwnerError::Equivocation)
    ));
    Ok(())
}

#[test]
fn higher_generations_cannot_hide_private_or_mark_time_regression()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(&mut fixture, 1, 130, root, 4, 120, 119, Vec::new())?;
    let mut owner = ScalpingEpisodeDeadlineOwner::open_or_restore(
        directory.path().join("deadline-owner.json"),
        fixture.binding.clone(),
    )?;
    owner.turn(&fixture.host, clock(131, root))?;

    let next_root = "private-readback:2:129:10";
    install_receipt(&mut fixture, 2, 129, next_root, 5, 121, 120, Vec::new())?;
    assert!(matches!(
        owner.turn(&fixture.host, clock(132, next_root)),
        Err(EpisodeDeadlineOwnerError::CursorRegression)
    ));

    let next_root = "private-readback:2:140:11";
    install_receipt(&mut fixture, 2, 140, next_root, 5, 119, 118, Vec::new())?;
    assert!(matches!(
        owner.turn(&fixture.host, clock(141, next_root)),
        Err(EpisodeDeadlineOwnerError::CursorRegression)
    ));
    Ok(())
}

#[test]
fn wrong_clock_multiple_deadline_actions_and_tampered_owner_state_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("deadline-owner.json");
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        4,
        110,
        109,
        vec![
            EpisodeAction::ArmFaultDeadline {
                kind: EpisodeFaultKind::UnprotectedExposure,
                no_later_than_ms: 180,
            },
            EpisodeAction::CancelFaultDeadline {
                deadline_id: "not-active".to_owned(),
            },
        ],
    )?;
    let mut owner = ScalpingEpisodeDeadlineOwner::open_or_restore(&path, fixture.binding.clone())?;
    assert!(matches!(
        owner.turn(&fixture.host, clock(140, "another-private-root")),
        Err(EpisodeDeadlineOwnerError::Clock)
    ));
    assert!(matches!(
        owner.turn(&fixture.host, clock(140, root)),
        Err(EpisodeDeadlineOwnerError::ActionBound)
    ));

    let mut checkpoint = owner.checkpoint();
    checkpoint.last_clock_ms = Some(999);
    ProjectionStore::new(&path).save(&checkpoint)?;
    assert!(matches!(
        ScalpingEpisodeDeadlineOwner::open_or_restore(&path, fixture.binding),
        Err(EpisodeDeadlineOwnerError::OwnerCheckpoint)
    ));
    Ok(())
}

fn completion_from_turn(
    turn: &EpisodeDeadlineOwnerTurn,
) -> Result<EpisodeDeadlineCompletion, Box<dyn std::error::Error>> {
    match turn {
        EpisodeDeadlineOwnerTurn::Persisted(completion)
        | EpisodeDeadlineOwnerTurn::PendingReplay(completion) => Ok(completion.clone()),
        _ => Err("deadline owner turn has no completion".into()),
    }
}

fn newer_private_gate(generation: u64, observed_at_ms: u64) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::None,
        },
        entry_ready: false,
        forwarded_private: Some(PrivateFacts {
            generation,
            observed_at_ms,
            root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:10"),
            safety: SafetyProjection {
                private_snapshot_ready: true,
                exposure: ExposureState::Open,
                execution_unknown: false,
                protection: ProtectionState::Complete,
                owner_conflict: false,
                risk_budget_available: true,
            },
            custody: CustodyStatus::Complete,
        }),
        control: None,
    }
}

fn newer_observation(
    fixture: &Fixture,
    generation: u64,
    observed_at_ms: u64,
) -> Result<EpisodeObservation, Box<dyn std::error::Error>> {
    let mut observation = EpisodeObservation {
        binding_digest: fixture.host.strategy.binding_digest.clone(),
        episode_id: fixture.episode_id.clone(),
        generation,
        observed_at_ms,
        private_root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:10"),
        observation_fact_id: String::new(),
        mark_symbol: fixture.binding.symbol.clone(),
        mark_generation: 4 + generation,
        mark_received_at_ms: observed_at_ms - 1,
        mark_exchange_time_ms: observed_at_ms - 2,
        mark_price: Price::new(Decimal::new(100, 0))?,
    };
    observation.observation_fact_id = episode_observation_fact_id(&observation)?;
    Ok(observation)
}

#[test]
fn restored_host_applies_durable_arm_before_private_and_stays_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let host_path = directory.path().join("host.json");
    let owner_path = directory.path().join("owner.json");
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        4,
        110,
        109,
        vec![EpisodeAction::ArmFaultDeadline {
            kind: EpisodeFaultKind::UnprotectedExposure,
            no_later_than_ms: 180,
        }],
    )?;
    ProjectionStore::new(&host_path).save(&fixture.host)?;
    let mut owner =
        ScalpingEpisodeDeadlineOwner::open_or_restore(&owner_path, fixture.binding.clone())?;
    let persisted = owner.turn(&fixture.host, clock(140, root))?;
    let completion = completion_from_turn(&persisted)?;
    drop(owner);

    let mut host = ScalpingShadowHost::open_or_restore(
        &host_path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.recover_pending_episode_deadline_completion(completion.clone())?;
    assert!(
        host.checkpoint()
            .strategy
            .episode
            .as_ref()
            .is_some_and(|episode| episode.episode_fault_deadline.is_some())
    );
    let mut owner =
        ScalpingEpisodeDeadlineOwner::open_or_restore(&owner_path, fixture.binding.clone())?;
    assert!(matches!(
        owner.turn(&host.checkpoint(), clock(141, root))?,
        EpisodeDeadlineOwnerTurn::Acknowledged { .. }
    ));
    assert!(matches!(
        host.on_episode_deadline_completion(completion),
        Err(ScalpingShadowHostError::RecoveryGeneration)
    ));
    assert!(matches!(
        host.on_episode_observation(newer_observation(&fixture, 2, 200)?),
        Err(ScalpingShadowHostError::RecoveryGeneration)
    ));
    assert_eq!(
        host.on_market(frame()?, 131, authorization(&fixture.binding), Vec::new())?
            .disposition,
        ShadowDisposition::RemainFenced
    );
    host.on_private_gate(&newer_private_gate(2, 200))?;
    host.on_episode_observation(newer_observation(&fixture, 2, 200)?)?;
    Ok(())
}

#[test]
fn restored_host_applies_durable_cancel_before_private_and_replays_exact_ack()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let host_path = directory.path().join("host.json");
    let owner_path = directory.path().join("owner.json");
    let mut fixture = fixture()?;
    let root = "private-readback:1:130:9";
    let deadline_id = "restored-cancel-deadline".to_owned();
    fixture
        .host
        .strategy
        .episode
        .as_mut()
        .ok_or("fixture episode is absent")?
        .episode_fault_deadline = Some(ArmedEpisodeFaultDeadline {
        kind: EpisodeFaultKind::UnprotectedExposure,
        deadline: SafetyDeadline {
            deadline_id: deadline_id.clone(),
            generation: 1,
            armed_at_ms: 130,
            expires_at_ms: 180,
        },
    });
    install_receipt(
        &mut fixture,
        1,
        130,
        root,
        4,
        110,
        109,
        vec![EpisodeAction::CancelFaultDeadline { deadline_id }],
    )?;
    ProjectionStore::new(&host_path).save(&fixture.host)?;
    let mut owner =
        ScalpingEpisodeDeadlineOwner::open_or_restore(&owner_path, fixture.binding.clone())?;
    let persisted = owner.turn(&fixture.host, clock(140, root))?;
    let completion = completion_from_turn(&persisted)?;

    let mut host = ScalpingShadowHost::open_or_restore(
        &host_path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.recover_pending_episode_deadline_completion(completion.clone())?;
    assert!(
        host.checkpoint()
            .strategy
            .episode
            .as_ref()
            .is_some_and(|episode| episode.episode_fault_deadline.is_none())
    );
    drop(host);
    let mut host = ScalpingShadowHost::open_or_restore(
        &host_path,
        fixture.binding.clone(),
        fixture.params.clone(),
    )?;
    host.recover_pending_episode_deadline_completion(completion)?;
    assert!(matches!(
        owner.turn(&host.checkpoint(), clock(141, root))?,
        EpisodeDeadlineOwnerTurn::Acknowledged { .. }
    ));
    assert_eq!(
        host.on_market(frame()?, 131, authorization(&fixture.binding), Vec::new())?
            .disposition,
        ShadowDisposition::RemainFenced
    );
    Ok(())
}

#[test]
fn restored_host_poisons_on_cross_root_late_or_actionless_completion()
-> Result<(), Box<dyn std::error::Error>> {
    for fault in ["cross-root", "late", "actionless"] {
        let directory = tempdir()?;
        let host_path = directory.path().join("host.json");
        let owner_path = directory.path().join("owner.json");
        let mut fixture = fixture()?;
        let root = "private-readback:1:130:9";
        install_receipt(
            &mut fixture,
            1,
            130,
            root,
            4,
            110,
            109,
            vec![EpisodeAction::ArmFaultDeadline {
                kind: EpisodeFaultKind::UnprotectedExposure,
                no_later_than_ms: 180,
            }],
        )?;
        let mut owner =
            ScalpingEpisodeDeadlineOwner::open_or_restore(&owner_path, fixture.binding.clone())?;
        let persisted = owner.turn(&fixture.host, clock(140, root))?;
        let mut completion = completion_from_turn(&persisted)?;
        match fault {
            "cross-root" => completion.private_root_cause_fact_id = "another-root".to_owned(),
            "late" => completion.completed_at_ms = 180,
            "actionless" => fixture
                .host
                .last_episode_projection
                .as_mut()
                .ok_or("fixture receipt is absent")?
                .actions
                .clear(),
            _ => return Err("unknown fixture fault".into()),
        }
        ProjectionStore::new(&host_path).save(&fixture.host)?;
        let mut host =
            ScalpingShadowHost::open_or_restore(&host_path, fixture.binding, fixture.params)?;
        assert!(matches!(
            host.recover_pending_episode_deadline_completion(completion.clone()),
            Err(ScalpingShadowHostError::Coordinator(_))
        ));
        assert!(matches!(
            host.recover_pending_episode_deadline_completion(completion),
            Err(ScalpingShadowHostError::Poisoned)
        ));
    }
    Ok(())
}
