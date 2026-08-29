use std::{collections::BTreeMap, fs, num::NonZeroUsize, path::Path};

use rust_decimal::Decimal;
use tempfile::tempdir;

use crate::{
    controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
    domain::{Amount, Asset, FieldState, MarkFunding, Price},
    exchange::binance::PublicStream,
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues,
        ScalpingPublicMarketSource, SourceCursor, TRADES_SOURCE,
    },
    market::{MarketSession, RawMarketRecorder, SessionState},
    runtime::{
        BoundRiskRevaluation, ControlDisposition, CustodyStatus, DeadlineClockObservation,
        EntryDisposition, LifecycleReport, PrivateEntryGateReport, PrivateFacts,
        PublicCaptureCompletion, PublicCaptureEffect, PublicCaptureEffectExecutor,
        PublicCaptureOutput, PublicCaptureTransportError, ScalpingCandidateEvidenceConfig,
        ScalpingCandidateEvidenceCoordinator, ScalpingPublicMarketWorker, ScalpingResidentRuntime,
        ScalpingShadowHost,
    },
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        CalibrationKey, CalibrationManifest, CalibrationSlice, CandidateCosts, CandidateEvidence,
        CandidatePreparation, ExposureState, FillSlice, OutcomeProbabilities, ProtectionState,
        ResearchCheckStatus, ResearchEvidence, ResearchSliceEvidence, RiskRevaluation, RiskUnit,
        SafetyProjection, ScalpingDecision, ScalpingParams, ScalpingStrategy, StrategyBinding,
        StrategyKind,
    },
};

use super::{
    DeadlinePreflight, ScalpingResidentSources, ScalpingResidentSourcesConfig,
    ScalpingResidentSourcesTurnReport,
};

const SNAPSHOT: &str = r#"{"lastUpdateId":10,"bids":[["100.0","10.0"]],"asks":[["101.0","10.0"]]}"#;
const DEPTH_BRIDGE: &str = r#"{"e":"depthUpdate","E":190,"T":190,"s":"BTCUSDT","U":11,"u":11,"pu":10,"st":1,"b":[["100.0","10.0"]],"a":[["101.0","10.0"]]}"#;
const MARK: &str = r#"{"e":"markPriceUpdate","E":190,"s":"BTCUSDT","p":"100.5","i":"100.4","r":"0.0001","T":400,"st":1}"#;

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "resident-sources".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "resident-sources:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn params(binding: &StrategyBinding) -> ScalpingParams {
    ScalpingParams::shadow(binding.risk_budget.clone())
}

fn source_config(root: &Path, binding: &StrategyBinding) -> ScalpingResidentSourcesConfig {
    ScalpingResidentSourcesConfig {
        artifacts_root: root.to_path_buf(),
        binding: binding.clone(),
        params: params(binding),
        mark_stale_after_ms: 65_000,
    }
}

fn worker(path: &Path) -> Result<ScalpingPublicMarketWorker, Box<dyn std::error::Error>> {
    let symbol: crate::domain::Symbol = "BTC/USDT".parse()?;
    let session = MarketSession::new(symbol.clone(), RawMarketRecorder::open(path.to_path_buf())?);
    let source = ScalpingPublicMarketSource::new(
        symbol,
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?;
    Ok(ScalpingPublicMarketWorker::new(session, source))
}

#[derive(Default)]
struct FakeExecutor {
    calls: Vec<PublicCaptureEffect>,
    fail_next: bool,
}

impl PublicCaptureEffectExecutor for FakeExecutor {
    fn execute_effect(
        &mut self,
        effect: PublicCaptureEffect,
        received_at_ms: u64,
    ) -> Result<PublicCaptureCompletion, PublicCaptureTransportError> {
        self.calls.push(effect);
        if std::mem::take(&mut self.fail_next) {
            return Err(PublicCaptureTransportError::NotConnected);
        }
        Ok(match effect {
            PublicCaptureEffect::Connect { stream } => {
                PublicCaptureCompletion::StreamConnected { stream }
            }
            PublicCaptureEffect::FetchDepthSnapshot { .. } => {
                PublicCaptureCompletion::DepthSnapshot {
                    received_at_ms,
                    payload: SNAPSHOT.to_owned(),
                }
            }
            PublicCaptureEffect::FetchClosedKlineBootstrap => {
                let bootstrap_received_at_ms = received_at_ms.max(23 * 60_000);
                let rows = (1_u64..=22)
                    .map(|sequence| {
                        serde_json::json!([
                            (sequence - 1) * 60_000,
                            "100",
                            "101",
                            "99",
                            "100",
                            "1",
                            sequence * 60_000 - 1
                        ])
                    })
                    .collect::<Vec<_>>();
                PublicCaptureCompletion::ClosedKlineBootstrap {
                    received_at_ms: bootstrap_received_at_ms,
                    payload: serde_json::to_string(&rows)
                        .map_err(|_| PublicCaptureTransportError::NotConnected)?,
                }
            }
            PublicCaptureEffect::Read {
                stream: PublicStream::DiffDepth,
            } if self
                .calls
                .iter()
                .filter(|call| {
                    matches!(
                        call,
                        PublicCaptureEffect::Read {
                            stream: PublicStream::DiffDepth
                        }
                    )
                })
                .count()
                == 1 =>
            {
                PublicCaptureCompletion::StreamFrame {
                    stream: PublicStream::DiffDepth,
                    received_at_ms,
                    payload: DEPTH_BRIDGE.to_owned(),
                }
            }
            PublicCaptureEffect::Read {
                stream: PublicStream::MarkFunding,
            } => PublicCaptureCompletion::StreamFrame {
                stream: PublicStream::MarkFunding,
                received_at_ms,
                payload: MARK.to_owned(),
            },
            PublicCaptureEffect::Read { stream } => PublicCaptureCompletion::StreamReady { stream },
        })
    }
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

fn authorization(binding: &StrategyBinding) -> crate::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "sources-control".to_owned(),
        idempotency_key: "sources-control-1".to_owned(),
        safety_deadline_ms: None,
        revision: 1,
    }
    .authorize(
        &ControlAuthority {
            generation: 1,
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
        generation: 1,
        watermark_ms: 100,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 1,
                        sequence: 10 + index as u64,
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

fn next_frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut value = frame()?;
    value.watermark_ms = 101;
    for cursor in value.cursors.values_mut() {
        cursor.sequence += 1;
        cursor.event_time_ms = 101;
    }
    Ok(value)
}

fn candidate_coordinator(
    root: &Path,
    binding: &StrategyBinding,
    preparation: &CandidatePreparation,
) -> Result<(ScalpingCandidateEvidenceCoordinator, std::path::PathBuf), Box<dyn std::error::Error>>
{
    let candidate = preparation.candidates.first().ok_or("candidate")?;
    let key = CalibrationKey {
        symbol: candidate.symbol.clone(),
        expert: candidate.expert,
        regime: preparation.market_regime,
        direction: candidate.direction,
        entry_style: candidate.entry_style,
    };
    let mut params = params(binding);
    let manifest = CalibrationManifest {
        schema_version: 1,
        release_id: binding.parameter_release_id.clone(),
        model_version: params.calibration_model_version.clone(),
        artifact_digest: String::new(),
        research: ResearchEvidence {
            schema_version: 1,
            dataset_digest: "1".repeat(64),
            preregistration_digest: "2".repeat(64),
            evidence_cursor_ms: preparation.watermark_ms,
            approved_for_live: true,
            slices: vec![ResearchSliceEvidence {
                key: key.clone(),
                sample_count: 10,
                after_cost_ev_lower_bps: Decimal::ONE,
                fill_calibration: ResearchCheckStatus::Passed,
                cost_calibration: ResearchCheckStatus::Passed,
                markout_calibration: ResearchCheckStatus::Passed,
                stress_budget: ResearchCheckStatus::Passed,
            }],
        },
        slices: vec![CalibrationSlice {
            key,
            release_id: binding.parameter_release_id.clone(),
            model_version: params.calibration_model_version.clone(),
            artifact_digest: String::new(),
            model_generation: 1,
            evidence_cursor_ms: preparation.watermark_ms,
            valid_from_ms: 1,
            valid_until_ms: preparation.valid_until_ms,
            sample_count: 10,
            live_approved: true,
            fill_distribution: vec![FillSlice {
                fill_ratio: Decimal::ONE,
                probability: Decimal::ONE,
            }],
            outcomes: OutcomeProbabilities {
                target: Decimal::new(7, 1),
                stop: Decimal::new(1, 1),
                other: Decimal::new(2, 1),
            },
            target_pnl_bps: Decimal::new(10, 0),
            stop_pnl_bps: -Decimal::new(5, 0),
            other_pnl_bps: Decimal::ZERO,
            nonfill_cancel_cost_bps: Decimal::ONE,
            opportunity_cost_bps: Decimal::ONE,
            ev_sigma_bps: Decimal::ONE,
        }],
    }
    .seal()?;
    params.calibration_model_digest = manifest.artifact_digest.clone();
    let calibration_path = root.join("calibration.json");
    let evidence_path = root.join("scalping_evidence.jsonl");
    fs::write(&calibration_path, serde_json::to_vec(&manifest)?)?;
    let coordinator = ScalpingCandidateEvidenceCoordinator::open(
        ScalpingCandidateEvidenceConfig {
            calibration_artifact_path: calibration_path,
            core_quote_receipt_path: root.join("core_quote_receipts.jsonl"),
            evidence_journal_path: evidence_path.clone(),
            checkpoint_path: root.join("scalping_candidate_evidence.json"),
            live_calibration: false,
        },
        binding.clone(),
        params,
    )?;
    Ok((coordinator, evidence_path))
}

fn host_with_persisted_preparation(
    path: &Path,
    binding: &StrategyBinding,
) -> Result<(ScalpingShadowHost, CandidatePreparation), Box<dyn std::error::Error>> {
    let mut host = ScalpingShadowHost::open_or_restore(path, binding.clone(), params(binding))?;
    let gate = PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Armed,
            control: ControlDisposition::None,
        },
        entry_ready: true,
        forwarded_private: Some(private(1, 100)),
        control: None,
    };
    host.on_private_gate(&gate)?;
    let output = host.on_market(frame()?, 100, authorization(binding), Vec::new())?;
    let preparation = output.preparation.ok_or("preparation")?;
    Ok((host, preparation))
}

fn arm_sources_for_market(
    sources: &mut ScalpingResidentSources,
    root: &Path,
    binding: &StrategyBinding,
    generation: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let control_path = root.join("controller.json");
    let control_store = crate::controller::InstanceControlStore::new(&control_path);
    let previous_revision = control_store.load()?.map(|record| record.revision);
    control_store.save(
        &InstanceControlRecord {
            schema_version: 1,
            binding: binding.clone(),
            target: ControlTarget::Running,
            command_id: format!("sources-running-{generation}"),
            idempotency_key: format!("sources-running-{generation}"),
            safety_deadline_ms: Some(1_000),
            revision: previous_revision.unwrap_or(0).saturating_add(1),
        },
        previous_revision,
    )?;
    let mut controller = crate::controller::ScalpingControllerSource::open(
        &control_path,
        root.join("controller-source.json"),
        binding.clone(),
    )?;
    let authority = ControlAuthority {
        generation,
        parameter_release_id: binding.parameter_release_id.clone(),
        private_snapshot_ready: true,
        execution_unknown: false,
        protection_complete: true,
        owner_conflict: false,
    };
    let mut ready = private_report(private(generation, 100));
    ready.entry_ready = true;
    ready.lifecycle.entry = EntryDisposition::Armed;
    sources.drive_control_private(
        Some(controller.observe(Some(&authority), 100)?),
        Some(ready),
        None,
    )?;
    Ok(())
}

fn reopen_delivery_sources(
    root: &Path,
    binding: &StrategyBinding,
    preparation: &CandidatePreparation,
) -> Result<ScalpingResidentSources, Box<dyn std::error::Error>> {
    let host = ScalpingShadowHost::open_or_restore(
        root.join("host.json"),
        binding.clone(),
        params(binding),
    )?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&root.join("public-reopened.ndjson"))?,
        source_config(root, binding),
    )?;
    let (coordinator, _) = candidate_coordinator(root, binding, preparation)?;
    sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut sources, root, binding, 3)?;
    Ok(sources)
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

fn active_host(
    path: &Path,
    binding: &StrategyBinding,
) -> Result<ScalpingShadowHost, Box<dyn std::error::Error>> {
    let params = params(binding);
    let mut strategy = ScalpingStrategy::new(binding.clone(), params.clone())?;
    let preparation =
        match strategy.evaluate_at(&frame()?, &safety(), &authorization(binding), 100)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            _ => return Err("preparation missing".into()),
        };
    strategy.admit(&[evidence(&preparation)], 100)?;
    let checkpoint = crate::runtime::ScalpingCoordinatorCheckpoint {
        schema_version: crate::runtime::SCALPING_COORDINATOR_SCHEMA_VERSION,
        strategy: strategy.checkpoint(),
        last_private_generation: Some(1),
        last_private_observed_at_ms: Some(100),
        last_private_root_cause_fact_id: Some("private-readback:1:100:1".to_owned()),
        last_risk_cursor_sequence: None,
        last_risk_proof_id: None,
        risk_control_target: None,
        control_target: ControlTarget::Running,
        last_episode_projection: None,
        last_episode_deadline_completion: None,
        last_market_delivery: None,
        control_stopped: false,
    };
    crate::storage::ProjectionStore::new(path.to_path_buf()).save(&checkpoint)?;
    Ok(ScalpingShadowHost::open_or_restore(
        path,
        binding.clone(),
        params,
    )?)
}

fn private(generation: u64, observed_at_ms: u64) -> PrivateFacts {
    PrivateFacts {
        generation,
        observed_at_ms,
        root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:1"),
        safety: safety(),
        custody: CustodyStatus::Complete,
    }
}

fn unprotected_private(generation: u64, observed_at_ms: u64) -> PrivateFacts {
    let mut private = private(generation, observed_at_ms);
    private.safety.exposure = ExposureState::Open;
    private.safety.protection = ProtectionState::Gap;
    private.custody = CustodyStatus::Incomplete;
    private
}

fn private_report(private: PrivateFacts) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::None,
        },
        entry_ready: false,
        forwarded_private: Some(private),
        control: None,
    }
}

fn bound_risk(
    binding: &StrategyBinding,
    cursor_sequence: u64,
    proof_id: &str,
    generation: u64,
    through_ms: u64,
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
            valuation_generation: generation,
        },
        proof: RiskRevaluation {
            proof_id: proof_id.to_owned(),
            target_generation: generation,
            risk_unit,
            window_start_ms: 0,
            complete_through_ms: through_ms,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        },
        cursor_sequence,
    })
}

fn mark(received_at_ms: u64) -> Result<MarkFunding, Box<dyn std::error::Error>> {
    Ok(MarkFunding {
        symbol: "BTC/USDT".parse()?,
        generation: 1,
        received_at_ms,
        exchange_time_ms: received_at_ms.saturating_sub(1),
        next_funding_time_ms: received_at_ms + 1_000,
        mark_price: Price::new(Decimal::new(100, 0))?,
        index_price: Price::new(Decimal::new(100, 0))?,
        funding_rate: Decimal::ZERO,
        estimated_settle_price: FieldState::Missing,
        predicted_funding_rate: FieldState::Missing,
        unknown_reason: None,
    })
}

fn drive_to_mark(
    sources: &mut ScalpingResidentSources,
    executor: &mut FakeExecutor,
    now_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..15 {
        sources.drive_public_once(executor, now_ms, None)?;
    }
    Ok(())
}

#[test]
fn transport_error_reports_backoff_and_clears_market_work() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let binding = binding()?;
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params(&binding),
    )?;
    let resident = ScalpingResidentRuntime::new(host);
    let public = worker(&directory.path().join("public.ndjson"))?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        resident,
        public,
        source_config(directory.path(), &binding),
    )?;
    let mut executor = FakeExecutor {
        fail_next: true,
        ..FakeExecutor::default()
    };

    let report = sources.drive_public_once(&mut executor, 100, None)?;

    assert!(report.public_fault_backoff);
    assert_eq!(sources.status().public_session_state, SessionState::Backoff);
    assert!(!sources.status().pending_preparation);
    assert_eq!(executor.calls.len(), 1);
    Ok(())
}

#[test]
fn pending_mark_survives_crash_and_waits_for_strictly_new_private()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host_path = directory.path().join("host.json");
    let host = active_host(&host_path, &binding)?;
    let resident = ScalpingResidentRuntime::new(host);
    let public_path = directory.path().join("public.ndjson");
    let public = worker(&public_path)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        resident,
        public,
        source_config(directory.path(), &binding),
    )?;
    sources.drive_control_private(None, Some(private_report(private(2, 200))), None)?;
    let saved_host = fs::read(&host_path)?;
    fs::remove_file(&host_path)?;
    fs::create_dir(&host_path)?;
    let mut executor = FakeExecutor::default();
    let error = match drive_to_mark(&mut sources, &mut executor, 200) {
        Err(error) => error,
        Ok(()) => return Err("host save unexpectedly succeeded".into()),
    };
    assert!(error.to_string().contains("storage"));
    drop(sources);
    fs::remove_dir(&host_path)?;
    fs::write(&host_path, saved_host)?;

    let restored_host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params(&binding))?;
    let restored_worker = ScalpingPublicMarketWorker::open_recovered(
        binding.symbol.clone(),
        &public_path,
        ScalpingPublicMarketSource::new(
            binding.symbol.clone(),
            "scalping-shadow-v1",
            "0".repeat(64),
            65_000,
            NonZeroUsize::new(2_048).ok_or("history")?,
        )?,
    )?;
    let mut restored = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(restored_host),
        restored_worker,
        source_config(directory.path(), &binding),
    )?;
    assert!(restored.status().awaiting_private_recovery);
    assert!(restored.status().pending_mark);
    assert!(!restored.status().latest_private);

    let report =
        restored.drive_control_private(None, Some(private_report(private(3, 300))), None)?;
    assert!(report.episode_observation_applied);
    assert!(!restored.status().pending_mark);
    assert!(!restored.status().awaiting_private_recovery);
    assert_eq!(
        restored
            .resident()
            .host()
            .checkpoint()
            .last_episode_projection
            .as_ref()
            .ok_or("projection")?
            .generation,
        3
    );
    Ok(())
}

#[test]
fn each_public_turn_executes_one_effect_and_restart_advances_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host_path = directory.path().join("host.json");
    let host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params(&binding))?;
    let public_dir = directory.path().join("public");
    fs::create_dir(&public_dir)?;
    let public_path = public_dir.join("raw_market.jsonl");
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&public_path)?,
        source_config(directory.path(), &binding),
    )?;
    let mut executor = FakeExecutor::default();
    for expected in 1..=5 {
        sources.drive_public_once(&mut executor, 100, None)?;
        assert_eq!(executor.calls.len(), expected);
    }
    assert_eq!(sources.status().public_generation, 1);
    drop(sources);

    let mut restored = ScalpingResidentSources::open_recovered(
        ScalpingResidentRuntime::new(ScalpingShadowHost::open_or_restore(
            &host_path,
            binding.clone(),
            params(&binding),
        )?),
        source_config(directory.path(), &binding),
    )?;
    assert_eq!(restored.status().public_generation, 2);
    assert_eq!(restored.drain_pending_deadline()?, DeadlinePreflight::Clear);
    Ok(())
}

#[test]
fn pending_mark_waits_for_future_private_and_stale_crash_mark_is_dropped()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host_path = directory.path().join("host.json");
    let host = active_host(&host_path, &binding)?;
    let mut config = source_config(directory.path(), &binding);
    config.mark_stale_after_ms = 50;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        config,
    )?;
    sources.drive_control_private(None, Some(private_report(private(2, 200))), None)?;

    sources.checkpoint.pending_mark = Some(mark(250)?);
    sources.persist()?;
    let early = sources.drive_control_private(None, Some(private_report(private(3, 240))), None)?;
    assert!(!early.episode_observation_applied);
    assert!(sources.status().pending_mark);

    let fresh = sources.drive_control_private(None, Some(private_report(private(4, 260))), None)?;
    assert!(fresh.episode_observation_applied);
    assert!(!sources.status().pending_mark);

    sources.checkpoint.pending_mark = Some(mark(300)?);
    sources.persist()?;
    let stale = sources.drive_control_private(None, Some(private_report(private(5, 351))), None)?;
    assert!(!stale.episode_observation_applied);
    assert!(!sources.status().pending_mark);
    Ok(())
}

#[test]
fn owner_risk_runs_after_private_phase_and_before_post_private_work()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params(&binding),
    )?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;

    sources.drive_control_private_phase(None, Some(private_report(private(1, 100))))?;
    assert!(
        sources
            .resident()
            .host()
            .checkpoint()
            .last_risk_proof_id
            .is_none()
    );
    let applied = sources.drive_applied_risk(bound_risk(&binding, 1, "risk-proof-1", 1, 200)?)?;
    assert_eq!(applied.receipt.proof_id, "risk-proof-1");
    assert_eq!(
        sources.applied_risk_last_ack_proof_id(),
        Some("risk-proof-1")
    );
    sources.drive_episode_deadline(None)?;
    Ok(())
}

#[test]
fn durable_deadline_pending_blocks_public_until_consecutive_preflight_ack()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host_path = directory.path().join("host.json");
    let public_path = directory.path().join("public.ndjson");
    let host = active_host(&host_path, &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&public_path)?,
        source_config(directory.path(), &binding),
    )?;
    let private = unprotected_private(2, 200);
    let root = private.root_cause_fact_id.clone();
    sources.drive_control_private(None, Some(private_report(private)), None)?;
    let mut executor = FakeExecutor::default();
    for _ in 0..14 {
        sources.drive_public_once(&mut executor, 200, None)?;
    }
    let clock = DeadlineClockObservation {
        now_ms: 200,
        root_cause_fact_id: root,
    };
    let mark = sources.drive_public_once(&mut executor, 200, Some(&clock))?;
    assert!(mark.deadline_persisted);
    assert!(sources.status().deadline_pending);
    assert!(matches!(
        sources.drive_public_once(&mut executor, 201, Some(&clock)),
        Err(super::ScalpingResidentSourcesError::PendingPriority)
    ));
    assert!(matches!(
        sources.drive_applied_risk(bound_risk(&binding, 1, "risk-proof-1", 1, 200)?),
        Err(super::ScalpingResidentSourcesError::PendingPriority)
    ));
    assert!(sources.applied_risk_last_ack_proof_id().is_none());
    let calls_before_restart = executor.calls.len();
    drop(sources);

    let restored_host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params(&binding))?;
    let restored_worker = ScalpingPublicMarketWorker::open_recovered(
        binding.symbol.clone(),
        &public_path,
        ScalpingPublicMarketSource::new(
            binding.symbol.clone(),
            "scalping-shadow-v1",
            "0".repeat(64),
            65_000,
            NonZeroUsize::new(2_048).ok_or("history")?,
        )?,
    )?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(restored_host),
        restored_worker,
        source_config(directory.path(), &binding),
    )?;

    assert!(matches!(
        sources.drain_pending_deadline()?,
        DeadlinePreflight::AppliedAndAcknowledged { .. }
    ));
    assert_eq!(executor.calls.len(), calls_before_restart);
    assert!(sources.status().awaiting_private_recovery);
    assert_eq!(sources.drain_pending_deadline()?, DeadlinePreflight::Clear);
    assert!(!sources.status().deadline_pending);
    Ok(())
}

#[test]
fn direct_resident_frames_admit_without_external_evidence() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let binding = binding()?;
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params(&binding),
    )?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let control_path = directory.path().join("controller.json");
    crate::controller::InstanceControlStore::new(&control_path).save(
        &InstanceControlRecord {
            schema_version: 1,
            binding: binding.clone(),
            target: ControlTarget::Running,
            command_id: "sources-running".to_owned(),
            idempotency_key: "sources-running-1".to_owned(),
            safety_deadline_ms: Some(1_000),
            revision: 1,
        },
        None,
    )?;
    let mut controller = crate::controller::ScalpingControllerSource::open(
        &control_path,
        directory.path().join("controller-source.json"),
        binding.clone(),
    )?;
    let authority = ControlAuthority {
        generation: 1,
        parameter_release_id: binding.parameter_release_id.clone(),
        private_snapshot_ready: true,
        execution_unknown: false,
        protection_complete: true,
        owner_conflict: false,
    };
    let mut ready = private_report(private(1, 100));
    ready.entry_ready = true;
    ready.lifecycle.entry = EntryDisposition::Armed;
    sources.drive_control_private(
        Some(controller.observe(Some(&authority), 100)?),
        Some(ready),
        None,
    )?;

    for sequence in 1..=2 {
        let mut feature_frame = frame()?;
        feature_frame.watermark_ms = 100 + sequence;
        for cursor in feature_frame.cursors.values_mut() {
            cursor.sequence += sequence;
            cursor.event_time_ms = 100 + sequence;
        }
        let mut report = ScalpingResidentSourcesTurnReport::default();
        sources.route_public_output(
            PublicCaptureOutput {
                event: crate::indicator::RecordedPublicEvent {
                    capture_sequence: sequence,
                    received_at_ms: 100 + sequence,
                    event: crate::domain::MarketEvent::Bar(crate::domain::PublicBar {
                        symbol: binding.symbol.clone(),
                        generation: 1,
                        received_at_ms: 100 + sequence,
                        sequence,
                        open_time_ms: 1,
                        close_time_ms: 100,
                        interval_ms: 100,
                        open: Price::new(Decimal::new(99, 0))?,
                        high: Price::new(Decimal::new(101, 0))?,
                        low: Price::new(Decimal::new(98, 0))?,
                        close: Price::new(Decimal::new(100, 0))?,
                    }),
                },
                generation: 1,
                state: FeatureState::Ready,
                frame: Some(feature_frame),
            },
            100 + sequence,
            None,
            &mut report,
        )?;
        assert_eq!(report.market_evidence_count, 0);
        assert!(
            sources
                .resident()
                .host()
                .checkpoint()
                .strategy
                .episode
                .is_some()
        );
    }
    Ok(())
}

#[test]
fn restored_host_preparation_backfills_candidate_coordinator_for_n_plus_one()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let host_path = directory.path().join("host.json");
    let (host, preparation) = host_with_persisted_preparation(&host_path, &binding)?;
    let sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    drop(sources);

    let restored_host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params(&binding))?;
    let mut restored = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(restored_host),
        worker(&directory.path().join("public-reopened.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let (coordinator, _evidence_path) =
        candidate_coordinator(directory.path(), &binding, &preparation)?;
    restored.attach_candidate_evidence(coordinator)?;

    assert_eq!(
        restored
            .candidate_evidence
            .as_ref()
            .and_then(|coordinator| coordinator.checkpoint().pending_preparation.as_ref()),
        Some(&preparation)
    );
    Ok(())
}

#[test]
fn retryable_candidate_source_failure_replays_the_same_durable_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&directory.path().join("host.json"), &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let (coordinator, evidence_path) =
        candidate_coordinator(directory.path(), &binding, &preparation)?;
    sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut sources, directory.path(), &binding, 2)?;

    let retry_frame = next_frame()?;
    sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: retry_frame,
        decision_at_ms: 101,
        evidence: Vec::new(),
        phase: super::PendingPublicFramePhase::Assemble,
        authority: sources.current_public_delivery_authority()?,
    });
    sources.persist()?;
    fs::write(&evidence_path, b"")?;
    fs::remove_file(&evidence_path)?;
    fs::create_dir(&evidence_path)?;

    let mut first = ScalpingResidentSourcesTurnReport::default();
    sources.retry_pending_public_frame(&mut first)?;
    assert!(first.candidate_evidence_retry_pending);
    assert!(sources.checkpoint.pending_public_frame.is_some());
    assert!(
        sources
            .resident()
            .host()
            .checkpoint()
            .strategy
            .episode
            .is_none()
    );

    fs::remove_dir(&evidence_path)?;
    fs::write(&evidence_path, b"")?;
    let mut second = ScalpingResidentSourcesTurnReport::default();
    sources.retry_pending_public_frame(&mut second)?;
    assert!(!second.candidate_evidence_retry_pending);
    assert!(sources.checkpoint.pending_public_frame.is_none());
    assert_eq!(
        sources
            .candidate_evidence
            .as_ref()
            .and_then(|coordinator| coordinator.checkpoint().last_frame.as_ref())
            .map(|frame| frame.watermark_ms),
        Some(101)
    );
    Ok(())
}

#[test]
fn cross_binding_candidate_coordinator_cannot_attach() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&directory.path().join("host.json"), &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let mut other_binding = binding.clone();
    other_binding.account = "other-account".to_owned();
    let other_root = directory.path().join("other");
    fs::create_dir(&other_root)?;
    let (coordinator, _evidence_path) =
        candidate_coordinator(&other_root, &other_binding, &preparation)?;

    assert!(matches!(
        sources.attach_candidate_evidence(coordinator),
        Err(super::ScalpingResidentSourcesError::CandidateEvidenceBinding)
    ));
    Ok(())
}

#[test]
fn restart_after_assemble_fences_delivery_when_private_authority_advanced()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&directory.path().join("host.json"), &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let (coordinator, _) = candidate_coordinator(directory.path(), &binding, &preparation)?;
    sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut sources, directory.path(), &binding, 2)?;

    let delivered = next_frame()?;
    sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: delivered.clone(),
        decision_at_ms: 101,
        evidence: Vec::new(),
        phase: super::PendingPublicFramePhase::Assemble,
        authority: sources.current_public_delivery_authority()?,
    });
    sources.persist()?;
    let assembled = sources
        .candidate_evidence
        .as_mut()
        .ok_or("candidate")?
        .assemble(delivered.clone(), 101)?;
    assert!(assembled.evidence.is_empty());
    drop(sources);

    let mut restored = reopen_delivery_sources(directory.path(), &binding, &preparation)?;
    let mut report = ScalpingResidentSourcesTurnReport::default();
    assert!(matches!(
        restored.retry_pending_public_frame(&mut report),
        Err(super::ScalpingResidentSourcesError::PendingDeliveryAuthority)
    ));

    assert!(restored.checkpoint.pending_public_frame.is_some());
    assert!(
        !restored
            .candidate_evidence
            .as_ref()
            .ok_or("candidate")?
            .is_fenced()
    );
    assert_eq!(
        restored
            .resident()
            .host()
            .checkpoint()
            .last_market_delivery
            .as_ref()
            .map(|receipt| receipt.watermark_ms),
        Some(100)
    );
    Ok(())
}

#[test]
fn restart_after_delivery_phase_fences_when_private_authority_advanced()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&directory.path().join("host.json"), &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let (coordinator, _) = candidate_coordinator(directory.path(), &binding, &preparation)?;
    sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut sources, directory.path(), &binding, 2)?;

    let delivered = next_frame()?;
    let market = sources
        .candidate_evidence
        .as_mut()
        .ok_or("candidate")?
        .assemble(delivered.clone(), 101)?;
    sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: delivered,
        decision_at_ms: 101,
        evidence: market.evidence,
        phase: super::PendingPublicFramePhase::Deliver,
        authority: sources.current_public_delivery_authority()?,
    });
    sources.persist()?;
    drop(sources);

    let mut restored = reopen_delivery_sources(directory.path(), &binding, &preparation)?;
    let mut report = ScalpingResidentSourcesTurnReport::default();
    assert!(matches!(
        restored.retry_pending_public_frame(&mut report),
        Err(super::ScalpingResidentSourcesError::PendingDeliveryAuthority)
    ));

    assert!(restored.checkpoint.pending_public_frame.is_some());
    assert!(
        !restored
            .candidate_evidence
            .as_ref()
            .ok_or("candidate")?
            .is_fenced()
    );
    Ok(())
}

#[test]
fn restart_after_host_delivery_acknowledges_source_without_repeating_host()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&directory.path().join("host.json"), &binding)?;
    let mut sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&directory.path().join("public.ndjson"))?,
        source_config(directory.path(), &binding),
    )?;
    let (coordinator, _) = candidate_coordinator(directory.path(), &binding, &preparation)?;
    sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut sources, directory.path(), &binding, 2)?;

    let delivered = next_frame()?;
    let market = sources
        .candidate_evidence
        .as_mut()
        .ok_or("candidate")?
        .assemble(delivered.clone(), 101)?;
    sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: delivered,
        decision_at_ms: 101,
        evidence: market.evidence.clone(),
        phase: super::PendingPublicFramePhase::Deliver,
        authority: sources.current_public_delivery_authority()?,
    });
    sources.persist()?;
    let mut before_crash = ScalpingResidentSourcesTurnReport::default();
    sources.drive_market(market, &mut before_crash)?;
    let receipt = sources
        .resident()
        .host()
        .checkpoint()
        .last_market_delivery
        .clone()
        .ok_or("receipt")?;
    drop(sources);

    let mut restored = reopen_delivery_sources(directory.path(), &binding, &preparation)?;
    let mut report = ScalpingResidentSourcesTurnReport::default();
    restored.retry_pending_public_frame(&mut report)?;

    assert!(restored.checkpoint.pending_public_frame.is_none());
    assert_eq!(
        restored.resident().host().checkpoint().last_market_delivery,
        Some(receipt)
    );
    assert!(
        !restored
            .candidate_evidence
            .as_ref()
            .ok_or("candidate")?
            .is_fenced()
    );
    Ok(())
}

#[test]
fn pending_delivery_is_fenced_when_private_or_applied_risk_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let private_directory = tempdir()?;
    let binding = binding()?;
    let (host, preparation) =
        host_with_persisted_preparation(&private_directory.path().join("host.json"), &binding)?;
    let mut private_sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&private_directory.path().join("public.ndjson"))?,
        source_config(private_directory.path(), &binding),
    )?;
    let (coordinator, _) = candidate_coordinator(private_directory.path(), &binding, &preparation)?;
    private_sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut private_sources, private_directory.path(), &binding, 2)?;
    private_sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: next_frame()?,
        decision_at_ms: 101,
        evidence: Vec::new(),
        phase: super::PendingPublicFramePhase::Deliver,
        authority: private_sources.current_public_delivery_authority()?,
    });
    private_sources.persist()?;

    arm_sources_for_market(&mut private_sources, private_directory.path(), &binding, 3)?;
    assert!(matches!(
        private_sources
            .retry_pending_public_frame(&mut ScalpingResidentSourcesTurnReport::default()),
        Err(super::ScalpingResidentSourcesError::PendingDeliveryAuthority)
    ));
    assert!(private_sources.checkpoint.pending_public_frame.is_some());
    assert_ne!(
        private_sources
            .resident()
            .host()
            .checkpoint()
            .last_market_delivery
            .as_ref()
            .map(|receipt| receipt.watermark_ms),
        Some(101)
    );

    let risk_directory = tempdir()?;
    let (host, preparation) =
        host_with_persisted_preparation(&risk_directory.path().join("host.json"), &binding)?;
    let mut risk_sources = ScalpingResidentSources::open_with_worker(
        ScalpingResidentRuntime::new(host),
        worker(&risk_directory.path().join("public.ndjson"))?,
        source_config(risk_directory.path(), &binding),
    )?;
    let (coordinator, _) = candidate_coordinator(risk_directory.path(), &binding, &preparation)?;
    risk_sources.attach_candidate_evidence(coordinator)?;
    arm_sources_for_market(&mut risk_sources, risk_directory.path(), &binding, 2)?;
    risk_sources.checkpoint.pending_public_frame = Some(super::PendingPublicFrame {
        frame: next_frame()?,
        decision_at_ms: 101,
        evidence: Vec::new(),
        phase: super::PendingPublicFramePhase::Deliver,
        authority: risk_sources.current_public_delivery_authority()?,
    });
    risk_sources.persist()?;

    risk_sources.drive_applied_risk(bound_risk(&binding, 1, "risk-proof-1", 1, 200)?)?;
    assert!(matches!(
        risk_sources.retry_pending_public_frame(&mut ScalpingResidentSourcesTurnReport::default()),
        Err(super::ScalpingResidentSourcesError::PendingDeliveryAuthority)
    ));
    assert!(risk_sources.checkpoint.pending_public_frame.is_some());
    assert_ne!(
        risk_sources
            .resident()
            .host()
            .checkpoint()
            .last_market_delivery
            .as_ref()
            .map(|receipt| receipt.watermark_ms),
        Some(101)
    );
    Ok(())
}
