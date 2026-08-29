use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset, Price},
    execution::{
        SCALPING_ENTRY_QUOTE_SCHEMA_VERSION, ScalpingAdmissionFacts, ScalpingBoundExposure,
        ScalpingBoundLimits, ScalpingBoundQuoteAmount, ScalpingBoundRiskLimit, ScalpingEntryQuote,
        ScalpingPrivateAdmission, ScalpingQuoteAuthority, scalping_entry_quote_digest,
    },
    indicator::{FeatureFrame, FeatureState, FeatureValues, SourceCursor},
    runtime::{
        AppliedRiskReceipt, BoundRiskRevaluation, SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION,
        ScalpingCandidateEvidenceConfig, ScalpingCandidateEvidenceCoordinator,
        ScalpingCoreQuoteReceipt, ScalpingCoreQuoteReceiptJournal,
    },
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        CalibrationKey, CalibrationManifest, CalibrationSlice, CandidateEvidenceBundle,
        CandidatePreparation, Direction, EntryStyle, ExitTemplate, Expert, ExposureState,
        FillSlice, MarketRegime, OutcomeProbabilities, ProtectionState, ResearchCheckStatus,
        ResearchEvidence, ResearchSliceEvidence, RiskLimit, RiskPlan, RiskRevaluation, RiskUnit,
        ScalpingParams, SemanticIntent, SemanticPurpose, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "candidate-evidence".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio-margin".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-release-v1".to_owned(),
        owner_scope: "candidate-evidence:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(10, 0)),
    })
}

fn binding_digest(binding: &StrategyBinding) -> String {
    let mut digest = Sha256::new();
    for field in [
        b"scalping".as_slice(),
        binding.strategy_instance_id.as_bytes(),
        binding.run_id.as_bytes(),
        binding.exchange.as_bytes(),
        binding.account.as_bytes(),
        binding.symbol.to_string().as_bytes(),
        binding.parameter_release_id.as_bytes(),
        binding.owner_scope.as_bytes(),
        binding.risk_budget.asset.as_str().as_bytes(),
        binding.risk_budget.value.normalize().to_string().as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("{:x}", digest.finalize())
}

fn candidate_and_preparation(
    binding: &StrategyBinding,
) -> Result<(SemanticIntent, CandidatePreparation), Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    let quote = Amount::new("USDT".parse::<Asset>()?, Decimal::new(8, 0));
    let candidate = SemanticIntent {
        intent_id: "candidate-1".to_owned(),
        symbol: binding.symbol.clone(),
        direction: Direction::Long,
        purpose: SemanticPurpose::Entry,
        expert: Expert::RangeFade,
        entry_style: EntryStyle::PassiveMaker,
        exit_template: ExitTemplate::FairValue,
        attempt_cap: 2,
        max_reprices: 1,
        risk_plan: RiskPlan {
            risk_per_episode: RiskLimit::new(unit.clone(), Decimal::new(5, 0)),
            quote_cap: quote.clone(),
            max_episode_loss: RiskLimit::new(unit.clone(), Decimal::new(8, 0)),
        },
        target_quote: quote,
        reference_price: Price::new(Decimal::new(100, 0))?,
        max_slippage_bps: Decimal::ONE,
        valid_until_ms: 500,
        entry_ttl_ms: 100,
        hard_stop_distance_bps: Decimal::new(20, 0),
        target_distance_bps: Decimal::new(10, 0),
        max_hold_ms: 10_000,
        max_unprotected_ms: 1_000,
        requires_server_protection: true,
        opportunity_key: "range".to_owned(),
        breakout_cursor: None,
        idempotency_seed: "seed".to_owned(),
    };
    let preparation = CandidatePreparation {
        preparation_id: "preparation-1".to_owned(),
        binding_digest: binding_digest(binding),
        controller_revision: 3,
        authority_generation: 7,
        market_regime: MarketRegime::Range,
        frame_generation: 1,
        watermark_ms: 100,
        valid_until_ms: 450,
        candidates: vec![candidate.clone()],
    };
    Ok((candidate, preparation))
}

fn bound_risk(unit: &RiskUnit, value: i64, generation: u64) -> ScalpingBoundRiskLimit {
    ScalpingBoundRiskLimit {
        limit: RiskLimit::new(unit.clone(), Decimal::new(value, 0)),
        generation,
    }
}

fn zero_exposure(generation: u64) -> ScalpingBoundExposure {
    ScalpingBoundExposure {
        value: Decimal::ZERO,
        unit: "risk".to_owned(),
        generation,
    }
}

fn quote_receipt(
    binding: &StrategyBinding,
    candidate: &SemanticIntent,
    preparation: &CandidatePreparation,
) -> Result<ScalpingCoreQuoteReceipt, Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    let digest = binding_digest(binding);
    let private = ScalpingPrivateAdmission {
        fact_id: "private-readback:7:100:1".to_owned(),
        generation: 7,
        observed_at_ms: 100,
        safety: venue::strategy::scalping::SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Flat,
            execution_unknown: false,
            protection: ProtectionState::Complete,
            owner_conflict: false,
            risk_budget_available: true,
        },
    };
    let limits = ScalpingBoundLimits {
        risk_per_episode: bound_risk(&unit, 5, 7),
        quote_cap: ScalpingBoundQuoteAmount {
            amount: Amount::new("USDT".parse::<Asset>()?, Decimal::new(6, 0)),
            generation: 7,
        },
        max_episode_loss: bound_risk(&unit, 8, 7),
        worst_loss_at_quote_cap: bound_risk(&unit, 4, 7),
    };
    let quote = ScalpingEntryQuote {
        schema_version: SCALPING_ENTRY_QUOTE_SCHEMA_VERSION,
        quote_id: "quote-1".to_owned(),
        quote_release_digest: "b".repeat(64),
        binding_digest: digest,
        symbol: candidate.symbol.clone(),
        direction: candidate.direction,
        entry_style: candidate.entry_style,
        target_quote: limits.quote_cap.amount.clone(),
        bound_limits_generation: 7,
        generation: 13,
        capability_generation: 7,
        valid_until_ms: 420,
        admission: ScalpingAdmissionFacts {
            fact_id: private.fact_id.clone(),
            generation: private.generation,
            observed_at_ms: private.observed_at_ms,
            private_snapshot_ready: true,
            execution_unknown: false,
            owner_conflict: false,
            entry_terminal: true,
            residual_protection: zero_exposure(7),
            protection_gap: zero_exposure(7),
            open_permission_generation: 7,
        },
        maker_fee_bps: Decimal::new(1, 1),
        taker_fee_bps: Decimal::new(3, 1),
        spread_cross_bps: Decimal::new(2, 1),
        entry_slippage_impact_bps: Decimal::new(4, 1),
        urgent_exit_spread_cross_bps: Decimal::new(5, 1),
        urgent_exit_slippage_impact_bps: Decimal::new(6, 1),
        funding_bps: Decimal::new(7, 1),
        price_tick: Price::new(Decimal::new(1, 2))?,
        max_executable_price: candidate.reference_price,
        worst_loss: bound_risk(&unit, 4, 7),
    };
    let quote_authority = ScalpingQuoteAuthority {
        quote_id: quote.quote_id.clone(),
        quote_content_digest: scalping_entry_quote_digest(&quote)?,
        quote_release_digest: quote.quote_release_digest.clone(),
        quote_generation: quote.generation,
        capability_generation: quote.capability_generation,
        max_funding_abs_bps: Decimal::ONE,
        max_private_stale_ms: 25,
    };
    Ok(ScalpingCoreQuoteReceipt {
        schema_version: SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION,
        binding: binding.clone(),
        preparation_id: preparation.preparation_id.clone(),
        candidate_id: candidate.intent_id.clone(),
        candidate_digest: venue::runtime::scalping_candidate_digest(candidate)?,
        preparation: preparation.clone(),
        candidate: candidate.clone(),
        limits,
        private,
        quote_authority,
        quote,
        issued_at_ms: 100,
        received_at_ms: 100,
        expires_at_ms: 125,
        core_sequence: 1,
    })
}

fn calibration(
    binding: &StrategyBinding,
    candidate: &SemanticIntent,
    preparation: &CandidatePreparation,
    params: &ScalpingParams,
) -> Result<CalibrationManifest, Box<dyn std::error::Error>> {
    let key = CalibrationKey {
        symbol: candidate.symbol.clone(),
        expert: candidate.expert,
        regime: preparation.market_regime,
        direction: candidate.direction,
        entry_style: candidate.entry_style,
    };
    let slice = CalibrationSlice {
        key: key.clone(),
        release_id: binding.parameter_release_id.clone(),
        model_version: params.calibration_model_version.clone(),
        artifact_digest: String::new(),
        model_generation: 1,
        evidence_cursor_ms: 100,
        valid_from_ms: 1,
        valid_until_ms: 450,
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
    };
    Ok(CalibrationManifest {
        schema_version: 1,
        release_id: binding.parameter_release_id.clone(),
        model_version: params.calibration_model_version.clone(),
        artifact_digest: String::new(),
        research: ResearchEvidence {
            schema_version: 1,
            dataset_digest: "1".repeat(64),
            preregistration_digest: "2".repeat(64),
            evidence_cursor_ms: 100,
            approved_for_live: true,
            slices: vec![ResearchSliceEvidence {
                key,
                sample_count: 10,
                after_cost_ev_lower_bps: Decimal::ONE,
                fill_calibration: ResearchCheckStatus::Passed,
                cost_calibration: ResearchCheckStatus::Passed,
                markout_calibration: ResearchCheckStatus::Passed,
                stress_budget: ResearchCheckStatus::Passed,
            }],
        },
        slices: vec![slice],
    }
    .seal()?)
}

fn proof_and_receipt(
    binding: &StrategyBinding,
) -> Result<(BoundRiskRevaluation, AppliedRiskReceipt), Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    let risk_binding = ScalpingRiskBinding {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        owner_scope: binding.owner_scope.clone(),
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        parameter_release_id: binding.parameter_release_id.clone(),
        symbol: binding.symbol.clone(),
        risk_unit: unit.clone(),
        valuation_generation: 7,
    };
    let proof = RiskRevaluation {
        proof_id: "proof-1".to_owned(),
        target_generation: 7,
        risk_unit: unit,
        window_start_ms: 1,
        complete_through_ms: 100,
        source_fact_ids: Vec::new(),
        revalued_facts: Vec::new(),
    };
    let digest = venue::strategy::scalping::risk_revaluation_digest(&proof)?;
    let applied = BoundRiskRevaluation {
        binding: risk_binding,
        proof,
        cursor_sequence: 1,
    };
    let receipt = AppliedRiskReceipt {
        binding: binding.clone(),
        proof_id: applied.proof.proof_id.clone(),
        cursor_sequence: 1,
        risk_revaluation_digest: digest,
        target_generation: 7,
        valuation_generation: 7,
    };
    Ok((applied, receipt))
}

fn frame(generation: u64, watermark_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut cursors = BTreeMap::new();
    let mut versions = BTreeMap::new();
    for source in ["book", "trades", "bars"] {
        cursors.insert(
            source.to_owned(),
            SourceCursor {
                generation,
                sequence: generation,
                event_time_ms: watermark_ms,
                fresh: true,
            },
        );
        versions.insert(source.to_owned(), "v1".to_owned());
    }
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation,
        watermark_ms,
        state: FeatureState::Ready,
        cursors,
        feature_versions: versions,
        values: FeatureValues {
            mid_price: Price::new(Decimal::new(100, 0))?,
            fair_price: Price::new(Decimal::new(100, 0))?,
            spread_bps: Decimal::ONE,
            depth_quote: Decimal::new(100, 0),
            book_imbalance: Decimal::ZERO,
            trade_imbalance: Decimal::ZERO,
            short_return_bps: Decimal::ZERO,
            trend_efficiency: Decimal::ZERO,
            bandwidth_expansion: Decimal::ZERO,
            expected_move_bps: Decimal::ONE,
            toxicity: Decimal::ZERO,
        },
        breakout: None,
    })
}

type Setup = (
    ScalpingCandidateEvidenceCoordinator,
    StrategyBinding,
    CandidatePreparation,
    BoundRiskRevaluation,
    AppliedRiskReceipt,
);

fn setup(directory: &std::path::Path) -> Result<Setup, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let (candidate, preparation) = candidate_and_preparation(&binding)?;
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    let manifest = calibration(&binding, &candidate, &preparation, &params)?;
    params.calibration_model_digest = manifest.artifact_digest.clone();
    std::fs::write(
        directory.join("calibration.json"),
        serde_json::to_vec(&manifest)?,
    )?;
    let quote_path = directory.join("core-quotes.jsonl");
    let receipt = quote_receipt(&binding, &candidate, &preparation)?;
    ScalpingCoreQuoteReceiptJournal::open(&quote_path, binding.clone())?.append(receipt)?;
    let (applied, applied_receipt) = proof_and_receipt(&binding)?;
    let coordinator = ScalpingCandidateEvidenceCoordinator::open(
        ScalpingCandidateEvidenceConfig {
            calibration_artifact_path: directory.join("calibration.json"),
            core_quote_receipt_path: quote_path,
            evidence_journal_path: directory.join("evidence.jsonl"),
            checkpoint_path: directory.join("candidate-evidence.json"),
            live_calibration: false,
        },
        binding.clone(),
        params,
    )?;
    Ok((coordinator, binding, preparation, applied, applied_receipt))
}

#[test]
fn frame_n_is_empty_and_frame_n_plus_one_is_durable_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let (mut coordinator, _binding, preparation, applied, receipt) = setup(directory.path())?;
    let first = coordinator.assemble(frame(1, 100)?, 110)?;
    assert!(first.evidence.is_empty());
    coordinator.record_preparation(Some(preparation))?;
    coordinator.record_applied_risk(applied, receipt)?;
    let second = coordinator.assemble(frame(1, 200)?, 110)?;
    assert_eq!(second.evidence.len(), 1);
    assert_eq!(coordinator.checkpoint().last_evidence_sequence, Some(1));
    assert!(std::fs::read(directory.path().join("evidence.jsonl"))?.ends_with(b"\n"));
    Ok(())
}

#[test]
fn restart_replays_pending_preparation_without_duplicate_bundle()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let (mut coordinator, binding, preparation, applied, receipt) = setup(directory.path())?;
    coordinator.assemble(frame(1, 100)?, 110)?;
    coordinator.record_preparation(Some(preparation))?;
    coordinator.record_applied_risk(applied, receipt)?;
    drop(coordinator);

    let (mut recovered, _binding, _preparation, _applied, _receipt) = setup(directory.path())?;
    let market = recovered.assemble(frame(1, 200)?, 110)?;
    assert_eq!(market.evidence.len(), 1);
    assert_eq!(recovered.checkpoint().last_evidence_sequence, Some(1));
    assert_eq!(recovered.checkpoint().binding, binding);
    Ok(())
}

#[test]
fn checkpoint_digest_tamper_fences_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let (mut coordinator, binding, _preparation, _applied, _receipt) = setup(directory.path())?;
    coordinator.assemble(frame(1, 100)?, 110)?;
    let path = directory.path().join("candidate-evidence.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    value["state_digest"] = Value::String("0".repeat(64));
    std::fs::write(&path, serde_json::to_vec(&value)?)?;

    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let result = ScalpingCandidateEvidenceCoordinator::open(
        ScalpingCandidateEvidenceConfig {
            calibration_artifact_path: directory.path().join("calibration.json"),
            core_quote_receipt_path: directory.path().join("core-quotes.jsonl"),
            evidence_journal_path: directory.path().join("evidence.jsonl"),
            checkpoint_path: path,
            live_calibration: false,
        },
        binding,
        params,
    );
    assert!(result.is_err());
    Ok(())
}

#[test]
fn invalid_next_frame_is_preflighted_before_bundle_append() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let (mut coordinator, _binding, preparation, applied, receipt) = setup(directory.path())?;
    coordinator.assemble(frame(1, 100)?, 110)?;
    coordinator.record_preparation(Some(preparation))?;
    coordinator.record_applied_risk(applied, receipt)?;

    assert!(coordinator.assemble(frame(1, 100)?, 110).is_err());
    assert!(coordinator.is_fenced());
    assert_eq!(coordinator.checkpoint().last_evidence_sequence, None);
    assert!(!directory.path().join("evidence.jsonl").exists());
    Ok(())
}

#[test]
fn zero_decision_time_is_preflighted_before_bundle_append() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let (mut coordinator, _binding, preparation, applied, receipt) = setup(directory.path())?;
    coordinator.assemble(frame(1, 100)?, 110)?;
    coordinator.record_preparation(Some(preparation))?;
    coordinator.record_applied_risk(applied, receipt)?;

    assert!(coordinator.assemble(frame(1, 200)?, 0).is_err());
    assert!(coordinator.is_fenced());
    assert_eq!(coordinator.checkpoint().last_evidence_sequence, None);
    assert!(!directory.path().join("evidence.jsonl").exists());
    Ok(())
}

fn rewritten_bundle_record(
    record: &Value,
    worst_loss: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut record = record.clone();
    for owner in ["calibration", "costs", "risk"] {
        record["bundle"][owner]["identity"]["watermark_ms"] = Value::from(200_u64);
    }
    record["bundle"]["risk"]["worst_loss"]["value"] = Value::String(worst_loss.to_owned());
    let bundle: CandidateEvidenceBundle = serde_json::from_value(record["bundle"].clone())?;
    let bundle_bytes = serde_json::to_vec(&bundle)?;
    record["content_sha256"] = Value::String(format!("{:x}", Sha256::digest(bundle_bytes)));
    let mut encoded = serde_json::to_vec(&record)?;
    encoded.push(b'\n');
    Ok(encoded)
}

#[test]
fn retryable_join_error_does_not_permanently_fence_coordinator()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let (mut coordinator, _binding, preparation, applied, receipt) = setup(directory.path())?;
    coordinator.assemble(frame(1, 100)?, 110)?;
    coordinator.record_preparation(Some(preparation.clone()))?;
    coordinator.record_applied_risk(applied, receipt)?;
    coordinator.assemble(frame(1, 200)?, 110)?;

    let evidence_path = directory.path().join("evidence.jsonl");
    let record: Value = serde_json::from_slice(&std::fs::read(&evidence_path)?)?;
    let invalid = rewritten_bundle_record(&record, "6")?;
    let valid = rewritten_bundle_record(&record, "4")?;
    std::fs::write(&evidence_path, invalid)?;
    coordinator.refresh_evidence_source()?;
    std::fs::write(directory.path().join("core-quotes.jsonl"), b"")?;
    coordinator.refresh_core_quote_source()?;

    let mut next_preparation = preparation;
    next_preparation.watermark_ms = 200;
    coordinator.record_preparation(Some(next_preparation))?;
    assert!(coordinator.assemble(frame(1, 300)?, 300).is_err());
    assert!(!coordinator.is_fenced());
    let last_frame = coordinator
        .checkpoint()
        .last_frame
        .as_ref()
        .ok_or_else(|| std::io::Error::other("missing last frame checkpoint"))?;
    assert_eq!(last_frame.watermark_ms, 200);

    std::fs::write(&evidence_path, valid)?;
    coordinator.refresh_evidence_source()?;
    let market = coordinator.assemble(frame(1, 300)?, 300)?;
    assert_eq!(market.evidence.len(), 1);
    assert!(!coordinator.is_fenced());
    Ok(())
}
