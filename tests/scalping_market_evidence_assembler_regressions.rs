use std::collections::BTreeMap;

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset, Price},
    indicator::{FeatureFrame, FeatureState, FeatureValues},
    runtime::{
        BoundRiskRevaluation, ScalpingEvidenceSource, ScalpingMarketEvidenceAssembler,
        ScalpingMarketEvidenceAssemblerError, ScalpingMarketEvidenceFence,
    },
    storage::{ScalpingEvidenceJournal, ScalpingRiskBinding},
    strategy::scalping::{
        CalibrationEvidence, CandidateEvidenceBundle, CandidatePreparation, CostEvidence,
        Direction, EntryStyle, EvidenceIdentity, ExitTemplate, Expert, FillSlice, MarketRegime,
        OutcomeProbabilities, RiskEvidence, RiskLimit, RiskPlan, RiskRevaluation, RiskUnit,
        SemanticIntent, SemanticPurpose, risk_revaluation_digest,
    },
};

fn preparation() -> Result<CandidatePreparation, Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    let quote = Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0));
    let candidate = SemanticIntent {
        intent_id: "candidate-1".to_owned(),
        symbol: "SOL/USDT".parse()?,
        direction: Direction::Long,
        purpose: SemanticPurpose::Entry,
        expert: Expert::RangeFade,
        entry_style: EntryStyle::PassiveMaker,
        exit_template: ExitTemplate::FairValue,
        attempt_cap: 1,
        max_reprices: 1,
        risk_plan: RiskPlan {
            risk_per_episode: RiskLimit::new(unit.clone(), Decimal::ONE),
            quote_cap: quote.clone(),
            max_episode_loss: RiskLimit::new(unit, Decimal::ONE),
        },
        target_quote: quote,
        reference_price: Price::new(Decimal::new(100, 0))?,
        max_slippage_bps: Decimal::ONE,
        valid_until_ms: 200,
        entry_ttl_ms: 1_000,
        hard_stop_distance_bps: Decimal::ONE,
        target_distance_bps: Decimal::ONE,
        max_hold_ms: 1_000,
        max_unprotected_ms: 100,
        requires_server_protection: true,
        opportunity_key: "range".to_owned(),
        breakout_cursor: None,
        idempotency_seed: "seed".to_owned(),
    };
    Ok(CandidatePreparation {
        preparation_id: "preparation-1".to_owned(),
        binding_digest: "b".repeat(64),
        controller_revision: 1,
        authority_generation: 1,
        market_regime: MarketRegime::Range,
        frame_generation: 3,
        watermark_ms: 100,
        valid_until_ms: 200,
        candidates: vec![candidate],
    })
}

fn preparation_at(
    generation: u64,
    watermark_ms: u64,
) -> Result<CandidatePreparation, Box<dyn std::error::Error>> {
    let mut value = preparation()?;
    value.frame_generation = generation;
    value.watermark_ms = watermark_ms;
    Ok(value)
}

fn proof(id: &str) -> Result<RiskRevaluation, Box<dyn std::error::Error>> {
    Ok(RiskRevaluation {
        proof_id: id.to_owned(),
        target_generation: 7,
        risk_unit: RiskUnit::new("risk")?,
        window_start_ms: 1,
        complete_through_ms: 100,
        source_fact_ids: Vec::new(),
        revalued_facts: Vec::new(),
    })
}

fn bound(proof: RiskRevaluation) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    Ok(BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            owner_scope: "scalping:shadow".to_owned(),
            strategy_instance_id: "shadow".to_owned(),
            run_id: "run-1".to_owned(),
            parameter_release_id: "release-1".to_owned(),
            symbol: "SOL/USDT".parse()?,
            risk_unit: proof.risk_unit.clone(),
            valuation_generation: proof.target_generation,
        },
        proof,
        cursor_sequence: 1,
    })
}

fn identity(kind: &str, preparation: &CandidatePreparation, risk_digest: &str) -> EvidenceIdentity {
    EvidenceIdentity {
        schema_version: 1,
        evidence_id: format!("{kind}-1"),
        candidate_id: preparation.candidates[0].intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        producer_generation: if kind == "risk" { 7 } else { 1 },
        release_digest: if kind == "risk" {
            risk_digest.to_owned()
        } else {
            "a".repeat(64)
        },
        valid_until_ms: preparation.valid_until_ms,
    }
}

fn bundle(
    preparation: &CandidatePreparation,
    proof: &RiskRevaluation,
) -> Result<CandidateEvidenceBundle, Box<dyn std::error::Error>> {
    let risk_digest = risk_revaluation_digest(proof)?;
    Ok(CandidateEvidenceBundle {
        calibration: CalibrationEvidence {
            identity: identity("calibration", preparation, &risk_digest),
            model_version: "calibration-v1".to_owned(),
            fill_distribution: vec![FillSlice {
                fill_ratio: Decimal::ONE,
                probability: Decimal::ONE,
            }],
            outcomes: OutcomeProbabilities {
                target: Decimal::ONE,
                stop: Decimal::ZERO,
                other: Decimal::ZERO,
            },
            target_pnl_bps: Decimal::new(10, 0),
            stop_pnl_bps: -Decimal::ONE,
            other_pnl_bps: Decimal::ZERO,
            uncertainty_bps: Decimal::ONE,
        },
        costs: CostEvidence {
            identity: identity("cost", preparation, &risk_digest),
            entry_cost_bps: Decimal::ONE,
            exit_cost_bps: Decimal::ONE,
            funding_cost_bps: Decimal::ZERO,
            nonfill_cost_bps: Decimal::ZERO,
            opportunity_cost_bps: Decimal::ZERO,
        },
        risk: RiskEvidence {
            identity: identity("risk", preparation, &risk_digest),
            policy_digest: "c".repeat(64),
            worst_loss: preparation.candidates[0].risk_plan.risk_per_episode.clone(),
            admissible: true,
        },
    })
}

fn frame(generation: u64, watermark_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "SOL/USDT".parse()?,
        schema_version: 1,
        generation,
        watermark_ms,
        state: FeatureState::Ready,
        cursors: BTreeMap::new(),
        feature_versions: BTreeMap::new(),
        values: FeatureValues {
            mid_price: Price::new(Decimal::new(100, 0))?,
            fair_price: Price::new(Decimal::new(100, 0))?,
            spread_bps: Decimal::ZERO,
            depth_quote: Decimal::ZERO,
            book_imbalance: Decimal::ZERO,
            trade_imbalance: Decimal::ZERO,
            short_return_bps: Decimal::ZERO,
            trend_efficiency: Decimal::ZERO,
            bandwidth_expansion: Decimal::ZERO,
            expected_move_bps: Decimal::ZERO,
            toxicity: Decimal::ZERO,
        },
        breakout: None,
    })
}

#[test]
fn preparation_frame_is_empty_and_next_strict_frame_joins_exact_bundle()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("evidence.jsonl");
    let preparation = preparation()?;
    let proof = proof("proof-1")?;
    ScalpingEvidenceJournal::open(&path)?.append(bundle(&preparation, &proof)?)?;

    let mut assembler = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(path)?);
    assembler.record_applied_risk(bound(proof)?)?;
    assert!(assembler.assemble(frame(3, 100)?, 100)?.evidence.is_empty());

    assembler.record_preparation(Some(preparation.clone()))?;
    let joined = assembler.assemble(frame(3, 101)?, 101)?;
    assert_eq!(joined.evidence.len(), 1);
    assert_eq!(
        joined.evidence[0].candidate_id,
        preparation.candidates[0].intent_id
    );
    assert!(!assembler.has_pending_preparation());
    Ok(())
}

#[test]
fn refreshed_immutable_source_sees_later_bundle_without_losing_pending_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("evidence.jsonl");
    let mut journal = ScalpingEvidenceJournal::open(&path)?;
    let preparation = preparation()?;
    let proof = proof("proof-1")?;
    let applied = bound(proof.clone())?;
    let mut stale = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(&path)?);
    let mut refreshed = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(&path)?);
    for assembler in [&mut stale, &mut refreshed] {
        assembler.record_applied_risk(applied.clone())?;
        assert!(assembler.assemble(frame(3, 100)?, 100)?.evidence.is_empty());
        assembler.record_preparation(Some(preparation.clone()))?;
    }

    journal.append(bundle(&preparation, &proof)?)?;
    assert!(stale.assemble(frame(3, 101)?, 101)?.evidence.is_empty());

    refreshed.refresh_evidence_source(&path)?;
    let joined = refreshed.assemble(frame(3, 101)?, 101)?;
    assert_eq!(joined.evidence.len(), 1);

    let pending_path = directory.path().join("pending.jsonl");
    ScalpingEvidenceJournal::open(&pending_path)?;
    let mut pending =
        ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(&pending_path)?);
    pending.record_applied_risk(applied)?;
    pending.assemble(frame(3, 100)?, 100)?;
    pending.record_preparation(Some(preparation.clone()))?;

    let invalid_path = directory.path().join("invalid.jsonl");
    let mut invalid = bundle(&preparation, &proof)?;
    invalid.costs.identity.preparation_id = "other-preparation".to_owned();
    ScalpingEvidenceJournal::open(&invalid_path)?.append(invalid)?;
    assert!(matches!(
        pending.refresh_evidence_source(&invalid_path),
        Err(ScalpingMarketEvidenceAssemblerError::Evidence(_))
    ));
    assert!(pending.has_pending_preparation());

    let ambiguous_path = directory.path().join("ambiguous.jsonl");
    let first = bundle(&preparation, &proof)?;
    let mut second = first.clone();
    second.calibration.identity.evidence_id = "calibration-2".to_owned();
    second.costs.identity.evidence_id = "cost-2".to_owned();
    second.risk.identity.evidence_id = "risk-2".to_owned();
    let mut ambiguous = ScalpingEvidenceJournal::open(&ambiguous_path)?;
    ambiguous.append(first)?;
    ambiguous.append(second)?;
    assert!(matches!(
        pending.refresh_evidence_source(&ambiguous_path),
        Err(ScalpingMarketEvidenceAssemblerError::Evidence(_))
    ));
    assert!(pending.has_pending_preparation());
    pending.refresh_evidence_source(&path)?;
    assert_eq!(pending.assemble(frame(3, 101)?, 101)?.evidence.len(), 1);
    Ok(())
}

#[test]
fn failed_join_preserves_pending_proof_and_frame_for_same_cursor_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let empty_path = directory.path().join("empty.jsonl");
    ScalpingEvidenceJournal::open(&empty_path)?;
    let preparation = preparation()?;
    let proof = proof("proof-1")?;

    let invalid_path = directory.path().join("invalid-bundle.jsonl");
    let mut invalid = bundle(&preparation, &proof)?;
    invalid.risk.worst_loss.value = Decimal::new(2, 0);
    ScalpingEvidenceJournal::open(&invalid_path)?.append(invalid)?;

    let valid_path = directory.path().join("valid-bundle.jsonl");
    ScalpingEvidenceJournal::open(&valid_path)?.append(bundle(&preparation, &proof)?)?;

    let mut assembler =
        ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(empty_path)?);
    assembler.record_applied_risk(bound(proof)?)?;
    assembler.assemble(frame(3, 100)?, 100)?;
    assembler.record_preparation(Some(preparation))?;
    assembler.refresh_evidence_source(&invalid_path)?;

    assert!(matches!(
        assembler.assemble(frame(3, 101)?, 101),
        Err(ScalpingMarketEvidenceAssemblerError::Evidence(_))
    ));
    assert!(assembler.has_pending_preparation());

    assembler.refresh_evidence_source(&valid_path)?;
    let joined = assembler.assemble(frame(3, 101)?, 101)?;
    assert_eq!(joined.evidence.len(), 1);
    assert!(!assembler.has_pending_preparation());
    Ok(())
}

#[test]
fn changed_proof_fence_and_expiry_clear_pending_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("empty.jsonl");
    ScalpingEvidenceJournal::open(&path)?;
    let mut assembler = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(path)?);
    let preparation = preparation()?;

    assembler.record_applied_risk(bound(proof("proof-1")?)?)?;
    assembler.assemble(frame(3, 100)?, 100)?;
    assembler.record_preparation(Some(preparation.clone()))?;
    let mut newer = bound(proof("proof-2")?)?;
    newer.cursor_sequence = 2;
    assembler.record_applied_risk(newer)?;
    assert!(!assembler.has_pending_preparation());
    assert!(assembler.assemble(frame(3, 101)?, 101)?.evidence.is_empty());

    assembler.record_preparation(Some(preparation_at(3, 101)?))?;
    assembler.fence(ScalpingMarketEvidenceFence::ControlStopped);
    assert!(!assembler.has_pending_preparation());
    assert!(assembler.assemble(frame(3, 102)?, 102)?.evidence.is_empty());

    assembler.record_preparation(Some(preparation_at(3, 102)?))?;
    assembler.fence(ScalpingMarketEvidenceFence::PrivateFenced);
    assert!(!assembler.has_pending_preparation());
    assert!(assembler.assemble(frame(3, 103)?, 103)?.evidence.is_empty());

    assembler.record_preparation(Some(preparation_at(3, 103)?))?;
    assert!(assembler.assemble(frame(3, 201)?, 201)?.evidence.is_empty());
    assert!(!assembler.has_pending_preparation());
    Ok(())
}

#[test]
fn exact_risk_repeat_is_idempotent_but_same_id_or_cursor_conflicts_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("empty.jsonl");
    ScalpingEvidenceJournal::open(&path)?;
    let mut assembler = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(path)?);
    let applied = bound(proof("proof-1")?)?;

    assembler.record_applied_risk(applied.clone())?;
    assembler.assemble(frame(3, 100)?, 100)?;
    assembler.record_preparation(Some(preparation()?))?;
    assembler.record_applied_risk(applied.clone())?;
    assert!(assembler.has_pending_preparation());

    let mut changed_binding = applied.clone();
    changed_binding.binding.account = "other-account".to_owned();
    assert!(matches!(
        assembler.record_applied_risk(changed_binding),
        Err(ScalpingMarketEvidenceAssemblerError::RiskProof)
    ));

    let mut changed_proof = applied.clone();
    changed_proof.proof.complete_through_ms = 101;
    assert!(matches!(
        assembler.record_applied_risk(changed_proof),
        Err(ScalpingMarketEvidenceAssemblerError::RiskProof)
    ));

    let mut reused_proof_id = applied.clone();
    reused_proof_id.cursor_sequence = 2;
    assert!(matches!(
        assembler.record_applied_risk(reused_proof_id),
        Err(ScalpingMarketEvidenceAssemblerError::RiskProof)
    ));
    Ok(())
}

#[test]
fn lower_risk_cursor_and_public_generation_change_cannot_reuse_preparation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("empty.jsonl");
    ScalpingEvidenceJournal::open(&path)?;
    let mut assembler = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(path)?);
    let first = bound(proof("proof-1")?)?;
    let mut second = bound(proof("proof-2")?)?;
    second.cursor_sequence = 2;
    assembler.record_applied_risk(first)?;
    assembler.record_applied_risk(second)?;

    let mut rollback = bound(proof("proof-3")?)?;
    rollback.cursor_sequence = 1;
    assert!(matches!(
        assembler.record_applied_risk(rollback),
        Err(ScalpingMarketEvidenceAssemblerError::RiskProof)
    ));

    assembler.assemble(frame(3, 100)?, 100)?;
    assembler.record_preparation(Some(preparation()?))?;
    assert!(assembler.assemble(frame(4, 101)?, 101)?.evidence.is_empty());
    assert!(!assembler.has_pending_preparation());
    Ok(())
}

#[test]
fn repeated_or_regressing_frame_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("empty.jsonl");
    ScalpingEvidenceJournal::open(&path)?;
    let mut assembler = ScalpingMarketEvidenceAssembler::new(ScalpingEvidenceSource::open(path)?);

    assembler.assemble(frame(3, 100)?, 100)?;
    assert!(matches!(
        assembler.assemble(frame(3, 100)?, 100),
        Err(ScalpingMarketEvidenceAssemblerError::FrameOrder)
    ));
    assert!(matches!(
        assembler.assemble(frame(2, 101)?, 101),
        Err(ScalpingMarketEvidenceAssemblerError::FrameOrder)
    ));
    Ok(())
}

#[test]
fn ambiguous_exact_journal_bundle_is_an_error_not_an_empty_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("ambiguous.jsonl");
    let preparation = preparation()?;
    let proof = proof("proof-1")?;
    let first = bundle(&preparation, &proof)?;
    let mut second = first.clone();
    second.calibration.identity.evidence_id = "calibration-2".to_owned();
    second.costs.identity.evidence_id = "cost-2".to_owned();
    second.risk.identity.evidence_id = "risk-2".to_owned();
    let mut journal = ScalpingEvidenceJournal::open(&path)?;
    journal.append(first)?;
    journal.append(second)?;

    assert!(matches!(
        ScalpingEvidenceSource::open(path),
        Err(venue::runtime::ScalpingEvidenceSourceError::Ambiguous)
    ));
    Ok(())
}
