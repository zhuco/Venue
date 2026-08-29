use std::{collections::BTreeMap, str::FromStr};

use rust_decimal::Decimal;
use serde::Deserialize;
use venue::{
    controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, BREAKOUT_OPPORTUNITY_VERSION_KEY, BreakoutDirection,
        BreakoutOpportunity, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    strategy::scalping::{
        CalibrationEvidence, CandidateEvidenceBundle, CandidatePreparation, CostEvidence,
        Direction, EntryStyle, EvidenceIdentity, Expert, FillSlice, NoopReason,
        OutcomeProbabilities, RiskEvidence, SafetyProjection, ScalpingDecision, ScalpingParams,
        ScalpingStrategy, SemanticIntent, StrategyBinding, StrategyKind, join_candidate_evidence,
    },
};

const FIXTURE: &str = include_str!("fixtures/scalping_legacy_decision_v1.json");
const LEGACY_DECISION_SHA256: &str =
    "26bdbb18e7a02f017ab3f09bb959a032adc0937c379d7d32825aec2589c74ac4";

#[derive(Deserialize)]
struct Fixture {
    schema_version: u16,
    provenance: Provenance,
    range_fade: RangeFadeFixture,
    reprice: RepriceFixture,
    breakout: BreakoutFixture,
}

#[derive(Deserialize)]
struct Provenance {
    source_path: String,
    source_sha256: String,
    extraction_symbols: Vec<String>,
    migration_exceptions: Vec<MigrationException>,
}

#[derive(Deserialize)]
struct MigrationException {
    legacy: String,
    root: String,
    reason: String,
}

#[derive(Deserialize)]
struct RangeFadeFixture {
    frame: FrameFixture,
    evidence: EvidenceFixture,
    expected: ExpectedFixture,
}

#[derive(Deserialize)]
struct FrameFixture {
    mid_price: String,
    fair_price: String,
    spread_bps: String,
    depth_quote: String,
    book_imbalance: String,
    trade_imbalance: String,
    short_return_bps: String,
    trend_efficiency: String,
    bandwidth_expansion: String,
    expected_move_bps: String,
    toxicity: String,
}

#[derive(Deserialize)]
struct EvidenceFixture {
    fill_ratio: String,
    fill_probability: String,
    target_probability: String,
    stop_probability: String,
    other_probability: String,
    target_pnl_bps: String,
    stop_pnl_bps: String,
    other_pnl_bps: String,
    entry_cost_bps: String,
    exit_cost_bps: String,
    funding_cost_bps: String,
    nonfill_cost_bps: String,
    opportunity_cost_bps: String,
    uncertainty_bps: String,
    calibration_digest: String,
    cost_digest: String,
    risk_digest: String,
}

#[derive(Deserialize)]
struct ExpectedFixture {
    direction: String,
    expert: String,
    entry_style: String,
    outcome_expected_value_bps: String,
    net_expected_value_bps: String,
    robust_expected_value_bps: String,
}

#[derive(Deserialize)]
struct RepriceFixture {
    observed_at_ms: u64,
    fresh_entry_cost_bps: String,
    degraded_entry_cost_bps: String,
}

#[derive(Deserialize)]
struct BreakoutFixture {
    initial_watermark_ms: u64,
    mature_watermark_ms: u64,
    same_cycle_watermark_ms: u64,
    next_cycle_watermark_ms: u64,
    boundary_sequence: u64,
    compression_cycle_sequence: u64,
    next_boundary_sequence: u64,
    next_compression_cycle_sequence: u64,
}

fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    Ok(serde_json::from_str(FIXTURE)?)
}

fn decimal(value: &str) -> Result<Decimal, Box<dyn std::error::Error>> {
    Ok(Decimal::from_str(value)?)
}

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "legacy_fixture".to_owned(),
        run_id: "decision_v1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "legacy_fixture:decision_v1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn authorization(binding: &StrategyBinding) -> venue::controller::EntryAuthorization {
    InstanceControlRecord {
        schema_version: venue::controller::CONTROL_SCHEMA_VERSION,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "legacy-fixture-start".to_owned(),
        idempotency_key: "legacy-fixture-start-1".to_owned(),
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
        1,
    )
}

fn safety() -> SafetyProjection {
    SafetyProjection {
        private_snapshot_ready: true,
        exposure: venue::strategy::scalping::ExposureState::Flat,
        execution_unknown: false,
        protection: venue::strategy::scalping::ProtectionState::Complete,
        owner_conflict: false,
        risk_budget_available: true,
    }
}

fn frame(
    values: &FrameFixture,
    watermark_ms: u64,
    sequence: u64,
) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "BTC/USDT".parse()?,
        schema_version: 1,
        generation: 1,
        watermark_ms,
        state: FeatureState::Ready,
        cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
            .into_iter()
            .map(|source| {
                (
                    source.to_owned(),
                    SourceCursor {
                        generation: 1,
                        sequence,
                        event_time_ms: watermark_ms,
                        fresh: true,
                    },
                )
            })
            .collect(),
        feature_versions: BTreeMap::from([
            (BOOK_SOURCE.to_owned(), "v1".to_owned()),
            (TRADES_SOURCE.to_owned(), "v1".to_owned()),
            (BARS_SOURCE.to_owned(), "v1".to_owned()),
            (
                "_feature_profile".to_owned(),
                "scalping-shadow-v1".to_owned(),
            ),
            ("_feature_profile_digest".to_owned(), "0".repeat(64)),
        ]),
        values: FeatureValues {
            mid_price: Price::new(decimal(&values.mid_price)?)?,
            fair_price: Price::new(decimal(&values.fair_price)?)?,
            spread_bps: decimal(&values.spread_bps)?,
            depth_quote: decimal(&values.depth_quote)?,
            book_imbalance: decimal(&values.book_imbalance)?,
            trade_imbalance: decimal(&values.trade_imbalance)?,
            short_return_bps: decimal(&values.short_return_bps)?,
            trend_efficiency: decimal(&values.trend_efficiency)?,
            bandwidth_expansion: decimal(&values.bandwidth_expansion)?,
            expected_move_bps: decimal(&values.expected_move_bps)?,
            toxicity: decimal(&values.toxicity)?,
        },
        breakout: None,
    })
}

fn evidence_identity(
    kind: &str,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    release_digest: String,
) -> EvidenceIdentity {
    EvidenceIdentity {
        schema_version: 1,
        evidence_id: format!("legacy-{kind}-{}", candidate.intent_id),
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

fn evidence(
    fixture: &EvidenceFixture,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    entry_cost_bps: &str,
    cost_digest: String,
) -> Result<venue::strategy::scalping::CandidateEvidence, Box<dyn std::error::Error>> {
    let bundle = CandidateEvidenceBundle {
        calibration: CalibrationEvidence {
            identity: evidence_identity(
                "calibration",
                preparation,
                candidate,
                fixture.calibration_digest.clone(),
            ),
            model_version: "scalping-shadow-calibration-v1".to_owned(),
            fill_distribution: vec![FillSlice {
                fill_ratio: decimal(&fixture.fill_ratio)?,
                probability: decimal(&fixture.fill_probability)?,
            }],
            outcomes: OutcomeProbabilities {
                target: decimal(&fixture.target_probability)?,
                stop: decimal(&fixture.stop_probability)?,
                other: decimal(&fixture.other_probability)?,
            },
            target_pnl_bps: decimal(&fixture.target_pnl_bps)?,
            stop_pnl_bps: decimal(&fixture.stop_pnl_bps)?,
            other_pnl_bps: decimal(&fixture.other_pnl_bps)?,
            uncertainty_bps: decimal(&fixture.uncertainty_bps)?,
        },
        costs: CostEvidence {
            identity: evidence_identity("cost", preparation, candidate, cost_digest),
            entry_cost_bps: decimal(entry_cost_bps)?,
            exit_cost_bps: decimal(&fixture.exit_cost_bps)?,
            funding_cost_bps: decimal(&fixture.funding_cost_bps)?,
            nonfill_cost_bps: decimal(&fixture.nonfill_cost_bps)?,
            opportunity_cost_bps: decimal(&fixture.opportunity_cost_bps)?,
        },
        risk: RiskEvidence {
            identity: evidence_identity(
                "risk",
                preparation,
                candidate,
                fixture.risk_digest.clone(),
            ),
            policy_digest: "c".repeat(64),
            worst_loss: candidate.risk_plan.risk_per_episode.clone(),
            admissible: true,
        },
    };
    Ok(join_candidate_evidence(
        preparation,
        candidate,
        &bundle,
        preparation.watermark_ms,
    )?)
}

fn prepared(
    strategy: &mut ScalpingStrategy,
    frame: &FeatureFrame,
    authorization: &venue::controller::EntryAuthorization,
) -> Result<CandidatePreparation, Box<dyn std::error::Error>> {
    match strategy.evaluate(frame, &safety(), authorization)? {
        ScalpingDecision::Prepared(preparation) => Ok(*preparation),
        decision => Err(format!("expected prepared decision, got {decision:?}").into()),
    }
}

#[test]
fn fixture_provenance_is_pinned_and_declares_the_two_authorized_repairs()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    assert_eq!(fixture.schema_version, 1);
    assert_eq!(
        fixture.provenance.source_path,
        "bak/VenueAlpha/strategies/multi-alpha-scalper/src/decision.rs"
    );
    assert_eq!(fixture.provenance.source_sha256, LEGACY_DECISION_SHA256);
    assert!(
        fixture
            .provenance
            .extraction_symbols
            .iter()
            .any(|symbol| symbol == "tests::range_fade_is_deterministic_and_deduplicated")
    );
    assert_eq!(fixture.provenance.migration_exceptions.len(), 2);
    assert!(fixture.provenance.migration_exceptions.iter().all(|item| {
        !item.legacy.is_empty() && !item.root.is_empty() && !item.reason.is_empty()
    }));
    Ok(())
}

#[test]
fn legacy_range_fade_fixture_preserves_candidate_direction_style_and_robust_ev()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding, params.clone())?;
    let first_frame = frame(&fixture.range_fade.frame, 100, 1)?;
    let preparation = prepared(&mut strategy, &first_frame, &authorization)?;
    let candidate = preparation
        .candidates
        .first()
        .ok_or("range fixture has no candidate")?;
    assert_eq!(fixture.range_fade.expected.direction, "long");
    assert_eq!(candidate.direction, Direction::Long);
    assert_eq!(fixture.range_fade.expected.expert, "range_fade");
    assert_eq!(candidate.expert, Expert::RangeFade);
    assert_eq!(fixture.range_fade.expected.entry_style, "passive_maker");
    assert_eq!(candidate.entry_style, EntryStyle::PassiveMaker);

    let proof = evidence(
        &fixture.range_fade.evidence,
        &preparation,
        candidate,
        &fixture.range_fade.evidence.entry_cost_bps,
        fixture.range_fade.evidence.cost_digest.clone(),
    )?;
    assert_eq!(
        proof.outcome_expected_value_bps,
        decimal(&fixture.range_fade.expected.outcome_expected_value_bps)?
    );
    assert_eq!(
        proof.net_expected_value_bps,
        decimal(&fixture.range_fade.expected.net_expected_value_bps)?
    );
    assert_eq!(
        proof.net_expected_value_bps - params.uncertainty_multiplier * proof.uncertainty_bps,
        decimal(&fixture.range_fade.expected.robust_expected_value_bps)?
    );
    assert!(matches!(
        strategy.admit(&[proof], first_frame.watermark_ms)?,
        ScalpingDecision::Intent(_)
    ));
    Ok(())
}

#[test]
fn legacy_reprice_fixture_keeps_identity_and_rejects_cost_deterioration()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    let binding = binding()?;
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(
        binding,
        ScalpingParams::shadow(Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0))),
    )?;
    let decision_frame = frame(&fixture.range_fade.frame, 100, 1)?;
    let preparation = prepared(&mut strategy, &decision_frame, &authorization)?;
    let candidate = preparation
        .candidates
        .first()
        .ok_or("reprice fixture has no candidate")?;
    let initial = evidence(
        &fixture.range_fade.evidence,
        &preparation,
        candidate,
        &fixture.range_fade.evidence.entry_cost_bps,
        fixture.range_fade.evidence.cost_digest.clone(),
    )?;
    assert!(matches!(
        strategy.admit(&[initial], decision_frame.watermark_ms)?,
        ScalpingDecision::Intent(_)
    ));
    let fresh = evidence(
        &fixture.range_fade.evidence,
        &preparation,
        candidate,
        &fixture.reprice.fresh_entry_cost_bps,
        "d".repeat(64),
    )?;
    strategy.validate_reprice(&fresh, fixture.reprice.observed_at_ms)?;
    let degraded = evidence(
        &fixture.range_fade.evidence,
        &preparation,
        candidate,
        &fixture.reprice.degraded_entry_cost_bps,
        "e".repeat(64),
    )?;
    assert!(
        strategy
            .validate_reprice(&degraded, fixture.reprice.observed_at_ms)
            .is_err()
    );
    Ok(())
}

#[test]
fn legacy_breakout_fixture_requires_and_deduplicates_boundary_cycle_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let authorization = authorization(&binding);
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let mut initial = frame(
        &fixture.range_fade.frame,
        fixture.breakout.initial_watermark_ms,
        1,
    )?;
    initial.values.trend_efficiency = Decimal::ONE;
    initial.values.bandwidth_expansion = Decimal::ONE;
    initial.feature_versions.insert(
        BREAKOUT_OPPORTUNITY_VERSION_KEY.to_owned(),
        "pulse-breakout-opportunity-v1".to_owned(),
    );
    assert!(matches!(
        strategy.evaluate(&initial, &safety(), &authorization)?,
        ScalpingDecision::Noop(NoopReason::NoSignal)
    ));

    let first = breakout_frame(&fixture, fixture.breakout.mature_watermark_ms, 2, 1, 1)?;
    let prepared_first = prepared(&mut strategy, &first, &authorization)?;
    let first_candidate = prepared_first
        .candidates
        .first()
        .ok_or("breakout fixture has no candidate")?;
    assert_eq!(first_candidate.expert, Expert::BreakoutContinuation);
    let first_proof = evidence(
        &fixture.range_fade.evidence,
        &prepared_first,
        first_candidate,
        &fixture.range_fade.evidence.entry_cost_bps,
        fixture.range_fade.evidence.cost_digest.clone(),
    )?;
    let first_intent = match strategy.admit(&[first_proof], first.watermark_ms)? {
        ScalpingDecision::Intent(intent) => intent,
        decision => return Err(format!("expected breakout intent, got {decision:?}").into()),
    };
    strategy.acknowledge_shadow_intent(&first_intent.intent_id, first.watermark_ms)?;

    let same = breakout_frame(
        &fixture,
        fixture.breakout.same_cycle_watermark_ms,
        3,
        fixture.breakout.boundary_sequence,
        fixture.breakout.compression_cycle_sequence,
    )?;
    let repeated = prepared(&mut strategy, &same, &authorization)?;
    let repeated_candidate = repeated
        .candidates
        .first()
        .ok_or("same-cycle breakout fixture has no candidate")?;
    let repeated_proof = evidence(
        &fixture.range_fade.evidence,
        &repeated,
        repeated_candidate,
        &fixture.range_fade.evidence.entry_cost_bps,
        fixture.range_fade.evidence.cost_digest.clone(),
    )?;
    assert!(matches!(
        strategy.admit(&[repeated_proof], same.watermark_ms)?,
        ScalpingDecision::Noop(NoopReason::DuplicateOpportunity)
    ));

    let next = breakout_frame(
        &fixture,
        fixture.breakout.next_cycle_watermark_ms,
        4,
        fixture.breakout.next_boundary_sequence,
        fixture.breakout.next_compression_cycle_sequence,
    )?;
    let next_prepared = prepared(&mut strategy, &next, &authorization)?;
    let next_candidate = next_prepared
        .candidates
        .first()
        .ok_or("next-cycle breakout fixture has no candidate")?;
    let next_proof = evidence(
        &fixture.range_fade.evidence,
        &next_prepared,
        next_candidate,
        &fixture.range_fade.evidence.entry_cost_bps,
        fixture.range_fade.evidence.cost_digest.clone(),
    )?;
    assert!(matches!(
        strategy.admit(&[next_proof], next.watermark_ms)?,
        ScalpingDecision::Intent(_)
    ));
    Ok(())
}

fn breakout_frame(
    fixture: &Fixture,
    watermark_ms: u64,
    sequence: u64,
    boundary_sequence: u64,
    compression_cycle_sequence: u64,
) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    let mut frame = frame(&fixture.range_fade.frame, watermark_ms, sequence)?;
    frame.values.trend_efficiency = Decimal::ONE;
    frame.values.bandwidth_expansion = Decimal::ONE;
    frame.feature_versions.insert(
        BREAKOUT_OPPORTUNITY_VERSION_KEY.to_owned(),
        "pulse-breakout-opportunity-v1".to_owned(),
    );
    frame.breakout = Some(BreakoutOpportunity {
        schema_version: 1,
        generation: frame.generation,
        feature_version: "pulse-breakout-opportunity-v1".to_owned(),
        direction: BreakoutDirection::Long,
        boundary_id: format!("boundary-{boundary_sequence}"),
        boundary_sequence,
        compression_cycle_id: format!("compression-{compression_cycle_sequence}"),
        compression_cycle_sequence,
        detected_at_ms: fixture.breakout.initial_watermark_ms,
        valid_until_ms: 10_000,
    });
    Ok(frame)
}
