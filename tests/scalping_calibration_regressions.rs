use rust_decimal::Decimal;
use venue::{
    domain::{Amount, Asset, Price},
    strategy::scalping::{
        CALIBRATION_SCHEMA_VERSION, CalibrationBook, CalibrationKey, CalibrationManifest,
        CalibrationSlice, CandidatePreparation, Direction, EntryStyle, EvidenceIdentity,
        ExitTemplate, Expert, FillSlice, MarketRegime, OutcomeProbabilities,
        RESEARCH_EVIDENCE_SCHEMA_VERSION, ResearchCheckStatus, ResearchEvidence, RiskLimit,
        RiskPlan, RiskUnit, ScalpingParams, SemanticIntent, SemanticPurpose, StrategyBinding,
        StrategyKind,
    },
};

struct Fixture {
    binding: StrategyBinding,
    params: ScalpingParams,
    manifest: CalibrationManifest,
    preparation: CandidatePreparation,
    candidate: SemanticIntent,
}

fn fixture(live_approved: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let symbol = "BTC/USDT".parse()?;
    let binding = StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "calibration_shadow".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol,
        parameter_release_id: "scalping-release-v1".to_owned(),
        owner_scope: "calibration_shadow:shadow_1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    };
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
            risk_per_episode: RiskLimit::new(RiskUnit::new("risk")?, Decimal::ONE),
            quote_cap: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
            max_episode_loss: RiskLimit::new(RiskUnit::new("risk")?, Decimal::ONE),
        },
        target_quote: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        reference_price: Price::new(Decimal::new(100, 0))?,
        max_slippage_bps: Decimal::ONE,
        valid_until_ms: 400,
        entry_ttl_ms: 1_000,
        hard_stop_distance_bps: Decimal::new(20, 0),
        target_distance_bps: Decimal::new(10, 0),
        max_hold_ms: 10_000,
        max_unprotected_ms: 1_500,
        requires_server_protection: true,
        opportunity_key: "range:long:maker".to_owned(),
        breakout_cursor: None,
        idempotency_seed: "candidate-seed".to_owned(),
    };
    let preparation = CandidatePreparation {
        preparation_id: "preparation-1".to_owned(),
        binding_digest: "c".repeat(64),
        controller_revision: 1,
        authority_generation: 4,
        market_regime: MarketRegime::Range,
        frame_generation: 3,
        watermark_ms: 100,
        valid_until_ms: 350,
        candidates: vec![candidate.clone()],
    };
    let key = CalibrationKey::from_candidate(&preparation, &candidate);
    let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
    params.calibration_model_version = "calibration-model-v7".to_owned();
    let slice = CalibrationSlice {
        key: key.clone(),
        release_id: binding.parameter_release_id.clone(),
        model_version: params.calibration_model_version.clone(),
        artifact_digest: String::new(),
        model_generation: 7,
        evidence_cursor_ms: 90,
        valid_from_ms: 50,
        valid_until_ms: 500,
        sample_count: 1_000,
        live_approved,
        fill_distribution: vec![
            FillSlice {
                fill_ratio: Decimal::ONE,
                probability: Decimal::new(8, 1),
            },
            FillSlice {
                fill_ratio: Decimal::ZERO,
                probability: Decimal::new(2, 1),
            },
        ],
        outcomes: OutcomeProbabilities {
            target: Decimal::new(8, 1),
            stop: Decimal::new(1, 1),
            other: Decimal::new(1, 1),
        },
        target_pnl_bps: Decimal::new(10, 0),
        stop_pnl_bps: Decimal::new(-5, 0),
        other_pnl_bps: Decimal::ZERO,
        nonfill_cancel_cost_bps: Decimal::new(1, 1),
        opportunity_cost_bps: Decimal::new(2, 1),
        ev_sigma_bps: Decimal::ONE,
    };
    let manifest = CalibrationManifest {
        schema_version: CALIBRATION_SCHEMA_VERSION,
        release_id: binding.parameter_release_id.clone(),
        model_version: params.calibration_model_version.clone(),
        artifact_digest: String::new(),
        research: ResearchEvidence {
            schema_version: RESEARCH_EVIDENCE_SCHEMA_VERSION,
            dataset_digest: "a".repeat(64),
            preregistration_digest: "b".repeat(64),
            evidence_cursor_ms: 100,
            approved_for_live: live_approved,
            slices: vec![venue::strategy::scalping::ResearchSliceEvidence {
                key,
                sample_count: 1_000,
                after_cost_ev_lower_bps: Decimal::ONE,
                fill_calibration: ResearchCheckStatus::Passed,
                cost_calibration: ResearchCheckStatus::Passed,
                markout_calibration: ResearchCheckStatus::Passed,
                stress_budget: ResearchCheckStatus::Passed,
            }],
        },
        slices: vec![slice],
    }
    .seal()?;
    params
        .calibration_model_digest
        .clone_from(&manifest.artifact_digest);
    Ok(Fixture {
        binding,
        params,
        manifest,
        preparation,
        candidate,
    })
}

fn open(
    binding: &StrategyBinding,
    params: &ScalpingParams,
    manifest: &CalibrationManifest,
) -> Result<CalibrationBook, venue::strategy::scalping::ScalpingError> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|_| venue::strategy::scalping::ScalpingError::Evidence)?;
    CalibrationBook::from_json(&bytes, binding, params)
}

fn reseal(
    fixture: &Fixture,
    manifest: CalibrationManifest,
) -> Result<(ScalpingParams, CalibrationManifest), Box<dyn std::error::Error>> {
    let manifest = manifest.seal()?;
    let mut params = fixture.params.clone();
    params
        .calibration_model_digest
        .clone_from(&manifest.artifact_digest);
    Ok((params, manifest))
}

fn identity(fixture: &Fixture) -> EvidenceIdentity {
    EvidenceIdentity {
        schema_version: 1,
        evidence_id: "calibration-evidence-1".to_owned(),
        candidate_id: fixture.candidate.intent_id.clone(),
        preparation_id: fixture.preparation.preparation_id.clone(),
        binding_digest: fixture.preparation.binding_digest.clone(),
        frame_generation: fixture.preparation.frame_generation,
        watermark_ms: fixture.preparation.watermark_ms,
        producer_generation: fixture.manifest.slices[0].model_generation,
        release_digest: fixture.manifest.artifact_digest.clone(),
        valid_until_ms: fixture.preparation.valid_until_ms,
    }
}

#[test]
fn manifest_is_content_addressed_tamper_evident_and_pin_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(false)?;
    assert!(open(&fixture.binding, &fixture.params, &fixture.manifest).is_ok());

    let mut differently_stamped = fixture.manifest.clone();
    differently_stamped.artifact_digest = "not-part-of-content".to_owned();
    differently_stamped.slices[0].artifact_digest = "also-cleared".to_owned();
    let resealed = differently_stamped.seal()?;
    assert_eq!(resealed.artifact_digest, fixture.manifest.artifact_digest);

    let mut tampered = fixture.manifest.clone();
    tampered.slices[0].sample_count += 1;
    assert!(open(&fixture.binding, &fixture.params, &tampered).is_err());

    let mut wrong_schema = fixture.manifest.clone();
    wrong_schema.schema_version += 1;
    let (params, wrong_schema) = reseal(&fixture, wrong_schema)?;
    assert!(open(&fixture.binding, &params, &wrong_schema).is_err());

    let mut unknown = serde_json::to_value(&fixture.manifest)?;
    unknown["slices"][0]["sample_cout"] = serde_json::json!(1_000);
    let bytes = serde_json::to_vec(&unknown)?;
    assert!(CalibrationBook::from_json(&bytes, &fixture.binding, &fixture.params).is_err());

    let mut wrong = fixture.params.clone();
    wrong.calibration_model_version = "wrong-model".to_owned();
    assert!(open(&fixture.binding, &wrong, &fixture.manifest).is_err());
    let mut wrong = fixture.params.clone();
    wrong.calibration_model_digest = "d".repeat(64);
    assert!(open(&fixture.binding, &wrong, &fixture.manifest).is_err());
    let mut wrong_binding = fixture.binding.clone();
    wrong_binding.parameter_release_id = "wrong-release".to_owned();
    assert!(open(&wrong_binding, &fixture.params, &fixture.manifest).is_err());
    Ok(())
}

#[test]
fn canonical_digest_ignores_equivalent_slice_order_and_preserves_params_pin()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(false)?;
    let mut two_slices = fixture.manifest.clone();
    let second_key = CalibrationKey {
        direction: Direction::Short,
        ..two_slices.slices[0].key.clone()
    };
    let mut second_slice = two_slices.slices[0].clone();
    second_slice.key = second_key.clone();
    two_slices.slices.push(second_slice);
    let mut second_research = two_slices.research.slices[0].clone();
    second_research.key = second_key;
    two_slices.research.slices.push(second_research);

    let (params, canonical) = reseal(&fixture, two_slices)?;
    let mut reordered = canonical.clone();
    reordered.slices.reverse();
    reordered.research.slices.reverse();
    let reordered = reordered.seal()?;

    assert_eq!(canonical.artifact_digest, reordered.artifact_digest);
    assert_eq!(params.calibration_model_digest, reordered.artifact_digest);
    assert!(open(&fixture.binding, &params, &canonical).is_ok());
    assert!(open(&fixture.binding, &params, &reordered).is_ok());
    Ok(())
}

#[test]
fn research_provenance_sample_cursor_and_unique_key_are_sealed()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(false)?;
    let mut variants = Vec::new();

    let mut invalid = fixture.manifest.clone();
    invalid.research.dataset_digest = "not-a-digest".to_owned();
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid.research.preregistration_digest = "short".to_owned();
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid.research.evidence_cursor_ms = 0;
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid.research.slices[0].sample_count += 1;
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    let mut extra_research = invalid.research.slices[0].clone();
    extra_research.key.direction = Direction::Short;
    invalid.research.slices.push(extra_research);
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    let mut extra_slice = invalid.slices[0].clone();
    extra_slice.key.direction = Direction::Short;
    invalid.slices.push(extra_slice);
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid.slices[0].evidence_cursor_ms = 101;
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid
        .research
        .slices
        .push(invalid.research.slices[0].clone());
    variants.push(invalid);
    let mut invalid = fixture.manifest.clone();
    invalid.slices.push(invalid.slices[0].clone());
    variants.push(invalid);

    for manifest in variants {
        let (params, manifest) = reseal(&fixture, manifest)?;
        assert!(open(&fixture.binding, &params, &manifest).is_err());
    }
    Ok(())
}

#[test]
fn live_slice_requires_positive_after_cost_and_every_research_check()
-> Result<(), Box<dyn std::error::Error>> {
    let live_fixture = fixture(true)?;
    let book = open(
        &live_fixture.binding,
        &live_fixture.params,
        &live_fixture.manifest,
    )?;
    assert!(
        book.lookup(&live_fixture.preparation, &live_fixture.candidate, true)
            .is_ok()
    );

    for failed_check in 0..4 {
        let mut manifest = live_fixture.manifest.clone();
        let research = &mut manifest.research.slices[0];
        match failed_check {
            0 => research.fill_calibration = ResearchCheckStatus::Failed,
            1 => research.cost_calibration = ResearchCheckStatus::Failed,
            2 => research.markout_calibration = ResearchCheckStatus::Failed,
            _ => research.stress_budget = ResearchCheckStatus::Failed,
        }
        let (params, manifest) = reseal(&live_fixture, manifest)?;
        assert!(open(&live_fixture.binding, &params, &manifest).is_err());
    }
    let mut manifest = live_fixture.manifest.clone();
    manifest.research.slices[0].after_cost_ev_lower_bps = Decimal::ZERO;
    let (params, manifest) = reseal(&live_fixture, manifest)?;
    assert!(open(&live_fixture.binding, &params, &manifest).is_err());
    let mut manifest = live_fixture.manifest.clone();
    manifest.research.approved_for_live = false;
    let (params, manifest) = reseal(&live_fixture, manifest)?;
    assert!(open(&live_fixture.binding, &params, &manifest).is_err());

    let shadow_only = fixture(false)?;
    let book = open(
        &shadow_only.binding,
        &shadow_only.params,
        &shadow_only.manifest,
    )?;
    assert!(
        book.lookup(&shadow_only.preparation, &shadow_only.candidate, true)
            .is_err()
    );
    Ok(())
}

#[test]
fn slice_validation_preserves_legacy_probability_time_pnl_and_cost_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(false)?;
    let key = CalibrationKey::from_candidate(&fixture.preparation, &fixture.candidate);
    let valid = fixture.manifest.slices[0].clone();
    assert!(
        valid
            .validate_for(
                &fixture.binding,
                &fixture.params,
                &key,
                fixture.preparation.watermark_ms,
                false,
            )
            .is_ok()
    );
    let rejects = |slice: CalibrationSlice| {
        slice
            .validate_for(
                &fixture.binding,
                &fixture.params,
                &key,
                fixture.preparation.watermark_ms,
                false,
            )
            .is_err()
    };

    let mut slice = valid.clone();
    slice.evidence_cursor_ms = 101;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.valid_from_ms = 101;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.valid_until_ms = 99;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.sample_count = 0;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.fill_distribution[0].probability = Decimal::new(7, 1);
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.fill_distribution[0].fill_ratio = Decimal::new(11, 1);
    assert!(rejects(slice));
    let mut slice = valid.clone();
    for fill in &mut slice.fill_distribution {
        fill.fill_ratio = Decimal::ZERO;
    }
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.outcomes.other = Decimal::new(2, 1);
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.target_pnl_bps = Decimal::ZERO;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.stop_pnl_bps = Decimal::ZERO;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.nonfill_cancel_cost_bps = Decimal::NEGATIVE_ONE;
    assert!(rejects(slice));
    let mut slice = valid.clone();
    slice.opportunity_cost_bps = Decimal::NEGATIVE_ONE;
    assert!(rejects(slice));
    let mut slice = valid;
    slice.ev_sigma_bps = Decimal::NEGATIVE_ONE;
    assert!(rejects(slice));
    Ok(())
}

#[test]
fn exact_key_lookup_projects_only_calibration_with_caller_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(false)?;
    let book = open(&fixture.binding, &fixture.params, &fixture.manifest)?;
    assert_eq!(
        book.artifact_digest(),
        fixture.params.calibration_model_digest
    );
    assert_eq!(book.research_dataset_digest(), "a".repeat(64));
    assert_eq!(book.preregistration_digest(), "b".repeat(64));

    let projected = book.project_evidence(
        &fixture.preparation,
        &fixture.candidate,
        identity(&fixture),
        false,
    )?;
    assert_eq!(
        projected.evidence.model_version,
        fixture.params.calibration_model_version
    );
    assert_eq!(projected.evidence.uncertainty_bps, Decimal::ONE);
    assert_eq!(projected.evidence.identity.producer_generation, 7);
    assert_eq!(
        projected.cost_priors.nonfill_cancel_cost_bps,
        fixture.manifest.slices[0].nonfill_cancel_cost_bps
    );
    assert_eq!(
        projected.cost_priors.opportunity_cost_bps,
        fixture.manifest.slices[0].opportunity_cost_bps
    );

    let mut wrong_candidate = fixture.candidate.clone();
    wrong_candidate.direction = Direction::Short;
    assert!(
        book.lookup(&fixture.preparation, &wrong_candidate, false)
            .is_err()
    );
    let mut wrong_preparation = fixture.preparation.clone();
    wrong_preparation.market_regime = MarketRegime::TrendUp;
    assert!(
        book.lookup(&wrong_preparation, &fixture.candidate, false)
            .is_err()
    );
    let mut wrong_candidate = fixture.candidate.clone();
    wrong_candidate.expert = Expert::TrendPullback;
    assert!(
        book.lookup(&fixture.preparation, &wrong_candidate, false)
            .is_err()
    );
    let mut wrong_candidate = fixture.candidate.clone();
    wrong_candidate.entry_style = EntryStyle::MarketableLimit;
    assert!(
        book.lookup(&fixture.preparation, &wrong_candidate, false)
            .is_err()
    );
    let mut wrong_candidate = fixture.candidate.clone();
    wrong_candidate.symbol = "ETH/USDT".parse()?;
    assert!(
        book.lookup(&fixture.preparation, &wrong_candidate, false)
            .is_err()
    );

    let mut wrong = identity(&fixture);
    wrong.release_digest = "d".repeat(64);
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.producer_generation += 1;
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.candidate_id = "different-candidate".to_owned();
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.preparation_id = "different-preparation".to_owned();
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.binding_digest = "e".repeat(64);
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.frame_generation += 1;
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.watermark_ms += 1;
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.valid_until_ms = fixture.preparation.watermark_ms - 1;
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut wrong = identity(&fixture);
    wrong.valid_until_ms -= 1;
    assert!(
        book.project_evidence(&fixture.preparation, &fixture.candidate, wrong, false)
            .is_err()
    );
    let mut invented = fixture.candidate.clone();
    invented.intent_id = "invented-same-key".to_owned();
    let mut invented_identity = identity(&fixture);
    invented_identity
        .candidate_id
        .clone_from(&invented.intent_id);
    assert!(
        book.project_evidence(&fixture.preparation, &invented, invented_identity, false,)
            .is_err()
    );
    Ok(())
}
