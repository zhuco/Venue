use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue::{
    domain::{Amount, Asset, Price},
    execution::{
        SCALPING_ENTRY_QUOTE_SCHEMA_VERSION, ScalpingAdmissionFacts, ScalpingBoundExposure,
        ScalpingBoundLimits, ScalpingBoundQuoteAmount, ScalpingBoundRiskLimit, ScalpingEntryQuote,
        ScalpingPrivateAdmission, ScalpingQuoteAuthority, scalping_entry_quote_digest,
        validate_scalping_bound_limits, validate_scalping_entry_quote,
    },
    runtime::{AppliedRiskReceipt, BoundRiskRevaluation, project_scalping_entry_evidence},
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        CalibrationCostPriors, CalibrationEvidence, CalibrationProjection, CandidatePreparation,
        Direction, EntryStyle, EvidenceIdentity, ExitTemplate, Expert, ExposureState, FillSlice,
        MarketRegime, OutcomeProbabilities, ProtectionState, RiskLimit, RiskPlan, RiskRevaluation,
        RiskUnit, SafetyProjection, SemanticIntent, SemanticPurpose, StrategyBinding, StrategyKind,
        risk_revaluation_digest,
    },
};

struct Fixture {
    binding: StrategyBinding,
    preparation: CandidatePreparation,
    candidate: SemanticIntent,
    calibration: CalibrationProjection,
    limits: ScalpingBoundLimits,
    private: ScalpingPrivateAdmission,
    quote_authority: ScalpingQuoteAuthority,
    quote: ScalpingEntryQuote,
    risk: BoundRiskRevaluation,
    risk_receipt: AppliedRiskReceipt,
}

fn fixture(direction: Direction, style: EntryStyle) -> Result<Fixture, Box<dyn std::error::Error>> {
    let risk_unit = RiskUnit::new("risk")?;
    let binding = StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "scalping-primary".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio-margin".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-release-v1".to_owned(),
        owner_scope: "scalping-primary:shadow-1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(10, 0)),
    };
    let candidate = SemanticIntent {
        intent_id: "candidate-1".to_owned(),
        symbol: binding.symbol.clone(),
        direction,
        purpose: SemanticPurpose::Entry,
        expert: Expert::RangeFade,
        entry_style: style,
        exit_template: ExitTemplate::FairValue,
        attempt_cap: 2,
        max_reprices: u32::from(style == EntryStyle::PassiveMaker),
        risk_plan: RiskPlan {
            risk_per_episode: RiskLimit::new(risk_unit.clone(), Decimal::new(5, 0)),
            quote_cap: Amount::new("USDT".parse::<Asset>()?, Decimal::new(10, 0)),
            max_episode_loss: RiskLimit::new(risk_unit.clone(), Decimal::new(8, 0)),
        },
        target_quote: Amount::new("USDT".parse::<Asset>()?, Decimal::new(8, 0)),
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
    let binding_digest = binding_digest(&binding);
    let preparation = CandidatePreparation {
        preparation_id: "preparation-1".to_owned(),
        binding_digest: binding_digest.clone(),
        controller_revision: 3,
        authority_generation: 7,
        market_regime: MarketRegime::Range,
        frame_generation: 11,
        watermark_ms: 100,
        valid_until_ms: 450,
        candidates: vec![candidate.clone()],
    };
    let calibration = CalibrationProjection {
        evidence: CalibrationEvidence {
            identity: EvidenceIdentity {
                schema_version: 1,
                evidence_id: "calibration-evidence".to_owned(),
                candidate_id: candidate.intent_id.clone(),
                preparation_id: preparation.preparation_id.clone(),
                binding_digest: binding_digest.clone(),
                frame_generation: preparation.frame_generation,
                watermark_ms: preparation.watermark_ms,
                producer_generation: 3,
                release_digest: "a".repeat(64),
                valid_until_ms: 430,
            },
            model_version: "legacy-calibration-v1".to_owned(),
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
                target: Decimal::new(7, 1),
                stop: Decimal::new(2, 1),
                other: Decimal::new(1, 1),
            },
            target_pnl_bps: Decimal::new(20, 0),
            stop_pnl_bps: Decimal::new(-10, 0),
            other_pnl_bps: Decimal::ZERO,
            uncertainty_bps: Decimal::new(5, 1),
        },
        cost_priors: CalibrationCostPriors {
            nonfill_cancel_cost_bps: Decimal::new(3, 1),
            opportunity_cost_bps: Decimal::new(4, 1),
        },
    };
    let limits = ScalpingBoundLimits {
        risk_per_episode: bound_risk(&risk_unit, 5, 7),
        quote_cap: ScalpingBoundQuoteAmount {
            amount: Amount::new("USDT".parse::<Asset>()?, Decimal::new(6, 0)),
            generation: 7,
        },
        max_episode_loss: bound_risk(&risk_unit, 8, 7),
        worst_loss_at_quote_cap: bound_risk(&risk_unit, 4, 7),
    };
    let safety = SafetyProjection {
        private_snapshot_ready: true,
        exposure: ExposureState::Flat,
        execution_unknown: false,
        protection: ProtectionState::Complete,
        owner_conflict: false,
        risk_budget_available: true,
    };
    let private = ScalpingPrivateAdmission {
        fact_id: "private-readback:7:100:1".to_owned(),
        generation: 7,
        observed_at_ms: 100,
        safety,
    };
    let quote = ScalpingEntryQuote {
        schema_version: SCALPING_ENTRY_QUOTE_SCHEMA_VERSION,
        quote_id: "quote-1".to_owned(),
        quote_release_digest: "b".repeat(64),
        binding_digest,
        symbol: candidate.symbol.clone(),
        direction,
        entry_style: style,
        target_quote: limits.quote_cap.amount.clone(),
        bound_limits_generation: 7,
        generation: 13,
        capability_generation: 7,
        valid_until_ms: 420,
        admission: ScalpingAdmissionFacts {
            fact_id: private.fact_id.clone(),
            generation: 7,
            observed_at_ms: 100,
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
        funding_bps: match direction {
            Direction::Long => Decimal::new(7, 1),
            Direction::Short => Decimal::new(-7, 1),
        },
        price_tick: Price::new(Decimal::new(1, 2))?,
        max_executable_price: match style {
            EntryStyle::PassiveMaker => candidate.reference_price,
            EntryStyle::MarketableLimit => match direction {
                Direction::Long => Price::new(Decimal::new(10_001, 2))?,
                Direction::Short => Price::new(Decimal::new(9_999, 2))?,
            },
        },
        worst_loss: bound_risk(&risk_unit, 4, 7),
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
    let proof = RiskRevaluation {
        proof_id: "risk-proof-1".to_owned(),
        target_generation: 7,
        risk_unit: risk_unit.clone(),
        window_start_ms: 1,
        complete_through_ms: 100,
        source_fact_ids: Vec::new(),
        revalued_facts: Vec::new(),
    };
    let risk_digest = risk_revaluation_digest(&proof)?;
    let risk = BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit,
            valuation_generation: 7,
        },
        proof,
        cursor_sequence: 1,
    };
    let risk_receipt = AppliedRiskReceipt {
        binding: binding.clone(),
        proof_id: risk.proof.proof_id.clone(),
        cursor_sequence: risk.cursor_sequence,
        risk_revaluation_digest: risk_digest,
        target_generation: risk.proof.target_generation,
        valuation_generation: risk.binding.valuation_generation,
    };
    Ok(Fixture {
        binding,
        preparation,
        candidate,
        calibration,
        limits,
        private,
        quote_authority,
        quote,
        risk,
        risk_receipt,
    })
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

fn project(
    fixture: &Fixture,
) -> Result<
    venue::runtime::ScalpingEntryEvidenceProjection,
    venue::runtime::ScalpingEntryEvidenceError,
> {
    project_scalping_entry_evidence(
        &fixture.binding,
        &fixture.preparation,
        &fixture.candidate,
        &fixture.calibration,
        &fixture.limits,
        &fixture.private,
        &fixture.quote_authority,
        &fixture.quote,
        &fixture.risk,
        &fixture.risk_receipt,
        100,
    )
}

#[test]
fn legacy_style_costs_preserve_priors_and_directional_funding()
-> Result<(), Box<dyn std::error::Error>> {
    let passive = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    let passive = project(&passive)?;
    assert_eq!(passive.bundle.costs.entry_cost_bps, Decimal::new(5, 1));
    assert_eq!(passive.bundle.costs.exit_cost_bps, Decimal::new(14, 1));
    assert_eq!(passive.bundle.costs.funding_cost_bps, Decimal::new(7, 1));
    assert_eq!(passive.bundle.costs.nonfill_cost_bps, Decimal::new(3, 1));
    assert_eq!(
        passive.bundle.costs.opportunity_cost_bps,
        Decimal::new(4, 1)
    );

    let marketable = fixture(Direction::Short, EntryStyle::MarketableLimit)?;
    let marketable = project(&marketable)?;
    assert_eq!(marketable.bundle.costs.entry_cost_bps, Decimal::new(9, 1));
    assert_eq!(
        marketable.bundle.costs.funding_cost_bps,
        Decimal::new(-7, 1)
    );
    assert_eq!(
        marketable.bundle.risk.identity.release_digest,
        marketable.bundle.risk.policy_digest
    );
    assert_eq!(marketable.bundle.costs.identity.producer_generation, 13);
    assert_eq!(marketable.bundle.risk.identity.producer_generation, 7);
    assert_eq!(marketable.candidate.valid_until_ms, 420);
    Ok(())
}

#[test]
fn marketable_tick_boundaries_apply_in_both_directions() -> Result<(), Box<dyn std::error::Error>> {
    let mut long = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    assert!(project(&long).is_ok());
    long.quote.max_executable_price = Price::new(Decimal::new(10_002, 2))?;
    assert!(project(&long).is_err());

    let mut short = fixture(Direction::Short, EntryStyle::MarketableLimit)?;
    assert!(project(&short).is_ok());
    short.quote.max_executable_price = Price::new(Decimal::new(9_998, 2))?;
    assert!(project(&short).is_err());
    short.quote.max_executable_price = Price::new(Decimal::new(99_995, 3))?;
    assert!(project(&short).is_err());
    Ok(())
}

#[test]
fn bound_limits_reject_generation_unit_and_cap_expansion() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    let mut limits = fixture.limits.clone();
    limits.quote_cap.generation += 1;
    assert!(validate_scalping_bound_limits(&fixture.candidate, &limits).is_err());

    let mut limits = fixture.limits.clone();
    limits.max_episode_loss.limit.unit = RiskUnit::new("other")?;
    assert!(validate_scalping_bound_limits(&fixture.candidate, &limits).is_err());

    let mut limits = fixture.limits.clone();
    limits.quote_cap.amount.value = fixture.candidate.target_quote.value + Decimal::ONE;
    assert!(validate_scalping_bound_limits(&fixture.candidate, &limits).is_err());

    let mut limits = fixture.limits.clone();
    limits.worst_loss_at_quote_cap.limit.value = Decimal::new(6, 0);
    assert!(validate_scalping_bound_limits(&fixture.candidate, &limits).is_err());
    Ok(())
}

#[test]
fn quote_fences_ttl_cost_worst_loss_and_exact_private_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &fixture.quote,
            0
        )
        .is_err()
    );
    let mut expired = fixture.quote.clone();
    expired.valid_until_ms = 99;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &expired,
            100
        )
        .is_err()
    );
    let mut zero_ttl = fixture.quote.clone();
    zero_ttl.valid_until_ms = 0;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &zero_ttl,
            100
        )
        .is_err()
    );

    let mut negative_fee = fixture.quote.clone();
    negative_fee.taker_fee_bps = Decimal::NEGATIVE_ONE;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &negative_fee,
            100
        )
        .is_err()
    );

    let mut excess_loss = fixture.quote.clone();
    excess_loss.worst_loss.limit.value = Decimal::new(6, 0);
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &excess_loss,
            100
        )
        .is_err()
    );

    let mut zero_loss = fixture.quote.clone();
    zero_loss.worst_loss.limit.value = Decimal::ZERO;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &zero_loss,
            100
        )
        .is_err()
    );

    let mut wrong_authority = fixture.quote_authority.clone();
    wrong_authority.quote_release_digest = "c".repeat(64);
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &wrong_authority,
            &fixture.quote,
            100
        )
        .is_err()
    );

    let mut replaced_quote = fixture.quote.clone();
    replaced_quote.quote_id = "quote-2".to_owned();
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &replaced_quote,
            100
        )
        .is_err()
    );
    let mut repriced_without_receipt = fixture.quote.clone();
    repriced_without_receipt.maker_fee_bps += Decimal::new(1, 1);
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &fixture.quote_authority,
            &repriced_without_receipt,
            100
        )
        .is_err()
    );
    let mut wrong_authority = fixture.quote_authority.clone();
    wrong_authority.quote_generation += 1;
    assert!(
        validate_scalping_entry_quote(
            &fixture.preparation,
            &fixture.candidate,
            &fixture.limits,
            &fixture.private,
            &wrong_authority,
            &fixture.quote,
            100
        )
        .is_err()
    );

    for mutate in 0..5 {
        let mut quote = fixture.quote.clone();
        match mutate {
            0 => quote.admission.fact_id.push_str("-wrong"),
            1 => quote.admission.open_permission_generation += 1,
            2 => quote.capability_generation += 1,
            3 => quote.admission.execution_unknown = true,
            _ => quote.admission.protection_gap.value = Decimal::ONE,
        }
        assert!(
            validate_scalping_entry_quote(
                &fixture.preparation,
                &fixture.candidate,
                &fixture.limits,
                &fixture.private,
                &fixture.quote_authority,
                &quote,
                100
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn projector_rejects_wrong_binding_risk_digest_and_stale_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let mut wrong_binding = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    wrong_binding.preparation.binding_digest = "f".repeat(64);
    wrong_binding.quote.binding_digest = "f".repeat(64);
    wrong_binding.calibration.evidence.identity.binding_digest = "f".repeat(64);
    assert!(project(&wrong_binding).is_err());

    let mut wrong_digest = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    wrong_digest.risk_receipt.risk_revaluation_digest = "d".repeat(64);
    assert!(project(&wrong_digest).is_err());

    let mut stale = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    stale.risk.proof.complete_through_ms = 99;
    stale.risk_receipt.risk_revaluation_digest = risk_revaluation_digest(&stale.risk.proof)?;
    assert!(project(&stale).is_err());

    let mut wrong_generation = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    wrong_generation.risk.binding.valuation_generation = 8;
    assert!(project(&wrong_generation).is_err());

    let mut wrong_receipt = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    wrong_receipt.risk_receipt.proof_id.push_str("-other");
    assert!(project(&wrong_receipt).is_err());

    let mut wrong_receipt = fixture(Direction::Long, EntryStyle::PassiveMaker)?;
    wrong_receipt.risk_receipt.cursor_sequence += 1;
    assert!(project(&wrong_receipt).is_err());
    Ok(())
}

#[test]
fn quote_fences_funding_stress_private_freshness_and_slippage_domain()
-> Result<(), Box<dyn std::error::Error>> {
    let mut funding = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    funding.quote_authority.max_funding_abs_bps = Decimal::new(6, 1);
    assert!(project(&funding).is_err());

    let stale = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    assert!(
        validate_scalping_entry_quote(
            &stale.preparation,
            &stale.candidate,
            &stale.limits,
            &stale.private,
            &stale.quote_authority,
            &stale.quote,
            126
        )
        .is_err()
    );

    let mut future = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    future.private.observed_at_ms = 101;
    future.quote.admission.observed_at_ms = 101;
    future.quote_authority.quote_content_digest = scalping_entry_quote_digest(&future.quote)?;
    assert!(
        validate_scalping_entry_quote(
            &future.preparation,
            &future.candidate,
            &future.limits,
            &future.private,
            &future.quote_authority,
            &future.quote,
            100
        )
        .is_err()
    );

    let mut slippage = fixture(Direction::Long, EntryStyle::MarketableLimit)?;
    slippage.candidate.max_slippage_bps = Decimal::new(10_000, 0);
    slippage.preparation.candidates[0] = slippage.candidate.clone();
    assert!(project(&slippage).is_err());
    Ok(())
}
