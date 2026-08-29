use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset, Price},
    execution::{
        SCALPING_ENTRY_QUOTE_SCHEMA_VERSION, ScalpingAdmissionFacts, ScalpingBoundExposure,
        ScalpingBoundLimits, ScalpingBoundQuoteAmount, ScalpingBoundRiskLimit, ScalpingEntryQuote,
        ScalpingPrivateAdmission, ScalpingQuoteAuthority, scalping_entry_quote_digest,
    },
    runtime::{
        SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION, ScalpingCoreQuoteReceipt,
        ScalpingCoreQuoteReceiptError, ScalpingCoreQuoteReceiptJournal,
        ScalpingCoreQuoteReceiptRecord, ScalpingCoreQuoteReceiptSource, scalping_candidate_digest,
        scalping_core_quote_receipt_digest,
    },
    strategy::scalping::{
        CandidatePreparation, Direction, EntryStyle, ExitTemplate, Expert, ExposureState,
        MarketRegime, ProtectionState, RiskLimit, RiskPlan, RiskUnit, SafetyProjection,
        SemanticIntent, SemanticPurpose, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "core-quote-receipt".to_owned(),
        run_id: "shadow-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio-margin".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "scalping-release-v1".to_owned(),
        owner_scope: "core-quote-receipt:shadow-1".to_owned(),
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

fn receipt() -> Result<ScalpingCoreQuoteReceipt, Box<dyn std::error::Error>> {
    let binding = binding()?;
    let digest = binding_digest(&binding);
    let risk_unit = RiskUnit::new("risk")?;
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
    let preparation = CandidatePreparation {
        preparation_id: "preparation-1".to_owned(),
        binding_digest: digest.clone(),
        controller_revision: 3,
        authority_generation: 7,
        market_regime: MarketRegime::Range,
        frame_generation: 11,
        watermark_ms: 100,
        valid_until_ms: 450,
        candidates: vec![candidate.clone()],
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
    let private = ScalpingPrivateAdmission {
        fact_id: "private-readback:7:100:1".to_owned(),
        generation: 7,
        observed_at_ms: 100,
        safety: SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Flat,
            execution_unknown: false,
            protection: ProtectionState::Complete,
            owner_conflict: false,
            risk_budget_available: true,
        },
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
    Ok(ScalpingCoreQuoteReceipt {
        schema_version: SCALPING_CORE_QUOTE_RECEIPT_SCHEMA_VERSION,
        binding,
        preparation_id: preparation.preparation_id.clone(),
        candidate_id: candidate.intent_id.clone(),
        candidate_digest: scalping_candidate_digest(&candidate)?,
        preparation,
        candidate,
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

fn refresh(
    mut receipt: ScalpingCoreQuoteReceipt,
) -> Result<ScalpingCoreQuoteReceipt, Box<dyn std::error::Error>> {
    receipt.quote.quote_id = "quote-2".to_owned();
    receipt.quote.generation = 14;
    receipt.quote.maker_fee_bps = Decimal::new(2, 1);
    receipt.quote_authority.quote_id = receipt.quote.quote_id.clone();
    receipt.quote_authority.quote_generation = receipt.quote.generation;
    receipt.quote_authority.quote_content_digest = scalping_entry_quote_digest(&receipt.quote)?;
    receipt.issued_at_ms = 110;
    receipt.received_at_ms = 110;
    receipt.core_sequence = 2;
    Ok(receipt)
}

#[test]
fn fsynced_append_exact_retry_and_reopen_lookup_are_stable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("core-quotes.jsonl");
    let receipt = receipt()?;
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&path, receipt.binding.clone())?;
    let first = journal.append(receipt.clone())?;
    assert_eq!(first.sequence, 1);
    assert_eq!(
        first.content_sha256,
        scalping_core_quote_receipt_digest(&receipt)?
    );
    assert_eq!(journal.append(receipt.clone())?, first);
    assert!(std::fs::read(&path)?.ends_with(b"\n"));
    drop(journal);

    let source = ScalpingCoreQuoteReceiptSource::open(&path, receipt.binding.clone())?;
    assert_eq!(
        source.lookup(&receipt.preparation, &receipt.candidate, 100)?,
        Some(first)
    );
    Ok(())
}

#[test]
fn refresh_is_latest_but_retry_of_older_exact_receipt_remains_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("core-quotes.jsonl");
    let first_receipt = receipt()?;
    let second_receipt = refresh(first_receipt.clone())?;
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&path, first_receipt.binding.clone())?;
    let first = journal.append(first_receipt.clone())?;
    let second = journal.append(second_receipt.clone())?;
    assert_eq!(second.sequence, 2);
    assert_eq!(journal.append(first_receipt.clone())?, first);

    let source = ScalpingCoreQuoteReceiptSource::open(&path, first_receipt.binding.clone())?;
    let selected = source
        .lookup(&first_receipt.preparation, &first_receipt.candidate, 110)?
        .ok_or("latest quote missing")?;
    assert_eq!(selected.receipt, second_receipt);
    Ok(())
}

#[test]
fn quote_or_core_identity_reuse_with_changed_content_is_conflicting()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("core-quotes.jsonl");
    let first = receipt()?;
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&path, first.binding.clone())?;
    journal.append(first.clone())?;

    let mut reused_quote_id = refresh(first.clone())?;
    reused_quote_id.quote.quote_id = first.quote.quote_id.clone();
    reused_quote_id.quote_authority.quote_id = reused_quote_id.quote.quote_id.clone();
    reused_quote_id.quote_authority.quote_content_digest =
        scalping_entry_quote_digest(&reused_quote_id.quote)?;
    assert!(matches!(
        journal.append(reused_quote_id),
        Err(ScalpingCoreQuoteReceiptError::Conflict)
    ));

    let mut reused_core_sequence = refresh(first)?;
    reused_core_sequence.core_sequence = 1;
    assert!(journal.append(reused_core_sequence).is_err());
    Ok(())
}

#[test]
fn candidate_rebinding_and_authority_clock_regression_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let base = receipt()?;

    let rebind_path = directory.path().join("candidate-rebind.jsonl");
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&rebind_path, base.binding.clone())?;
    journal.append(base.clone())?;
    let mut rebound = refresh(base.clone())?;
    rebound.preparation.preparation_id = "preparation-2".to_owned();
    rebound.preparation_id = rebound.preparation.preparation_id.clone();
    assert!(matches!(
        journal.append(rebound),
        Err(ScalpingCoreQuoteReceiptError::Conflict)
    ));

    let timing_path = directory.path().join("clock-regression.jsonl");
    let mut first = base.clone();
    first.issued_at_ms = 110;
    first.received_at_ms = 110;
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&timing_path, base.binding.clone())?;
    journal.append(first)?;
    let mut regressed = refresh(base)?;
    regressed.issued_at_ms = 105;
    regressed.received_at_ms = 105;
    assert!(matches!(
        journal.append(regressed),
        Err(ScalpingCoreQuoteReceiptError::Timing)
    ));
    Ok(())
}

#[test]
fn cross_binding_private_and_candidate_content_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let base = receipt()?;

    let mut cross_binding = base.clone();
    cross_binding.binding.account = "other-account".to_owned();
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(
        directory.path().join("binding.jsonl"),
        base.binding.clone(),
    )?;
    assert!(journal.append(cross_binding).is_err());

    let mut cross_private = base.clone();
    cross_private.private.fact_id = "private-other".to_owned();
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(
        directory.path().join("private.jsonl"),
        base.binding.clone(),
    )?;
    assert!(journal.append(cross_private).is_err());

    let path = directory.path().join("candidate.jsonl");
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&path, base.binding.clone())?;
    journal.append(base.clone())?;
    drop(journal);
    let source = ScalpingCoreQuoteReceiptSource::open(&path, base.binding.clone())?;
    let mut changed = base.candidate.clone();
    changed.max_hold_ms += 1;
    assert!(matches!(
        source.lookup(&base.preparation, &changed, 100),
        Err(ScalpingCoreQuoteReceiptError::Conflict)
    ));
    let mut missing = base.candidate.clone();
    missing.intent_id = "candidate-missing".to_owned();
    assert!(source.lookup(&base.preparation, &missing, 100)?.is_none());
    Ok(())
}

#[test]
fn ttl_boundary_is_exact_and_never_falls_back_to_an_external_substitute()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("core-quotes.jsonl");
    let receipt = receipt()?;
    let missing = ScalpingCoreQuoteReceiptSource::open(&path, receipt.binding.clone())?;
    assert!(
        missing
            .lookup(&receipt.preparation, &receipt.candidate, 100)?
            .is_none()
    );
    let mut journal = ScalpingCoreQuoteReceiptJournal::open(&path, receipt.binding.clone())?;
    journal.append(receipt.clone())?;
    drop(journal);
    let source = ScalpingCoreQuoteReceiptSource::open(&path, receipt.binding.clone())?;
    assert!(
        source
            .lookup(&receipt.preparation, &receipt.candidate, 125)?
            .is_some()
    );
    assert!(
        source
            .lookup(&receipt.preparation, &receipt.candidate, 126)?
            .is_none()
    );
    assert!(
        source
            .lookup(&receipt.preparation, &receipt.candidate, 99)
            .is_err()
    );
    Ok(())
}

#[test]
fn truncated_hash_and_sequence_damage_fail_closed_on_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let receipt = receipt()?;

    let truncated = directory.path().join("truncated.jsonl");
    std::fs::write(&truncated, b"{")?;
    assert!(matches!(
        ScalpingCoreQuoteReceiptSource::open(&truncated, receipt.binding.clone()),
        Err(ScalpingCoreQuoteReceiptError::Truncated)
    ));

    let bad_hash = directory.path().join("hash.jsonl");
    let record = ScalpingCoreQuoteReceiptRecord {
        sequence: 1,
        content_sha256: "0".repeat(64),
        receipt: receipt.clone(),
    };
    let mut encoded = serde_json::to_vec(&record)?;
    encoded.push(b'\n');
    std::fs::write(&bad_hash, encoded)?;
    assert!(matches!(
        ScalpingCoreQuoteReceiptSource::open(&bad_hash, receipt.binding.clone()),
        Err(ScalpingCoreQuoteReceiptError::Hash)
    ));

    let bad_sequence = directory.path().join("sequence.jsonl");
    let record = ScalpingCoreQuoteReceiptRecord {
        sequence: 2,
        content_sha256: scalping_core_quote_receipt_digest(&receipt)?,
        receipt: receipt.clone(),
    };
    let mut encoded = serde_json::to_vec(&record)?;
    encoded.push(b'\n');
    std::fs::write(&bad_sequence, encoded)?;
    assert!(matches!(
        ScalpingCoreQuoteReceiptSource::open(&bad_sequence, receipt.binding),
        Err(ScalpingCoreQuoteReceiptError::Sequence)
    ));
    Ok(())
}
