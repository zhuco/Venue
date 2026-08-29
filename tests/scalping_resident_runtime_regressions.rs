use std::{collections::BTreeMap, path::Path};

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    controller::{
        ControlAuthority, ControlTarget, InstanceControlRecord, InstanceControlStore,
        ScalpingControllerSource, ScalpingControllerUpdate,
    },
    domain::{Amount, Asset, Price},
    indicator::{
        BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
        TRADES_SOURCE,
    },
    runtime::{
        BoundRiskRevaluation, CustodyStatus, DeadlineClockObservation, EntryDisposition,
        LifecycleReport, PrivateEntryGateReport, PrivateFacts, ScalpingResidentCycle,
        ScalpingResidentMarket, ScalpingResidentRuntime, ScalpingResidentRuntimeError,
        ScalpingShadowHost, ScalpingShadowHostError, ShadowDisposition,
    },
    storage::ScalpingRiskBinding,
    strategy::scalping::{
        ExposureState, ProtectionState, RiskRevaluation, RiskUnit, SafetyProjection,
        ScalpingParams, StrategyBinding, StrategyKind,
    },
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "resident_shadow".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "resident_shadow:shadow_1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn private_gate() -> PrivateEntryGateReport {
    private_gate_at(1, 100)
}

fn private_gate_at(generation: u64, observed_at_ms: u64) -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Armed,
            control: venue::runtime::ControlDisposition::None,
        },
        entry_ready: true,
        forwarded_private: Some(PrivateFacts {
            generation,
            observed_at_ms,
            root_cause_fact_id: format!("private-readback:{generation}:{observed_at_ms}:0"),
            safety: SafetyProjection {
                private_snapshot_ready: true,
                exposure: ExposureState::Flat,
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

fn controller_update(
    root: &Path,
    binding: &StrategyBinding,
) -> Result<ScalpingControllerUpdate, Box<dyn std::error::Error>> {
    let control_path = root.join("controller.json");
    let cursor_path = root.join("controller_source.json");
    let record = InstanceControlRecord {
        schema_version: 1,
        binding: binding.clone(),
        target: ControlTarget::Running,
        command_id: "resident-control".to_owned(),
        idempotency_key: "resident-control-1".to_owned(),
        safety_deadline_ms: Some(1_000),
        revision: 1,
    };
    InstanceControlStore::new(&control_path).save(&record, None)?;
    let mut source = ScalpingControllerSource::open(control_path, cursor_path, binding.clone())?;
    Ok(source.observe(
        Some(&ControlAuthority {
            generation: 1,
            parameter_release_id: binding.parameter_release_id.clone(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        }),
        100,
    )?)
}

fn frame() -> Result<FeatureFrame, Box<dyn std::error::Error>> {
    Ok(FeatureFrame {
        symbol: "SOL/USDT".parse()?,
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

fn market() -> Result<ScalpingResidentMarket, Box<dyn std::error::Error>> {
    Ok(ScalpingResidentMarket {
        frame: frame()?,
        decision_at_ms: 100,
        evidence: Vec::new(),
        direct_admission: false,
    })
}

fn wrong_bound_risk(
    binding: &StrategyBinding,
) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    Ok(BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: "wrong-account".to_owned(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: unit.clone(),
            valuation_generation: 1,
        },
        proof: RiskRevaluation {
            proof_id: "wrong-bound-proof".to_owned(),
            target_generation: 1,
            risk_unit: unit,
            window_start_ms: 100,
            complete_through_ms: 100,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        },
        cursor_sequence: 1,
    })
}

fn bound_risk_through(
    binding: &StrategyBinding,
    complete_through_ms: u64,
) -> Result<BoundRiskRevaluation, Box<dyn std::error::Error>> {
    let unit = RiskUnit::new("risk")?;
    Ok(BoundRiskRevaluation {
        binding: ScalpingRiskBinding {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            owner_scope: binding.owner_scope.clone(),
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            parameter_release_id: binding.parameter_release_id.clone(),
            symbol: binding.symbol.clone(),
            risk_unit: unit.clone(),
            valuation_generation: 1,
        },
        proof: RiskRevaluation {
            proof_id: format!("proof-through-{complete_through_ms}"),
            target_generation: 1,
            risk_unit: unit,
            window_start_ms: complete_through_ms,
            complete_through_ms,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        },
        cursor_sequence: 1,
    })
}

#[test]
fn private_reconciliation_precedes_market_in_the_same_cycle()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);
    let report = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(controller_update(directory.path(), &binding)?),
        private_gate: Some(private_gate()),
        market: Some(market()?),
        ..ScalpingResidentCycle::default()
    })?;

    assert_eq!(
        report.private_gate.map(|value| value.disposition),
        Some(ShadowDisposition::ShadowOnly)
    );
    assert_eq!(
        report.market.map(|value| value.disposition),
        Some(ShadowDisposition::ShadowOnly)
    );
    assert_eq!(
        runtime.host().checkpoint().strategy.last_watermark_ms,
        Some(100)
    );
    Ok(())
}

#[test]
fn deadline_error_aborts_lower_priority_market_work() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);
    let result = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(controller_update(directory.path(), &binding)?),
        private_gate: Some(private_gate()),
        deadline_clock: Some(DeadlineClockObservation {
            now_ms: 0,
            root_cause_fact_id: "invalid-clock".to_owned(),
        }),
        market: Some(market()?),
        ..ScalpingResidentCycle::default()
    });

    assert!(matches!(
        result,
        Err(ScalpingResidentRuntimeError::Deadline(_))
    ));
    assert_eq!(runtime.host().checkpoint().strategy.last_watermark_ms, None);
    Ok(())
}

#[test]
fn control_fence_precedes_and_blocks_market_work() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);
    let mut gate = private_gate();
    gate.entry_ready = false;
    gate.forwarded_private = None;
    gate.control = Some(ControlTarget::StopAndProtect);
    let report = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(controller_update(directory.path(), &binding)?),
        private_gate: Some(gate),
        market: Some(market()?),
        ..ScalpingResidentCycle::default()
    })?;

    assert_eq!(
        report.market.map(|value| value.disposition),
        Some(ShadowDisposition::RemainFenced)
    );
    assert_eq!(runtime.host().checkpoint().strategy.last_watermark_ms, None);
    Ok(())
}

#[test]
fn forwarded_private_is_persisted_before_a_rejected_risk_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);

    let result = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(controller_update(directory.path(), &binding)?),
        private_gate: Some(private_gate()),
        risk: Some(wrong_bound_risk(&binding)?),
        ..ScalpingResidentCycle::default()
    });

    assert!(matches!(
        result,
        Err(ScalpingResidentRuntimeError::Host(
            ScalpingShadowHostError::RiskBinding
        ))
    ));
    assert_eq!(runtime.host().checkpoint().last_private_generation, Some(1));
    assert_eq!(
        runtime.host().checkpoint().last_private_observed_at_ms,
        Some(100)
    );
    Ok(())
}

#[test]
fn same_cycle_old_risk_proof_cannot_cover_new_private_or_reach_market()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);

    let result = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(controller_update(directory.path(), &binding)?),
        private_gate: Some(private_gate_at(1, 200)),
        risk: Some(bound_risk_through(&binding, 100)?),
        market: Some(market()?),
        ..ScalpingResidentCycle::default()
    });

    assert!(matches!(
        result,
        Err(ScalpingResidentRuntimeError::Host(
            ScalpingShadowHostError::RiskWatermark
        ))
    ));
    let checkpoint = runtime.host().checkpoint();
    assert_eq!(checkpoint.last_private_observed_at_ms, Some(200));
    assert_eq!(checkpoint.last_risk_cursor_sequence, None);
    assert_eq!(checkpoint.strategy.last_watermark_ms, None);
    Ok(())
}

#[test]
fn durable_missing_target_stops_then_explicit_running_update_resumes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let control_path = directory.path().join("controller.json");
    let cursor_path = directory.path().join("controller_source.json");
    let mut source = ScalpingControllerSource::open(&control_path, &cursor_path, binding.clone())?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let host = ScalpingShadowHost::open_or_restore(
        directory.path().join("host.json"),
        binding.clone(),
        params,
    )?;
    let mut runtime = ScalpingResidentRuntime::new(host);

    let stopped = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(source.observe(None, 50)?),
        ..ScalpingResidentCycle::default()
    })?;
    assert_eq!(
        stopped.controller_control.map(|report| report.disposition),
        Some(ShadowDisposition::StopAndProtect)
    );

    InstanceControlStore::new(&control_path).save(
        &InstanceControlRecord {
            schema_version: 1,
            binding: binding.clone(),
            target: ControlTarget::Running,
            command_id: "resume-control".to_owned(),
            idempotency_key: "resume-control-1".to_owned(),
            safety_deadline_ms: Some(1_000),
            revision: 1,
        },
        None,
    )?;
    let resumed = source.observe(
        Some(&ControlAuthority {
            generation: 1,
            parameter_release_id: binding.parameter_release_id.clone(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        }),
        100,
    )?;
    let report = runtime.drive_cycle(ScalpingResidentCycle {
        controller: Some(resumed),
        private_gate: Some(private_gate()),
        market: Some(market()?),
        ..ScalpingResidentCycle::default()
    })?;
    assert_eq!(
        report.market.map(|output| output.disposition),
        Some(ShadowDisposition::ShadowOnly)
    );
    Ok(())
}
