use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{Amount, Asset},
    runtime::{RiskProofClock, RiskRevaluationProducer, ScalpingShadowHost},
    storage::{ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskFact},
    strategy::scalping::{RiskFact, RiskUnit, ScalpingParams, StrategyBinding, StrategyKind},
};

fn strategy_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "scalping_primary".to_owned(),
        run_id: "shadow_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "SOL/USDT".parse()?,
        parameter_release_id: "scalping-shadow-v1".to_owned(),
        owner_scope: "scalping_primary:shadow_1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(10, 0)),
    })
}

fn risk_binding(
    strategy: &StrategyBinding,
    generation: u64,
) -> Result<ScalpingRiskBinding, Box<dyn std::error::Error>> {
    Ok(ScalpingRiskBinding {
        exchange: strategy.exchange.clone(),
        account: strategy.account.clone(),
        owner_scope: strategy.owner_scope.clone(),
        strategy_instance_id: strategy.strategy_instance_id.clone(),
        run_id: strategy.run_id.clone(),
        parameter_release_id: strategy.parameter_release_id.clone(),
        symbol: strategy.symbol.clone(),
        risk_unit: RiskUnit::new("risk")?,
        valuation_generation: generation,
    })
}

fn page(
    strategy: &StrategyBinding,
    generation: u64,
    source_sequence: u64,
    event_time_ms: u64,
) -> Result<(Vec<ScalpingRiskFact>, ScalpingRiskCursor), Box<dyn std::error::Error>> {
    let binding = risk_binding(strategy, generation)?;
    let fact_id = format!("risk-{generation}");
    let fact = ScalpingRiskFact {
        binding: binding.clone(),
        fact: RiskFact {
            fact_id: fact_id.clone(),
            event_time_ms,
            valuation_generation: generation,
            risk_unit: binding.risk_unit.clone(),
            realized_pnl: Decimal::ZERO,
        },
    };
    let cursor = ScalpingRiskCursor {
        cursor_id: format!("cursor-{generation}"),
        binding,
        source_sequence,
        complete_from_ms: 0,
        observed_through_ms: event_time_ms,
        has_more: false,
        source_fact_ids: vec![fact_id],
    };
    Ok((vec![fact], cursor))
}

fn revaluation_page(
    strategy: &StrategyBinding,
    generation: u64,
    source_sequence: u64,
    observed_through_ms: u64,
    facts: &[(&str, u64)],
) -> Result<(Vec<ScalpingRiskFact>, ScalpingRiskCursor), Box<dyn std::error::Error>> {
    let binding = risk_binding(strategy, generation)?;
    let revalued = facts
        .iter()
        .map(|(fact_id, event_time_ms)| ScalpingRiskFact {
            binding: binding.clone(),
            fact: RiskFact {
                fact_id: (*fact_id).to_owned(),
                event_time_ms: *event_time_ms,
                valuation_generation: generation,
                risk_unit: binding.risk_unit.clone(),
                realized_pnl: Decimal::ZERO,
            },
        })
        .collect::<Vec<_>>();
    let cursor = ScalpingRiskCursor {
        cursor_id: format!("cursor-{generation}"),
        binding,
        source_sequence,
        complete_from_ms: 0,
        observed_through_ms,
        has_more: false,
        source_fact_ids: facts
            .iter()
            .map(|(fact_id, _)| (*fact_id).to_owned())
            .collect(),
    };
    Ok((revalued, cursor))
}

fn clock(now_ms: u64) -> RiskProofClock {
    RiskProofClock {
        now_ms,
        max_stale_ms: 5,
    }
}

#[test]
fn durable_producer_to_host_recovery_keeps_one_exact_applied_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let journal_path = directory.path().join("risk.jsonl");
    let host_path = directory.path().join("shadow-host.json");
    let binding = strategy_binding()?;
    let params = ScalpingParams::shadow(binding.risk_budget.clone());
    let risk_unit = params.risk_per_episode.unit.clone();
    let mut producer = RiskRevaluationProducer::open(&journal_path, &binding, risk_unit.clone())?;
    let mut host =
        ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params.clone())?;

    let (facts, cursor) = page(&binding, 1, 1, 100)?;
    let first = producer
        .commit_page(clock(100), facts, cursor)?
        .ok_or("terminal page did not produce a proof")?;
    let first_report = host.on_bound_risk_revaluation(first.clone())?;
    assert_eq!(first_report.checkpoint.last_risk_cursor_sequence, Some(2));
    assert_eq!(
        first_report.checkpoint.last_risk_proof_id.as_deref(),
        Some(first.proof.proof_id.as_str())
    );

    drop(host);
    drop(producer);
    let mut producer = RiskRevaluationProducer::open(&journal_path, &binding, risk_unit)?;
    let mut host = ScalpingShadowHost::open_or_restore(&host_path, binding.clone(), params)?;
    assert_eq!(
        producer.recover_complete(clock(100), Some(&first.proof.proof_id))?,
        None
    );

    let (facts, cursor) =
        revaluation_page(&binding, 2, 2, 101, &[("risk-1", 100), ("risk-2", 101)])?;
    let second = producer
        .commit_page(clock(101), facts, cursor)?
        .ok_or("terminal page did not produce a proof")?;
    let second_report = host.on_bound_risk_revaluation(second.clone())?;
    assert_eq!(second_report.checkpoint.last_risk_cursor_sequence, Some(5));
    assert_eq!(
        second_report.checkpoint.last_risk_proof_id.as_deref(),
        Some(second.proof.proof_id.as_str())
    );
    assert_eq!(
        second_report.checkpoint.strategy.risk.valuation_generation,
        Some(2)
    );
    Ok(())
}
