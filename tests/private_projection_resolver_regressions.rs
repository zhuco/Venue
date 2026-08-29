use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{
        Amount, Asset, CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, Position,
        PositionSide, Price, StopMarketFullPositionCommand,
    },
    execution::{
        AlgoProtectionCustody, CommandJournal, CommandState, PrivateProjectionResolverInput,
        WriterScope, WriterSession, resolve_private_facts_projection,
    },
    risk::AccountRiskView,
    runtime::{
        ExecutionProjection, OwnerProjection, PrivateExposure, PrivateFactsReadiness,
        ProtectionProjection, RiskBudgetProjection,
    },
    strategy::scalping::{StrategyBinding, StrategyKind},
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: StrategyKind::Scalping,
        strategy_instance_id: "instance_1".to_owned(),
        run_id: "run_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "release_1".to_owned(),
        owner_scope: "instance_1:run_1".to_owned(),
        risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
    })
}

fn readiness(ordinary_order_debt: bool, algo_order_debt: bool) -> PrivateFactsReadiness {
    PrivateFactsReadiness {
        generation: 7,
        observed_at_ms: 100,
        root_cause_fact_id: "private-readback:7:100:0".to_owned(),
        exposure: PrivateExposure::Flat,
        ordinary_order_debt,
        algo_order_debt,
    }
}

fn writer(binding: &StrategyBinding) -> WriterSession {
    WriterSession {
        scope: WriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: binding.owner_scope.clone(),
        },
        token: "writer_token".to_owned(),
        generation: 9,
        revision: 1,
        readback_generation: 7,
        valid_until_ms: 200,
    }
}

fn account(asset: &str, value: Decimal) -> Result<AccountRiskView, Box<dyn std::error::Error>> {
    Ok(AccountRiskView {
        available_margin: Amount::new(asset.parse()?, value),
        unresolved_commands: 0,
    })
}

fn positions(binding: &StrategyBinding, long: Decimal, short: Decimal) -> Vec<Position> {
    vec![
        Position {
            symbol: binding.symbol.clone(),
            side: PositionSide::Long,
            quantity: long,
            entry_price: None,
            mark_price: None,
        },
        Position {
            symbol: binding.symbol.clone(),
            side: PositionSide::Short,
            quantity: short,
            entry_price: None,
            mark_price: None,
        },
    ]
}

fn accepted_place(
    journal: &mut CommandJournal,
    binding: &StrategyBinding,
    client_id: &str,
    account: &str,
) -> Result<CommandId, Box<dyn std::error::Error>> {
    let command_id = CommandId::new(format!("cmd_{client_id}"))?;
    let client_order_id = CommandId::new(client_id)?;
    journal.prepare_place(OrderCommand {
        command_id: command_id.clone(),
        client_order_id: client_order_id.clone(),
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: account.to_owned(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        reduce_only: false,
    })?;
    journal.transition(&command_id, CommandState::Submitted)?;
    journal.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: format!("venue_{client_id}"),
        },
    )?;
    Ok(client_order_id)
}

fn accepted_algo(
    journal: &mut CommandJournal,
    binding: &StrategyBinding,
    client_id: &str,
) -> Result<CommandId, Box<dyn std::error::Error>> {
    let command_id = CommandId::new(format!("cmd_{client_id}"))?;
    let client_algo_id = CommandId::new(client_id)?;
    journal.prepare_stop_market_full_position(StopMarketFullPositionCommand {
        command_id: command_id.clone(),
        client_algo_id: client_algo_id.clone(),
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Protection,
        },
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        trigger_price: Price::new(Decimal::new(99, 0))?,
        position_generation: 7,
    })?;
    journal.transition(&command_id, CommandState::Submitted)?;
    journal.transition(
        &command_id,
        CommandState::Accepted {
            venue_order_id: format!("venue_{client_id}"),
        },
    )?;
    Ok(client_algo_id)
}

fn custody(binding: &StrategyBinding, client_algo_id: &CommandId) -> AlgoProtectionCustody {
    AlgoProtectionCustody {
        command_id: format!("cmd_{}", client_algo_id.as_str()),
        client_algo_id: client_algo_id.as_str().to_owned(),
        venue_algo_id: "venue_algo".to_owned(),
        symbol: binding.symbol.clone(),
        position_side: PositionSide::Long,
        full_position_quantity: Decimal::ONE,
        private_generation: 7,
        writer_generation: 9,
        valid_until_ms: 200,
        content_sha256: "a".repeat(64),
    }
}

#[test]
fn emits_one_same_generation_projection_from_clean_authority_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let binding = binding()?;
    let risk = account("USDT", Decimal::new(5, 0))?;
    let positions = positions(&binding, Decimal::ZERO, Decimal::ZERO);
    let output = resolve_private_facts_projection(PrivateProjectionResolverInput {
        binding: &binding,
        readiness: readiness(false, false),
        positions: &positions,
        open_ordinary_client_ids: &[],
        open_algo_client_ids: &[],
        journal: &journal,
        writer: None,
        algo_custodies: &[],
        account_risk: Some(&risk),
        now_ms: 101,
    });

    assert_eq!(output.execution.value, ExecutionProjection::Known);
    assert_eq!(output.owner.value, OwnerProjection::Clear);
    assert_eq!(output.protection.value, ProtectionProjection::Complete);
    assert_eq!(output.risk_budget.value, RiskBudgetProjection::Available);
    assert_eq!(
        [
            output.execution.generation,
            output.owner.generation,
            output.protection.generation,
            output.risk_budget.generation,
        ],
        [7; 4]
    );
    assert_eq!(
        [
            output.execution.observed_at_ms,
            output.owner.observed_at_ms,
            output.protection.observed_at_ms,
            output.risk_budget.observed_at_ms,
        ],
        [100; 4]
    );
    Ok(())
}

#[test]
fn foreign_owner_conflicts_and_unresolved_wal_is_execution_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let risk = account("USDT", Decimal::new(5, 0))?;
    let mut journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let positions = positions(&binding, Decimal::ZERO, Decimal::ZERO);
    let foreign_id = accepted_place(&mut journal, &binding, "foreign_client", "secondary")?;
    let session = writer(&binding);
    let foreign = resolve_private_facts_projection(PrivateProjectionResolverInput {
        binding: &binding,
        readiness: readiness(true, false),
        positions: &positions,
        open_ordinary_client_ids: &[foreign_id],
        open_algo_client_ids: &[],
        journal: &journal,
        writer: Some(&session),
        algo_custodies: &[],
        account_risk: Some(&risk),
        now_ms: 101,
    });
    assert_eq!(foreign.execution.value, ExecutionProjection::Known);
    assert_eq!(foreign.owner.value, OwnerProjection::Conflict);

    let unresolved_id = CommandId::new("unresolved_client")?;
    let unresolved_command = OrderCommand {
        command_id: CommandId::new("unresolved_command")?,
        client_order_id: unresolved_id.clone(),
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        reduce_only: false,
    };
    journal.prepare_place(unresolved_command)?;
    let unresolved = resolve_private_facts_projection(PrivateProjectionResolverInput {
        binding: &binding,
        readiness: readiness(true, false),
        positions: &positions,
        open_ordinary_client_ids: &[unresolved_id],
        open_algo_client_ids: &[],
        journal: &journal,
        writer: Some(&session),
        algo_custodies: &[],
        account_risk: Some(&risk),
        now_ms: 101,
    });
    assert_eq!(unresolved.execution.value, ExecutionProjection::Unknown);
    Ok(())
}

#[test]
fn rejects_expired_partial_or_duplicate_algo_custody() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let binding = binding()?;
    let risk = account("USDT", Decimal::new(5, 0))?;
    let mut journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let algo_id = accepted_algo(&mut journal, &binding, "algo_protect_1")?;
    let session = writer(&binding);
    let positions = positions(&binding, Decimal::ONE, Decimal::ZERO);
    let mut open_readiness = readiness(false, true);
    open_readiness.exposure = PrivateExposure::Open;
    let complete = custody(&binding, &algo_id);
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: open_readiness.clone(),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: std::slice::from_ref(&algo_id),
            journal: &journal,
            writer: Some(&session),
            algo_custodies: std::slice::from_ref(&complete),
            account_risk: Some(&risk),
            now_ms: 101,
        })
        .protection
        .value,
        ProtectionProjection::Complete
    );
    let mut expired = custody(&binding, &algo_id);
    expired.valid_until_ms = 101;
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: open_readiness.clone(),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: std::slice::from_ref(&algo_id),
            journal: &journal,
            writer: Some(&session),
            algo_custodies: std::slice::from_ref(&expired),
            account_risk: Some(&risk),
            now_ms: 101,
        })
        .protection
        .value,
        ProtectionProjection::Gap
    );
    let mut partial = custody(&binding, &algo_id);
    partial.full_position_quantity = Decimal::new(5, 1);
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: open_readiness.clone(),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: std::slice::from_ref(&algo_id),
            journal: &journal,
            writer: Some(&session),
            algo_custodies: std::slice::from_ref(&partial),
            account_risk: Some(&risk),
            now_ms: 101,
        })
        .protection
        .value,
        ProtectionProjection::Gap
    );
    let duplicate = [complete.clone(), complete.clone()];
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: open_readiness.clone(),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: std::slice::from_ref(&algo_id),
            journal: &journal,
            writer: Some(&session),
            algo_custodies: &duplicate,
            account_risk: Some(&risk),
            now_ms: 101,
        })
        .protection
        .value,
        ProtectionProjection::Gap
    );

    let extra_algo = accepted_algo(&mut journal, &binding, "algo_protect_extra")?;
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: open_readiness,
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: &[algo_id, extra_algo],
            journal: &journal,
            writer: Some(&session),
            algo_custodies: std::slice::from_ref(&complete),
            account_risk: Some(&risk),
            now_ms: 101,
        })
        .protection
        .value,
        ProtectionProjection::Gap
    );
    Ok(())
}

#[test]
fn budget_shortfall_is_unavailable_and_asset_mismatch_is_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let journal = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let binding = binding()?;
    let short = account("USDT", Decimal::new(49, 1))?;
    let wrong_asset = account("USDC", Decimal::new(10, 0))?;
    let positions = positions(&binding, Decimal::ZERO, Decimal::ZERO);
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: readiness(false, false),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: &[],
            journal: &journal,
            writer: None,
            algo_custodies: &[],
            account_risk: Some(&short),
            now_ms: 101,
        })
        .risk_budget
        .value,
        RiskBudgetProjection::Unavailable
    );
    assert_eq!(
        resolve_private_facts_projection(PrivateProjectionResolverInput {
            binding: &binding,
            readiness: readiness(false, false),
            positions: &positions,
            open_ordinary_client_ids: &[],
            open_algo_client_ids: &[],
            journal: &journal,
            writer: None,
            algo_custodies: &[],
            account_risk: Some(&wrong_asset),
            now_ms: 101,
        })
        .risk_budget
        .value,
        RiskBudgetProjection::Unknown
    );
    Ok(())
}
