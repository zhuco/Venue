use rust_decimal::Decimal;
use venue::{
    domain::{
        AccountRiskSnapshot, Amount, Asset, CommandId, ExecutionCommand, Instrument,
        LegRiskSnapshot, MarketKind, MarketReduceCommand, OrderOwner, OrderPurpose, OrderSide,
        PositionSide, Price, RiskSourceStatus,
    },
    execution::{CommandJournal, CommandJournalError, CommandState},
    risk::{RiskError, authorize_market_reduction},
};

struct Fixture {
    command: MarketReduceCommand,
    instrument: Instrument,
    account: AccountRiskSnapshot,
    leg: LegRiskSnapshot,
}

fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let currency: Asset = "USDT".parse()?;
    let symbol: venue::domain::Symbol = "DOGE/USDT".parse()?;
    Ok(Fixture {
        command: MarketReduceCommand {
            command_id: CommandId::new("risk_reduce_1")?,
            client_order_id: CommandId::new("venue_risk_reduce_1")?,
            owner: OrderOwner {
                strategy_instance_id: "hedged_grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "gate".to_owned(),
                account: "usdt_futures".to_owned(),
                symbol: symbol.clone(),
                purpose: OrderPurpose::ExposureTakeProfit,
            },
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::new(180, 0),
            risk_episode_id: CommandId::new("risk_episode_1")?,
            position_generation: 23,
        },
        instrument: Instrument {
            symbol: symbol.clone(),
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(currency.clone()),
            generation: 3,
            price_tick: Price::new(Decimal::new(1, 4))?,
            quantity_step: Decimal::ONE,
            minimum_notional: Amount::new(currency.clone(), Decimal::new(5, 0)),
        },
        account: AccountRiskSnapshot {
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            risk_currency: currency.clone(),
            account_equity: Decimal::new(22, 0),
            private_generation: 23,
            observed_at_ms: 1_000,
            source_status: RiskSourceStatus::Complete,
        },
        leg: LegRiskSnapshot {
            symbol,
            position_side: PositionSide::Long,
            quantity: Decimal::new(600, 0),
            mark_price: Price::new(Decimal::new(1, 1))?,
            contract_multiplier: Decimal::ONE,
            notional: Decimal::new(60, 0),
            unrealized_pnl: Decimal::new(111, 2),
            risk_currency: currency,
            private_generation: 23,
            observed_at_ms: 1_000,
        },
    })
}

#[test]
fn approved_market_reduce_is_durable_and_unknown_blocks_another_reduction()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture()?;
    authorize_market_reduction(
        &fixture.command,
        &fixture.instrument,
        &fixture.account,
        &fixture.leg,
        4_000,
        3_000,
    )?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("commands.jsonl");
    let mut journal = CommandJournal::open(&path)?;
    journal.prepare_market_reduce(fixture.command.clone())?;
    journal.transition(&fixture.command.command_id, CommandState::Submitted)?;

    let mut recovered = CommandJournal::open(path)?;
    assert_eq!(recovered.fence_interrupted_dispatches()?, (0, 1));
    let receipt = recovered
        .receipt(&fixture.command.command_id)
        .ok_or(CommandJournalError::Missing)?;
    assert_eq!(
        receipt.command,
        ExecutionCommand::MarketReduce(fixture.command)
    );
    assert!(matches!(receipt.state, CommandState::Unknown { .. }));
    assert!(recovered.has_unresolved_entry_or_reduce());
    Ok(())
}

#[test]
fn reduction_is_rejected_when_quantity_generation_or_freshness_is_unproved()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture()?;
    fixture.command.quantity = Decimal::new(601, 0);
    assert_eq!(
        authorize_market_reduction(
            &fixture.command,
            &fixture.instrument,
            &fixture.account,
            &fixture.leg,
            4_000,
            3_000,
        ),
        Err(RiskError::Position)
    );
    fixture.command.quantity = Decimal::new(180, 0);
    fixture.command.position_generation = 24;
    assert_eq!(
        authorize_market_reduction(
            &fixture.command,
            &fixture.instrument,
            &fixture.account,
            &fixture.leg,
            4_000,
            3_000,
        ),
        Err(RiskError::PositionGeneration)
    );
    fixture.command.position_generation = 23;
    assert!(matches!(
        authorize_market_reduction(
            &fixture.command,
            &fixture.instrument,
            &fixture.account,
            &fixture.leg,
            4_001,
            3_000,
        ),
        Err(RiskError::RiskSnapshot(_))
    ));
    Ok(())
}

#[test]
fn settlement_minimum_is_not_distorted_by_risk_currency_conversion()
-> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = fixture()?;
    fixture.command.quantity = Decimal::new(50, 0);
    fixture.leg.contract_multiplier = Decimal::new(99, 2);
    fixture.leg.notional = Decimal::new(5940, 2);
    let approval = authorize_market_reduction(
        &fixture.command,
        &fixture.instrument,
        &fixture.account,
        &fixture.leg,
        4_000,
        3_000,
    )?;
    assert_eq!(approval.notional.value, Decimal::new(495, 2));
    Ok(())
}
