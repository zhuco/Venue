use rust_decimal::Decimal;
use venue::{
    domain::{
        CommandId, FieldState, OrderOwner, OrderPurpose, OrderSide, Position, PositionSide, Price,
        StopMarketCloseAllCommand, Symbol,
    },
    exchange::binance_private::{ConditionalStrategyReadback, ConditionalStrategyStatus},
    execution::{
        CustodyWriterRole, ProtectionCustodyError, ProtectionCustodyInput, ProtectionEvidence,
        WriterScope, WriterSession, prove_protection_custody,
    },
};

fn command(side: PositionSide) -> Result<StopMarketCloseAllCommand, Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let order_side = match side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err("NET is not a hedge protection side".into()),
    };
    Ok(StopMarketCloseAllCommand {
        command_id: CommandId::new("protect_1")?,
        client_strategy_id: CommandId::new("client_protect_1")?,
        owner: OrderOwner {
            strategy_instance_id: "scalping_1".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol,
            purpose: OrderPurpose::Protection,
        },
        side: order_side,
        position_side: side,
        stop_price: Price::new(Decimal::new(99, 0))?,
        position_generation: 7,
    })
}

fn writer(command: &StopMarketCloseAllCommand, generation: u64) -> WriterSession {
    WriterSession {
        scope: WriterScope {
            exchange: command.owner.exchange.clone(),
            account: command.owner.account.clone(),
            symbol: command.owner.symbol.clone(),
            owner_scope: "scalping_run_1".to_owned(),
        },
        token: "a".repeat(64),
        generation,
        revision: 1,
        readback_generation: 7,
        valid_until_ms: 2_000,
    }
}

fn strategy(command: &StopMarketCloseAllCommand) -> ConditionalStrategyReadback {
    ConditionalStrategyReadback {
        strategy_id: "venue_strategy_1".to_owned(),
        status: ConditionalStrategyStatus::Current,
        side: FieldState::Known(command.side),
        position_side: FieldState::Known(command.position_side),
        stop_price: FieldState::Known(command.stop_price),
        close_position: FieldState::Known(true),
    }
}

fn position(command: &StopMarketCloseAllCommand, quantity: Decimal) -> Position {
    Position {
        symbol: command.owner.symbol.clone(),
        side: command.position_side,
        quantity,
        entry_price: None,
        mark_price: None,
    }
}

fn input<'a>(
    command: &'a StopMarketCloseAllCommand,
    position: &'a Position,
    strategy: &'a ConditionalStrategyReadback,
    writer: &'a WriterSession,
) -> ProtectionCustodyInput<'a> {
    ProtectionCustodyInput {
        command,
        position,
        strategy,
        writer,
        evidence: ProtectionEvidence {
            private_generation: 7,
            readback_generation: 7,
            valid_until_ms: 1_500,
            observed_at_ms: 1_000,
        },
        writer_role: CustodyWriterRole {
            predecessor_protected: false,
            protection_only: false,
        },
        now_ms: 1_100,
    }
}

#[test]
fn long_and_short_close_all_custody_preserves_the_full_authoritative_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    for (side, quantity) in [
        (PositionSide::Long, Decimal::new(25, 1)),
        (PositionSide::Short, Decimal::new(125, 2)),
    ] {
        let command = command(side)?;
        let writer = writer(&command, 7);
        let position = position(&command, quantity);
        let strategy = strategy(&command);
        let custody = prove_protection_custody(input(&command, &position, &strategy, &writer))?;
        assert_eq!(custody.position_side, side);
        assert_eq!(custody.full_position_quantity, quantity);
        assert_eq!(custody.valid_until_ms, 1_500);
        assert!(!custody.permits_entry());
        assert!(custody.permits_protection_or_stop());
        assert_eq!(custody.content_sha256.len(), 64);
    }
    Ok(())
}

#[test]
fn partial_or_wrong_side_position_never_counts_as_full_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command(PositionSide::Long)?;
    let writer = writer(&command, 7);
    let mut position = position(&command, Decimal::ZERO);
    let strategy = strategy(&command);
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &strategy, &writer)),
        Err(ProtectionCustodyError::Position)
    ));
    position.quantity = Decimal::ONE;
    position.side = PositionSide::Short;
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &strategy, &writer)),
        Err(ProtectionCustodyError::Position)
    ));
    position.side = PositionSide::Long;
    let mut wrong_strategy_side = strategy.clone();
    wrong_strategy_side.side = FieldState::Known(OrderSide::Buy);
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &wrong_strategy_side, &writer)),
        Err(ProtectionCustodyError::Strategy)
    ));
    Ok(())
}

#[test]
fn unknown_cancelled_and_expired_evidence_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let command = command(PositionSide::Long)?;
    let writer = writer(&command, 7);
    let position = position(&command, Decimal::ONE);
    let mut strategy = strategy(&command);
    strategy.status = ConditionalStrategyStatus::Unknown;
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &strategy, &writer)),
        Err(ProtectionCustodyError::Strategy)
    ));
    strategy.status = ConditionalStrategyStatus::Cancelled;
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &strategy, &writer)),
        Err(ProtectionCustodyError::Strategy)
    ));
    strategy.status = ConditionalStrategyStatus::NonCancelledTerminal;
    assert!(matches!(
        prove_protection_custody(input(&command, &position, &strategy, &writer)),
        Err(ProtectionCustodyError::Strategy)
    ));
    strategy.status = ConditionalStrategyStatus::Current;
    let mut expired = input(&command, &position, &strategy, &writer);
    expired.evidence.valid_until_ms = 1_100;
    assert!(matches!(
        prove_protection_custody(expired),
        Err(ProtectionCustodyError::Evidence)
    ));
    Ok(())
}

#[test]
fn readback_generation_and_protected_predecessor_role_are_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command(PositionSide::Short)?;
    let mut wrong_generation_writer = writer(&command, 8);
    wrong_generation_writer.readback_generation = 8;
    let position = position(&command, Decimal::ONE);
    let strategy = strategy(&command);
    assert!(matches!(
        prove_protection_custody(input(
            &command,
            &position,
            &strategy,
            &wrong_generation_writer
        )),
        Err(ProtectionCustodyError::Evidence)
    ));
    let writer = writer(&command, 7);
    let mut protected = input(&command, &position, &strategy, &writer);
    protected.writer_role = CustodyWriterRole {
        predecessor_protected: true,
        protection_only: false,
    };
    assert!(matches!(
        prove_protection_custody(protected),
        Err(ProtectionCustodyError::ProtectedPredecessor)
    ));
    Ok(())
}
