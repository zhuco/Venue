use rust_decimal::Decimal;
use venue::{
    domain::{
        Amount, Asset, CommandId, FieldState, Instrument, MarketKind, OrderOwner, OrderPurpose,
        OrderSide, Position, PositionSide, Price, StopMarketFullPositionCommand, Symbol,
    },
    exchange::binance_private::{AlgoOrderReadback, ConditionalStrategyStatus, parse_algo_order},
    execution::{
        AlgoProtectionCustodyInput, CustodyWriterRole, ProtectionCustodyError, ProtectionEvidence,
        WriterScope, WriterSession, prove_algo_protection_custody,
    },
    risk::authorize_stop_market_full_position,
};

fn command() -> Result<StopMarketFullPositionCommand, Box<dyn std::error::Error>> {
    Ok(StopMarketFullPositionCommand {
        command_id: CommandId::new("protect_algo_1")?,
        client_algo_id: CommandId::new("client_algo_1")?,
        owner: OrderOwner {
            strategy_instance_id: "manual_canary".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: "SOL/USDT".parse()?,
            purpose: OrderPurpose::Protection,
        },
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(7, 2),
        trigger_price: Price::new(Decimal::new(70, 0))?,
        position_generation: 7,
    })
}

fn position(command: &StopMarketFullPositionCommand) -> Position {
    Position {
        symbol: command.owner.symbol.clone(),
        side: command.position_side,
        quantity: command.quantity,
        entry_price: None,
        mark_price: None,
    }
}

fn algo(command: &StopMarketFullPositionCommand) -> AlgoOrderReadback {
    AlgoOrderReadback {
        algo_id: "42".to_owned(),
        client_algo_id: command.client_algo_id.as_str().to_owned(),
        status: ConditionalStrategyStatus::Current,
        order_type: FieldState::Known("STOP_MARKET".to_owned()),
        side: FieldState::Known(command.side),
        position_side: FieldState::Known(command.position_side),
        quantity: FieldState::Known(command.quantity),
        trigger_price: FieldState::Known(command.trigger_price),
        working_type: FieldState::Known("MARK_PRICE".to_owned()),
        close_position: FieldState::Missing,
        reduce_only: FieldState::Known(true),
    }
}

fn writer(command: &StopMarketFullPositionCommand) -> WriterSession {
    WriterSession {
        scope: WriterScope {
            exchange: command.owner.exchange.clone(),
            account: command.owner.account.clone(),
            symbol: command.owner.symbol.clone(),
            owner_scope: "canary_run_1".to_owned(),
        },
        token: "a".repeat(64),
        generation: 2,
        revision: 1,
        readback_generation: 7,
        valid_until_ms: 2_000,
    }
}

#[test]
fn algo_stop_requires_the_exact_full_authoritative_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let position = position(&command);
    let algo = algo(&command);
    let writer = writer(&command);
    let custody = prove_algo_protection_custody(AlgoProtectionCustodyInput {
        command: &command,
        position: &position,
        algo: &algo,
        writer: &writer,
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
    })?;
    assert_eq!(custody.full_position_quantity, Decimal::new(7, 2));

    let mut smaller = position.clone();
    smaller.quantity = Decimal::new(6, 2);
    assert!(matches!(
        prove_algo_protection_custody(AlgoProtectionCustodyInput {
            command: &command,
            position: &smaller,
            algo: &algo,
            writer: &writer,
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
        }),
        Err(ProtectionCustodyError::Position)
    ));
    let mut unsafe_wire = algo;
    unsafe_wire.reduce_only = FieldState::Known(false);
    assert!(matches!(
        prove_algo_protection_custody(AlgoProtectionCustodyInput {
            command: &command,
            position: &position,
            algo: &unsafe_wire,
            writer: &writer,
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
        }),
        Err(ProtectionCustodyError::Strategy)
    ));
    Ok(())
}

#[test]
fn fresh_algo_custody_survives_an_expired_entry_lease() -> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let position = position(&command);
    let algo = algo(&command);
    let mut writer = writer(&command);
    writer.valid_until_ms = 1_050;
    let custody = prove_algo_protection_custody(AlgoProtectionCustodyInput {
        command: &command,
        position: &position,
        algo: &algo,
        writer: &writer,
        evidence: ProtectionEvidence {
            private_generation: 7,
            readback_generation: 7,
            valid_until_ms: 1_500,
            observed_at_ms: 1_100,
        },
        writer_role: CustodyWriterRole {
            predecessor_protected: false,
            protection_only: false,
        },
        now_ms: 1_100,
    })?;
    assert_eq!(custody.valid_until_ms, 1_500);
    assert!(!custody.permits_entry());
    Ok(())
}

#[test]
fn newer_private_generation_can_reconfirm_existing_algo_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let position = position(&command);
    let algo = algo(&command);
    let writer = writer(&command);
    let custody = prove_algo_protection_custody(AlgoProtectionCustodyInput {
        command: &command,
        position: &position,
        algo: &algo,
        writer: &writer,
        evidence: ProtectionEvidence {
            private_generation: 9,
            readback_generation: 9,
            valid_until_ms: 1_500,
            observed_at_ms: 1_100,
        },
        writer_role: CustodyWriterRole {
            predecessor_protected: false,
            protection_only: false,
        },
        now_ms: 1_100,
    })?;
    assert_eq!(custody.private_generation, 9);
    assert_eq!(custody.writer_generation, 2);
    Ok(())
}

#[test]
fn algo_readback_parser_selects_exact_identity_and_preserves_safety_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "SOL/USDT".parse()?;
    let payload = r#"[{"algoId":41,"clientAlgoId":"other","algoStatus":"NEW","symbol":"SOLUSDT"},{"algoId":42,"clientAlgoId":"client_algo_1","algoStatus":"NEW","orderType":"STOP_MARKET","symbol":"SOLUSDT","side":"SELL","positionSide":"LONG","quantity":"0.07","triggerPrice":"70","workingType":"MARK_PRICE","reduceOnly":true}]"#;
    let parsed = parse_algo_order(payload, &symbol, "client_algo_1")?;
    assert_eq!(parsed.algo_id, "42");
    assert_eq!(parsed.quantity, FieldState::Known(Decimal::new(7, 2)));
    assert_eq!(parsed.close_position, FieldState::Missing);
    assert_eq!(parsed.reduce_only, FieldState::Known(true));
    assert!(parse_algo_order(payload, &symbol, "missing").is_err());
    Ok(())
}

#[test]
fn risk_rejects_partial_algo_protection() -> Result<(), Box<dyn std::error::Error>> {
    let mut command = command()?;
    let asset: Asset = "USDT".parse()?;
    let instrument = Instrument {
        symbol: command.owner.symbol.clone(),
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(asset.clone()),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 2))?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: Amount::new(asset, Decimal::new(5, 0)),
    };
    let position = position(&command);
    authorize_stop_market_full_position(&command, &instrument, &position)?;
    command.quantity = Decimal::new(6, 2);
    assert!(authorize_stop_market_full_position(&command, &instrument, &position).is_err());
    Ok(())
}
