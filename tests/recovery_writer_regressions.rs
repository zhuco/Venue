use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{
        Amount, Asset, CommandId, Instrument, MarketKind, NativeOrderFamily, OrderCommand,
        OrderOwner, OrderPurpose, OrderSide, PositionSide, Price,
    },
    execution::{
        CanaryRunBinding, CommandJournal, RecoveryCancelInput, RecoveryObservationProof,
        RecoveryReduceInput, RecoveryWriterAuthority, RecoveryWriterScope, WriterLeaseAuthority,
        WriterScope, authorize_recovery_cancel, authorize_recovery_reduce,
    },
};

#[test]
fn exact_cancel_is_derived_only_from_the_durable_target_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let entry = entry_command()?;
    commands.prepare_place(entry.clone())?;
    let binding = binding()?;
    let proof = proof();
    let authorization = authorize_recovery_cancel(RecoveryCancelInput {
        binding: &binding,
        original_command_id: entry.command_id.as_str(),
        client_id: entry.client_order_id.as_str(),
        family: NativeOrderFamily::UmOrder,
        commands: &commands,
        proof: &proof,
        now_ms: 200,
    })?;
    assert_eq!(
        authorization.command.target_client_order_id,
        entry.client_order_id
    );
    assert_eq!(authorization.command.owner, entry.owner);

    assert!(
        authorize_recovery_cancel(RecoveryCancelInput {
            binding: &binding,
            original_command_id: entry.command_id.as_str(),
            client_id: "different_client",
            family: NativeOrderFamily::UmOrder,
            commands: &commands,
            proof: &proof,
            now_ms: 200,
        })
        .is_err()
    );
    Ok(())
}

#[test]
fn reduction_is_full_hedge_side_ioc_authority_and_rejects_unresolved_risk()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let commands = CommandJournal::open(directory.path().join("clean.jsonl"))?;
    let binding = binding()?;
    let proof = proof();
    let instrument = instrument()?;
    let authorization = authorize_recovery_reduce(RecoveryReduceInput {
        binding: &binding,
        position_side: PositionSide::Long,
        quantity: Decimal::new(5, 2),
        instrument: &instrument,
        market_price: Price::new(Decimal::new(100, 0))?,
        market_price_valid_until_ms: 400,
        commands: &commands,
        proof: &proof,
        now_ms: 200,
    })?;
    assert_eq!(authorization.command.side, OrderSide::Sell);
    assert!(authorization.command.reduce_only);
    assert_eq!(
        authorization.command.quantity,
        authorization.position.quantity
    );

    let mut unresolved = CommandJournal::open(directory.path().join("unresolved.jsonl"))?;
    unresolved.prepare_place(entry_command()?)?;
    assert!(
        authorize_recovery_reduce(RecoveryReduceInput {
            binding: &binding,
            position_side: PositionSide::Long,
            quantity: Decimal::new(5, 2),
            instrument: &instrument,
            market_price: Price::new(Decimal::new(100, 0))?,
            market_price_valid_until_ms: 400,
            commands: &unresolved,
            proof: &proof,
            now_ms: 200,
        })
        .is_err()
    );
    Ok(())
}

#[test]
fn recovery_and_normal_writer_share_the_same_os_dispatch_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let authority_path = directory.path().join("writer.json");
    let binding = binding()?;
    let normal_scope = WriterScope {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        owner_scope: "manual_canary_mainnet".to_owned(),
    };
    let normal = WriterLeaseAuthority::open(&authority_path, normal_scope)?;
    let session = normal.register_initial(100, binding.readback_generation)?;
    let normal_guard = normal.dispatch_guard(&session, 101)?;

    let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let entry = entry_command()?;
    commands.prepare_place(entry.clone())?;
    let authorization = authorize_recovery_cancel(RecoveryCancelInput {
        binding: &binding,
        original_command_id: entry.command_id.as_str(),
        client_id: entry.client_order_id.as_str(),
        family: NativeOrderFamily::UmOrder,
        commands: &commands,
        proof: &proof(),
        now_ms: 200,
    })?;
    let recovery = RecoveryWriterAuthority::open(
        &authority_path,
        RecoveryWriterScope {
            exchange: binding.exchange,
            account: binding.account,
            symbol: binding.symbol,
        },
    )?;
    assert!(recovery.dispatch_cancel(&authorization, 200).is_err());
    drop(normal_guard);
    assert!(recovery.dispatch_cancel(&authorization, 200).is_ok());
    Ok(())
}

#[test]
fn stale_or_unsigned_observation_never_gets_recovery_power()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut commands = CommandJournal::open(directory.path().join("commands.jsonl"))?;
    let entry = entry_command()?;
    commands.prepare_place(entry.clone())?;
    let binding = binding()?;
    let mut invalid = proof();
    invalid.signature_verified = false;
    assert!(
        authorize_recovery_cancel(RecoveryCancelInput {
            binding: &binding,
            original_command_id: entry.command_id.as_str(),
            client_id: entry.client_order_id.as_str(),
            family: NativeOrderFamily::UmOrder,
            commands: &commands,
            proof: &invalid,
            now_ms: 200,
        })
        .is_err()
    );
    Ok(())
}

fn binding() -> Result<CanaryRunBinding, Box<dyn std::error::Error>> {
    Ok(CanaryRunBinding {
        canary_id: "protection_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        owner_scope: "manual_canary_mainnet".to_owned(),
        release_id: "stage4_manual_canary_v1".to_owned(),
        position_side: PositionSide::Long,
        writer_generation: 1,
        readback_generation: 1,
        valid_until_ms: 150,
    })
}

fn proof() -> RecoveryObservationProof {
    RecoveryObservationProof {
        generation: 1,
        observed_at_ms: 190,
        valid_until_ms: 500,
        payload_sha256: "a".repeat(64),
        signature_verified: true,
    }
}

fn entry_command() -> Result<OrderCommand, Box<dyn std::error::Error>> {
    Ok(OrderCommand {
        time_in_force: Default::default(),
        command_id: CommandId::new("entry_1")?,
        client_order_id: CommandId::new("client_1")?,
        owner: OrderOwner {
            strategy_instance_id: "manual_canary".to_owned(),
            run_id: "protection_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::new(5, 2),
        limit_price: Price::new(Decimal::new(100, 0))?,
        reduce_only: false,
    })
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    let usdt: Asset = "USDT".parse()?;
    Ok(Instrument {
        symbol: "SOL/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(usdt.clone()),
        generation: 1,
        price_tick: Price::new(Decimal::ONE)?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: Amount::new(usdt, Decimal::new(5, 0)),
    })
}
