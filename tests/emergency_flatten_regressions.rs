use rust_decimal::Decimal;
use venue::{
    domain::{
        Amount, Asset, CommandId, Instrument, MarketKind, OrderOwner, OrderPurpose, OrderSide,
        Position, PositionSide, Price,
    },
    execution::{
        CanaryEvidenceBinding, EMERGENCY_FLATTEN_PERMIT_TTL_MS, EmergencyDispatchState,
        EmergencyFlattenError, EmergencyFlattenInput, EmergencyRiskEnvelope, WriterScope,
        WriterSession, authorize_emergency_flatten, validate_emergency_flatten_permit,
    },
};

fn binding() -> Result<CanaryEvidenceBinding, Box<dyn std::error::Error>> {
    let usdt: Asset = "USDT".parse()?;
    Ok(CanaryEvidenceBinding {
        canary_id: "sol-usdt-emergency-flat-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        owner_scope: "sol_emergency_owner".to_owned(),
        release_id: "scalping-canary-v1".to_owned(),
        position_side: PositionSide::Long,
        quote_cap: Amount::new(usdt.clone(), Decimal::new(5, 0)),
        risk_cap: Amount::new(usdt, Decimal::new(1, 1)),
        valid_until_ms: 2_000,
    })
}

fn position() -> Result<Position, Box<dyn std::error::Error>> {
    Ok(Position {
        symbol: "SOL/USDT".parse()?,
        side: PositionSide::Long,
        quantity: Decimal::new(2, 0),
        entry_price: Some(Price::new(Decimal::new(5, 0))?),
        mark_price: Some(Price::new(Decimal::new(6, 0))?),
    })
}

fn writer() -> Result<WriterSession, Box<dyn std::error::Error>> {
    Ok(WriterSession {
        scope: WriterScope {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            owner_scope: "sol_emergency_owner".to_owned(),
        },
        token: "writer-token".to_owned(),
        generation: 4,
        revision: 9,
        readback_generation: 7,
        valid_until_ms: 1_000,
    })
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    let usdt: Asset = "USDT".parse()?;
    Ok(Instrument {
        symbol: "SOL/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(usdt.clone()),
        generation: 7,
        price_tick: Price::new(Decimal::ONE)?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(usdt, Decimal::ONE),
    })
}

fn owner() -> Result<OrderOwner, Box<dyn std::error::Error>> {
    Ok(OrderOwner {
        strategy_instance_id: "sol_emergency".to_owned(),
        run_id: "run_1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        purpose: OrderPurpose::Reduce,
    })
}

fn dispatch() -> EmergencyDispatchState {
    EmergencyDispatchState {
        now_ms: 100,
        private_generation: 7,
        readback_generation: 7,
        position_generation: 7,
        private_readback_valid_until_ms: 800,
        reconciliation_clean: true,
        reconciliation_valid_until_ms: 700,
        entry_or_reduce_wal_clean: true,
        filled_at_ms: 100,
        unprotected_deadline_ms: 1_600,
        dispatch_writer_generation: 4,
        dispatch_writer_revision: 9,
    }
}

fn risk(binding: &CanaryEvidenceBinding) -> EmergencyRiskEnvelope {
    EmergencyRiskEnvelope {
        quote_cap: binding.quote_cap.clone(),
        risk_cap: binding.risk_cap.clone(),
        valid_until_ms: 600,
    }
}

fn input<'a>(
    binding: &'a CanaryEvidenceBinding,
    position: &'a Position,
    writer: &'a WriterSession,
    instrument: &'a Instrument,
    risk: &'a EmergencyRiskEnvelope,
    owner: &'a OrderOwner,
    ids: (&'a CommandId, &'a CommandId),
) -> Result<EmergencyFlattenInput<'a>, Box<dyn std::error::Error>> {
    Ok(EmergencyFlattenInput {
        binding,
        authoritative_position: position,
        writer,
        dispatch: dispatch(),
        instrument,
        market_price: Price::new(Decimal::new(6, 0))?,
        market_price_valid_until_ms: 650,
        risk,
        command_id: ids.0,
        client_order_id: ids.1,
        owner,
    })
}

#[test]
fn full_hedge_reduction_is_command_bound_and_short_lived() -> Result<(), Box<dyn std::error::Error>>
{
    let binding = binding()?;
    let position = position()?;
    let writer = writer()?;
    let instrument = instrument()?;
    let risk = risk(&binding);
    let owner = owner()?;
    let command_id = CommandId::new("sol_emergency_flat_1")?;
    let client_order_id = CommandId::new("sol_emergency_client_1")?;
    let authorization = authorize_emergency_flatten(input(
        &binding,
        &position,
        &writer,
        &instrument,
        &risk,
        &owner,
        (&command_id, &client_order_id),
    )?)?;

    assert_eq!(authorization.command.quantity, position.quantity);
    assert_eq!(authorization.command.side, OrderSide::Sell);
    assert_eq!(authorization.command.owner.purpose, OrderPurpose::Reduce);
    assert!(authorization.command.reduce_only);
    assert_eq!(
        authorization.permit().valid_until_ms(),
        100 + EMERGENCY_FLATTEN_PERMIT_TTL_MS
    );
    validate_emergency_flatten_permit(authorization.permit(), &authorization.command, 599)?;
    let mut tampered = authorization.command.clone();
    tampered.quantity = Decimal::ONE;
    assert!(matches!(
        validate_emergency_flatten_permit(authorization.permit(), &tampered, 200),
        Err(EmergencyFlattenError::CommandFingerprint)
    ));
    Ok(())
}

#[test]
fn unknown_or_stale_or_non_authoritative_state_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let position = position()?;
    let writer = writer()?;
    let instrument = instrument()?;
    let risk = risk(&binding);
    let owner = owner()?;
    let command_id = CommandId::new("sol_emergency_flat_2")?;
    let client_order_id = CommandId::new("sol_emergency_client_2")?;

    let mut before_deadline = input(
        &binding,
        &position,
        &writer,
        &instrument,
        &risk,
        &owner,
        (&command_id, &client_order_id),
    )?;
    before_deadline.dispatch.unprotected_deadline_ms = 1_599;
    assert!(matches!(
        authorize_emergency_flatten(before_deadline),
        Err(EmergencyFlattenError::Deadline)
    ));

    let mut unknown = input(
        &binding,
        &position,
        &writer,
        &instrument,
        &risk,
        &owner,
        (&command_id, &client_order_id),
    )?;
    unknown.dispatch.entry_or_reduce_wal_clean = false;
    assert!(matches!(
        authorize_emergency_flatten(unknown),
        Err(EmergencyFlattenError::CommandWal)
    ));

    let mut stale_generation = input(
        &binding,
        &position,
        &writer,
        &instrument,
        &risk,
        &owner,
        (&command_id, &client_order_id),
    )?;
    stale_generation.dispatch.position_generation = 8;
    assert!(matches!(
        authorize_emergency_flatten(stale_generation),
        Err(EmergencyFlattenError::Generation)
    ));

    let mut wrong_scope = writer.clone();
    wrong_scope.scope.account = "other_account".to_owned();
    assert!(matches!(
        authorize_emergency_flatten(input(
            &binding,
            &position,
            &wrong_scope,
            &instrument,
            &risk,
            &owner,
            (&command_id, &client_order_id),
        )?),
        Err(EmergencyFlattenError::Writer)
    ));

    let mut expired_price = input(
        &binding,
        &position,
        &writer,
        &instrument,
        &risk,
        &owner,
        (&command_id, &client_order_id),
    )?;
    expired_price.market_price_valid_until_ms = 100;
    assert!(matches!(
        authorize_emergency_flatten(expired_price),
        Err(EmergencyFlattenError::MarketPrice)
    ));

    let mut wrong_side = position.clone();
    wrong_side.side = PositionSide::Short;
    assert!(matches!(
        authorize_emergency_flatten(input(
            &binding,
            &wrong_side,
            &writer,
            &instrument,
            &risk,
            &owner,
            (&command_id, &client_order_id),
        )?),
        Err(EmergencyFlattenError::Position)
    ));
    Ok(())
}
