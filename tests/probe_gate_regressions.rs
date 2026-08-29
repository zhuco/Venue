use std::collections::BTreeMap;

use rust_decimal::Decimal;
use venue::{
    domain::{
        Amount, Asset, CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide,
        Price,
    },
    execution::{
        CanaryEvidenceBinding, CanaryPreflightApproval, Capability, CapabilityEvidence,
        ProbeExecutionState, ProbeGateError, ProbeKind, ProbePermitInput, WriterScope,
        WriterSession, authorize_probe_permit, validate_probe_permit,
    },
};

fn binding() -> Result<CanaryEvidenceBinding, Box<dyn std::error::Error>> {
    let usdt: Asset = "USDT".parse()?;
    Ok(CanaryEvidenceBinding {
        canary_id: "sol-usdt-place-cancel-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDT".parse()?,
        owner_scope: "sol_probe_owner".to_owned(),
        release_id: "scalping-canary-v1".to_owned(),
        position_side: PositionSide::Long,
        quote_cap: Amount::new(usdt.clone(), Decimal::new(5, 0)),
        risk_cap: Amount::new(usdt, Decimal::new(1, 1)),
        valid_until_ms: 500,
    })
}

fn command() -> Result<OrderCommand, Box<dyn std::error::Error>> {
    Ok(OrderCommand {
        command_id: CommandId::new("sol_probe_place_1")?,
        client_order_id: CommandId::new("sol_probe_client_1")?,
        owner: OrderOwner {
            strategy_instance_id: "sol_probe".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::new(5, 0))?,
        reduce_only: false,
    })
}

fn writer() -> Result<WriterSession, Box<dyn std::error::Error>> {
    Ok(WriterSession {
        scope: WriterScope {
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            owner_scope: "sol_probe_owner".to_owned(),
        },
        token: "writer-token".to_owned(),
        generation: 1,
        revision: 1,
        readback_generation: 7,
        valid_until_ms: 450,
    })
}

fn preflight() -> Result<CanaryPreflightApproval, Box<dyn std::error::Error>> {
    Ok(CanaryPreflightApproval {
        quantity: Decimal::ONE,
        notional: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
        final_generation: 7,
        valid_until_ms: 350,
    })
}

fn capabilities() -> BTreeMap<Capability, CapabilityEvidence> {
    [
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
    ]
    .into_iter()
    .map(|capability| {
        (
            capability,
            CapabilityEvidence {
                evidence_hash: "a".repeat(64),
                generation: 1,
                verified_at_ms: 10,
                valid_until_ms: 400,
            },
        )
    })
    .collect()
}

fn input<'a>(
    binding: &'a CanaryEvidenceBinding,
    preflight: &'a CanaryPreflightApproval,
    writer: &'a WriterSession,
    command: &'a OrderCommand,
    capabilities: &'a BTreeMap<Capability, CapabilityEvidence>,
) -> ProbePermitInput<'a> {
    ProbePermitInput {
        kind: ProbeKind::PostOnlyPlaceCancel,
        now_ms: 100,
        probe_ttl_ms: 3_000,
        binding,
        preflight,
        writer,
        command,
        execution: ProbeExecutionState {
            command_wal_clean: true,
            reconciliation_clean: true,
            reconciliation_generation: 7,
            reconciliation_valid_until_ms: 300,
        },
        capabilities,
    }
}

#[test]
fn permit_is_short_lived_command_bound_and_read_only() -> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let preflight = preflight()?;
    let writer = writer()?;
    let command = command()?;
    let capabilities = capabilities();
    let permit = authorize_probe_permit(input(
        &binding,
        &preflight,
        &writer,
        &command,
        &capabilities,
    ))?;

    assert_eq!(permit.valid_until_ms(), 300);
    validate_probe_permit(permit, ProbeKind::PostOnlyPlaceCancel, &command, 299)?;
    assert!(matches!(
        validate_probe_permit(permit, ProbeKind::PostOnlyPlaceCancel, &command, 300),
        Err(ProbeGateError::PermitExpired)
    ));
    let mut tampered = command.clone();
    tampered.quantity = Decimal::new(2, 0);
    assert!(matches!(
        validate_probe_permit(permit, ProbeKind::PostOnlyPlaceCancel, &tampered, 200),
        Err(ProbeGateError::CommandFingerprint)
    ));
    Ok(())
}

#[test]
fn permit_rejects_scope_side_expiry_missing_read_only_unknown_wal_and_over_cap()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = binding()?;
    let preflight = preflight()?;
    let writer = writer()?;
    let command = command()?;
    let capabilities = capabilities();

    let mut wrong_side = command.clone();
    wrong_side.side = OrderSide::Sell;
    assert!(matches!(
        authorize_probe_permit(input(
            &binding,
            &preflight,
            &writer,
            &wrong_side,
            &capabilities
        )),
        Err(ProbeGateError::Command)
    ));

    let mut wrong_scope = writer.clone();
    wrong_scope.scope.owner_scope = "other_owner".to_owned();
    assert!(matches!(
        authorize_probe_permit(input(
            &binding,
            &preflight,
            &wrong_scope,
            &command,
            &capabilities
        )),
        Err(ProbeGateError::Scope)
    ));

    let mut expired_preflight = preflight.clone();
    expired_preflight.valid_until_ms = 100;
    assert!(matches!(
        authorize_probe_permit(input(
            &binding,
            &expired_preflight,
            &writer,
            &command,
            &capabilities
        )),
        Err(ProbeGateError::PreflightExpired)
    ));

    let mut missing = capabilities.clone();
    missing.remove(&Capability::PrivateStream);
    assert!(matches!(
        authorize_probe_permit(input(&binding, &preflight, &writer, &command, &missing)),
        Err(ProbeGateError::Capability(Capability::PrivateStream))
    ));

    let mut unknown_wal = input(&binding, &preflight, &writer, &command, &capabilities);
    unknown_wal.execution.command_wal_clean = false;
    assert!(matches!(
        authorize_probe_permit(unknown_wal),
        Err(ProbeGateError::CommandWal)
    ));

    let mut over_cap = binding.clone();
    over_cap.quote_cap.value = Decimal::new(1_001, 2);
    assert!(matches!(
        authorize_probe_permit(input(
            &over_cap,
            &preflight,
            &writer,
            &command,
            &capabilities
        )),
        Err(ProbeGateError::Binding)
    ));
    Ok(())
}
