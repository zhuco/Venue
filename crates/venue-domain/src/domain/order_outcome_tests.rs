use rust_decimal::Decimal;

use crate::domain::{
    AuthoritativeOrderOutcome, CancelCommand, CommandId, ExecutionCommand, NativeOrderFamily,
    OrderCommand, OrderOutcomeBinding, OrderOutcomeError, OrderOutcomeStatus, OrderOwner,
    OrderPurpose, OrderReadbackCoverage, OrderReadbackObservation, OrderSide, OrderState,
    PositionSide, Price, SignedOrderReadback, Symbol, UnknownOrderContract, UnresolvedOrderReason,
};

fn owner(run_id: &str) -> Result<OrderOwner, Box<dyn std::error::Error>> {
    Ok(OrderOwner {
        strategy_instance_id: "grid-a".to_owned(),
        run_id: run_id.to_owned(),
        exchange: "binance".to_owned(),
        account: "account-a".to_owned(),
        symbol: Symbol::new("BTC", "USDT")?,
        purpose: OrderPurpose::Entry,
    })
}

fn command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new("command-a")?,
        client_order_id: CommandId::new("client-a")?,
        owner: owner("run-a")?,
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::from(50_000_u32))?,
        reduce_only: false,
    }))
}

fn contract() -> Result<UnknownOrderContract, Box<dyn std::error::Error>> {
    Ok(UnknownOrderContract::new(
        OrderOutcomeBinding::from_command(&command()?)?,
        7,
    )?)
}

fn readback(
    binding: OrderOutcomeBinding,
    generation: u64,
    coverage: OrderReadbackCoverage,
    observation: OrderReadbackObservation,
) -> Result<SignedOrderReadback, OrderOutcomeError> {
    SignedOrderReadback::verified(binding, generation, coverage, observation, [0xA5; 32])
}

#[test]
fn command_derivation_freezes_command_native_family_and_owner_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let binding = OrderOutcomeBinding::from_command(&command)?;

    assert_eq!(binding.command_id().as_str(), "command-a");
    assert_eq!(binding.native_client_id().as_str(), "client-a");
    assert_eq!(binding.native_order_family(), NativeOrderFamily::UmOrder);
    assert_eq!(binding.owner(), command.mutation_owner());
    Ok(())
}

#[test]
fn cancellation_requires_durable_target_family_mapping() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = ExecutionCommand::Cancel(CancelCommand {
        command_id: CommandId::new("cancel-a")?,
        owner: owner("run-a")?,
        target_client_order_id: CommandId::new("client-a")?,
    });

    assert_eq!(
        OrderOutcomeBinding::from_command(&cancel),
        Err(OrderOutcomeError::MissingNativeIdentity)
    );
    Ok(())
}

#[test]
fn newer_signed_found_rows_classify_open_and_terminal() -> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let open = readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::ExactIdentity,
        OrderReadbackObservation::Found {
            native_client_id: CommandId::new("client-a")?,
            exchange_order_id: "venue-101".to_owned(),
            state: OrderState::PartiallyFilled,
        },
    )?;
    let terminal = readback(
        contract.binding().clone(),
        9,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 2 },
        OrderReadbackObservation::Found {
            native_client_id: CommandId::new("client-a")?,
            exchange_order_id: "venue-101".to_owned(),
            state: OrderState::Filled,
        },
    )?;

    assert_eq!(contract.classify(open).status(), OrderOutcomeStatus::Open);
    assert_eq!(
        contract.classify(terminal).status(),
        OrderOutcomeStatus::Terminal
    );
    Ok(())
}

#[test]
fn point_404_and_partial_empty_page_remain_unresolved() -> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let not_found = readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::ExactIdentity,
        OrderReadbackObservation::NotFound,
    )?;
    let partial_empty = readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::IncompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?;

    assert_eq!(
        contract.classify(not_found).status(),
        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::PointLookupNotFound)
    );
    assert_eq!(
        contract.classify(partial_empty).status(),
        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::IncompleteCollection)
    );
    Ok(())
}

#[test]
fn proven_absent_requires_complete_newer_signed_family_collection()
-> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let complete_empty = readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?;
    let outcome = contract.classify(complete_empty);

    assert_eq!(outcome.status(), OrderOutcomeStatus::ProvenAbsent);
    assert!(!outcome.grants_original_command_redispatch());
    Ok(())
}

#[test]
fn same_generation_complete_collection_is_not_new_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let stale = readback(
        contract.binding().clone(),
        7,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?;

    assert_eq!(
        contract.classify(stale).status(),
        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::StaleReadbackGeneration)
    );
    Ok(())
}

#[test]
fn binding_and_native_identity_mismatches_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let other_binding = OrderOutcomeBinding::new(
        CommandId::new("command-a")?,
        CommandId::new("client-a")?,
        NativeOrderFamily::UmOrder,
        owner("run-b")?,
    )?;
    let wrong_binding = readback(
        other_binding,
        8,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?;
    let wrong_native_identity = readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::ExactIdentity,
        OrderReadbackObservation::Found {
            native_client_id: CommandId::new("client-b")?,
            exchange_order_id: "venue-101".to_owned(),
            state: OrderState::New,
        },
    )?;

    assert_eq!(
        contract.classify(wrong_binding).status(),
        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::BindingMismatch)
    );
    assert_eq!(
        contract.classify(wrong_native_identity).status(),
        OrderOutcomeStatus::Unresolved(UnresolvedOrderReason::NativeIdentityMismatch)
    );
    Ok(())
}

#[test]
fn malformed_signed_envelopes_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let binding = contract()?.binding().clone();
    assert_eq!(
        SignedOrderReadback::verified(
            binding.clone(),
            8,
            OrderReadbackCoverage::CompleteFamilyCollection { page_count: 0 },
            OrderReadbackObservation::EmptyCollection,
            [0xA5; 32],
        ),
        Err(OrderOutcomeError::InvalidCoverage)
    );
    assert_eq!(
        SignedOrderReadback::verified(
            binding,
            8,
            OrderReadbackCoverage::ExactIdentity,
            OrderReadbackObservation::NotFound,
            [0; 32],
        ),
        Err(OrderOutcomeError::UnsignedReadback)
    );
    Ok(())
}

#[test]
fn canonical_outcome_round_trips_for_durable_consumers() -> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let outcome = contract.classify(readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?);

    let encoded = serde_json::to_vec(&outcome)?;
    let decoded: AuthoritativeOrderOutcome = serde_json::from_slice(&encoded)?;
    assert_eq!(decoded, outcome);
    Ok(())
}

#[test]
fn durable_consumer_cannot_relabel_an_authoritative_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let contract = contract()?;
    let outcome = contract.classify(readback(
        contract.binding().clone(),
        8,
        OrderReadbackCoverage::CompleteFamilyCollection { page_count: 1 },
        OrderReadbackObservation::EmptyCollection,
    )?);
    let mut encoded = serde_json::to_value(outcome)?;
    encoded["status"] = serde_json::json!({ "outcome": "open" });

    assert!(serde_json::from_value::<AuthoritativeOrderOutcome>(encoded).is_err());
    Ok(())
}
