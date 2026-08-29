use rust_decimal::Decimal;
use tempfile::tempdir;
use venue_domain::domain::{
    CancelCommand, CommandId, ExecutionCommand, NativeOrderFamily, OrderCommand, OrderOwner,
    OrderPurpose, OrderSide, PositionSide, Price,
};
use venue_execution::{
    AccountOwnerRouteScope, CommandJournal, CommandState, DurableOwnerRoutes, OwnerRouteFence,
    OwnerRoutesError,
};
use venue_gateway_api::VenueId;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000035";

fn fence(generation: u64) -> Result<OwnerRouteFence, OwnerRoutesError> {
    OwnerRouteFence::new(
        AccountOwnerRouteScope::new(VenueId::Gate, ACCOUNT, "a".repeat(64))?,
        generation,
    )
}

fn owner(instance: &str) -> Result<OrderOwner, Box<dyn std::error::Error>> {
    Ok(OrderOwner {
        strategy_instance_id: instance.to_owned(),
        run_id: format!("{instance}_run_1"),
        exchange: "gate".to_owned(),
        account: ACCOUNT.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        purpose: OrderPurpose::Entry,
    })
}

fn place(
    command_id: &str,
    client_id: &str,
    instance: &str,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new(command_id)?,
        client_order_id: CommandId::new(client_id)?,
        owner: owner(instance)?,
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        reduce_only: false,
    }))
}

#[test]
fn create_unknown_generation_and_restart_keep_one_durable_route()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("commands.jsonl");
    let generation_seven = fence(7)?;
    let command = place("place_1", "client_1", "grid_doge")?;
    let command_id = command.command_id().clone();
    let client_id = command
        .native_client_id()
        .cloned()
        .ok_or("missing client id")?;

    let mut routes = DurableOwnerRoutes::open(&path, generation_seven.clone())?;
    let reserved = routes.reserve_create(&generation_seven, command.clone())?;
    assert_eq!(reserved.state, CommandState::Prepared);
    assert_eq!(
        routes
            .reserve_create(&generation_seven, command)?
            .command_id,
        command_id
    );
    routes.mark_submitted(&generation_seven, &command_id)?;
    routes.record_unknown(&generation_seven, &command_id, "ack_lost")?;
    let before_restart = routes.projection(&generation_seven)?;
    assert_eq!(
        before_restart.unresolved_command_ids,
        vec![command_id.clone()]
    );
    drop(routes);

    let mut recovered = DurableOwnerRoutes::open(&path, generation_seven.clone())?;
    assert_eq!(recovered.projection(&generation_seven)?, before_restart);
    let generation_eight = fence(8)?;
    recovered.advance_fence(&generation_seven, generation_eight.clone())?;
    assert!(matches!(
        recovered.route_by_client_id(&generation_seven, NativeOrderFamily::UmOrder, &client_id),
        Err(OwnerRoutesError::StaleFence)
    ));
    assert!(matches!(
        recovered
            .route_by_client_id(&generation_eight, NativeOrderFamily::UmOrder, &client_id)?
            .map(|route| &route.state),
        Some(CommandState::Unknown { .. })
    ));

    recovered.record_accepted(&generation_eight, &command_id, "native_501")?;
    let accepted = recovered
        .route_by_venue_order_id(&generation_eight, NativeOrderFamily::UmOrder, "native_501")?
        .ok_or("missing accepted native route")?;
    assert_eq!(accepted.key.client_id, client_id);
    drop(recovered);

    let restarted = DurableOwnerRoutes::open(&path, generation_eight.clone())?;
    assert!(
        restarted
            .route_by_venue_order_id(&generation_eight, NativeOrderFamily::UmOrder, "native_501")?
            .is_some()
    );
    Ok(())
}

#[test]
fn cancel_is_reserved_only_for_the_exact_durable_owner_family_route()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("commands.jsonl");
    let route_fence = fence(3)?;
    let command = place("place_target", "client_target", "grid_doge")?;
    let command_id = command.command_id().clone();
    let target_client_id = command
        .native_client_id()
        .cloned()
        .ok_or("missing client id")?;
    let target_owner = command.mutation_owner().clone();
    let mut routes = DurableOwnerRoutes::open(&path, route_fence.clone())?;
    routes.reserve_create(&route_fence, command)?;
    routes.mark_submitted(&route_fence, &command_id)?;
    routes.record_accepted(&route_fence, &command_id, "native_target")?;

    let cancel = CancelCommand {
        command_id: CommandId::new("cancel_target")?,
        owner: target_owner,
        target_client_order_id: target_client_id.clone(),
    };
    assert!(matches!(
        routes.reserve_cancel(
            &route_fence,
            cancel.clone(),
            NativeOrderFamily::UmConditional
        ),
        Err(OwnerRoutesError::CancelRoute)
    ));
    assert!(
        routes
            .cancel_route(&route_fence, &cancel.command_id)?
            .is_none()
    );

    let exact = routes.reserve_cancel(&route_fence, cancel.clone(), NativeOrderFamily::UmOrder)?;
    assert_eq!(exact.target.family, NativeOrderFamily::UmOrder);
    assert_eq!(exact.target.client_id, target_client_id);
    assert_eq!(
        exact.target_venue_order_id.as_deref(),
        Some("native_target")
    );
    drop(routes);

    let recovered = DurableOwnerRoutes::open(path, route_fence.clone())?;
    assert_eq!(
        recovered
            .cancel_route(&route_fence, &cancel.command_id)?
            .ok_or("missing recovered cancel route")?,
        exact
    );
    Ok(())
}

#[test]
fn duplicate_accepted_native_identity_fails_before_journal_transition()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("commands.jsonl");
    let route_fence = fence(11)?;
    let first = place("place_first", "client_first", "grid_doge")?;
    let second = place("place_second", "client_second", "grid_doge")?;
    let first_id = first.command_id().clone();
    let second_id = second.command_id().clone();
    let mut routes = DurableOwnerRoutes::open(&path, route_fence.clone())?;
    routes.reserve_create(&route_fence, first)?;
    routes.mark_submitted(&route_fence, &first_id)?;
    routes.record_accepted(&route_fence, &first_id, "same_native")?;
    routes.reserve_create(&route_fence, second)?;
    routes.mark_submitted(&route_fence, &second_id)?;

    assert!(matches!(
        routes.record_accepted(&route_fence, &second_id, "same_native"),
        Err(OwnerRoutesError::NativeOrderConflict)
    ));
    drop(routes);

    let recovered = DurableOwnerRoutes::open(path, route_fence.clone())?;
    let second_client = CommandId::new("client_second")?;
    assert_eq!(
        recovered
            .route_by_client_id(&route_fence, NativeOrderFamily::UmOrder, &second_client)?
            .map(|route| &route.state),
        Some(&CommandState::Submitted)
    );
    Ok(())
}

#[test]
fn recovery_rejects_foreign_account_and_preexisting_native_id_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let foreign_path = directory.path().join("foreign.jsonl");
    let route_fence = fence(1)?;
    let mut foreign = place("foreign_place", "foreign_client", "grid_doge")?;
    if let ExecutionCommand::PlaceLimit(command) = &mut foreign {
        command.owner.account = "00000000-0000-4000-8000-000000000099".to_owned();
    }
    CommandJournal::open(&foreign_path)?.prepare(foreign)?;
    assert!(matches!(
        DurableOwnerRoutes::open(&foreign_path, route_fence.clone()),
        Err(OwnerRoutesError::OwnerScope)
    ));

    let conflict_path = directory.path().join("conflict.jsonl");
    let first = place("raw_first", "raw_client_first", "grid_doge")?;
    let second = place("raw_second", "raw_client_second", "grid_doge")?;
    let first_id = first.command_id().clone();
    let second_id = second.command_id().clone();
    let mut journal = CommandJournal::open(&conflict_path)?;
    journal.prepare(first)?;
    journal.transition(&first_id, CommandState::Submitted)?;
    journal.transition(
        &first_id,
        CommandState::Accepted {
            venue_order_id: "raw_collision".to_owned(),
        },
    )?;
    journal.prepare(second)?;
    journal.transition(&second_id, CommandState::Submitted)?;
    journal.transition(
        &second_id,
        CommandState::Accepted {
            venue_order_id: "raw_collision".to_owned(),
        },
    )?;
    drop(journal);

    assert!(matches!(
        DurableOwnerRoutes::open(conflict_path, route_fence),
        Err(OwnerRoutesError::NativeOrderConflict)
    ));
    Ok(())
}
