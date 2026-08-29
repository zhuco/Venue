use std::{collections::BTreeSet, error::Error};

use rust_decimal::Decimal;

use super::tests::{
    EvidenceFixture, account, binding, establish_empty_signed_orders, owner, place_request,
    pop_applied_strategy_input, private_fact, runtime_place_intent,
};
use super::*;
use crate::{
    domain::{
        AccountBalance, CommandId, DomainEvent, NativeOrderFamily, OrderPurpose, Position,
        PositionSide,
    },
    execution::{
        AccountExecutionRequest, AccountLaneError, AccountLaneFollowUp, AccountLanePriority,
        UnknownReadbackProof, UnknownResolution,
    },
    runtime::strategy::StrategyInput,
};

fn recovery_roots_with_boundaries(
    tail_sequences: [u64; 5],
    record_counts: [u64; 5],
) -> Result<RecoveryJournalRoots, Box<dyn Error>> {
    Ok(RecoveryJournalRoots::test_verified(
        [[0x11; 32], [0x22; 32], [0x33; 32], [0x44; 32], [0x55; 32]],
        tail_sequences,
        record_counts,
    )?)
}

fn recovery_roots() -> Result<RecoveryJournalRoots, Box<dyn Error>> {
    recovery_roots_with_boundaries([0; 5], [0; 5])
}

fn empty_private_cursor() -> Result<RecoveredPrivateCursor, Box<dyn Error>> {
    Ok(RecoveredPrivateCursor::verified(0, 0, None)?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "test snapshots enumerate every recovered authority boundary"
)]
fn recovery_snapshot(
    account: AccountKey,
    journal_roots: RecoveryJournalRoots,
    last_connection_generation: u64,
    applied_private_cursor: RecoveredPrivateCursor,
    strategy_states: Vec<RecoveredStrategyState>,
    pending_private_batches: Vec<RecoveredPrivateBatch>,
    routes: Vec<RecoveredOrderRoute>,
    unresolved_mutations: Vec<AccountExecutionRequest>,
) -> Result<AccountRecoverySnapshot, Box<dyn Error>> {
    let commitment = RecoveryManifestCommitment::test_for_replayed_state(
        &account,
        &journal_roots,
        last_connection_generation,
        &applied_private_cursor,
        &strategy_states,
        &pending_private_batches,
        &routes,
        &unresolved_mutations,
    )?;
    Ok(AccountRecoverySnapshot::verified(
        account,
        journal_roots,
        commitment,
        last_connection_generation,
        applied_private_cursor,
        strategy_states,
        pending_private_batches,
        routes,
        unresolved_mutations,
    )?)
}

pub(super) fn install_persisted_order_route(
    runtime: &mut AccountRuntime,
    route: RecoveredOrderRoute,
) -> Result<(), Box<dyn Error>> {
    let (previous_root, previous_tail, _) = runtime
        .owner_index_boundary_for_test()
        .ok_or("owner-index recovery root missing")?;
    let receipt = PersistedOrderRouteAppendReceipt::test_persisted_after_append(
        route,
        previous_root,
        previous_tail
            .checked_add(1)
            .ok_or("owner-index test sequence exhausted")?,
    )?;
    runtime.install_order_route(receipt)?;
    Ok(())
}

pub(super) fn restore_empty_recovery(runtime: &mut AccountRuntime) -> Result<(), Box<dyn Error>> {
    let strategy_states = runtime
        .registry()
        .registrations()
        .map(|registration| {
            RecoveredStrategyState::verified(
                registration.binding.clone(),
                registration.config_epoch,
                registration.lifecycle,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    runtime.restore_durable_state(recovery_snapshot(
        runtime.account().clone(),
        recovery_roots()?,
        0,
        empty_private_cursor()?,
        strategy_states,
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )?)?;
    Ok(())
}

fn balance_event() -> Result<DomainEvent, Box<dyn Error>> {
    Ok(DomainEvent::Balance(AccountBalance {
        asset: "USDT".parse()?,
        wallet_balance: Decimal::ZERO,
        available_balance: Decimal::ZERO,
        initial_margin: Decimal::ZERO,
        maintenance_margin: Decimal::ZERO,
    }))
}

#[test]
fn startup_requires_durable_recovery_and_restores_unknown_fence() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut missing_recovery = AccountRuntime::new(account()?);
    missing_recovery.register_strategy(grid.clone())?;
    assert!(matches!(
        missing_recovery.mark_account_ready(),
        Err(AccountRuntimeError::DurableRecoveryRequired)
    ));

    let recovered_request = place_request(&grid, "recovered", AccountLanePriority::Normal)?;
    let snapshot = recovery_snapshot(
        account()?,
        recovery_roots_with_boundaries([1, 0, 0, 1, 1], [1, 0, 0, 1, 1])?,
        1,
        empty_private_cursor()?,
        vec![RecoveredStrategyState::verified(
            grid.clone(),
            1,
            InstanceLifecycle::Registered,
            None,
        )?],
        Vec::new(),
        vec![RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_recovered")?,
            "client_recovered".to_owned(),
            Some("venue_recovered".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        )],
        vec![recovered_request],
    )?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    runtime.restore_durable_state(snapshot)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;

    assert!(matches!(
        runtime.enqueue_execution(runtime_place_intent(
            &runtime,
            &grid,
            "blocked_by_recovery",
            AccountLanePriority::Normal,
        )?),
        Err(AccountRuntimeError::ExecutionLane(
            AccountLaneError::UnknownFence
        ))
    ));
    let follow_up = runtime.resolve_unknown_execution(UnknownReadbackProof::test_verified(
        CommandId::new("cmd_recovered")?,
        grid.key.clone(),
        CommandId::new("client_recovered")?,
        Some(NativeOrderFamily::UmOrder),
        2,
        1,
        UnknownResolution::ProvenAbsent,
    )?)?;
    assert!(matches!(
        follow_up,
        AccountLaneFollowUp::StrategyReplanRequired {
            reason: crate::execution::AccountReplanReason::ProvenAbsent,
            ..
        }
    ));
    Ok(())
}

#[test]
fn durable_recovery_restores_epoch_lifecycle_shutdown_fence_and_connection_floor()
-> Result<(), Box<dyn Error>> {
    let paused = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let stopping = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(paused.clone())?;
    runtime.register_strategy(stopping.clone())?;
    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(9, 100, "recovered actor inbox")?;
    let recovered_fact = private_fact(
        &persisted,
        DomainEvent::Position(Position {
            symbol: paused.key.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        }),
    )?;
    runtime.restore_durable_state(recovery_snapshot(
        account()?,
        recovery_roots_with_boundaries([2, 1, 1, 0, 0], [2, 1, 1, 0, 0])?,
        9,
        empty_private_cursor()?,
        vec![
            RecoveredStrategyState::verified(paused.clone(), 7, InstanceLifecycle::Paused, None)?,
            RecoveredStrategyState::verified(
                stopping.clone(),
                4,
                InstanceLifecycle::Stopping,
                Some(RecoveredShutdownState::verified(
                    RecoveredShutdownMode::Stop,
                    8,
                    3,
                )?),
            )?,
        ],
        vec![RecoveredPrivateBatch::verified(
            vec![recovered_fact.clone()],
            vec![RecoveredActorInboxEntry::verified(
                paused.key.clone(),
                recovered_fact,
            )],
            BTreeSet::new(),
        )?],
        Vec::new(),
        Vec::new(),
    )?)?;

    assert!(matches!(
        runtime.mark_account_ready(),
        Err(AccountRuntimeError::ReconnectWithUnappliedActorState)
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &paused.key)?,
        Some(StrategyInput::Private(_))
    ));
    runtime.mark_account_ready()?;
    assert_eq!(runtime.connection_generation(), 10);
    assert_eq!(runtime.applied_private_sequence(), 1);
    let paused_state = runtime
        .registry()
        .registration(&paused.key)
        .ok_or("paused recovery missing")?;
    assert_eq!(paused_state.config_epoch, 7);
    assert_eq!(paused_state.lifecycle, InstanceLifecycle::Paused);
    let stopping_state = runtime
        .registry()
        .registration(&stopping.key)
        .ok_or("stopping recovery missing")?;
    assert_eq!(stopping_state.config_epoch, 4);
    assert_eq!(stopping_state.lifecycle, InstanceLifecycle::Stopping);
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &stopping.key)?,
        Some(StrategyInput::Control(
            crate::runtime::strategy::StrategyControl::Stop
        ))
    ));
    Ok(())
}

#[test]
fn manifest_commitment_rejects_truncated_routes_and_unknown_mutations() -> Result<(), Box<dyn Error>>
{
    let account = account()?;
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let roots = recovery_roots_with_boundaries([1, 0, 0, 1, 1], [1, 0, 0, 1, 1])?;
    let cursor = empty_private_cursor()?;
    let states = vec![RecoveredStrategyState::verified(
        grid.clone(),
        1,
        InstanceLifecycle::Registered,
        None,
    )?];
    let routes = vec![RecoveredOrderRoute::verified(
        NativeOrderFamily::UmOrder,
        CommandId::new("cmd_manifest")?,
        "client_manifest".to_owned(),
        Some("venue_manifest".to_owned()),
        owner(&grid, OrderPurpose::Entry),
    )];
    let unknown = vec![place_request(
        &grid,
        "manifest",
        AccountLanePriority::Normal,
    )?];
    let commitment = RecoveryManifestCommitment::test_for_replayed_state(
        &account,
        &roots,
        1,
        &cursor,
        &states,
        &[],
        &routes,
        &unknown,
    )?;

    assert!(matches!(
        AccountRecoverySnapshot::verified(
            account.clone(),
            roots.clone(),
            commitment.clone(),
            1,
            cursor.clone(),
            states.clone(),
            Vec::new(),
            Vec::new(),
            unknown,
        ),
        Err(RecoverySnapshotError::ManifestCommitment)
    ));
    assert!(matches!(
        AccountRecoverySnapshot::verified(
            account,
            roots,
            commitment,
            1,
            cursor,
            states,
            Vec::new(),
            routes,
            Vec::new(),
        ),
        Err(RecoverySnapshotError::ManifestCommitment)
    ));
    Ok(())
}

#[test]
fn recovery_advances_across_zero_delivery_and_fully_applied_batches() -> Result<(), Box<dyn Error>>
{
    let mut zero_evidence = EvidenceFixture::new()?;
    let mut zero_batches = Vec::new();
    for (received_at_ms, payload) in [(100, "zero one"), (101, "zero two")] {
        let persisted = zero_evidence.append(9, received_at_ms, payload)?;
        zero_batches.push(RecoveredPrivateBatch::verified(
            vec![private_fact(&persisted, balance_event()?)?],
            Vec::new(),
            BTreeSet::new(),
        )?);
    }
    let mut zero_runtime = AccountRuntime::new(account()?);
    zero_runtime.restore_durable_state(recovery_snapshot(
        account()?,
        recovery_roots_with_boundaries([0, 2, 0, 0, 0], [0, 2, 0, 0, 0])?,
        9,
        empty_private_cursor()?,
        Vec::new(),
        zero_batches,
        Vec::new(),
        Vec::new(),
    )?)?;
    assert_eq!(zero_runtime.applied_private_sequence(), 2);
    zero_runtime.mark_account_ready()?;

    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut applied_runtime = AccountRuntime::new(account()?);
    applied_runtime.register_strategy(grid.clone())?;
    let mut applied_evidence = EvidenceFixture::new()?;
    let mut applied_batches = Vec::new();
    for (received_at_ms, payload) in [(200, "applied one"), (201, "applied two")] {
        let persisted = applied_evidence.append(9, received_at_ms, payload)?;
        let fact = private_fact(
            &persisted,
            DomainEvent::Position(Position {
                symbol: grid.key.symbol.clone(),
                side: PositionSide::Long,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            }),
        )?;
        applied_batches.push(RecoveredPrivateBatch::verified(
            vec![fact.clone()],
            vec![RecoveredActorInboxEntry::verified(grid.key.clone(), fact)],
            BTreeSet::from([(grid.key.clone(), 0)]),
        )?);
    }
    applied_runtime.restore_durable_state(recovery_snapshot(
        account()?,
        recovery_roots_with_boundaries([1, 2, 2, 0, 0], [1, 2, 2, 0, 0])?,
        9,
        empty_private_cursor()?,
        vec![RecoveredStrategyState::verified(
            grid.clone(),
            1,
            InstanceLifecycle::Registered,
            None,
        )?],
        applied_batches,
        Vec::new(),
        Vec::new(),
    )?)?;
    assert_eq!(applied_runtime.applied_private_sequence(), 2);
    assert!(pop_applied_strategy_input(&mut applied_runtime, &grid.key)?.is_none());
    applied_runtime.mark_account_ready()?;
    Ok(())
}

#[test]
fn order_route_receipt_must_extend_recovered_tail_once() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    runtime.restore_durable_state(recovery_snapshot(
        account()?,
        recovery_roots_with_boundaries([1, 0, 0, 0, 7], [1, 0, 0, 0, 5])?,
        0,
        empty_private_cursor()?,
        vec![RecoveredStrategyState::verified(
            grid.clone(),
            1,
            InstanceLifecycle::Registered,
            None,
        )?],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )?)?;
    let (root, tail, count) = runtime
        .owner_index_boundary_for_test()
        .ok_or("owner-index recovery boundary missing")?;
    assert_eq!((tail, count), (7, 5));
    let route = RecoveredOrderRoute::verified(
        NativeOrderFamily::UmOrder,
        CommandId::new("cmd_after_recovery")?,
        "client_after_recovery".to_owned(),
        Some("venue_after_recovery".to_owned()),
        owner(&grid, OrderPurpose::Entry),
    );

    let out_of_order = PersistedOrderRouteAppendReceipt::test_persisted_after_append(
        route.clone(),
        root,
        tail + 2,
    )?;
    assert!(matches!(
        runtime.install_order_route(out_of_order),
        Err(AccountRuntimeError::OrderRouteReceipt)
    ));
    assert_eq!(runtime.owner_index_boundary_for_test(), Some((root, 7, 5)));

    let receipt =
        PersistedOrderRouteAppendReceipt::test_persisted_after_append(route, root, tail + 1)?;
    let stale_receipt = receipt.clone();
    runtime.install_order_route(receipt)?;
    let installed_boundary = runtime
        .owner_index_boundary_for_test()
        .ok_or("installed owner-index boundary missing")?;
    assert_eq!((installed_boundary.1, installed_boundary.2), (8, 6));
    assert!(matches!(
        runtime.install_order_route(stale_receipt),
        Err(AccountRuntimeError::OrderRouteReceipt)
    ));
    assert_eq!(
        runtime.owner_index_boundary_for_test(),
        Some(installed_boundary)
    );
    Ok(())
}
