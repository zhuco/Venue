use std::{collections::BTreeMap, error::Error};

use rust_decimal::Decimal;
use tempfile::TempDir;

use super::recovery_tests::{install_persisted_order_route, restore_empty_recovery};
use super::*;
use crate::{
    domain::{
        AccountBalance, CancelCommand, CommandId, DomainEvent, EventHeader, EventId, EventSource,
        ExecutionCommand, FieldState, Fill, NativeOrderFamily, Order, OrderCommand, OrderOwner,
        OrderPurpose, OrderSide, OrderState, Position, PositionSide, Price, PublicTicker, Symbol,
    },
    execution::{
        AccountDispatchDecision, AccountDispatchPermit, AccountExecutionIntent,
        AccountExecutionLane, AccountExecutionRequest, AccountLaneError, AccountLaneFollowUp,
        AccountLanePriority, AccountMutationOutcome, AccountWriterCapability,
        PersistedMutationOutcomeReceipt, PersistedWalPreparedReceipt, PersistedWriterLeaseReceipt,
        UnknownReadbackProof, UnknownResolution, WalNotPreparedReceipt,
    },
    runtime::strategy::{AccountMarketEvent, PersistedPrivateFact, StrategyInput},
    storage::{PersistedPrivateEvidence, PrivateEvidence, PrivateEvidenceJournal},
    strategy::hedged_grid::HedgedGridBinding,
};

pub(super) fn account() -> Result<AccountKey, Box<dyn Error>> {
    Ok(AccountKey::new(ExchangeId::Binance, "portfolio")?)
}

pub(super) fn binding(
    kind: StrategyKind,
    instance_id: &str,
    symbol: &str,
) -> Result<StrategyBinding, Box<dyn Error>> {
    let key = StrategyInstanceKey::new(account()?, kind, instance_id, symbol.parse()?)?;
    Ok(StrategyBinding::new(key, "run_1", "config_1")?)
}

pub(super) fn owner(binding: &StrategyBinding, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.key.instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.key.account.exchange.as_str().to_owned(),
        account: binding.key.account.account.clone(),
        symbol: binding.key.symbol.clone(),
        purpose,
    }
}

fn price(value: i64) -> Result<Price, Box<dyn Error>> {
    Ok(Price::new(Decimal::new(value, 0))?)
}

fn open_order(
    binding: &StrategyBinding,
    venue_order_id: &str,
    client_order_id: &str,
) -> Result<Order, Box<dyn Error>> {
    Ok(Order {
        order_id: venue_order_id.to_owned(),
        client_order_id: FieldState::Known(client_order_id.to_owned()),
        symbol: binding.key.symbol.clone(),
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Known(OrderPurpose::Entry),
        state: OrderState::New,
        quantity: Decimal::ONE,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(price(10)?),
        average_price: FieldState::Missing,
        reduce_only: false,
    })
}

fn fill(
    binding: &StrategyBinding,
    fill_id: &str,
    venue_order_id: &str,
) -> Result<Fill, Box<dyn Error>> {
    Ok(Fill {
        fill_id: fill_id.to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: venue_order_id.to_owned(),
        symbol: binding.key.symbol.clone(),
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        quantity: Decimal::ONE,
        price: price(10)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(true),
        exchange_time_ms: Some(10),
    })
}

pub(super) struct EvidenceFixture {
    _directory: TempDir,
    journal: PrivateEvidenceJournal,
}

impl EvidenceFixture {
    pub(super) fn new() -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        Ok(Self {
            _directory: directory,
            journal,
        })
    }

    pub(super) fn append(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        payload: &str,
    ) -> Result<PersistedPrivateEvidence, Box<dyn Error>> {
        Ok(self.journal.append_persisted(PrivateEvidence::new(
            generation,
            received_at_ms,
            payload.to_owned(),
        )?)?)
    }
}

pub(super) fn private_fact(
    evidence: &PersistedPrivateEvidence,
    event: DomainEvent,
) -> Result<PersistedPrivateFact, Box<dyn Error>> {
    private_fact_indexed(evidence, event, 0, 1)
}

fn private_fact_indexed(
    evidence: &PersistedPrivateEvidence,
    event: DomainEvent,
    fact_index: u32,
    fact_count: u32,
) -> Result<PersistedPrivateFact, Box<dyn Error>> {
    let order_family = matches!(event, DomainEvent::Order(_) | DomainEvent::Fill(_))
        .then_some(NativeOrderFamily::UmOrder);
    let record = crate::domain::FactRecord {
        header: EventHeader {
            schema_version: 1,
            event_id: EventId::new(format!("private_{}", evidence.sequence()))?,
            source: EventSource::PrivateAccount,
            source_sequence: Some(evidence.sequence()),
            received_at_ms: evidence.received_at_ms(),
            generation: evidence.generation(),
        },
        event,
    };
    Ok(PersistedPrivateFact::new_indexed(
        evidence,
        order_family,
        fact_index,
        fact_count,
        record,
    )?)
}

fn place_intent(
    applied: &crate::domain::AppliedStrategyTurnReceipt,
    binding: &StrategyBinding,
    suffix: &str,
    priority: AccountLanePriority,
) -> Result<AccountExecutionIntent, Box<dyn Error>> {
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new(format!("cmd_{suffix}"))?,
        client_order_id: CommandId::new(format!("client_{suffix}"))?,
        owner: owner(binding, OrderPurpose::Entry),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: price(10)?,
        reduce_only: false,
    });
    let identity = crate::execution::CommandIdentityReceipt::test_persisted_output_allocation(
        applied, &command, None, 1,
    )?;
    Ok(AccountExecutionIntent::from_applied_turn(
        applied, priority, command, identity,
    )?)
}

fn cancel_intent(
    applied: &crate::domain::AppliedStrategyTurnReceipt,
    binding: &StrategyBinding,
    suffix: &str,
) -> Result<AccountExecutionIntent, Box<dyn Error>> {
    let command = ExecutionCommand::Cancel(CancelCommand {
        command_id: CommandId::new(format!("cancel_{suffix}"))?,
        owner: owner(binding, OrderPurpose::Entry),
        target_client_order_id: CommandId::new(format!("target_{suffix}"))?,
    });
    let identity = crate::execution::CommandIdentityReceipt::test_persisted_output_allocation(
        applied,
        &command,
        Some(NativeOrderFamily::UmOrder),
        1,
    )?;
    Ok(AccountExecutionIntent::from_applied_turn(
        applied,
        AccountLanePriority::Critical,
        command,
        identity,
    )?)
}

fn fake_applied_receipt(
    binding: &StrategyBinding,
) -> Result<crate::domain::AppliedStrategyTurnReceipt, Box<dyn Error>> {
    let token = crate::domain::StrategyTurnToken::issue(
        binding.key.clone(),
        1,
        1,
        binding.config_digest.clone(),
        1,
        1,
    )?;
    Ok(crate::domain::AppliedStrategyTurnReceipt::persisted(token))
}

pub(super) fn runtime_place_intent(
    runtime: &AccountRuntime,
    binding: &StrategyBinding,
    suffix: &str,
    priority: AccountLanePriority,
) -> Result<AccountExecutionIntent, Box<dyn Error>> {
    let applied = runtime
        .latest_applied_turn_receipt(&binding.key)
        .ok_or("applied actor turn missing")?;
    place_intent(&applied, binding, suffix, priority)
}

fn runtime_cancel_intent(
    runtime: &AccountRuntime,
    binding: &StrategyBinding,
    suffix: &str,
    target_client_order_id: &str,
    family: NativeOrderFamily,
) -> Result<AccountExecutionIntent, Box<dyn Error>> {
    let applied = runtime
        .latest_applied_turn_receipt(&binding.key)
        .ok_or("applied actor turn missing")?;
    let command = ExecutionCommand::Cancel(CancelCommand {
        command_id: CommandId::new(format!("cancel_{suffix}"))?,
        owner: owner(binding, OrderPurpose::Entry),
        target_client_order_id: CommandId::new(target_client_order_id)?,
    });
    let identity = crate::execution::CommandIdentityReceipt::test_persisted_output_allocation(
        &applied,
        &command,
        Some(family),
        1,
    )?;
    Ok(AccountExecutionIntent::from_applied_turn(
        &applied,
        AccountLanePriority::Critical,
        command,
        identity,
    )?)
}

fn authorized_request(
    intent: AccountExecutionIntent,
) -> Result<AccountExecutionRequest, Box<dyn Error>> {
    Ok(AccountExecutionRequest::authorize(intent)?)
}

fn lane_next_dispatch_permit(
    lane: &mut AccountExecutionLane,
    dispatch_revision: u64,
    wal_sequence: u64,
) -> Result<AccountDispatchPermit, Box<dyn Error>> {
    let candidate = lane
        .next_for_wal(dispatch_revision)?
        .ok_or("scheduled mutation missing")?;
    let wal = PersistedWalPreparedReceipt::test_persisted(candidate, wal_sequence)?;
    let writer = PersistedWriterLeaseReceipt::test_verified_current(
        &wal,
        AccountWriterCapability::EntryAndRiskReduction,
        wal_sequence,
    )?;
    match lane.authorize_dispatch(wal, writer, dispatch_revision, |_| true)? {
        AccountDispatchDecision::Permit(permit) => Ok(permit),
        AccountDispatchDecision::Fenced(_) => Err("dispatch unexpectedly fenced".into()),
    }
}

fn runtime_next_dispatch_permit(
    runtime: &mut AccountRuntime,
    wal_sequence: u64,
) -> Result<AccountDispatchPermit, Box<dyn Error>> {
    let candidate = runtime
        .next_execution_for_wal()?
        .ok_or("runtime mutation candidate missing")?;
    let wal = PersistedWalPreparedReceipt::test_persisted(candidate, wal_sequence)?;
    let writer = PersistedWriterLeaseReceipt::test_verified_current(
        &wal,
        AccountWriterCapability::EntryAndRiskReduction,
        wal_sequence,
    )?;
    match runtime.authorize_execution_dispatch(wal, writer)? {
        AccountDispatchDecision::Permit(permit) => Ok(permit),
        AccountDispatchDecision::Fenced(_) => Err("runtime dispatch unexpectedly fenced".into()),
    }
}

pub(super) fn place_request(
    binding: &StrategyBinding,
    suffix: &str,
    priority: AccountLanePriority,
) -> Result<AccountExecutionRequest, Box<dyn Error>> {
    let applied = fake_applied_receipt(binding)?;
    authorized_request(place_intent(&applied, binding, suffix, priority)?)
}

fn cancel_request(
    binding: &StrategyBinding,
    suffix: &str,
) -> Result<AccountExecutionRequest, Box<dyn Error>> {
    let applied = fake_applied_receipt(binding)?;
    authorized_request(cancel_intent(&applied, binding, suffix)?)
}

fn zero_hedge_positions(keys: impl IntoIterator<Item = StrategyInstanceKey>) -> Vec<Position> {
    keys.into_iter()
        .flat_map(|key| {
            [PositionSide::Long, PositionSide::Short].map(move |side| Position {
                symbol: key.symbol.clone(),
                side,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            })
        })
        .collect()
}

fn set_desired(
    runtime: &AccountRuntime,
    desired: &mut DesiredOrderSets,
    key: &StrategyInstanceKey,
    client_order_ids: impl IntoIterator<Item = String>,
) -> Result<(), Box<dyn Error>> {
    let registration = runtime
        .registry()
        .registration(key)
        .ok_or("strategy registration missing")?;
    let desired_price = price(10)?;
    let orders = client_order_ids
        .into_iter()
        .map(|client_order_id| {
            DesiredOrder::verified(
                NativeOrderFamily::UmOrder,
                client_order_id,
                OrderPurpose::Entry,
                OrderSide::Buy,
                PositionSide::Long,
                Some(Decimal::ONE),
                Some(desired_price.clone()),
                false,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(applied) = runtime.latest_applied_turn_receipt(key) {
        desired.set_from_applied_turn(&applied, orders)?;
    } else {
        let checkpoint = DesiredCheckpointFingerprint::verified("ab".repeat(32))?;
        let receipt = RecoveredDesiredOrdersReceipt::verified_checkpoint(
            key.clone(),
            runtime.connection_generation(),
            0,
            registration.binding.config_digest.clone(),
            registration.config_epoch,
            1,
            checkpoint,
        )?;
        desired.set_recovered(receipt, orders)?;
    }
    Ok(())
}

fn signed_orders(
    runtime: &AccountRuntime,
    private_generation: u64,
    orders: Vec<Order>,
    required_position_sides: Vec<PositionSide>,
    positions: Vec<Position>,
) -> Result<SignedOpenOrders, Box<dyn Error>> {
    let account = account()?;
    let connection_generation = runtime.connection_generation();
    let mode = if required_position_sides == vec![PositionSide::Net] {
        AccountPositionMode::Net
    } else {
        AccountPositionMode::Hedge
    };
    let position_snapshot = SignedPositionSnapshot::verified_complete(
        account.clone(),
        connection_generation,
        private_generation,
        mode,
        positions,
    )?;
    Ok(SignedOpenOrders::verified(
        account.clone(),
        connection_generation,
        private_generation,
        vec![
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                connection_generation,
                NativeOrderFamily::UmOrder,
                private_generation,
                orders,
                BTreeMap::new(),
            )?,
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                connection_generation,
                NativeOrderFamily::UmConditional,
                private_generation,
                Vec::new(),
                BTreeMap::new(),
            )?,
            SignedOrderFamilySnapshot::verified_complete(
                account,
                connection_generation,
                NativeOrderFamily::UmAlgo,
                private_generation,
                Vec::new(),
                BTreeMap::new(),
            )?,
        ],
        position_snapshot,
    )?)
}

pub(super) fn pop_applied_strategy_input(
    runtime: &mut AccountRuntime,
    key: &StrategyInstanceKey,
) -> Result<Option<StrategyInput>, Box<dyn Error>> {
    let Some(turn) = runtime.pop_strategy_input(key)? else {
        return Ok(None);
    };
    let input = turn.input().clone();
    runtime.acknowledge_strategy_turn(crate::domain::AppliedStrategyTurnReceipt::persisted(
        turn.token().clone(),
    ))?;
    Ok(Some(input))
}

fn route_persisted_private(
    runtime: &mut AccountRuntime,
    fact: PersistedPrivateFact,
) -> Result<PrivateRouteReport, Box<dyn Error>> {
    let plan = runtime.plan_private_route(fact)?;
    Ok(runtime.commit_private_route(PersistedPrivateDispatchReceipt::persisted(plan))?)
}

fn apply_next_reconciliation(
    runtime: &mut AccountRuntime,
    key: &StrategyInstanceKey,
) -> Result<crate::runtime::strategy::ReconciliationNotice, Box<dyn Error>> {
    loop {
        match pop_applied_strategy_input(runtime, key)? {
            Some(StrategyInput::Reconciliation(notice)) => return Ok(notice),
            Some(StrategyInput::Control(_) | StrategyInput::Private(_)) => continue,
            _ => return Err("reconciliation notice missing".into()),
        }
    }
}

pub(super) fn establish_empty_signed_orders(
    runtime: &mut AccountRuntime,
    private_generation: u64,
) -> Result<(), Box<dyn Error>> {
    let keys: Vec<_> = runtime
        .registry()
        .registrations()
        .map(|registration| registration.binding.key.clone())
        .collect();
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    for key in &keys {
        set_desired(runtime, &mut desired, key, Vec::new())?;
    }
    runtime.reconcile(
        &desired,
        signed_orders(
            runtime,
            private_generation,
            Vec::new(),
            vec![PositionSide::Long, PositionSide::Short],
            zero_hedge_positions(keys.clone()),
        )?,
    )?;
    for key in keys {
        let notice = apply_next_reconciliation(runtime, &key)?;
        if !notice.exact() {
            return Err("startup reconciliation was not exact".into());
        }
    }
    Ok(())
}

#[test]
fn registry_isolates_symbols_and_rejects_duplicate_ownership() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let scalp = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut registry = StrategyRegistry::new(account()?);
    registry.register(grid.clone())?;
    registry.register(scalp.clone())?;

    let same_symbol = binding(StrategyKind::Scalping, "scalp_sol", "SOL/USDT")?;
    assert_eq!(
        registry.register(same_symbol),
        Err(RegistryError::SymbolOccupied)
    );
    let same_instance = binding(StrategyKind::HedgedGrid, "grid_sol", "BTC/USDT")?;
    assert_eq!(
        registry.register(same_instance),
        Err(RegistryError::InstanceOccupied)
    );
    assert_eq!(
        registry.binding_by_symbol(&"ETH/USDT".parse()?),
        Some(&scalp)
    );
    Ok(())
}

#[test]
fn private_router_delivers_exact_owner_and_never_guesses_unknown_fill() -> Result<(), Box<dyn Error>>
{
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let scalp = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    runtime.register_strategy(scalp.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_grid_1")?,
            "grid_client_1".to_owned(),
            Some("grid_venue_1".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;

    let mut evidence = EvidenceFixture::new()?;
    let first = evidence.append(1, 100, "owned fill")?;
    let report = route_persisted_private(
        &mut runtime,
        private_fact(
            &first,
            DomainEvent::Fill(fill(&grid, "fill_1", "grid_venue_1")?),
        )?,
    )?;
    assert_eq!(report.deliveries.len(), 1);
    assert_eq!(report.deliveries[0].target, grid.key);
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    assert!(pop_applied_strategy_input(&mut runtime, &scalp.key)?.is_none());

    let second = evidence.append(1, 101, "unknown fill")?;
    let report = route_persisted_private(
        &mut runtime,
        private_fact(
            &second,
            DomainEvent::Fill(fill(&grid, "fill_2", "unknown_venue")?),
        )?,
    )?;
    assert!(report.deliveries.is_empty());
    assert_eq!(
        report.reconcile.as_ref().map(|value| value.reason),
        Some(ReconcileReason::UnknownOwner)
    );
    assert_eq!(
        runtime
            .registry()
            .registration(&grid.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::NeedsAttention)
    );
    assert_eq!(
        runtime
            .registry()
            .registration(&scalp.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::Running)
    );
    assert!(matches!(
        runtime.request_pause(&grid.key),
        Err(AccountRuntimeError::Registry(RegistryError::Lifecycle))
    ));
    Ok(())
}

#[test]
fn conflicting_client_and_venue_identities_are_not_delivered() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let scalp = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    runtime.register_strategy(scalp.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_grid")?,
            "grid_client".to_owned(),
            Some("grid_venue".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_scalp")?,
            "scalp_client".to_owned(),
            Some("scalp_venue".to_owned()),
            owner(&scalp, OrderPurpose::Entry),
        ),
    )?;

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "conflicting order")?;
    let mut order = open_order(&grid, "scalp_venue", "grid_client")?;
    order.symbol = grid.key.symbol.clone();
    let report = route_persisted_private(
        &mut runtime,
        private_fact(&persisted, DomainEvent::Order(order))?,
    )?;
    assert!(report.deliveries.is_empty());
    assert_eq!(
        report.reconcile.as_ref().map(|value| value.reason),
        Some(ReconcileReason::IdentityConflict)
    );
    assert!(pop_applied_strategy_input(&mut runtime, &grid.key)?.is_none());
    assert!(pop_applied_strategy_input(&mut runtime, &scalp.key)?.is_none());
    Ok(())
}

#[test]
fn execution_reserves_create_identity_and_cancel_requires_exact_owner_family()
-> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_cancel_target")?,
            "cancel_target".to_owned(),
            Some("venue_target".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;

    runtime.enqueue_execution(runtime_cancel_intent(
        &runtime,
        &grid,
        "exact",
        "cancel_target",
        NativeOrderFamily::UmOrder,
    )?)?;
    let permit = runtime_next_dispatch_permit(&mut runtime, 1)?;
    runtime.record_execution_outcome(PersistedMutationOutcomeReceipt::test_persisted(
        &permit,
        AccountMutationOutcome::Confirmed,
        1,
    )?)?;

    let applied = runtime
        .latest_applied_turn_receipt(&grid.key)
        .ok_or("applied actor turn missing")?;
    let reused_native_id = ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new("cmd_reuse_native")?,
        client_order_id: CommandId::new("cancel_target")?,
        owner: owner(&grid, OrderPurpose::Entry),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: price(10)?,
        reduce_only: false,
    });
    let reused_identity =
        crate::execution::CommandIdentityReceipt::test_persisted_output_allocation(
            &applied,
            &reused_native_id,
            None,
            2,
        )?;
    assert!(matches!(
        runtime.enqueue_execution(AccountExecutionIntent::from_applied_turn(
            &applied,
            AccountLanePriority::Normal,
            reused_native_id,
            reused_identity,
        )?),
        Err(AccountRuntimeError::PrivateRouter(
            PrivateRouterError::Conflict
        ))
    ));

    assert!(matches!(
        runtime.enqueue_execution(runtime_cancel_intent(
            &runtime,
            &grid,
            "wrong_family",
            "cancel_target",
            NativeOrderFamily::UmAlgo,
        )?),
        Err(AccountRuntimeError::PrivateRouter(
            PrivateRouterError::CancelTarget
        ))
    ));
    Ok(())
}

#[test]
fn actor_mailbox_preserves_private_priority_and_coalesces_market() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_grid")?,
            "grid_client".to_owned(),
            Some("grid_venue".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;

    for update_id in [1, 2] {
        runtime.publish_market(AccountMarketEvent::new(
            100 + update_id,
            crate::domain::MarketEvent::Ticker(PublicTicker {
                symbol: grid.key.symbol.clone(),
                generation: 1,
                received_at_ms: 100 + update_id,
                exchange_time_ms: 90 + update_id,
                transaction_time_ms: 90 + update_id,
                update_id,
                bid_price: price(9)?,
                bid_quantity: Decimal::ONE,
                ask_price: price(10)?,
                ask_quantity: Decimal::ONE,
            }),
        )?)?;
    }
    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 200, "fill before market")?;
    route_persisted_private(
        &mut runtime,
        private_fact(
            &persisted,
            DomainEvent::Fill(fill(&grid, "fill_1", "grid_venue")?),
        )?,
    )?;

    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    let Some(StrategyInput::Market(market)) = pop_applied_strategy_input(&mut runtime, &grid.key)?
    else {
        return Err("coalesced market event missing".into());
    };
    assert_eq!(market.sequence(), 2);
    assert!(pop_applied_strategy_input(&mut runtime, &grid.key)?.is_none());
    Ok(())
}

#[test]
fn account_lane_prioritizes_and_round_robins_within_priority() -> Result<(), Box<dyn Error>> {
    let a = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let b = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut lane = AccountExecutionLane::new(account()?);
    for (request, binding) in [
        (place_request(&a, "a1", AccountLanePriority::Normal)?, &a),
        (place_request(&a, "a2", AccountLanePriority::Normal)?, &a),
        (place_request(&b, "b1", AccountLanePriority::Normal)?, &b),
        (place_request(&b, "b2", AccountLanePriority::Normal)?, &b),
        (cancel_request(&b, "urgent")?, &b),
    ] {
        lane.enqueue(request, binding)?;
    }

    let mut observed = Vec::new();
    for _ in 0..5 {
        let permit = lane_next_dispatch_permit(&mut lane, 1, 1)?;
        let command_id = permit.command_id().clone();
        observed.push(command_id.as_str().to_owned());
        assert_eq!(
            lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
                &permit,
                AccountMutationOutcome::Confirmed,
                1,
            )?)?,
            AccountLaneFollowUp::None
        );
    }
    assert_eq!(
        observed,
        ["cancel_urgent", "cmd_a1", "cmd_b1", "cmd_a2", "cmd_b2"]
    );
    Ok(())
}

#[test]
fn account_lane_bounds_critical_starvation() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut lane = AccountExecutionLane::new(account()?);
    for index in 0..65 {
        lane.enqueue(cancel_request(&grid, &format!("critical_{index}"))?, &grid)?;
    }
    lane.enqueue(
        place_request(&grid, "normal_after_burst", AccountLanePriority::Normal)?,
        &grid,
    )?;
    for _ in 0..64 {
        let permit = lane_next_dispatch_permit(&mut lane, 1, 1)?;
        let command_id = permit.command_id().clone();
        assert!(command_id.as_str().starts_with("cancel_critical_"));
        lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
            &permit,
            AccountMutationOutcome::Confirmed,
            1,
        )?)?;
    }
    let command_id = lane
        .next_for_wal(1)?
        .ok_or("normal mutation starved")?
        .command_id()
        .clone();
    assert_eq!(command_id.as_str(), "cmd_normal_after_burst");
    Ok(())
}

#[test]
fn account_lane_bounds_starvation_across_all_priorities() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut lane = AccountExecutionLane::new(account()?);
    for index in 0..(64 * 17) {
        lane.enqueue(cancel_request(&grid, &format!("fair_{index}"))?, &grid)?;
    }
    for index in 0..16 {
        lane.enqueue(
            place_request(
                &grid,
                &format!("repair_{index}"),
                AccountLanePriority::FillRepair,
            )?,
            &grid,
        )?;
    }
    lane.enqueue(
        place_request(&grid, "normal_fair", AccountLanePriority::Normal)?,
        &grid,
    )?;

    let mut dispatched = 0;
    loop {
        let permit = lane_next_dispatch_permit(&mut lane, 1, 1)?;
        let command_id = permit.command_id().clone();
        dispatched += 1;
        if command_id.as_str() == "cmd_normal_fair" {
            break;
        }
        lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
            &permit,
            AccountMutationOutcome::Confirmed,
            1,
        )?)?;
        if dispatched > 1_200 {
            return Err("normal priority exceeded bounded service window".into());
        }
    }
    assert_eq!(dispatched, 64 * 17 + 16 + 1);
    Ok(())
}

#[test]
fn unknown_fences_only_new_risk_for_its_instance() -> Result<(), Box<dyn Error>> {
    let a = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let b = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut lane = AccountExecutionLane::new(account()?);
    let unknown = place_request(&a, "unknown", AccountLanePriority::Normal)?;
    let unknown_id = unknown.command_id().clone();
    lane.enqueue(unknown, &a)?;
    lane.enqueue(
        place_request(&a, "prequeued", AccountLanePriority::Normal)?,
        &a,
    )?;
    let unknown_permit = lane_next_dispatch_permit(&mut lane, 1, 1)?;
    assert_eq!(unknown_permit.command_id(), &unknown_id);
    assert!(matches!(
        lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
            &unknown_permit,
            AccountMutationOutcome::Unknown,
            1,
        )?)?,
        AccountLaneFollowUp::ReconcileUnknown { .. }
    ));

    assert_eq!(
        lane.enqueue(
            place_request(&a, "blocked", AccountLanePriority::Normal)?,
            &a
        ),
        Err(AccountLaneError::UnknownFence)
    );
    lane.enqueue(cancel_request(&a, "safe")?, &a)?;
    lane.enqueue(
        place_request(&b, "sibling", AccountLanePriority::Normal)?,
        &b,
    )?;
    let safe_permit = lane_next_dispatch_permit(&mut lane, 1, 2)?;
    let safe_id = safe_permit.command_id().clone();
    assert_eq!(safe_id.as_str(), "cancel_safe");
    lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
        &safe_permit,
        AccountMutationOutcome::Confirmed,
        2,
    )?)?;
    let sibling_permit = lane_next_dispatch_permit(&mut lane, 1, 3)?;
    let sibling_id = sibling_permit.command_id().clone();
    assert_eq!(sibling_id.as_str(), "cmd_sibling");
    lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
        &sibling_permit,
        AccountMutationOutcome::Confirmed,
        3,
    )?)?;

    assert_eq!(
        lane.resolve_unknown(UnknownReadbackProof::test_verified(
            unknown_id.clone(),
            a.key.clone(),
            CommandId::new("wrong_native_client")?,
            Some(NativeOrderFamily::UmOrder),
            1,
            2,
            UnknownResolution::ProvenAbsent,
        )?),
        Err(AccountLaneError::UnknownProof)
    );
    let follow_up = lane.resolve_unknown(UnknownReadbackProof::test_verified(
        unknown_id.clone(),
        a.key.clone(),
        CommandId::new("client_unknown")?,
        Some(NativeOrderFamily::UmOrder),
        1,
        2,
        UnknownResolution::ProvenAbsent,
    )?)?;
    assert!(matches!(
        follow_up,
        AccountLaneFollowUp::StrategyReplanRequired {
            reason: crate::execution::AccountReplanReason::ProvenAbsent,
            ..
        }
    ));
    let prequeued_permit = lane_next_dispatch_permit(&mut lane, 1, 4)?;
    let prequeued_id = prequeued_permit.command_id().clone();
    assert_eq!(prequeued_id.as_str(), "cmd_prequeued");
    lane.record_outcome(PersistedMutationOutcomeReceipt::test_persisted(
        &prequeued_permit,
        AccountMutationOutcome::Confirmed,
        4,
    )?)?;
    assert!(lane.next_for_wal(1)?.is_none());
    Ok(())
}

#[test]
fn reconnect_discards_pre_disconnect_risk_intents() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "before_disconnect",
        AccountLanePriority::Normal,
    )?)?;

    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    assert!(runtime.next_execution_for_wal()?.is_none());
    Ok(())
}

#[test]
fn wal_abort_requires_receipt_and_returns_no_authorized_request() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "wal_abort",
        AccountLanePriority::Normal,
    )?)?;
    let candidate = runtime
        .next_execution_for_wal()?
        .ok_or("WAL candidate missing")?;
    let absence = WalNotPreparedReceipt::test_verified(candidate, 1)?;
    assert!(matches!(
        runtime.abort_execution_before_wal(absence)?,
        AccountLaneFollowUp::StrategyReplanRequired {
            reason: crate::execution::AccountReplanReason::WalNotPrepared,
            ..
        }
    ));
    assert!(runtime.next_execution_for_wal()?.is_none());
    Ok(())
}

#[test]
fn reconnect_requires_in_flight_mutation_to_become_unknown() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "in_flight_disconnect",
        AccountLanePriority::Normal,
    )?)?;
    let permit = runtime_next_dispatch_permit(&mut runtime, 1)?;
    assert!(matches!(
        runtime.mark_account_ready(),
        Err(AccountRuntimeError::ReconnectWithInFlight)
    ));
    assert!(matches!(
        runtime.record_execution_outcome(PersistedMutationOutcomeReceipt::test_persisted(
            &permit,
            AccountMutationOutcome::Unknown,
            1,
        )?)?,
        AccountLaneFollowUp::ReconcileUnknown { .. }
    ));
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    assert!(matches!(
        runtime.enqueue_execution(runtime_place_intent(
            &runtime,
            &grid,
            "still_fenced",
            AccountLanePriority::Normal,
        )?),
        Err(AccountRuntimeError::ExecutionLane(
            AccountLaneError::UnknownFence
        ))
    ));
    Ok(())
}

#[test]
fn pause_survives_reconnect_and_resume_requires_new_exact_state() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.request_pause(&grid.key)?;

    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    assert_eq!(
        runtime
            .registry()
            .registration(&grid.key)
            .map(|registration| registration.lifecycle),
        Some(InstanceLifecycle::Paused)
    );
    assert!(matches!(
        runtime.enqueue_execution(runtime_place_intent(
            &runtime,
            &grid,
            "paused",
            AccountLanePriority::Normal,
        )?),
        Err(AccountRuntimeError::RiskFenced)
    ));

    runtime.request_resume(&grid.key)?;
    assert_eq!(
        runtime
            .registry()
            .registration(&grid.key)
            .map(|registration| registration.lifecycle),
        Some(InstanceLifecycle::Recovering)
    );
    establish_empty_signed_orders(&mut runtime, 2)?;
    assert_eq!(
        runtime
            .registry()
            .registration(&grid.key)
            .map(|registration| registration.lifecycle),
        Some(InstanceLifecycle::Running)
    );
    Ok(())
}

#[test]
fn parameter_change_is_epoch_bound_and_discards_old_intents() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.enqueue_execution(runtime_place_intent(
        &runtime,
        &grid,
        "old_configuration",
        AccountLanePriority::Normal,
    )?)?;

    runtime.change_parameters(&grid.key, "config_2".to_owned())?;
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Control(
            crate::runtime::strategy::StrategyControl::ParametersChanged {
                config_digest,
                config_epoch: 2,
            }
        )) if config_digest == "config_2"
    ));
    let registration = runtime
        .registry()
        .registration(&grid.key)
        .ok_or("changed registration missing")?;
    assert_eq!(registration.binding.config_digest, "config_2");
    assert_eq!(registration.config_epoch, 2);
    assert_eq!(registration.lifecycle, InstanceLifecycle::Recovering);

    let mut stale_desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    let stale_checkpoint = DesiredCheckpointFingerprint::verified("cd".repeat(32))?;
    let stale_receipt = RecoveredDesiredOrdersReceipt::verified_checkpoint(
        grid.key.clone(),
        runtime.connection_generation(),
        1,
        "config_1",
        1,
        1,
        stale_checkpoint,
    )?;
    stale_desired.set_recovered(stale_receipt, Vec::<DesiredOrder>::new())?;
    let stale_signed = signed_orders(
        &runtime,
        2,
        Vec::new(),
        vec![PositionSide::Long, PositionSide::Short],
        zero_hedge_positions([grid.key.clone()]),
    )?;
    assert!(matches!(
        runtime.reconcile(&stale_desired, stale_signed),
        Err(AccountRuntimeError::Reconciler(
            AccountReconcilerError::DesiredAuthority
        ))
    ));
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    assert!(runtime.next_execution_for_wal()?.is_none());
    Ok(())
}

#[test]
fn stop_preserves_residual_custody_and_releases_only_after_flat_zero_orders()
-> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let sibling = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    runtime.register_strategy(sibling.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;

    let plan = runtime.request_stop(&grid.key)?;
    assert!(plan.cancel_owned_orders);
    assert!(plan.preserve_position);
    assert_eq!(
        runtime
            .registry()
            .registration(&sibling.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::Running)
    );
    assert!(matches!(
        runtime.complete_stop(&grid.key),
        Err(AccountRuntimeError::Registry(RegistryError::StopNotProven))
    ));
    assert!(matches!(
        runtime.register_strategy(binding(StrategyKind::Scalping, "replacement", "SOL/USDT")?),
        Err(AccountRuntimeError::Registry(RegistryError::SymbolOccupied))
    ));
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &grid.key, Vec::new())?;
    set_desired(&runtime, &mut desired, &sibling.key, Vec::new())?;
    let mut positions = zero_hedge_positions([grid.key.clone(), sibling.key.clone()]);
    let grid_long = positions
        .iter_mut()
        .find(|position| position.symbol == grid.key.symbol && position.side == PositionSide::Long)
        .ok_or("grid long leg missing")?;
    grid_long.quantity = Decimal::ONE;
    grid_long.entry_price = Some(price(10)?);
    runtime.reconcile(
        &desired,
        signed_orders(
            &runtime,
            2,
            Vec::new(),
            vec![PositionSide::Long, PositionSide::Short],
            positions,
        )?,
    )?;
    assert!(matches!(
        runtime.complete_stop(&grid.key),
        Err(AccountRuntimeError::ResidualPositionCustody)
    ));
    let _grid_notice = apply_next_reconciliation(&mut runtime, &grid.key)?;
    let _sibling_notice = apply_next_reconciliation(&mut runtime, &sibling.key)?;
    establish_empty_signed_orders(&mut runtime, 3)?;
    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "pending Stop balance")?;
    route_persisted_private(
        &mut runtime,
        private_fact(
            &persisted,
            DomainEvent::Balance(AccountBalance {
                asset: "USDT".parse()?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            }),
        )?,
    )?;
    assert!(matches!(
        runtime.complete_stop(&grid.key),
        Err(AccountRuntimeError::ShutdownActorStatePending)
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    runtime.complete_stop(&grid.key)?;
    runtime.register_strategy(binding(StrategyKind::Scalping, "replacement", "SOL/USDT")?)?;
    Ok(())
}

#[test]
fn flatten_requires_complete_same_generation_zero_hedge_legs() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.request_flatten(&grid.key)?;

    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &grid.key, Vec::new())?;
    let incomplete_signed = signed_orders(
        &runtime,
        2,
        Vec::new(),
        vec![PositionSide::Long, PositionSide::Short],
        vec![Position {
            symbol: grid.key.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        }],
    )?;
    assert!(matches!(
        runtime.reconcile(&desired, incomplete_signed),
        Err(AccountRuntimeError::Reconciler(
            AccountReconcilerError::PositionCoverage
        ))
    ));

    runtime.mark_account_ready()?;
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &grid.key, Vec::new())?;

    let nonzero_signed = signed_orders(
        &runtime,
        1,
        Vec::new(),
        vec![PositionSide::Long, PositionSide::Short],
        vec![
            Position {
                symbol: grid.key.symbol.clone(),
                side: PositionSide::Long,
                quantity: Decimal::ONE,
                entry_price: Some(price(10)?),
                mark_price: Some(price(10)?),
            },
            Position {
                symbol: grid.key.symbol.clone(),
                side: PositionSide::Short,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            },
        ],
    )?;
    runtime.reconcile(&desired, nonzero_signed)?;
    assert!(matches!(
        runtime.complete_flatten(&grid.key),
        Err(AccountRuntimeError::FlattenNotProven)
    ));
    let _notice = apply_next_reconciliation(&mut runtime, &grid.key)?;
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &grid.key, Vec::new())?;

    let zero_signed = signed_orders(
        &runtime,
        2,
        Vec::new(),
        vec![PositionSide::Long, PositionSide::Short],
        zero_hedge_positions([grid.key.clone()]),
    )?;
    runtime.reconcile(&desired, zero_signed)?;
    let active = runtime
        .pop_strategy_input(&grid.key)?
        .ok_or("shutdown actor input missing")?;
    assert!(matches!(
        runtime.complete_flatten(&grid.key),
        Err(AccountRuntimeError::ShutdownActorStatePending)
    ));
    runtime.acknowledge_strategy_turn(crate::domain::AppliedStrategyTurnReceipt::persisted(
        active.token().clone(),
    ))?;
    runtime.complete_flatten(&grid.key)?;
    assert!(runtime.registry().registration(&grid.key).is_none());
    Ok(())
}

#[test]
fn signed_order_drift_immediately_recovers_only_missing_instance() -> Result<(), Box<dyn Error>> {
    let a = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let b = binding(StrategyKind::Scalping, "scalp_eth", "ETH/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(a.clone())?;
    runtime.register_strategy(b.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_a")?,
            "a_client".to_owned(),
            Some("a_venue".to_owned()),
            owner(&a, OrderPurpose::Entry),
        ),
    )?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_b")?,
            "b_client".to_owned(),
            Some("b_venue".to_owned()),
            owner(&b, OrderPurpose::Entry),
        ),
    )?;
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &a.key, ["a_client".to_owned()])?;
    set_desired(&runtime, &mut desired, &b.key, ["b_client".to_owned()])?;

    let drift_signed = signed_orders(
        &runtime,
        2,
        vec![open_order(&b, "b_venue", "b_client")?],
        vec![PositionSide::Long, PositionSide::Short],
        zero_hedge_positions([a.key.clone(), b.key.clone()]),
    )?;
    let report = runtime.reconcile(&desired, drift_signed)?;
    let by_instance: BTreeMap<_, _> = report
        .instances
        .iter()
        .map(|instance| (instance.target.instance_id.as_str(), &instance.notice))
        .collect();
    assert_eq!(
        by_instance["grid_sol"].missing_client_order_ids,
        ["UmOrder:a_client"]
    );
    assert!(by_instance["scalp_eth"].exact());
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &a.key)?,
        Some(StrategyInput::Reconciliation(_))
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &b.key)?,
        Some(StrategyInput::Reconciliation(_))
    ));
    assert_eq!(
        runtime
            .registry()
            .registration(&a.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::Recovering)
    );
    assert_eq!(
        runtime
            .registry()
            .registration(&b.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::Running)
    );

    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(&runtime, &mut desired, &a.key, ["a_client".to_owned()])?;
    set_desired(&runtime, &mut desired, &b.key, ["b_client".to_owned()])?;
    let exact_signed = signed_orders(
        &runtime,
        3,
        vec![
            open_order(&a, "a_venue", "a_client")?,
            open_order(&b, "b_venue", "b_client")?,
        ],
        vec![PositionSide::Long, PositionSide::Short],
        zero_hedge_positions([a.key.clone(), b.key.clone()]),
    )?;
    runtime.reconcile(&desired, exact_signed)?;
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &a.key)?,
        Some(StrategyInput::Reconciliation(_))
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &b.key)?,
        Some(StrategyInput::Reconciliation(_))
    ));
    assert_eq!(
        runtime
            .registry()
            .registration(&a.key)
            .map(|value| value.lifecycle),
        Some(InstanceLifecycle::Running)
    );
    Ok(())
}

#[test]
fn signed_order_identity_match_still_detects_semantic_drift() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_grid")?,
            "grid_client".to_owned(),
            Some("grid_venue".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;
    let mut desired = DesiredOrderSets::new(AccountPositionMode::Hedge);
    set_desired(
        &runtime,
        &mut desired,
        &grid.key,
        ["grid_client".to_owned()],
    )?;
    let mut drifted = open_order(&grid, "grid_venue", "grid_client")?;
    drifted.limit_price = Some(price(11)?);
    let report = runtime.reconcile(
        &desired,
        signed_orders(
            &runtime,
            2,
            vec![drifted],
            vec![PositionSide::Long, PositionSide::Short],
            zero_hedge_positions([grid.key.clone()]),
        )?,
    )?;
    assert_eq!(
        report.instances[0].notice.mismatched_client_order_ids,
        ["UmOrder:grid_client"]
    );
    assert!(!report.instances[0].notice.exact());
    assert_eq!(
        runtime
            .registry()
            .registration(&grid.key)
            .map(|registration| registration.lifecycle),
        Some(InstanceLifecycle::Recovering)
    );
    Ok(())
}

#[test]
fn private_evidence_gap_freezes_account_before_delivery() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    install_persisted_order_route(
        &mut runtime,
        RecoveredOrderRoute::verified(
            NativeOrderFamily::UmOrder,
            CommandId::new("cmd_grid")?,
            "grid_client".to_owned(),
            Some("grid_venue".to_owned()),
            owner(&grid, OrderPurpose::Entry),
        ),
    )?;
    let mut evidence = EvidenceFixture::new()?;
    let first = evidence.append(1, 100, "first")?;
    let _skipped = evidence.append(1, 101, "skipped")?;
    let third = evidence.append(1, 102, "third")?;
    route_persisted_private(
        &mut runtime,
        private_fact(
            &first,
            DomainEvent::Fill(fill(&grid, "fill_1", "grid_venue")?),
        )?,
    )?;
    let report = route_persisted_private(
        &mut runtime,
        private_fact(
            &third,
            DomainEvent::Fill(fill(&grid, "fill_3", "grid_venue")?),
        )?,
    )?;
    assert!(report.deliveries.is_empty());
    assert_eq!(runtime.health(), AccountHealth::Frozen);
    assert_eq!(
        runtime.fault_reason(),
        Some(AccountFault::PrivateEvidenceGap)
    );
    Ok(())
}

#[test]
fn multi_fact_private_evidence_is_atomic_and_cursor_waits_for_every_actor_ack()
-> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "two balance facts")?;
    let balance = AccountBalance {
        asset: "USDT".parse()?,
        wallet_balance: Decimal::ONE,
        available_balance: Decimal::ONE,
        initial_margin: Decimal::ZERO,
        maintenance_margin: Decimal::ZERO,
    };
    let first = private_fact_indexed(&persisted, DomainEvent::Balance(balance.clone()), 0, 2)?;
    let second = private_fact_indexed(&persisted, DomainEvent::Balance(balance), 1, 2)?;

    let first_report = route_persisted_private(&mut runtime, first)?;
    assert!(first_report.pending_batch);
    assert!(first_report.deliveries.is_empty());
    assert_eq!(runtime.health(), AccountHealth::Frozen);
    assert_eq!(
        runtime.fault_reason(),
        Some(AccountFault::PrivateEvidenceBatchIncomplete)
    );
    assert!(matches!(
        runtime.enqueue_execution(runtime_place_intent(
            &runtime,
            &grid,
            "pending_batch",
            AccountLanePriority::Normal,
        )?),
        Err(AccountRuntimeError::RiskFenced)
    ));

    let completed = route_persisted_private(&mut runtime, second)?;
    assert_eq!(completed.deliveries.len(), 2);
    assert_eq!(runtime.health(), AccountHealth::Ready);
    assert_eq!(runtime.applied_private_sequence(), 0);
    assert!(matches!(
        runtime.mark_account_ready(),
        Err(AccountRuntimeError::ReconnectWithUnappliedActorState)
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    assert_eq!(runtime.applied_private_sequence(), 0);
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    assert_eq!(runtime.applied_private_sequence(), 1);
    runtime.mark_account_ready()?;
    assert_eq!(runtime.connection_generation(), 2);
    Ok(())
}

#[test]
fn pending_private_delivery_revokes_old_turn_and_unwal_risk() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;

    let queued = runtime_place_intent(
        &runtime,
        &grid,
        "queued_before_private",
        AccountLanePriority::Normal,
    )?;
    let stale = runtime_place_intent(
        &runtime,
        &grid,
        "stale_after_private",
        AccountLanePriority::Normal,
    )?;
    runtime.enqueue_execution(queued)?;

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "durable actor inbox")?;
    route_persisted_private(
        &mut runtime,
        private_fact(
            &persisted,
            DomainEvent::Balance(AccountBalance {
                asset: "USDT".parse()?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            }),
        )?,
    )?;

    assert!(runtime.latest_applied_turn_receipt(&grid.key).is_none());
    assert!(runtime.next_execution_for_wal()?.is_none());
    assert!(matches!(
        runtime.enqueue_execution(stale),
        Err(AccountRuntimeError::StrategyTurnAuthority)
    ));
    assert!(matches!(
        pop_applied_strategy_input(&mut runtime, &grid.key)?,
        Some(StrategyInput::Private(_))
    ));
    assert!(runtime.latest_applied_turn_receipt(&grid.key).is_some());
    Ok(())
}

#[test]
fn private_batch_fence_preserves_preexisting_account_fault() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid)?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;
    runtime.freeze_account(AccountFault::WriterUnavailable);

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "faulted multi fact")?;
    let balance = AccountBalance {
        asset: "USDT".parse()?,
        wallet_balance: Decimal::ONE,
        available_balance: Decimal::ONE,
        initial_margin: Decimal::ZERO,
        maintenance_margin: Decimal::ZERO,
    };
    let first = private_fact_indexed(&persisted, DomainEvent::Balance(balance.clone()), 0, 2)?;
    let second = private_fact_indexed(&persisted, DomainEvent::Balance(balance), 1, 2)?;

    assert!(route_persisted_private(&mut runtime, first)?.pending_batch);
    assert_eq!(
        runtime.fault_reason(),
        Some(AccountFault::WriterUnavailable)
    );
    route_persisted_private(&mut runtime, second)?;
    assert_eq!(runtime.health(), AccountHealth::Frozen);
    assert_eq!(
        runtime.fault_reason(),
        Some(AccountFault::WriterUnavailable)
    );
    Ok(())
}

#[test]
fn private_route_plan_rejects_lifecycle_revision_change() -> Result<(), Box<dyn Error>> {
    let grid = binding(StrategyKind::HedgedGrid, "grid_sol", "SOL/USDT")?;
    let mut runtime = AccountRuntime::new(account()?);
    runtime.register_strategy(grid.clone())?;
    restore_empty_recovery(&mut runtime)?;
    runtime.mark_account_ready()?;
    establish_empty_signed_orders(&mut runtime, 1)?;

    let mut evidence = EvidenceFixture::new()?;
    let persisted = evidence.append(1, 100, "stale route plan")?;
    let plan = runtime.plan_private_route(private_fact(
        &persisted,
        DomainEvent::Balance(AccountBalance {
            asset: "USDT".parse()?,
            wallet_balance: Decimal::ONE,
            available_balance: Decimal::ONE,
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        }),
    )?)?;
    runtime.request_pause(&grid.key)?;
    assert!(matches!(
        runtime.commit_private_route(PersistedPrivateDispatchReceipt::persisted(plan)),
        Err(AccountRuntimeError::StalePrivateRoutePlan)
    ));
    Ok(())
}

#[test]
fn legacy_stage7_bridge_carries_identity_without_writer_authority() -> Result<(), Box<dyn Error>> {
    let legacy = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "futures".to_owned(),
        symbol: Symbol::new("SOL", "USDT")?,
        config_version: "stage7".to_owned(),
        owner_scope: "hedged_grid_sol_usdt_primary".to_owned(),
    };
    let bridged = legacy_stage7_strategy_binding(&legacy)?;
    assert_eq!(bridged.key.account.exchange, ExchangeId::Gate);
    assert_eq!(bridged.key.strategy_kind, StrategyKind::HedgedGrid);
    assert_eq!(bridged.key.symbol, legacy.symbol);
    Ok(())
}

mod runtime_safety_tests;
