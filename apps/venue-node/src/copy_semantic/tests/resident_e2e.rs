use std::sync::{Arc, Mutex};

use venue_control_protocol::{CopyRelationBinding, CopyRelationConfig, CopyRiskPolicy};
use venue_domain::domain::{FieldState, Fill, NativeOrderFamily};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountLimitNormalizationIntent, AccountRecoveryReport, AccountRecoveryRequest,
    AccountRiskEvidence, SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};

use super::*;
use crate::{NodeLaunch, ProductionResident};

#[derive(Default)]
struct State {
    dispatches: usize,
    generation: u64,
    command: Option<ExecutionCommand>,
    filled: Decimal,
    open: bool,
    unknown: bool,
    position: Option<Decimal>,
    fills: Vec<Fill>,
    filled_at_ms: Option<u64>,
    change_position_at_generation: Option<(u64, Decimal)>,
}

struct Gateway {
    binding: GatewayBinding,
    state: Arc<Mutex<State>>,
}

impl AccountPhysicalGateway for Gateway {
    type Error = std::io::Error;
    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }
    fn reconcile(
        &mut self,
        _: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        AccountRecoveryReport::new(
            self.binding.clone(),
            copy_clock().map_err(std::io::Error::other)?,
            Vec::new(),
        )
        .map_err(std::io::Error::other)
    }
    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        AccountRiskEvidence::complete(
            self.binding.clone(),
            copy_clock().map_err(|_| AccountHostValidationError::RiskEvidence)?,
            1,
            Vec::new(),
            Vec::new(),
        )
    }
    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: Default::default(),
            command_id: intent.command_id.clone(),
            client_order_id: intent.client_order_id.clone(),
            owner: intent.owner.clone(),
            side: intent.side,
            position_side: intent.position_side,
            quantity: intent.quote_delta,
            limit_price: Price::new(Decimal::ONE)
                .map_err(|_| AccountHostValidationError::Command)?,
            reduce_only: intent.reduce_only,
        }))
    }
    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        let now = copy_clock().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        state.generation += 1;
        if let Some((generation, position)) = state.change_position_at_generation
            && state.generation >= generation
        {
            state.position = Some(position);
        }
        let fill_time = *state.filled_at_ms.get_or_insert(now);
        let mut orders = Vec::new();
        let mut fills = state.fills.clone();
        if let Some(ExecutionCommand::PlaceLimit(command)) = &state.command {
            if state.open {
                orders.push(SignedAccountOrderFact {
                    time_in_force: Some(Default::default()),
                    client_order_id: command.client_order_id.as_str().to_owned(),
                    venue_order_id: Some("native-copy".to_owned()),
                    symbol: self.binding.symbol.clone(),
                    family: NativeOrderFamily::UmOrder,
                    side: command.side,
                    position_side: command.position_side,
                    quantity: command.quantity,
                    limit_price: Some(command.limit_price.value()),
                    reduce_only: command.reduce_only,
                    owner: Some(command.owner.clone()),
                    external: false,
                    state: None,
                    filled_quantity: Some(state.filled),
                });
            }
            if !state.filled.is_zero() {
                fills.push(Fill {
                    fill_id: format!("actual-fill-{}", state.filled),
                    execution_sequence: FieldState::Missing,
                    order_id: "native-copy".to_owned(),
                    symbol: self.binding.symbol.clone(),
                    side: command.side,
                    position_side: FieldState::Known(command.position_side),
                    quantity: state.filled,
                    price: command.limit_price,
                    fee: FieldState::Missing,
                    realized_pnl: FieldState::Missing,
                    maker: FieldState::Known(true),
                    exchange_time_ms: Some(fill_time),
                });
            }
        }
        SignedAccountSnapshot::complete_with_fills(
            self.binding.clone(),
            now,
            1,
            state.generation,
            1,
            SignedAccountPositionMode::Hedge,
            orders,
            vec![
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Long,
                    quantity: state.position.unwrap_or(state.filled).max(Decimal::ZERO),
                    entry_price: Some(Decimal::ONE),
                    mark_price: Some(Decimal::ONE),
                },
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Short,
                    quantity: (-state.position.unwrap_or(state.filled)).max(Decimal::ZERO),
                    entry_price: None,
                    mark_price: Some(Decimal::ONE),
                },
            ],
            fills,
            format!("cursor:{}", state.generation),
            request
                .unresolved()
                .iter()
                .map(|command| SignedUnknownFact {
                    command_id: command.command_id().clone(),
                    result: if state.unknown {
                        SignedUnknownResult::Unknown
                    } else {
                        SignedUnknownResult::Accepted {
                            venue_order_id: "native-copy".to_owned(),
                        }
                    },
                })
                .collect(),
        )?
        .with_balances(vec![SignedAccountBalance {
            asset: Asset::new("USDT").map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            equity: 100.into(),
            available_margin: Some(100.into()),
        }])
    }
    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        let Ok(mut state) = self.state.lock() else {
            return AccountGatewayResult::Unknown;
        };
        state.dispatches += 1;
        state.command = Some(permit.command().clone());
        state.open = true;
        if state.unknown {
            AccountGatewayResult::Unknown
        } else {
            AccountGatewayResult::Accepted {
                venue_order_id: "native-copy".to_owned(),
            }
        }
    }
}

#[allow(clippy::type_complexity)]
fn setup(
    directory: &std::path::Path,
    state: Arc<Mutex<State>>,
) -> Result<
    (
        ProductionResident<Gateway>,
        NodeLaunch,
        CopySemanticDelivery,
        CopyRelationRecord,
    ),
    Box<dyn std::error::Error>,
> {
    let (mut delivery, _) =
        delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 1)?;
    let now = copy_clock()?;
    delivery.manifest.issued_at_ms = now.saturating_sub(1);
    delivery.manifest.expires_at_ms = now + 60_000;
    delivery.target.exposure_ratio = Decimal::new(1, 1);
    let follower = CopyRelationBinding {
        venue: VenueId::Okx,
        mode: GatewayMode::Live,
        trading_account_id: delivery.owner.account.clone(),
        instance_id: delivery.actor.key.instance_id.clone(),
        symbol: delivery.owner.symbol.clone(),
    };
    let relation = CopyRelationRecord {
        revision: 1,
        relation: CopyRelationConfig {
            relation_id: delivery.manifest.binding.relation.relation_id.to_string(),
            leader: CopyRelationBinding {
                trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                instance_id: "leader".to_owned(),
                ..follower.clone()
            },
            follower,
            allocated_capital: 100.into(),
            multiplier: Decimal::ONE,
            safety_reserve_rate: Decimal::ZERO,
            risk: CopyRiskPolicy {
                max_total_notional: Decimal::TEN,
                max_order_notional: Decimal::TEN,
                max_leverage: Decimal::ONE,
            },
            lifecycle: CopyLifecyclePolicy::Active,
        },
    };
    delivery.manifest.binding.relation.policy_digest = relation.relation.policy_digest();
    delivery.delivery_digest = delivery.manifest.delivery_digest();
    let launch = NodeLaunch::try_parse_from(
        VenueId::Okx,
        [
            "venue-node-okx",
            "--mode",
            "LIVE",
            "--trading-account-id",
            &delivery.owner.account,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            directory.to_str().ok_or("path")?,
        ],
    )?;
    let gateway = Gateway {
        binding: launch.binding().clone(),
        state,
    };
    let mut resident = ProductionResident::open(&launch, gateway)?;
    resident.register_actor(delivery.actor.clone())?;
    Ok((resident, launch, delivery, relation))
}

#[test]
fn durable_request_failure_cannot_reach_actor_or_dispatch() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State::default()));
    let (mut resident, _, delivery, relation) = setup(directory.path(), state.clone())?;
    assert!(
        resident
            .apply_copy_delivery(delivery.clone(), delivery.actor(), &relation, |_| Err(
                CopySemanticError::RuntimeUnavailable
            ))
            .is_err()
    );
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 0);
    assert!(
        resident
            .recover_copy_actor_applied(delivery.clone(), delivery.actor())?
            .is_none()
    );
    Ok(())
}

#[test]
fn acknowledged_child_requires_signed_fills_and_can_recover_semantic_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State::default()));
    let (mut resident, launch, delivery, relation) = setup(directory.path(), state.clone())?;
    let mut persisted = None;
    let applied =
        resident.apply_copy_delivery(delivery.clone(), delivery.actor(), &relation, |request| {
            persisted = Some(request.clone());
            Ok(())
        })?;
    let request = persisted.ok_or("request not persisted")?;
    assert_eq!(applied.request, request);
    assert_eq!(
        applied.execution.ok_or("missing execution")?.state,
        CopyExecutionState::Accepted
    );
    let ack =
        resident.reconcile_copy_delivery(delivery.clone(), delivery.actor(), &request, &[])?;
    assert_eq!(ack.execution.state, CopyExecutionState::Accepted);
    assert!(ack.execution.reconciled_position.is_none());
    {
        let mut state = state.lock().map_err(|_| "lock")?;
        state.filled = Decimal::TEN;
        state.open = false;
    }
    let completed =
        resident.reconcile_copy_delivery(delivery.clone(), delivery.actor(), &request, &[])?;
    assert_eq!(completed.execution.state, CopyExecutionState::Reconciled);
    assert_eq!(
        completed.execution.reconciled_position.as_ref(),
        Some(&completed.position)
    );
    assert_ne!(
        completed.execution.fact_digest,
        completed.position.fact_digest
    );
    assert_eq!(completed.position.exposure.value, Decimal::TEN);
    let fill = completed.fills.first().ok_or("signed fill missing")?;
    assert!(
        resident
            .owner_for_signed_fill(fill)
            .is_some_and(|owner| delivery.actor().matches_owner(&owner))
    );
    let mut foreign = fill.clone();
    foreign.order_id = "manual-order".to_owned();
    assert!(resident.owner_for_signed_fill(&foreign).is_none());
    let receipt = resident
        .recover_copy_actor_applied(delivery.clone(), delivery.actor())?
        .ok_or("missing Applied")?;
    assert_eq!(
        receipt.account_fact_digest(),
        applied.applied.account_fact_digest()
    );
    drop(resident);
    let mut reopened = ProductionResident::open(
        &launch,
        Gateway {
            binding: launch.binding().clone(),
            state: state.clone(),
        },
    )?;
    reopened.register_actor(delivery.actor.clone())?;
    assert!(
        reopened
            .owner_for_signed_fill(fill)
            .is_some_and(|owner| delivery.actor().matches_owner(&owner))
    );
    let recovered = reopened
        .recover_copy_actor_applied(delivery.clone(), delivery.actor())?
        .ok_or("missing recovered Applied")?;
    assert_eq!(
        recovered.account_fact_digest(),
        receipt.account_fact_digest()
    );
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
    Ok(())
}

#[test]
fn unknown_reconciliation_and_recovery_only_delivery_never_resubmit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State {
        unknown: true,
        ..State::default()
    }));
    let (mut resident, _, mut delivery, relation) = setup(directory.path(), state.clone())?;
    let result =
        resident.apply_copy_delivery(delivery.clone(), delivery.actor(), &relation, |_| Ok(()))?;
    assert_eq!(
        result.execution.ok_or("missing execution")?.state,
        CopyExecutionState::Unknown
    );
    delivery.recovery_only = true;
    let reconciled = resident.reconcile_copy_delivery(
        delivery.clone(),
        delivery.actor(),
        &result.request,
        &[],
    )?;
    assert_eq!(reconciled.execution.state, CopyExecutionState::Unknown);
    assert!(
        resident
            .apply_copy_delivery(delivery.clone(), delivery.actor(), &relation, |_| Ok(()))
            .is_err()
    );
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
    Ok(())
}

fn cross_zero_reduce(
    resident: &mut ProductionResident<Gateway>,
    state: &Arc<Mutex<State>>,
    delivery: &mut CopySemanticDelivery,
    relation: &CopyRelationRecord,
) -> Result<crate::ResidentCopyReconciliation, Box<dyn std::error::Error>> {
    state.lock().map_err(|_| "lock")?.position = Some(Decimal::from(5));
    delivery.target.target_exposure.value = -Decimal::TEN;
    delivery.target.delta_exposure.value = Decimal::from(-15);
    delivery.target.exposure_ratio = Decimal::new(-1, 1);
    let first =
        resident.apply_copy_delivery(delivery.clone(), delivery.actor(), relation, |_| Ok(()))?;
    assert_eq!(first.request.phase, CopyExecutionPhase::ReduceToZero);
    assert_eq!(
        first.execution.ok_or("missing reduce")?.state,
        CopyExecutionState::Accepted
    );
    let ack = resident.reconcile_copy_delivery(
        delivery.clone(),
        delivery.actor(),
        &first.request,
        &[],
    )?;
    assert_eq!(ack.execution.state, CopyExecutionState::Accepted);
    {
        let mut state = state.lock().map_err(|_| "lock")?;
        let Some(ExecutionCommand::MarketReduce(command)) = state.command.clone() else {
            return Err("cross-zero did not use canonical reduce".into());
        };
        state.open = false;
        state.position = Some(Decimal::ZERO);
        state.fills.push(Fill {
            fill_id: "reduce-fill".to_owned(),
            execution_sequence: FieldState::Missing,
            order_id: "native-copy".to_owned(),
            symbol: command.owner.symbol,
            side: command.side,
            position_side: FieldState::Known(command.position_side),
            quantity: command.quantity,
            price: Price::new(Decimal::ONE)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(false),
            exchange_time_ms: Some(copy_clock()?),
        });
    }
    let completed = resident.reconcile_copy_delivery(
        delivery.clone(),
        delivery.actor(),
        &first.request,
        &[],
    )?;
    assert_eq!(completed.execution.state, CopyExecutionState::Reconciled);
    assert!(completed.position.exposure.value.is_zero());
    Ok(completed)
}

#[test]
fn cross_zero_only_starts_adjust_after_exact_reduce_fills_and_fresh_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State::default()));
    let (mut resident, _, mut delivery, relation) = setup(directory.path(), state.clone())?;
    let completed = cross_zero_reduce(&mut resident, &state, &mut delivery, &relation)?;
    let mut persisted = None;
    let second = resident.continue_cross_zero_copy_delivery(
        delivery.clone(),
        delivery.actor(),
        &relation,
        &completed.execution,
        &completed.fills,
        |request| {
            persisted = Some(request.clone());
            Ok(())
        },
    )?;
    assert_eq!(second.request.phase, CopyExecutionPhase::Adjust);
    assert_eq!(second.request.current_exposure.value, Decimal::ZERO);
    assert_eq!(second.request.requested_delta_exposure.value, -Decimal::TEN);
    assert_eq!(persisted.as_ref(), Some(&second.request));
    assert_ne!(
        CopyCommandIds::from_request(&completed.execution.request)?.command_id,
        CopyCommandIds::from_request(&second.request)?.command_id
    );
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 2);
    Ok(())
}

#[test]
fn cross_zero_changed_relation_or_expiry_never_reaches_next_request()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State::default()));
    let (mut resident, _, mut delivery, relation) = setup(directory.path(), state.clone())?;
    let completed = cross_zero_reduce(&mut resident, &state, &mut delivery, &relation)?;
    let mut paused = relation.clone();
    paused.revision += 1;
    paused.relation.lifecycle = CopyLifecyclePolicy::Paused;
    let mut next_request = false;
    assert!(
        resident
            .continue_cross_zero_copy_delivery(
                delivery.clone(),
                delivery.actor(),
                &paused,
                &completed.execution,
                &completed.fills,
                |_| {
                    next_request = true;
                    Ok(())
                },
            )
            .is_err()
    );
    delivery.manifest.expires_at_ms = copy_clock()?.saturating_sub(1);
    assert!(
        resident
            .continue_cross_zero_copy_delivery(
                delivery.clone(),
                delivery.actor(),
                &relation,
                &completed.execution,
                &completed.fills,
                |_| {
                    next_request = true;
                    Ok(())
                },
            )
            .is_err()
    );
    assert!(!next_request);
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
    Ok(())
}

#[test]
fn cross_zero_position_changing_between_two_reads_cannot_reinterpret_reduce_child()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let state = Arc::new(Mutex::new(State::default()));
    let (mut resident, _, mut delivery, relation) = setup(directory.path(), state.clone())?;
    let completed = cross_zero_reduce(&mut resident, &state, &mut delivery, &relation)?;
    {
        let mut state = state.lock().map_err(|_| "lock")?;
        state.change_position_at_generation = Some((state.generation + 2, Decimal::from(5)));
    }
    let mut next_request = false;
    assert!(
        resident
            .continue_cross_zero_copy_delivery(
                delivery.clone(),
                delivery.actor(),
                &relation,
                &completed.execution,
                &completed.fills,
                |_| {
                    next_request = true;
                    Ok(())
                },
            )
            .is_err()
    );
    assert!(!next_request);
    assert_eq!(state.lock().map_err(|_| "lock")?.dispatches, 1);
    Ok(())
}
