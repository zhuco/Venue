use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_control_protocol::{ControlCommandRequest, TradeIntent, TradingAction};
use venue_domain::{
    CancelCommand, CommandId, ExecutionCommand, LimitTimeInForce, OrderOwner, OrderPurpose,
    OrderSide, OrderState, PositionSide, Price,
};
use venue_runtime::{
    AccountLimitNormalizationIntent, AccountPhysicalGateway, AccountPricedLimitIntent,
    AppliedStrategyTurnReceipt, SignedAccountOrderFact, SignedAccountPositionMode,
    SignedAccountSnapshot, StrategyBinding, StrategyKind, account::AccountLanePriority,
};

use super::{NodeError, ProductionResident, persist_anchor};
use crate::{ActorDeliveryTurn, ReconciliationTurn};

const MANUAL_REPLAY_SCHEMA_VERSION: u16 = 1;
const MAX_MANUAL_PLANS: usize = 512;
const MAX_MANUAL_REPLAY_BYTES: usize = 1_048_576;

/// Node-owned semantic state inside the registered actor's existing replay envelope. The account
/// command WAL remains the source of physical status; this state only prevents a redelivery from
/// selecting a different cancellation target or creating a different client identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ManualActorState {
    #[serde(default = "manual_replay_schema")]
    schema_version: u16,
    #[serde(default)]
    plans: BTreeMap<String, ManualPlan>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ManualPlan {
    request_id: String,
    delivery_digest: [u8; 32],
    commands: Vec<ManualPlanCommand>,
}

impl ManualActorState {
    fn manual_client_order_ids(&self) -> BTreeSet<CommandId> {
        self.plans
            .values()
            .flat_map(|plan| plan.commands.iter())
            .filter_map(|command| match command {
                ManualPlanCommand::PlaceLimit {
                    client_order_id, ..
                } => Some(client_order_id.clone()),
                ManualPlanCommand::Cancel { .. } => None,
            })
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "payload")]
enum ManualPlanCommand {
    PlaceLimit {
        command_id: CommandId,
        client_order_id: CommandId,
        owner: OrderOwner,
        side: OrderSide,
        position_side: PositionSide,
        quote_delta: Decimal,
        limit_price: Price,
        time_in_force: LimitTimeInForce,
        maximum_quantity: Option<Decimal>,
        reduce_only: bool,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "rust_decimal::serde::str_option"
        )]
        position_before: Option<Decimal>,
    },
    Cancel {
        command: CancelCommand,
    },
}

/// Control receipts are emitted only after the command WAL and a fresh signed account read
/// agree on the plan. `Unknown` is terminal for this delivery but deliberately never retries a
/// physical mutation; recovery owns its later signed reconciliation.
pub(crate) enum ManualTradeOutcome {
    Applied(AppliedStrategyTurnReceipt),
    Rejected {
        applied: AppliedStrategyTurnReceipt,
        detail: String,
    },
    Unknown {
        applied: AppliedStrategyTurnReceipt,
        detail: String,
    },
}

pub(crate) enum ManualTradeReconciliation {
    Reconciled {
        account_fact_digest: [u8; 32],
        detail: String,
    },
    Pending,
}

impl ManualPlanCommand {
    fn command_id(&self) -> &CommandId {
        match self {
            Self::PlaceLimit { command_id, .. } => command_id,
            Self::Cancel { command } => &command.command_id,
        }
    }

    fn matches_existing(&self, command: &ExecutionCommand) -> bool {
        match (self, command) {
            (
                Self::PlaceLimit {
                    command_id,
                    client_order_id,
                    owner,
                    side,
                    position_side,
                    quote_delta,
                    limit_price,
                    time_in_force,
                    maximum_quantity,
                    reduce_only,
                    ..
                },
                ExecutionCommand::PlaceLimit(place),
            ) => {
                place.command_id == *command_id
                    && place.client_order_id == *client_order_id
                    && place.owner == *owner
                    && place.side == *side
                    && place.position_side == *position_side
                    && place.limit_price == *limit_price
                    && place.time_in_force == *time_in_force
                    && place.reduce_only == *reduce_only
                    && place.quantity > Decimal::ZERO
                    && place
                        .quantity
                        .checked_mul(place.limit_price.value())
                        .is_some_and(|notional| notional <= *quote_delta)
                    && maximum_quantity.is_none_or(|maximum| place.quantity <= maximum)
            }
            (Self::Cancel { command: expected }, ExecutionCommand::Cancel(actual)) => {
                actual == expected
            }
            _ => false,
        }
    }
}

impl<G: AccountPhysicalGateway> ProductionResident<G> {
    /// Applies a validated manual TradeIntent as a semantic control turn of the addressed actor.
    /// It deliberately reuses that actor's owner and durable receipt; there is no manual writer,
    /// journal or authority path.
    pub(crate) fn apply_manual_trade(
        &mut self,
        binding: &StrategyBinding,
        turn: &ActorDeliveryTurn,
    ) -> Result<ManualTradeOutcome, NodeError> {
        if binding.key.strategy_kind == StrategyKind::Copy {
            // Copy's latest actor receipt is its recovery commitment. A generic manual turn
            // would make that commitment ambiguous, so this is an explicit lifecycle boundary.
            return Err(NodeError::ResidentRuntime);
        }
        let trade = match turn.payload() {
            venue_control_protocol::AccountDeliveryPayload::ControlCommand(command)
                if command.action == venue_control_protocol::ControlAction::Trade =>
            {
                command.trade.as_ref().ok_or(NodeError::ResidentRuntime)?
            }
            _ => return Err(NodeError::ResidentRuntime),
        };
        trade.validate().map_err(|_| NodeError::ResidentRuntime)?;
        let (plan_key, request_id, delivery_digest) = manual_delivery_key(turn)?;
        let mut state = self.manual_actor_state(binding)?;
        if state
            .plans
            .values()
            .any(|plan| plan.request_id == request_id && plan.delivery_digest != delivery_digest)
        {
            // Control request ids are idempotency identities, never a namespace for a second
            // order with changed UI fields.
            return Err(NodeError::ResidentRuntime);
        }
        let plan = match state.plans.get(&plan_key) {
            Some(plan) if plan.delivery_digest == delivery_digest => plan.clone(),
            Some(_) => return Err(NodeError::ResidentRuntime),
            None => {
                if state.plans.len() >= MAX_MANUAL_PLANS {
                    return Err(NodeError::ResidentRuntime);
                }
                let snapshot = self.refresh_signed_snapshot()?;
                let manual_client_order_ids = state.manual_client_order_ids();
                let plan = manual_plan(
                    binding,
                    trade,
                    &snapshot.open_orders(),
                    &snapshot,
                    &plan_key,
                    &request_id,
                    delivery_digest,
                    &manual_client_order_ids,
                )?;
                state.plans.insert(plan_key.clone(), plan.clone());
                plan
            }
        };
        let bytes = encode_manual_state(&state)?;
        // A redelivery/restart may find its exact opening command already durable in the sole
        // WAL while the account is correctly paused by that open reservation. It needs a new
        // Actor receipt, not new risk authority or a second dispatch.
        let command_exists = plan
            .commands
            .iter()
            .map(|command| self.manual_command_exists(command))
            .collect::<Result<Vec<_>, _>>()?;
        let permits_risk_increase =
            plan.commands
                .iter()
                .zip(&command_exists)
                .any(|(command, exists)| {
                    !exists
                        && matches!(
                            command,
                            ManualPlanCommand::PlaceLimit {
                                reduce_only: false,
                                ..
                            }
                        )
                });
        let applied = self
            .runtime
            .persist_resident_manual_turn(binding, bytes, permits_risk_increase)
            .map_err(super::resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;

        let mut admitted = 0_usize;
        for (command, exists) in plan.commands.iter().zip(command_exists) {
            if exists {
                continue;
            }
            let admission = match command {
                ManualPlanCommand::PlaceLimit {
                    command_id,
                    client_order_id,
                    owner,
                    side,
                    position_side,
                    quote_delta,
                    limit_price,
                    time_in_force,
                    maximum_quantity,
                    reduce_only,
                    ..
                } => {
                    let intent = AccountPricedLimitIntent {
                        intent: AccountLimitNormalizationIntent {
                            command_id: command_id.clone(),
                            client_order_id: client_order_id.clone(),
                            owner: owner.clone(),
                            side: *side,
                            position_side: *position_side,
                            quote_delta: *quote_delta,
                            reduce_only: *reduce_only,
                        },
                        limit_price: *limit_price,
                        time_in_force: *time_in_force,
                        maximum_quantity: *maximum_quantity,
                    };
                    self.host
                        .normalize_and_prepare_priced_limit(
                            &mut self.runtime,
                            binding,
                            &applied,
                            AccountLanePriority::Normal,
                            &intent,
                        )
                        .map_err(|error| NodeError::LiveHost {
                            venue: self.host.binding().venue,
                            message: error.to_string(),
                        })
                }
                ManualPlanCommand::Cancel { command } => self
                    .host
                    .prepare_and_admit_operator(
                        &mut self.runtime,
                        binding,
                        &applied,
                        AccountLanePriority::Normal,
                        ExecutionCommand::Cancel(command.clone()),
                    )
                    .map_err(|error| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: error.to_string(),
                    }),
            };
            if let Err(error) = admission {
                self.host
                    .reject_prepared_batch(&mut self.runtime, "manual_trade_batch_rejected")
                    .map_err(|rejection| NodeError::LiveHost {
                        venue: self.host.binding().venue,
                        message: rejection.to_string(),
                    })?;
                return Ok(ManualTradeOutcome::Rejected {
                    applied,
                    detail: format!("manual trade preparation was rejected: {error}"),
                });
            }
            admitted = admitted.saturating_add(1);
        }
        for _ in 0..admitted {
            self.runtime
                .dispatch_next_with_host(&mut self.host)
                .map_err(|error| NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: error.to_string(),
                })?;
        }
        self.confirm_manual_plan(&plan, applied)
    }

    fn manual_actor_state(&self, binding: &StrategyBinding) -> Result<ManualActorState, NodeError> {
        let bytes = self
            .runtime
            .resident_manual_checkpoint(binding)
            .map_err(super::resident_error)?;
        let Some(bytes) = bytes else {
            return Ok(ManualActorState {
                schema_version: MANUAL_REPLAY_SCHEMA_VERSION,
                plans: BTreeMap::new(),
            });
        };
        let state = serde_json::from_slice::<ManualActorState>(&bytes)
            .map_err(|_| NodeError::ResidentRuntime)?;
        if state.schema_version != MANUAL_REPLAY_SCHEMA_VERSION {
            return Err(NodeError::ResidentRuntime);
        }
        Ok(state)
    }

    fn manual_command_exists(&self, plan: &ManualPlanCommand) -> Result<bool, NodeError> {
        let Some(existing) = self.host.command_snapshot(plan.command_id()) else {
            return Ok(false);
        };
        plan.matches_existing(&existing)
            .then_some(true)
            .ok_or(NodeError::ResidentRuntime)
    }

    fn confirm_manual_plan(
        &mut self,
        plan: &ManualPlan,
        applied: AppliedStrategyTurnReceipt,
    ) -> Result<ManualTradeOutcome, NodeError> {
        let mut accepted = BTreeMap::new();
        let mut rejected = Vec::new();
        let mut pending = false;
        for command in &plan.commands {
            let status = self
                .host
                .command_status(command.command_id())
                .map_err(|error| NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: error.to_string(),
                })?
                .ok_or(NodeError::ResidentRuntime)?;
            match status.state() {
                venue_runtime::CommandState::Accepted { venue_order_id } => {
                    accepted.insert(command.command_id().clone(), venue_order_id.clone());
                }
                venue_runtime::CommandState::Rejected { reason } => {
                    rejected.push(reason.clone());
                }
                venue_runtime::CommandState::Unknown { .. } => {
                    pending = true;
                }
                venue_runtime::CommandState::Prepared | venue_runtime::CommandState::Submitted => {
                    pending = true;
                }
            }
        }
        if pending {
            return Ok(ManualTradeOutcome::Unknown {
                applied,
                detail: "manual trade batch is durably pending signed reconciliation".to_owned(),
            });
        }
        if !rejected.is_empty() {
            return Ok(ManualTradeOutcome::Rejected {
                applied,
                detail: format!("manual trade command was rejected: {}", rejected.join("; ")),
            });
        }
        let commands = plan
            .commands
            .iter()
            .map(|planned| {
                self.host
                    .command_snapshot(planned.command_id())
                    .map(|command| (planned.command_id().clone(), command))
                    .ok_or(NodeError::ResidentRuntime)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let snapshot = self.refresh_signed_snapshot()?;
        if !signed_plan_matches(&plan.commands, &commands, &accepted, &snapshot) {
            return Ok(ManualTradeOutcome::Unknown {
                applied,
                detail: "manual trade is Accepted in the WAL but its fresh signed readback does not yet match"
                    .to_owned(),
            });
        }
        Ok(ManualTradeOutcome::Applied(applied))
    }

    /// Reconcile-only work never prepares or dispatches. It can only re-read durable command
    /// identities and newly signed account facts for an already persisted manual plan.
    pub(crate) fn reconcile_manual_trade(
        &mut self,
        binding: &StrategyBinding,
        turn: &ReconciliationTurn,
    ) -> Result<ManualTradeReconciliation, NodeError> {
        let command = manual_trade_command(turn.payload())?;
        let (plan_key, request_id, delivery_digest) = manual_command_key(command)?;
        let state = self.manual_actor_state(binding)?;
        let plan = state
            .plans
            .get(&plan_key)
            .ok_or(NodeError::ResidentRuntime)?;
        if plan.request_id != request_id || plan.delivery_digest != delivery_digest {
            return Err(NodeError::ResidentRuntime);
        }
        let mut accepted = BTreeMap::new();
        let mut rejected = Vec::new();
        let mut pending = false;
        for planned in &plan.commands {
            let status = self
                .host
                .reconcile_command_status(planned.command_id())
                .map_err(|error| NodeError::LiveHost {
                    venue: self.host.binding().venue,
                    message: error.to_string(),
                })?
                .ok_or(NodeError::ResidentRuntime)?;
            match status.state() {
                venue_runtime::CommandState::Accepted { venue_order_id } => {
                    accepted.insert(planned.command_id().clone(), venue_order_id.clone());
                }
                venue_runtime::CommandState::Rejected { reason } => {
                    rejected.push(reason.clone());
                }
                venue_runtime::CommandState::Prepared
                | venue_runtime::CommandState::Submitted
                | venue_runtime::CommandState::Unknown { .. } => {
                    pending = true;
                }
            }
        }
        if pending {
            return Ok(ManualTradeReconciliation::Pending);
        }
        if !rejected.is_empty() {
            let snapshot = self.refresh_signed_snapshot()?;
            return Ok(ManualTradeReconciliation::Reconciled {
                account_fact_digest: signed_snapshot_digest(&snapshot)?,
                detail: format!("manual trade reconciled Rejected: {}", rejected.join("; ")),
            });
        }
        let commands = plan
            .commands
            .iter()
            .map(|planned| {
                self.host
                    .command_snapshot(planned.command_id())
                    .map(|command| (planned.command_id().clone(), command))
                    .ok_or(NodeError::ResidentRuntime)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let snapshot = self.refresh_signed_snapshot()?;
        if signed_plan_matches(&plan.commands, &commands, &accepted, &snapshot) {
            Ok(ManualTradeReconciliation::Reconciled {
                account_fact_digest: signed_snapshot_digest(&snapshot)?,
                detail: "manual trade reconciled Accepted with a fresh signed account readback"
                    .to_owned(),
            })
        } else {
            Ok(ManualTradeReconciliation::Pending)
        }
    }

    /// A manual fill is consumed before the Grid reducer only when its WAL client identity is
    /// present in the recovered manual checkpoint. Prefixes and native string shapes are not
    /// ownership evidence.
    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    pub(crate) fn manual_owns_fill(
        &self,
        binding: &StrategyBinding,
        fill: &venue_domain::Fill,
    ) -> Result<bool, NodeError> {
        let command = self.host.command_snapshot_by_venue_order_id(
            venue_domain::NativeOrderFamily::UmOrder,
            &fill.order_id,
        );
        let Some(ExecutionCommand::PlaceLimit(order)) = command else {
            return Ok(false);
        };
        let state = self.manual_actor_state(binding)?;
        Ok(manual_fill_matches_command(
            binding,
            &state.manual_client_order_ids(),
            fill,
            &order,
        ))
    }

    #[cfg_attr(not(feature = "binance"), allow(dead_code))]
    pub(crate) fn manual_checkpoint_bytes(
        &self,
        binding: &StrategyBinding,
    ) -> Result<Vec<u8>, NodeError> {
        encode_manual_state(&self.manual_actor_state(binding)?)
    }
}

fn manual_plan(
    binding: &StrategyBinding,
    trade: &TradeIntent,
    open_orders: &[SignedAccountOrderFact],
    snapshot: &venue_runtime::SignedAccountSnapshot,
    plan_key: &str,
    request_id: &str,
    delivery_digest: [u8; 32],
    manual_client_order_ids: &BTreeSet<CommandId>,
) -> Result<ManualPlan, NodeError> {
    let commands = if trade.action.is_order_action() {
        let owner = owner(binding, trade.action)?;
        vec![place_plan(binding, trade, snapshot, plan_key, owner)?]
    } else {
        cancel_plan(
            binding,
            trade,
            open_orders,
            plan_key,
            manual_client_order_ids,
        )?
    };
    Ok(ManualPlan {
        request_id: request_id.to_owned(),
        delivery_digest,
        commands,
    })
}

fn place_plan(
    binding: &StrategyBinding,
    trade: &TradeIntent,
    snapshot: &venue_runtime::SignedAccountSnapshot,
    plan_key: &str,
    owner: OrderOwner,
) -> Result<ManualPlanCommand, NodeError> {
    let limit_price = Price::new(trade.selected_price.ok_or(NodeError::ResidentRuntime)?)
        .map_err(|_| NodeError::ResidentRuntime)?;
    let quote_delta = trade.quote_notional.ok_or(NodeError::ResidentRuntime)?;
    let (side, position_side, maximum_quantity) = match trade.action {
        TradingAction::OpenLong => (
            OrderSide::Buy,
            position_side(snapshot, PositionSide::Long)?,
            None,
        ),
        TradingAction::OpenShort => (
            OrderSide::Sell,
            position_side(snapshot, PositionSide::Short)?,
            None,
        ),
        TradingAction::CloseLong => close_leg(
            snapshot,
            &binding.key.symbol,
            PositionSide::Long,
            trade.close_quantity_cap,
        )?,
        TradingAction::CloseShort => close_leg(
            snapshot,
            &binding.key.symbol,
            PositionSide::Short,
            trade.close_quantity_cap,
        )?,
        _ => return Err(NodeError::ResidentRuntime),
    };
    Ok(ManualPlanCommand::PlaceLimit {
        command_id: manual_command_id("place", binding, plan_key, 0)?,
        client_order_id: manual_command_id("client", binding, plan_key, 0)?,
        owner,
        side,
        position_side,
        quote_delta,
        limit_price,
        time_in_force: if trade.post_only {
            LimitTimeInForce::PostOnly
        } else {
            LimitTimeInForce::Gtc
        },
        maximum_quantity,
        reduce_only: trade.reduce_only(),
        position_before: Some(signed_position_quantity(
            snapshot,
            &binding.key.symbol,
            position_side,
        )),
    })
}

fn cancel_plan(
    binding: &StrategyBinding,
    trade: &TradeIntent,
    open_orders: &[SignedAccountOrderFact],
    plan_key: &str,
    manual_client_order_ids: &BTreeSet<CommandId>,
) -> Result<Vec<ManualPlanCommand>, NodeError> {
    let is_working_for_symbol = |order: &SignedAccountOrderFact| {
        order.symbol == binding.key.symbol
            && matches!(
                order.state,
                Some(OrderState::New | OrderState::PartiallyFilled)
            )
    };
    let has_nonmanual_owned_working = open_orders.iter().any(|order| {
        !order.external
            && is_working_for_symbol(order)
            && order
                .owner
                .as_ref()
                .is_some_and(|owner| binding.matches_owner(owner))
            && matches!(
                order.state,
                Some(OrderState::New | OrderState::PartiallyFilled)
            )
            && CommandId::new(order.client_order_id.clone())
                .ok()
                .is_none_or(|id| !manual_client_order_ids.contains(&id))
    });
    // Grid desired state is not mutated by this manual path. Cancelling one of its orders then
    // returning Applied would make the next Grid turn recreate it, so all non-manual owned
    // Working orders are an explicit unsupported boundary until the Grid state transition is
    // made atomic with this plan.
    if has_nonmanual_owned_working {
        return Err(NodeError::ResidentRuntime);
    }
    if trade.action == TradingAction::CancelAllOrders
        && open_orders.iter().any(|order| {
            order.symbol == binding.key.symbol
                && (matches!(order.state, None | Some(OrderState::Unknown))
                    || (is_working_for_symbol(order)
                        && (order.external
                            || !order
                                .owner
                                .as_ref()
                                .is_some_and(|owner| binding.matches_owner(owner)))))
        })
    {
        // CancelAll has an account+symbol UI meaning. It cannot be reported as success while a
        // foreign, unowned, or not-provably-terminal order remains outside this actor's proof.
        return Err(NodeError::ResidentRuntime);
    }
    let mut candidates = open_orders
        .iter()
        .filter(|order| {
            !order.external
                && is_working_for_symbol(order)
                && order
                    .owner
                    .as_ref()
                    .is_some_and(|owner| binding.matches_owner(owner))
                && matches!(
                    order.state,
                    Some(OrderState::New | OrderState::PartiallyFilled)
                )
                && CommandId::new(order.client_order_id.clone())
                    .ok()
                    .is_some_and(|id| manual_client_order_ids.contains(&id))
        })
        .collect::<Vec<_>>();
    match trade.action {
        TradingAction::CancelSelectedOrder => {
            let selected = match trade.selected_order_id.as_deref() {
                Some(id) => {
                    candidates.retain(|order| {
                        order.client_order_id == id || order.venue_order_id.as_deref() == Some(id)
                    });
                    (candidates.len() == 1)
                        .then(|| candidates.remove(0))
                        .ok_or(NodeError::ResidentRuntime)?
                }
                None => select_recent_working(&mut candidates)?,
            };
            Ok(vec![cancel_command(binding, selected, plan_key, 0)?])
        }
        TradingAction::CancelAllOrders => {
            candidates.sort_by_key(|order| {
                (
                    order.venue_order_id.clone().unwrap_or_default(),
                    order.client_order_id.clone(),
                )
            });
            if candidates.is_empty() {
                return Err(NodeError::ResidentRuntime);
            }
            candidates
                .into_iter()
                .enumerate()
                .map(|(index, order)| cancel_command(binding, order, plan_key, index))
                .collect()
        }
        _ => Err(NodeError::ResidentRuntime),
    }
}

fn select_recent_working<'a>(
    candidates: &mut Vec<&'a SignedAccountOrderFact>,
) -> Result<&'a SignedAccountOrderFact, NodeError> {
    if candidates.iter().any(|order| order.created_at_ms.is_none()) {
        return Err(NodeError::ResidentRuntime);
    }
    let newest = candidates
        .iter()
        .filter_map(|order| order.created_at_ms)
        .max()
        .ok_or(NodeError::ResidentRuntime)?;
    let mut newest = candidates
        .iter()
        .copied()
        .filter(|order| order.created_at_ms == Some(newest))
        .collect::<Vec<_>>();
    if newest.len() == 1 {
        return Ok(newest.remove(0));
    }
    if newest.iter().any(|order| order.venue_order_id.is_none()) {
        return Err(NodeError::ResidentRuntime);
    }
    newest.sort_by_key(|order| {
        (
            order.venue_order_id.clone().unwrap_or_default(),
            order.client_order_id.clone(),
        )
    });
    newest.pop().ok_or(NodeError::ResidentRuntime)
}

fn cancel_command(
    binding: &StrategyBinding,
    order: &SignedAccountOrderFact,
    plan_key: &str,
    index: usize,
) -> Result<ManualPlanCommand, NodeError> {
    let owner = order.owner.clone().ok_or(NodeError::ResidentRuntime)?;
    if !binding.matches_owner(&owner) {
        return Err(NodeError::ResidentRuntime);
    }
    Ok(ManualPlanCommand::Cancel {
        command: CancelCommand {
            command_id: manual_command_id("cancel", binding, plan_key, index)?,
            owner,
            target_client_order_id: CommandId::new(order.client_order_id.clone())
                .map_err(|_| NodeError::ResidentRuntime)?,
        },
    })
}

fn signed_plan_matches(
    plan_commands: &[ManualPlanCommand],
    commands: &BTreeMap<CommandId, ExecutionCommand>,
    accepted: &BTreeMap<CommandId, String>,
    snapshot: &venue_runtime::SignedAccountSnapshot,
) -> bool {
    commands.iter().all(|(command_id, planned)| match planned {
        ExecutionCommand::PlaceLimit(_) => {
            if !accepted.contains_key(command_id) {
                return false;
            }
            let position_before = plan_commands
                .iter()
                .find(|plan| plan.command_id() == command_id)
                .and_then(|plan| match plan {
                    ManualPlanCommand::PlaceLimit {
                        position_before, ..
                    } => *position_before,
                    ManualPlanCommand::Cancel { .. } => None,
                });
            signed_operator_canary_matches(
                planned,
                accepted
                    .get(command_id)
                    .map(String::as_str)
                    .unwrap_or_default(),
                position_before,
                snapshot,
            )
        }
        ExecutionCommand::Cancel(_) => accepted.get(command_id).is_some_and(|venue_order_id| {
            signed_operator_canary_matches(planned, venue_order_id, None, snapshot)
        }),
        _ => false,
    })
}

/// Canary success requires the account-WAL acceptance and a newer complete signed account fact.
/// A full fill is accepted only when exact fills and the addressed position delta cover it.
pub(crate) fn signed_operator_canary_matches(
    command: &ExecutionCommand,
    venue_order_id: &str,
    position_before: Option<Decimal>,
    snapshot: &SignedAccountSnapshot,
) -> bool {
    if venue_order_id.trim().is_empty() {
        return false;
    }
    match command {
        ExecutionCommand::PlaceLimit(expected) => {
            snapshot.open_orders().iter().any(|actual| {
                actual.client_order_id == expected.client_order_id.as_str()
                    && actual.venue_order_id.as_deref() == Some(venue_order_id)
                    && command
                        .native_order_family()
                        .is_some_and(|family| actual.family == family)
                    && actual.symbol == expected.owner.symbol
                    && actual.side == expected.side
                    && actual.position_side == expected.position_side
                    && actual.quantity == expected.quantity
                    && actual.limit_price == Some(expected.limit_price.value())
                    && actual.time_in_force == Some(expected.time_in_force)
                    && actual.reduce_only == expected.reduce_only
                    && actual.owner.as_ref() == Some(&expected.owner)
                    && !actual.external
                    && matches!(
                        actual.state,
                        Some(OrderState::New | OrderState::PartiallyFilled)
                    )
            }) || signed_complete_limit(expected, venue_order_id, position_before, snapshot)
        }
        ExecutionCommand::Cancel(command) => !snapshot.open_orders().iter().any(|actual| {
            actual.client_order_id == command.target_client_order_id.as_str()
                && matches!(
                    actual.state,
                    Some(OrderState::New | OrderState::PartiallyFilled)
                )
        }),
        ExecutionCommand::PlaceMarket(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketFullPosition(_)
        | ExecutionCommand::StopMarketCloseAll(_) => false,
    }
}

fn manual_fill_matches_command(
    binding: &StrategyBinding,
    manual_client_order_ids: &BTreeSet<CommandId>,
    fill: &venue_domain::Fill,
    order: &venue_domain::OrderCommand,
) -> bool {
    binding.matches_owner(&order.owner)
        && fill.symbol == order.owner.symbol
        && fill.side == order.side
        && match fill.position_side {
            venue_domain::FieldState::Known(side) => {
                side == order.position_side || side == PositionSide::Net
            }
            venue_domain::FieldState::Missing => true,
            venue_domain::FieldState::Null
            | venue_domain::FieldState::Unavailable { .. }
            | venue_domain::FieldState::NotApplicable => false,
        }
        && manual_client_order_ids.contains(&order.client_order_id)
}

fn signed_complete_limit(
    expected: &venue_domain::OrderCommand,
    venue_order_id: &str,
    position_before: Option<Decimal>,
    snapshot: &venue_runtime::SignedAccountSnapshot,
) -> bool {
    let filled = snapshot
        .fills()
        .iter()
        .filter(|fill| {
            fill.order_id == venue_order_id
                && fill.symbol == expected.owner.symbol
                && fill.side == expected.side
                && match fill.position_side {
                    venue_domain::FieldState::Known(side) => {
                        side == expected.position_side || side == PositionSide::Net
                    }
                    venue_domain::FieldState::Missing => true,
                    venue_domain::FieldState::Null
                    | venue_domain::FieldState::Unavailable { .. }
                    | venue_domain::FieldState::NotApplicable => false,
                }
                && match expected.side {
                    OrderSide::Buy => fill.price.value() <= expected.limit_price.value(),
                    OrderSide::Sell => fill.price.value() >= expected.limit_price.value(),
                }
        })
        .try_fold(Decimal::ZERO, |total, fill| {
            total.checked_add(fill.quantity)
        });
    let Some(filled) = filled else {
        return false;
    };
    let Some(position_before) = position_before else {
        return false;
    };
    filled == expected.quantity
        && signed_position_delta_proves_fill(expected, position_before, snapshot, filled)
}

pub(crate) fn signed_position_quantity(
    snapshot: &venue_runtime::SignedAccountSnapshot,
    symbol: &venue_domain::Symbol,
    position_side: PositionSide,
) -> Decimal {
    snapshot
        .positions()
        .iter()
        .find(|position| position.symbol == *symbol && position.position_side == position_side)
        .map_or(Decimal::ZERO, |position| position.quantity)
}

fn signed_position_delta_proves_fill(
    expected: &venue_domain::OrderCommand,
    position_before: Decimal,
    snapshot: &venue_runtime::SignedAccountSnapshot,
    filled: Decimal,
) -> bool {
    let position_after =
        signed_position_quantity(snapshot, &expected.owner.symbol, expected.position_side);
    let Some(delta) = position_after.checked_sub(position_before) else {
        return false;
    };
    match expected.position_side {
        PositionSide::Net => match expected.side {
            OrderSide::Buy => delta >= filled,
            OrderSide::Sell => delta <= -filled,
        },
        PositionSide::Long | PositionSide::Short => {
            if expected.reduce_only {
                delta <= -filled
            } else {
                delta >= filled
            }
        }
    }
}

fn owner(binding: &StrategyBinding, action: TradingAction) -> Result<OrderOwner, NodeError> {
    let purpose = if action.is_close_action() {
        OrderPurpose::Reduce
    } else if action.is_order_action() {
        OrderPurpose::Entry
    } else {
        return Err(NodeError::ResidentRuntime);
    };
    Ok(OrderOwner {
        strategy_instance_id: binding.key.instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.key.account.exchange.as_str().to_owned(),
        account: binding.key.account.account.clone(),
        symbol: binding.key.symbol.clone(),
        purpose,
    })
}

fn position_side(
    snapshot: &venue_runtime::SignedAccountSnapshot,
    hedge_side: PositionSide,
) -> Result<PositionSide, NodeError> {
    match snapshot.position_mode() {
        SignedAccountPositionMode::Hedge => Ok(hedge_side),
        SignedAccountPositionMode::Net => Ok(PositionSide::Net),
    }
}

fn close_leg(
    snapshot: &venue_runtime::SignedAccountSnapshot,
    symbol: &venue_domain::Symbol,
    requested: PositionSide,
    ui_cap: Option<Decimal>,
) -> Result<(OrderSide, PositionSide, Option<Decimal>), NodeError> {
    let ui_cap = ui_cap.ok_or(NodeError::ResidentRuntime)?;
    let position = match snapshot.position_mode() {
        SignedAccountPositionMode::Hedge => snapshot
            .positions()
            .iter()
            .find(|position| position.symbol == *symbol && position.position_side == requested),
        SignedAccountPositionMode::Net => snapshot.positions().iter().find(|position| {
            position.symbol == *symbol
                && position.position_side == PositionSide::Net
                && match requested {
                    PositionSide::Long => position.quantity.is_sign_positive(),
                    PositionSide::Short => position.quantity.is_sign_negative(),
                    PositionSide::Net => false,
                }
        }),
    }
    .ok_or(NodeError::ResidentRuntime)?;
    let quantity = position.quantity.abs();
    if quantity <= Decimal::ZERO {
        return Err(NodeError::ResidentRuntime);
    }
    let side = match requested {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err(NodeError::ResidentRuntime),
    };
    Ok((side, position.position_side, Some(quantity.min(ui_cap))))
}

fn manual_delivery_key(turn: &ActorDeliveryTurn) -> Result<(String, String, [u8; 32]), NodeError> {
    manual_command_key(manual_trade_command(turn.payload())?)
}

fn manual_trade_command(
    payload: &venue_control_protocol::AccountDeliveryPayload,
) -> Result<&ControlCommandRequest, NodeError> {
    match payload {
        venue_control_protocol::AccountDeliveryPayload::ControlCommand(command)
            if command.action == venue_control_protocol::ControlAction::Trade =>
        {
            Ok(command)
        }
        _ => Err(NodeError::ResidentRuntime),
    }
}

fn manual_command_key(
    command: &ControlCommandRequest,
) -> Result<(String, String, [u8; 32]), NodeError> {
    let payload = serde_json::to_vec(command).map_err(|_| NodeError::ResidentRuntime)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.manual-plan.v1");
    digest.update((command.request_id.len() as u64).to_be_bytes());
    digest.update(command.request_id.as_bytes());
    digest.update((payload.len() as u64).to_be_bytes());
    digest.update(payload);
    let digest: [u8; 32] = digest.finalize().into();
    let mut key = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        write!(&mut key, "{byte:02x}").map_err(|_| NodeError::ResidentRuntime)?;
    }
    Ok((key, command.request_id.clone(), digest))
}

fn signed_snapshot_digest(
    snapshot: &venue_runtime::SignedAccountSnapshot,
) -> Result<[u8; 32], NodeError> {
    let encoded = serde_json::to_vec(snapshot).map_err(|_| NodeError::ResidentRuntime)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.manual-signed-account.v1");
    digest.update(encoded);
    Ok(digest.finalize().into())
}

fn manual_command_id(
    kind: &str,
    binding: &StrategyBinding,
    plan_key: &str,
    index: usize,
) -> Result<CommandId, NodeError> {
    let index = u64::try_from(index).map_err(|_| NodeError::ResidentRuntime)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.manual-command.v1");
    for field in [
        kind.as_bytes(),
        binding.key.account.exchange.as_str().as_bytes(),
        binding.key.account.account.as_bytes(),
        binding.key.instance_id.as_bytes(),
        binding.run_id.as_bytes(),
        binding.key.symbol.to_string().as_bytes(),
        plan_key.as_bytes(),
        &index.to_be_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    let mut value = String::from("m");
    for byte in digest.finalize().iter().take(12) {
        use std::fmt::Write;
        write!(&mut value, "{byte:02x}").map_err(|_| NodeError::ResidentRuntime)?;
    }
    CommandId::new(value).map_err(|_| NodeError::ResidentRuntime)
}

fn encode_manual_state(state: &ManualActorState) -> Result<Vec<u8>, NodeError> {
    let bytes = serde_json::to_vec(state).map_err(|_| NodeError::ResidentRuntime)?;
    (bytes.len() <= MAX_MANUAL_REPLAY_BYTES)
        .then_some(bytes)
        .ok_or(NodeError::ResidentRuntime)
}

const fn manual_replay_schema() -> u16 {
    MANUAL_REPLAY_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use venue_control_protocol::{
        CONTROL_SCHEMA_VERSION, ControlAction, ControlCommandRequest, TradingOrderType,
        TradingTimeInForce,
    };
    use venue_domain::{FieldState, Fill, NativeOrderFamily};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
    use venue_runtime::{SignedAccountOrderFact, SignedAccountPositionFact, SignedAccountSnapshot};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)
    }

    fn owner() -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid-a".to_owned(),
            run_id: "run-a".to_owned(),
            exchange: "binance".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        })
    }

    fn strategy_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding::new(
            venue_runtime::StrategyInstanceKey::new(
                venue_runtime::AccountKey::new(venue_runtime::ExchangeId::Binance, ACCOUNT)?,
                StrategyKind::HedgedGrid,
                "grid-a",
                "DOGE/USDT".parse()?,
            )?,
            "run-a",
            "config-a",
        )?)
    }

    fn trade_command(price: Decimal) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
        Ok(ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "trade-request-a".to_owned(),
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: ACCOUNT.to_owned(),
            instance_id: "grid-a".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            action: ControlAction::Trade,
            trade: Some(TradeIntent {
                action: TradingAction::OpenLong,
                quote_asset: "USDT".to_owned(),
                order_type: TradingOrderType::Limit,
                time_in_force: TradingTimeInForce::Gtc,
                post_only: false,
                reduce_only: false,
                selected_price: Some(price),
                quote_notional: Some(Decimal::new(20, 0)),
                close_quantity_cap: None,
                selected_order_id: None,
            }),
            expected_config_epoch: 1,
            confirmation: None,
        })
    }

    fn snapshot(
        positions: Vec<SignedAccountPositionFact>,
        fills: Vec<Fill>,
    ) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
        snapshot_with_orders(Vec::new(), positions, fills)
    }

    fn snapshot_with_orders(
        orders: Vec<SignedAccountOrderFact>,
        positions: Vec<SignedAccountPositionFact>,
        fills: Vec<Fill>,
    ) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
        Ok(SignedAccountSnapshot::complete_with_fills(
            binding()?,
            1,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            orders,
            positions,
            fills,
            "manual-fixture-fills".to_owned(),
            Vec::new(),
        )?)
    }

    #[test]
    fn canary_acceptance_requires_an_exact_signed_order_or_absent_cancel_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = owner()?;
        let order = venue_domain::OrderCommand {
            time_in_force: LimitTimeInForce::PostOnly,
            command_id: CommandId::new("canary-place-a")?,
            client_order_id: CommandId::new("canary-client-a")?,
            owner: owner.clone(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let command = ExecutionCommand::PlaceLimit(order.clone());
        let family = command
            .native_order_family()
            .ok_or("place command has no order family")?;
        let signed = SignedAccountOrderFact {
            client_order_id: order.client_order_id.as_str().to_owned(),
            venue_order_id: Some("canary-venue-a".to_owned()),
            symbol: owner.symbol.clone(),
            family,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            limit_price: Some(order.limit_price.value()),
            time_in_force: Some(order.time_in_force),
            created_at_ms: Some(1),
            reduce_only: order.reduce_only,
            owner: Some(owner.clone()),
            external: false,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        };
        let snapshot = snapshot_with_orders(vec![signed.clone()], Vec::new(), Vec::new())?;
        assert!(signed_operator_canary_matches(
            &command,
            "canary-venue-a",
            Some(Decimal::ZERO),
            &snapshot,
        ));

        let mut wrong = signed;
        wrong.time_in_force = Some(LimitTimeInForce::Gtc);
        let mismatched = snapshot_with_orders(vec![wrong], Vec::new(), Vec::new())?;
        assert!(!signed_operator_canary_matches(
            &command,
            "canary-venue-a",
            Some(Decimal::ZERO),
            &mismatched,
        ));

        let cancel = ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("canary-cancel-a")?,
            owner,
            target_client_order_id: order.client_order_id,
        });
        assert!(!signed_operator_canary_matches(
            &cancel,
            "canary-venue-a",
            None,
            &snapshot,
        ));
        let absent = snapshot_with_orders(Vec::new(), Vec::new(), Vec::new())?;
        assert!(signed_operator_canary_matches(
            &cancel,
            "canary-venue-a",
            None,
            &absent,
        ));
        Ok(())
    }

    #[test]
    fn same_request_id_with_changed_payload_has_a_distinct_digest()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = trade_command(Decimal::new(10, 0))?;
        let second = trade_command(Decimal::new(11, 0))?;
        let (_, first_request, first_digest) = manual_command_key(&first)?;
        let (_, second_request, second_digest) = manual_command_key(&second)?;
        assert_eq!(first_request, second_request);
        assert_ne!(first_digest, second_digest);
        Ok(())
    }

    #[test]
    fn manual_native_ids_are_stable_short_and_alphanumeric()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = strategy_binding()?;
        let first = manual_command_id("place", &binding, "stable-plan", 0)?;
        let repeated = manual_command_id("place", &binding, "stable-plan", 0)?;
        let other_kind = manual_command_id("cancel", &binding, "stable-plan", 0)?;
        assert_eq!(first, repeated);
        assert_ne!(first, other_kind);
        assert_eq!(first.as_str().len(), 25);
        assert!(
            first
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
        );
        Ok(())
    }

    #[test]
    fn close_leg_uses_the_addressed_symbol_not_another_account_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let doge: venue_domain::Symbol = "DOGE/USDT".parse()?;
        let btc: venue_domain::Symbol = "BTC/USDT".parse()?;
        let signed = snapshot(
            vec![
                SignedAccountPositionFact {
                    symbol: doge.clone(),
                    position_side: PositionSide::Long,
                    quantity: Decimal::new(2, 0),
                    entry_price: None,
                    mark_price: None,
                },
                SignedAccountPositionFact {
                    symbol: btc,
                    position_side: PositionSide::Long,
                    quantity: Decimal::new(99, 0),
                    entry_price: None,
                    mark_price: None,
                },
            ],
            Vec::new(),
        )?;
        let (side, position_side, cap) =
            close_leg(&signed, &doge, PositionSide::Long, Some(Decimal::new(3, 0)))?;
        assert_eq!(side, OrderSide::Sell);
        assert_eq!(position_side, PositionSide::Long);
        assert_eq!(cap, Some(Decimal::new(2, 0)));
        Ok(())
    }

    #[test]
    fn signed_position_delta_rejects_external_decimal_subtraction_overflow()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = owner()?;
        let expected = venue_domain::OrderCommand {
            time_in_force: LimitTimeInForce::Gtc,
            command_id: CommandId::new("manual-overflow-place")?,
            client_order_id: CommandId::new("manual-overflow-client")?,
            owner: owner.clone(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let position_after = Decimal::MAX
            .checked_sub(Decimal::ONE)
            .ok_or("decimal maximum unexpectedly cannot decrement")?;
        let position_before = Decimal::MIN
            .checked_add(Decimal::ONE)
            .ok_or("decimal minimum unexpectedly cannot increment")?;
        let signed = snapshot(
            vec![SignedAccountPositionFact {
                symbol: owner.symbol,
                position_side: PositionSide::Long,
                quantity: position_after,
                entry_price: None,
                mark_price: None,
            }],
            Vec::new(),
        )?;
        assert!(!signed_position_delta_proves_fill(
            &expected,
            position_before,
            &signed,
            Decimal::ONE,
        ));
        Ok(())
    }

    #[test]
    fn gtc_full_fill_without_open_order_requires_exact_fill_and_position_delta()
    -> Result<(), Box<dyn std::error::Error>> {
        let owner = owner()?;
        let command_id = CommandId::new("manual-place-a")?;
        let client_order_id = CommandId::new("manual-client-a")?;
        let price = Price::new(Decimal::new(10, 0))?;
        let command = venue_domain::OrderCommand {
            time_in_force: LimitTimeInForce::Gtc,
            command_id: command_id.clone(),
            client_order_id: client_order_id.clone(),
            owner: owner.clone(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(2, 0),
            limit_price: price,
            reduce_only: false,
        };
        let plan = ManualPlanCommand::PlaceLimit {
            command_id: command_id.clone(),
            client_order_id,
            owner: owner.clone(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quote_delta: Decimal::new(20, 0),
            limit_price: price,
            time_in_force: LimitTimeInForce::Gtc,
            maximum_quantity: None,
            reduce_only: false,
            position_before: Some(Decimal::ZERO),
        };
        let fill = Fill {
            fill_id: "manual-fill-a".to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: "manual-venue-a".to_owned(),
            symbol: owner.symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(2, 0),
            price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Missing,
            exchange_time_ms: Some(1),
        };
        let signed = snapshot(
            vec![SignedAccountPositionFact {
                symbol: owner.symbol.clone(),
                position_side: PositionSide::Long,
                quantity: Decimal::new(2, 0),
                entry_price: Some(price.value()),
                mark_price: Some(price.value()),
            }],
            vec![fill],
        )?;
        let commands =
            BTreeMap::from([(command_id.clone(), ExecutionCommand::PlaceLimit(command))]);
        let accepted = BTreeMap::from([(command_id, "manual-venue-a".to_owned())]);
        assert!(signed_plan_matches(&[plan], &commands, &accepted, &signed));
        Ok(())
    }

    #[test]
    fn manual_client_ids_do_not_include_grid_or_prefix_lookalikes()
    -> Result<(), Box<dyn std::error::Error>> {
        let client_order_id = CommandId::new("manual-client-a")?;
        let plan = ManualPlan {
            request_id: "request-a".to_owned(),
            delivery_digest: [7; 32],
            commands: vec![ManualPlanCommand::PlaceLimit {
                command_id: CommandId::new("manual-place-a")?,
                client_order_id: client_order_id.clone(),
                owner: owner()?,
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quote_delta: Decimal::ONE,
                limit_price: Price::new(Decimal::ONE)?,
                time_in_force: LimitTimeInForce::Gtc,
                maximum_quantity: None,
                reduce_only: false,
                position_before: Some(Decimal::ZERO),
            }],
        };
        let state = ManualActorState {
            schema_version: MANUAL_REPLAY_SCHEMA_VERSION,
            plans: BTreeMap::from([("plan-a".to_owned(), plan)]),
        };
        assert!(state.manual_client_order_ids().contains(&client_order_id));
        assert!(
            !state
                .manual_client_order_ids()
                .contains(&CommandId::new("manual-grid-lookalike")?)
        );
        Ok(())
    }

    #[test]
    fn exact_manual_client_id_consumes_its_fill_before_grid_routing()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = strategy_binding()?;
        let owner = owner()?;
        let client_order_id = CommandId::new("manual-client-a")?;
        let order = venue_domain::OrderCommand {
            time_in_force: LimitTimeInForce::Gtc,
            command_id: CommandId::new("manual-place-a")?,
            client_order_id: client_order_id.clone(),
            owner: owner.clone(),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let fill = Fill {
            fill_id: "manual-fill-a".to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: "native-a".to_owned(),
            symbol: owner.symbol,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::ONE,
            price: Price::new(Decimal::ONE)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Missing,
            exchange_time_ms: Some(1),
        };
        assert!(manual_fill_matches_command(
            &binding,
            &BTreeSet::from([client_order_id]),
            &fill,
            &order,
        ));
        assert!(!manual_fill_matches_command(
            &binding,
            &BTreeSet::new(),
            &fill,
            &order,
        ));
        Ok(())
    }

    #[test]
    fn cancel_all_rejects_when_an_owned_grid_order_would_be_left_working()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = strategy_binding()?;
        let order = SignedAccountOrderFact {
            client_order_id: "grid-client-a".to_owned(),
            venue_order_id: Some("grid-venue-a".to_owned()),
            symbol: binding.key.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Some(Decimal::ONE),
            time_in_force: Some(LimitTimeInForce::PostOnly),
            created_at_ms: Some(1),
            reduce_only: false,
            owner: Some(owner()?),
            external: false,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        };
        let intent = TradeIntent {
            action: TradingAction::CancelAllOrders,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            selected_price: None,
            quote_notional: None,
            close_quantity_cap: None,
            selected_order_id: None,
        };
        assert!(
            cancel_plan(
                &binding,
                &intent,
                &[order],
                "manual-plan-a",
                &BTreeSet::new()
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cancel_all_rejects_when_a_same_symbol_external_order_would_remain()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = strategy_binding()?;
        let order = SignedAccountOrderFact {
            client_order_id: "external-client-a".to_owned(),
            venue_order_id: Some("external-venue-a".to_owned()),
            symbol: binding.key.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Some(Decimal::ONE),
            time_in_force: Some(LimitTimeInForce::Gtc),
            created_at_ms: Some(1),
            reduce_only: false,
            owner: None,
            external: true,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        };
        let intent = TradeIntent {
            action: TradingAction::CancelAllOrders,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            selected_price: None,
            quote_notional: None,
            close_quantity_cap: None,
            selected_order_id: None,
        };
        assert!(
            cancel_plan(
                &binding,
                &intent,
                &[order],
                "manual-plan-a",
                &BTreeSet::new()
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn cancel_all_rejects_an_unknown_state_even_with_a_manual_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = strategy_binding()?;
        let manual_id = CommandId::new("manual-client-a")?;
        let manual = SignedAccountOrderFact {
            client_order_id: manual_id.as_str().to_owned(),
            venue_order_id: Some("manual-venue-a".to_owned()),
            symbol: binding.key.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Some(Decimal::ONE),
            time_in_force: Some(LimitTimeInForce::Gtc),
            created_at_ms: Some(1),
            reduce_only: false,
            owner: Some(owner()?),
            external: false,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        };
        let mut unknown = manual.clone();
        unknown.client_order_id = "unknown-client-a".to_owned();
        unknown.venue_order_id = Some("unknown-venue-a".to_owned());
        unknown.state = None;
        let intent = TradeIntent {
            action: TradingAction::CancelAllOrders,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            selected_price: None,
            quote_notional: None,
            close_quantity_cap: None,
            selected_order_id: None,
        };
        assert!(
            cancel_plan(
                &binding,
                &intent,
                &[manual, unknown],
                "manual-plan-a",
                &BTreeSet::from([manual_id]),
            )
            .is_err()
        );
        Ok(())
    }
}
