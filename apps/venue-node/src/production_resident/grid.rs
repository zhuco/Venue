use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    CancelCommand, CommandId, ExecutionCommand, FieldState, Fill, OrderCommand, OrderOwner,
    OrderPurpose, OrderState, PositionSide,
};
use venue_runtime::SignedAccountOrderFact;
use venue_strategies::hedged_grid::{
    GridAction, GridDecision, GridEpoch, GridInventory, GridOrderIntent, GridOrderKey,
    GridPosition, GridTransaction, HedgedGridError, HedgedGridState, OwnedGridFill,
};

use crate::{NodeError, runtime_config::NodeGridRecoveryPolicy};

const MAX_GRID_CHECKPOINT_BYTES: usize = 1_048_576;
const MAX_PARTIAL_FILL_SLICES_PER_ORDER: usize = 256;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GridBootstrapState {
    #[default]
    Eligible,
    Attempted,
    Confirmed,
}

/// Durable correlation for an order the Grid reducer already owns. It is a projection of the
/// actor checkpoint, not a second owner, journal, or authority: Host/WAL remain the only source
/// of mutation ownership and acceptance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GridOrderRoute {
    pub key: GridOrderKey,
    pub client_order_id: CommandId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_venue_order_id: Option<String>,
}

/// Durable portions of one exact accepted native order. This is only a checkpoint projection:
/// private evidence and Host/WAL remain the source of the individual executions. The projection
/// prevents two partial fills from being mistaken for two complete grid rolls after restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GridPartialFill {
    source_order: GridOrderKey,
    #[serde(with = "rust_decimal::serde::str")]
    cumulative_quantity: rust_decimal::Decimal,
    fills: BTreeMap<String, GridPartialFillSlice>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GridPartialFillSlice {
    #[serde(with = "rust_decimal::serde::str")]
    quantity: rust_decimal::Decimal,
    maker: FieldState<bool>,
}

/// The complete Grid checkpoint shape used by the production private-fill bridge. A native order
/// id alone is never enough: restart recovery requires the exact key/client/native triad, so it
/// cannot infer a grid level from price, side, or current BBO.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GridBridgeState {
    pub grid: HedgedGridState,
    #[serde(default)]
    bootstrap_state: GridBootstrapState,
    /// A completed reset may deliberately leave an uninstalled shape. It is distinct from the
    /// first epoch and receives one separately durable, signed-empty-surface rebuild attempt.
    #[serde(default)]
    reset_rebuild_attempted: bool,
    #[serde(with = "grid_routes")]
    routes: BTreeMap<GridOrderKey, GridOrderRoute>,
    #[serde(default)]
    partial_fills: BTreeMap<String, GridPartialFill>,
}

/// JSON object keys cannot encode `GridOrderKey` without inventing a lossy string identity.
/// Persist routes as validated records instead. The only representable legacy map was empty, so
/// that exact shape remains readable while every non-empty legacy object fails closed.
mod grid_routes {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
    use venue_strategies::hedged_grid::GridOrderKey;

    use super::GridOrderRoute;

    pub(super) fn serialize<S>(
        routes: &BTreeMap<GridOrderKey, GridOrderRoute>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        routes.values().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<GridOrderKey, GridOrderRoute>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum EncodedRoutes {
            Records(Vec<GridOrderRoute>),
            LegacyEmpty(BTreeMap<String, serde::de::IgnoredAny>),
        }

        let encoded = EncodedRoutes::deserialize(deserializer)?;
        let records = match encoded {
            EncodedRoutes::Records(records) => records,
            EncodedRoutes::LegacyEmpty(legacy) if legacy.is_empty() => return Ok(BTreeMap::new()),
            EncodedRoutes::LegacyEmpty(_) => {
                return Err(D::Error::custom("legacy grid route object must be empty"));
            }
        };
        let mut routes = BTreeMap::new();
        for route in records {
            if routes.insert(route.key.clone(), route).is_some() {
                return Err(D::Error::custom("duplicate grid route key"));
            }
        }
        Ok(routes)
    }
}

pub(crate) struct GridDispatchPlan {
    pub commands: Vec<ExecutionCommand>,
    accepted_routes: Vec<(GridOrderKey, CommandId, CommandId)>,
}

impl GridBridgeState {
    pub(crate) fn bootstrap(grid: HedgedGridState) -> Result<Self, NodeError> {
        let state = Self {
            grid,
            bootstrap_state: GridBootstrapState::Eligible,
            reset_rebuild_attempted: false,
            routes: BTreeMap::new(),
            partial_fills: BTreeMap::new(),
        };
        state.validate().map_err(|_| NodeError::ResidentRuntime)?;
        Ok(state)
    }

    /// Restores only a checkpoint that Runtime recovered from its verified Actor Applied store.
    /// `bootstrap_when_absent` never replaces malformed state with price-derived order keys.
    pub(crate) fn restore_or_bootstrap(
        checkpoint: Option<Vec<u8>>,
        initial: HedgedGridState,
        policy: NodeGridRecoveryPolicy,
    ) -> Result<Self, NodeError> {
        match checkpoint {
            Some(bytes) if bytes.len() <= MAX_GRID_CHECKPOINT_BYTES => {
                let mut restored: Self =
                    serde_json::from_slice(&bytes).map_err(|_| NodeError::ResidentArtifacts)?;
                restored
                    .grid
                    .migrate_checkpoint()
                    .map_err(|_| NodeError::ResidentRuntime)?;
                if restored.grid.binding != initial.binding
                    || restored.grid.params != initial.params
                {
                    return Err(NodeError::ResidentRuntime);
                }
                // Checkpoints written before this field existed are eligible only in the exact
                // uninstalled shape used by predecessor custody turns. Any durable epoch/route
                // means a physical bootstrap may already have crossed its no-retry boundary.
                if restored.bootstrap_state == GridBootstrapState::Eligible
                    && !restored.has_uninstalled_shape()
                {
                    restored.bootstrap_state = GridBootstrapState::Attempted;
                }
                restored
                    .validate()
                    .map_err(|_| NodeError::ResidentRuntime)?;
                Ok(restored)
            }
            Some(_) => Err(NodeError::ResidentArtifacts),
            None => match policy {
                NodeGridRecoveryPolicy::BootstrapWhenAbsent => Self::bootstrap(initial),
                NodeGridRecoveryPolicy::RequireExisting => Err(NodeError::ResidentArtifacts),
            },
        }
    }

    pub(crate) fn checkpoint_bytes(&self) -> Result<Vec<u8>, NodeError> {
        self.validate().map_err(|_| NodeError::ResidentRuntime)?;
        let bytes = serde_json::to_vec(self).map_err(|_| NodeError::ResidentArtifacts)?;
        if bytes.len() > MAX_GRID_CHECKPOINT_BYTES {
            return Err(NodeError::ResidentArtifacts);
        }
        Ok(bytes)
    }

    /// Legacy custody cancellation may persist Actor turns before the successor has installed
    /// its first epoch.  That checkpoint is still an uninstalled Grid, not evidence that an
    /// order-creating bootstrap was attempted.  Once an epoch exists, even a rejected or
    /// indeterminate physical batch must never re-arm first installation after restart.
    pub(crate) fn needs_initial_bootstrap(&self) -> bool {
        self.bootstrap_state == GridBootstrapState::Eligible && self.has_uninstalled_shape()
    }

    pub(crate) fn needs_reset_rebuild(&self) -> bool {
        self.bootstrap_state == GridBootstrapState::Attempted
            && !self.reset_rebuild_attempted
            && self.grid.phase == venue_strategies::hedged_grid::GridPhase::ResettingGrid
            && self.grid.epoch.is_none()
            && self.grid.inventory.is_none()
            && self.grid.owned_orders.is_empty()
            && self.grid.pending_transactions.is_empty()
            && self.grid.pending_replenishments.is_empty()
            && self.routes.is_empty()
            && self.partial_fills.is_empty()
    }

    pub(crate) fn bootstrap_requires_reconciliation(&self) -> bool {
        self.bootstrap_state == GridBootstrapState::Attempted
            || !self.grid.pending_transactions.is_empty()
    }

    /// Returns the exact deterministic WAL ids of every locally reserved rolling transaction.
    /// The checkpoint has no native ids for replacement routes until Host records Accepted, so a
    /// restart may classify only these ids; it must never infer whether an exchange mutation ran.
    pub(crate) fn pending_transaction_command_ids(
        &self,
    ) -> Result<Vec<(String, Vec<CommandId>)>, GridBridgeError> {
        self.grid
            .pending_transactions
            .values()
            .map(|transaction| {
                let mut command_ids = transaction
                    .places
                    .iter()
                    .map(|order| stable_identifier(b"place", &self.grid.binding, &order.key))
                    .collect::<Result<Vec<_>, _>>()?;
                command_ids.push(stable_identifier(
                    b"cancel",
                    &self.grid.binding,
                    &transaction.cancel,
                )?);
                Ok((transaction.id.clone(), command_ids))
            })
            .collect()
    }

    /// Drops only routes created by transactions proven never to have reached this Host WAL.
    /// The reducer restores the old cancellation target and moves to reconciliation; no exchange
    /// request is emitted here.
    pub(crate) fn abandon_unsubmitted_transactions_for_reconciliation(
        &mut self,
        transaction_ids: &[String],
    ) -> Result<(), GridBridgeError> {
        let replacements = self
            .grid
            .pending_transactions
            .values()
            .flat_map(|transaction| transaction.places.iter().map(|order| order.key.clone()))
            .collect::<Vec<_>>();
        self.grid
            .abandon_unsubmitted_transactions_for_reconciliation(transaction_ids)
            .map_err(GridBridgeError::Reducer)?;
        for key in replacements {
            self.routes.remove(&key);
        }
        self.validate()
    }

    pub(crate) fn mark_bootstrap_attempted(&mut self) -> Result<(), GridBridgeError> {
        if !self.needs_initial_bootstrap() {
            return Err(GridBridgeError::BootstrapState);
        }
        self.bootstrap_state = GridBootstrapState::Attempted;
        self.validate()
    }

    pub(crate) fn mark_reset_rebuild_attempted(&mut self) -> Result<(), GridBridgeError> {
        if !self.needs_reset_rebuild() {
            return Err(GridBridgeError::BootstrapState);
        }
        self.reset_rebuild_attempted = true;
        self.validate()
    }

    pub(crate) fn mark_bootstrap_confirmed(&mut self) -> Result<(), GridBridgeError> {
        if self.bootstrap_state != GridBootstrapState::Attempted
            || self.grid.epoch.is_none()
            || self.routes.is_empty()
            || self
                .routes
                .values()
                .any(|route| route.accepted_venue_order_id.is_none())
        {
            return Err(GridBridgeError::BootstrapState);
        }
        self.bootstrap_state = GridBootstrapState::Confirmed;
        self.validate()
    }

    fn has_uninstalled_shape(&self) -> bool {
        matches!(
            self.grid.phase,
            venue_strategies::hedged_grid::GridPhase::Recovering
                | venue_strategies::hedged_grid::GridPhase::ResettingGrid
        ) && self.grid.epoch.is_none()
            && self.grid.inventory.is_none()
            && self.grid.owned_orders.is_empty()
            && self.grid.pending_transactions.is_empty()
            && self.grid.pending_replenishments.is_empty()
            && self.grid.seen_fill_ids.is_empty()
            && self.grid.owned_fill_records.is_empty()
            && self.routes.is_empty()
            && self.partial_fills.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn install_test_accepted_open_route(
        &mut self,
        chosen_venue_order_id: &str,
    ) -> Result<ExecutionCommand, NodeError> {
        if self.needs_initial_bootstrap() {
            self.mark_bootstrap_attempted()
                .map_err(|_| NodeError::ResidentRuntime)?;
        }
        let plan = self
            .install_initial_epoch(
                GridInventory {
                    private_generation: 1,
                    private_observed_at_ms: 1,
                    mark_price: venue_domain::Price::new(rust_decimal::Decimal::new(100, 0))
                        .map_err(|_| NodeError::ResidentRuntime)?,
                    long_quantity: rust_decimal::Decimal::ONE,
                    short_quantity: rust_decimal::Decimal::ONE,
                },
                GridEpoch {
                    epoch: 1,
                    anchor_price: venue_domain::Price::new(rust_decimal::Decimal::new(100, 0))
                        .map_err(|_| NodeError::ResidentRuntime)?,
                    step: venue_domain::Price::new(rust_decimal::Decimal::ONE)
                        .map_err(|_| NodeError::ResidentRuntime)?,
                    grid_quantity: rust_decimal::Decimal::new(5, 2),
                    passive_book_fallback: None,
                },
            )
            .map_err(|_| NodeError::ResidentRuntime)?;
        let chosen = plan
            .commands
            .iter()
            .find(|command| {
                matches!(command, ExecutionCommand::PlaceLimit(order) if !order.reduce_only)
            })
            .cloned()
            .ok_or(NodeError::ResidentRuntime)?;
        let accepted = plan
            .accepted_routes
            .iter()
            .enumerate()
            .map(|(index, (_, _, command_id))| {
                (
                    command_id.clone(),
                    if command_id == chosen.command_id() {
                        chosen_venue_order_id.to_owned()
                    } else {
                        format!("unused-test-native-{index}")
                    },
                )
            })
            .collect::<Vec<_>>();
        self.bind_accepted_plan(&plan, &accepted)
            .map_err(|_| NodeError::ResidentRuntime)?;
        Ok(chosen)
    }

    /// Signed convergence is based on the reducer's current desired set, never on an order count.
    /// Retired routes are deliberately ignored: a completed rolling cancellation is historical,
    /// while every current desired key must retain its exact client/native/order-shape triad.
    pub(crate) fn signed_desired_matches(&self, orders: &[SignedAccountOrderFact]) -> bool {
        orders.len() == self.grid.owned_orders.len()
            && self.grid.owned_orders.iter().all(|(key, desired)| {
                let Some(route) = self.routes.get(key) else {
                    return false;
                };
                let Some(native) = route.accepted_venue_order_id.as_deref() else {
                    return false;
                };
                orders.iter().any(|actual| {
                    !actual.external
                        && actual.owner.as_ref() == Some(&owner_for_order(&self.grid, desired))
                        && actual.client_order_id == route.client_order_id.as_str()
                        && actual.venue_order_id.as_deref() == Some(native)
                        && actual.family == venue_domain::NativeOrderFamily::UmOrder
                        && actual.symbol == self.grid.binding.symbol
                        && actual.side == desired.side
                        && actual.position_side
                            == match desired.key.position {
                                GridPosition::Long => PositionSide::Long,
                                GridPosition::Short => PositionSide::Short,
                            }
                        && actual.quantity == desired.quantity
                        && actual.limit_price == Some(desired.price.value())
                        && actual.time_in_force == Some(venue_domain::LimitTimeInForce::PostOnly)
                        && actual.reduce_only == desired.reduce_only
                        && matches!(
                            actual.state,
                            Some(OrderState::New | OrderState::PartiallyFilled)
                        )
                })
            })
    }

    pub(crate) fn expected_signed_surface(
        &self,
    ) -> Result<BTreeMap<CommandId, OrderOwner>, NodeError> {
        self.grid
            .owned_orders
            .iter()
            .map(|(key, desired)| {
                let route = self.routes.get(key).ok_or(NodeError::ResidentRuntime)?;
                if route.accepted_venue_order_id.is_none() {
                    return Err(NodeError::ResidentRuntime);
                }
                Ok((
                    route.client_order_id.clone(),
                    owner_for_order(&self.grid, desired),
                ))
            })
            .collect()
    }

    /// Establishes the stable client id before Host prepares the order. A duplicated client id or
    /// a route for a non-owned key fails closed rather than being reassigned to a nearby level.
    pub(crate) fn reserve_client_route(
        &mut self,
        key: GridOrderKey,
        client_order_id: CommandId,
    ) -> Result<(), GridBridgeError> {
        self.require_owned(&key)?;
        if self.routes.contains_key(&key)
            || self
                .routes
                .values()
                .any(|route| route.client_order_id == client_order_id)
        {
            return Err(GridBridgeError::RouteConflict);
        }
        self.routes.insert(
            key.clone(),
            GridOrderRoute {
                key,
                client_order_id,
                accepted_venue_order_id: None,
            },
        );
        self.validate()?;
        Ok(())
    }

    /// Records the native id only after Host/WAL report Accepted for this exact stable client id.
    pub(crate) fn bind_accepted_native(
        &mut self,
        key: &GridOrderKey,
        client_order_id: &CommandId,
        venue_order_id: String,
    ) -> Result<(), GridBridgeError> {
        if venue_order_id.trim().is_empty() {
            return Err(GridBridgeError::Evidence);
        }
        if self.routes.values().any(|other| {
            other.key != *key
                && other.accepted_venue_order_id.as_deref() == Some(venue_order_id.as_str())
        }) {
            return Err(GridBridgeError::RouteConflict);
        }
        let route = self
            .routes
            .get_mut(key)
            .ok_or(GridBridgeError::UnknownOrder)?;
        if &route.client_order_id != client_order_id || route.accepted_venue_order_id.is_some() {
            return Err(GridBridgeError::RouteConflict);
        }
        route.accepted_venue_order_id = Some(venue_order_id);
        self.validate()?;
        Ok(())
    }

    /// Pure hot-path mapper: it makes no BBO or REST call. The caller must have persisted the
    /// normalized private fill through AccountRuntime first. A fill without an accepted native
    /// route is rejected instead of being matched by price, side, or client-id guesswork.
    pub(crate) fn observe_persisted_fill(
        &mut self,
        fill: &Fill,
        private_generation: u64,
    ) -> Result<GridDecision, GridBridgeError> {
        fill.validate().map_err(|_| GridBridgeError::Evidence)?;
        if private_generation == 0 {
            return Err(GridBridgeError::Evidence);
        }
        let key = self
            .routes
            .iter()
            .find_map(|(key, route)| {
                (route.accepted_venue_order_id.as_deref() == Some(fill.order_id.as_str()))
                    .then(|| key.clone())
            })
            .ok_or(GridBridgeError::UnknownOrder)?;
        let source = self.require_owned(&key)?.clone();
        let position = match source.key.position {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        };
        if fill.symbol != self.grid.binding.symbol
            || fill.side != source.side
            || !matches!(fill.position_side, FieldState::Known(value) if value == position)
            || fill.price != source.price
            || fill.quantity > source.quantity
        {
            return Err(GridBridgeError::Evidence);
        }
        let completed = self.record_partial_fill(&key, fill)?;
        let Some(completed) = completed else {
            self.validate()?;
            return Ok(GridDecision::Noop);
        };
        let prior_grid = self.grid.clone();
        let decision = match self.grid.observe_stream_owned_fill(OwnedGridFill {
            fill_id: fill.fill_id.clone(),
            private_generation,
            source_order: key.clone(),
            fill_price: source.price,
            complete: true,
            maker: aggregate_maker(&completed.fills),
        }) {
            Ok(decision) => decision,
            Err(error) => {
                self.grid = prior_grid;
                self.partial_fills.insert(fill.order_id.clone(), completed);
                return Err(GridBridgeError::Reducer(error));
            }
        };
        // Reducer retirement and route retirement are one checkpoint transition. Retaining the
        // native route after the reducer removes its owned order would make the checkpoint fail
        // validation and could not safely be replayed as a new order.
        self.routes.remove(&key);
        self.validate()?;
        Ok(decision)
    }

    fn record_partial_fill(
        &mut self,
        key: &GridOrderKey,
        fill: &Fill,
    ) -> Result<Option<GridPartialFill>, GridBridgeError> {
        let existing = self.partial_fills.get(&fill.order_id);
        if let Some(existing) = existing
            && existing.source_order != *key
        {
            return Err(GridBridgeError::Evidence);
        }
        if let Some(slice) = existing.and_then(|partial| partial.fills.get(&fill.fill_id)) {
            if slice.quantity != fill.quantity || slice.maker != fill.maker {
                return Err(GridBridgeError::Evidence);
            }
            return Ok(None);
        }
        if existing.is_some_and(|partial| partial.fills.len() >= MAX_PARTIAL_FILL_SLICES_PER_ORDER)
        {
            return Err(GridBridgeError::Evidence);
        }
        let source_quantity = self.require_owned(key)?.quantity;
        let cumulative_quantity = existing
            .map(|partial| partial.cumulative_quantity)
            .unwrap_or_default()
            .checked_add(fill.quantity)
            .ok_or(GridBridgeError::Evidence)?;
        if cumulative_quantity > source_quantity {
            return Err(GridBridgeError::Evidence);
        }
        let partial = self
            .partial_fills
            .entry(fill.order_id.clone())
            .or_insert_with(|| GridPartialFill {
                source_order: key.clone(),
                cumulative_quantity: rust_decimal::Decimal::ZERO,
                fills: BTreeMap::new(),
            });
        partial.cumulative_quantity = cumulative_quantity;
        partial.fills.insert(
            fill.fill_id.clone(),
            GridPartialFillSlice {
                quantity: fill.quantity,
                maker: fill.maker.clone(),
            },
        );
        if cumulative_quantity == source_quantity {
            return self
                .partial_fills
                .remove(&fill.order_id)
                .map(Some)
                .ok_or(GridBridgeError::Evidence);
        }
        Ok(None)
    }

    /// Converts only the reducer's already-reserved rolling transaction to the one account WAL
    /// command family. The two replacement limits precede the exact client-id cancellation; no
    /// BBO/REST lookup is permitted on this completed-fill path.
    pub(crate) fn plan_dispatch(
        &mut self,
        action: &GridAction,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        let GridAction::Dispatch(transaction) = action else {
            return Err(GridBridgeError::UnsupportedAction);
        };
        self.plan_transaction(transaction)
    }

    pub(crate) fn install_initial_epoch(
        &mut self,
        inventory: GridInventory,
        epoch: GridEpoch,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        if self.bootstrap_state != GridBootstrapState::Attempted {
            return Err(GridBridgeError::BootstrapState);
        }
        self.grid
            .begin_inventory_check()
            .map_err(GridBridgeError::Reducer)?;
        let decision = self
            .grid
            .observe_inventory(inventory)
            .map_err(GridBridgeError::Reducer)?;
        if !matches!(decision, GridDecision::Actions(actions) if actions.iter().all(|action| matches!(action, GridAction::Reset { .. })))
        {
            return Err(GridBridgeError::Evidence);
        }
        self.grid
            .reset_orders_settled()
            .map_err(GridBridgeError::Reducer)?;
        let GridDecision::Actions(actions) = self
            .grid
            .install_epoch(epoch)
            .map_err(GridBridgeError::Reducer)?
        else {
            return Err(GridBridgeError::Evidence);
        };
        self.plan_places(&actions)
    }

    pub(crate) fn bind_accepted_plan(
        &mut self,
        plan: &GridDispatchPlan,
        accepted: &[(CommandId, String)],
    ) -> Result<(), GridBridgeError> {
        for (key, client, expected_command_id) in &plan.accepted_routes {
            let venue_order_id = accepted
                .iter()
                .find_map(|(command_id, venue_order_id)| {
                    (command_id == expected_command_id).then_some(venue_order_id.clone())
                })
                .ok_or(GridBridgeError::Evidence)?;
            self.bind_accepted_native(key, client, venue_order_id)?;
        }
        Ok(())
    }

    fn plan_transaction(
        &mut self,
        transaction: &GridTransaction,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        let cancelled = self
            .routes
            .get(&transaction.cancel)
            .ok_or(GridBridgeError::UnknownOrder)?
            .client_order_id
            .clone();
        let mut commands = Vec::with_capacity(3);
        let mut accepted_routes = Vec::with_capacity(2);
        for order in &transaction.places {
            let client = stable_identifier(b"client", &self.grid.binding, &order.key)?;
            self.reserve_client_route(order.key.clone(), client.clone())?;
            let command_id = stable_identifier(b"place", &self.grid.binding, &order.key)?;
            commands.push(ExecutionCommand::PlaceLimit(OrderCommand {
                time_in_force: Default::default(),
                command_id: command_id.clone(),
                client_order_id: client.clone(),
                owner: owner_for_order(&self.grid, order),
                side: order.side,
                position_side: match order.key.position {
                    GridPosition::Long => PositionSide::Long,
                    GridPosition::Short => PositionSide::Short,
                },
                quantity: order.quantity,
                limit_price: order.price,
                reduce_only: order.reduce_only,
            }));
            accepted_routes.push((order.key.clone(), client, command_id));
        }
        commands.push(ExecutionCommand::Cancel(CancelCommand {
            command_id: stable_identifier(b"cancel", &self.grid.binding, &transaction.cancel)?,
            owner: owner_for_order(
                &self.grid,
                transaction
                    .cancelled_order
                    .as_ref()
                    .ok_or(GridBridgeError::Evidence)?,
            ),
            target_client_order_id: cancelled,
        }));
        self.validate()?;
        Ok(GridDispatchPlan {
            commands,
            accepted_routes,
        })
    }

    fn plan_places(&mut self, actions: &[GridAction]) -> Result<GridDispatchPlan, GridBridgeError> {
        let mut orders = actions
            .iter()
            .map(|action| match action {
                GridAction::Place(order) => Ok(order.clone()),
                _ => Err(GridBridgeError::Evidence),
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Existing close capacity is reduced before any new entry; within each wave key ordering
        // is durable and deterministic.
        orders.sort_by_key(|order| (!order.reduce_only, order.key.clone()));
        let mut commands = Vec::with_capacity(orders.len());
        let mut accepted_routes = Vec::with_capacity(orders.len());
        for order in orders {
            let client = stable_identifier(b"client", &self.grid.binding, &order.key)?;
            self.reserve_client_route(order.key.clone(), client.clone())?;
            let command_id = stable_identifier(b"place", &self.grid.binding, &order.key)?;
            commands.push(ExecutionCommand::PlaceLimit(OrderCommand {
                time_in_force: Default::default(),
                command_id: command_id.clone(),
                client_order_id: client.clone(),
                owner: owner_for_order(&self.grid, &order),
                side: order.side,
                position_side: match order.key.position {
                    GridPosition::Long => PositionSide::Long,
                    GridPosition::Short => PositionSide::Short,
                },
                quantity: order.quantity,
                limit_price: order.price,
                reduce_only: order.reduce_only,
            }));
            accepted_routes.push((order.key, client, command_id));
        }
        Ok(GridDispatchPlan {
            commands,
            accepted_routes,
        })
    }

    fn require_owned(&self, key: &GridOrderKey) -> Result<&GridOrderIntent, GridBridgeError> {
        self.grid
            .owned_orders
            .get(key)
            .ok_or(GridBridgeError::UnknownOrder)
    }

    fn validate(&self) -> Result<(), GridBridgeError> {
        if (self.bootstrap_state == GridBootstrapState::Eligible && !self.has_uninstalled_shape())
            || (self.bootstrap_state == GridBootstrapState::Confirmed && self.grid.epoch.is_none())
        {
            return Err(GridBridgeError::BootstrapState);
        }
        let mut seen_clients = BTreeMap::new();
        let mut seen_native = BTreeMap::new();
        for (key, route) in &self.routes {
            if key != &route.key
                || seen_clients
                    .insert(route.client_order_id.clone(), key)
                    .is_some()
                || route
                    .accepted_venue_order_id
                    .as_ref()
                    .is_some_and(|native| {
                        native.trim().is_empty() || seen_native.insert(native, key).is_some()
                    })
            {
                return Err(GridBridgeError::RouteConflict);
            }
        }
        for (native_order_id, partial) in &self.partial_fills {
            let Some(route) = self.routes.get(&partial.source_order) else {
                return Err(GridBridgeError::RouteConflict);
            };
            if route.accepted_venue_order_id.as_deref() != Some(native_order_id)
                || partial.fills.is_empty()
                || partial.fills.len() > MAX_PARTIAL_FILL_SLICES_PER_ORDER
                || partial.fills.iter().any(|(fill_id, slice)| {
                    fill_id.trim().is_empty()
                        || slice.quantity.is_zero()
                        || !slice.quantity.is_sign_positive()
                })
            {
                return Err(GridBridgeError::RouteConflict);
            }
            let source = self.require_owned(&partial.source_order)?;
            let quantity = partial
                .fills
                .values()
                .try_fold(rust_decimal::Decimal::ZERO, |total, slice| {
                    total.checked_add(slice.quantity)
                })
                .ok_or(GridBridgeError::Evidence)?;
            if quantity != partial.cumulative_quantity
                || quantity.is_zero()
                || !quantity.is_sign_positive()
                || quantity >= source.quantity
            {
                return Err(GridBridgeError::Evidence);
            }
        }
        Ok(())
    }
}

fn aggregate_maker(fills: &BTreeMap<String, GridPartialFillSlice>) -> FieldState<bool> {
    let mut all_maker = true;
    for fill in fills.values() {
        match fill.maker {
            FieldState::Known(true) => {}
            FieldState::Known(false) => return FieldState::Known(false),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => all_maker = false,
        }
    }
    if all_maker {
        FieldState::Known(true)
    } else {
        FieldState::Missing
    }
}

fn owner_for_order(grid: &HedgedGridState, order: &GridOrderIntent) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: grid.binding.strategy_instance_id.clone(),
        run_id: grid.binding.run_id.clone(),
        exchange: grid.binding.exchange.clone(),
        account: grid.binding.account.clone(),
        symbol: grid.binding.symbol.clone(),
        purpose: if order.reduce_only {
            OrderPurpose::Reduce
        } else {
            OrderPurpose::Entry
        },
    }
}

fn stable_identifier(
    label: &[u8],
    binding: &venue_strategies::hedged_grid::HedgedGridBinding,
    key: &GridOrderKey,
) -> Result<CommandId, GridBridgeError> {
    let encoded = serde_json::to_vec(&(binding, key)).map_err(|_| GridBridgeError::Evidence)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.grid.command.v1");
    digest.update(label);
    digest.update(encoded);
    let label = match label {
        b"client" => "c",
        b"place" => "p",
        b"cancel" => "x",
        _ => return Err(GridBridgeError::Evidence),
    };
    let raw = format!("g{label}-{:x}", digest.finalize());
    CommandId::new(raw[..36].to_owned()).map_err(|_| GridBridgeError::Evidence)
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum GridBridgeError {
    #[error("grid order route is missing or no longer owned")]
    UnknownOrder,
    #[error("grid route identities conflict")]
    RouteConflict,
    #[error("private fill evidence is insufficient for the owned grid order")]
    Evidence,
    #[error("the Grid action requires a startup/rebuild input path, not completed-fill rolling")]
    UnsupportedAction,
    #[error("grid bootstrap state crossed an invalid durable boundary")]
    BootstrapState,
    #[error("grid reducer rejected the persisted private fill: {0}")]
    Reducer(HedgedGridError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::Price;
    use venue_domain::domain::{Asset, Symbol};
    use venue_strategies::hedged_grid::{
        GridEpoch, GridInventory, HedgedGridBinding, HedgedGridParams, HedgedGridState,
    };

    use super::*;

    fn initial() -> Result<HedgedGridState, Box<dyn std::error::Error>> {
        let binding = HedgedGridBinding {
            strategy_instance_id: "grid_doge".to_owned(),
            run_id: "run_a".to_owned(),
            exchange: "binance".to_owned(),
            account: "account_a".to_owned(),
            symbol: "DOGE/USDT".parse::<Symbol>()?,
            config_version: "abc123".to_owned(),
            owner_scope: "grid_doge".to_owned(),
        };
        Ok(HedgedGridState::new_with_params(
            binding,
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 10)?,
        )?)
    }

    #[test]
    fn recovery_requires_verified_checkpoint_or_explicit_first_bootstrap()
    -> Result<(), Box<dyn std::error::Error>> {
        let initial = initial()?;
        assert!(
            GridBridgeState::restore_or_bootstrap(
                None,
                initial.clone(),
                NodeGridRecoveryPolicy::RequireExisting,
            )
            .is_err()
        );
        let bridge = GridBridgeState::restore_or_bootstrap(
            None,
            initial.clone(),
            NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        )?;
        let restored = GridBridgeState::restore_or_bootstrap(
            Some(bridge.checkpoint_bytes()?),
            initial,
            NodeGridRecoveryPolicy::RequireExisting,
        )?;
        assert_eq!(restored.grid.binding.symbol, "DOGE/USDT".parse()?);
        Ok(())
    }

    #[test]
    fn uninstalled_actor_checkpoint_rearms_only_before_an_epoch_is_planned()
    -> Result<(), Box<dyn std::error::Error>> {
        let initial = initial()?;
        let bridge = GridBridgeState::bootstrap(initial.clone())?;
        let mut legacy_uninstalled: serde_json::Value =
            serde_json::from_slice(&bridge.checkpoint_bytes()?)?;
        legacy_uninstalled
            .as_object_mut()
            .ok_or("grid checkpoint object")?
            .remove("bootstrap_state");
        let restored = GridBridgeState::restore_or_bootstrap(
            Some(serde_json::to_vec(&legacy_uninstalled)?),
            initial.clone(),
            NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        )?;
        assert!(restored.needs_initial_bootstrap());

        let mut planned = GridBridgeState::bootstrap(initial.clone())?;
        planned.mark_bootstrap_attempted()?;
        let plan = planned.install_initial_epoch(
            GridInventory {
                private_generation: 2,
                private_observed_at_ms: 10,
                mark_price: Price::new(Decimal::new(100, 0))?,
                long_quantity: Decimal::ONE,
                short_quantity: Decimal::ONE,
            },
            GridEpoch {
                epoch: 1,
                anchor_price: Price::new(Decimal::new(100, 0))?,
                step: Price::new(Decimal::ONE)?,
                grid_quantity: Decimal::new(5, 2),
                passive_book_fallback: None,
            },
        )?;
        assert!(!planned.needs_initial_bootstrap());
        assert!(planned.bootstrap_requires_reconciliation());
        let mut legacy_planned: serde_json::Value =
            serde_json::from_slice(&planned.checkpoint_bytes()?)?;
        legacy_planned
            .as_object_mut()
            .ok_or("grid checkpoint object")?
            .remove("bootstrap_state");
        let legacy_planned = GridBridgeState::restore_or_bootstrap(
            Some(serde_json::to_vec(&legacy_planned)?),
            initial.clone(),
            NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        )?;
        assert!(!legacy_planned.needs_initial_bootstrap());
        assert!(legacy_planned.bootstrap_requires_reconciliation());
        let accepted = plan
            .accepted_routes
            .iter()
            .enumerate()
            .map(|(index, (_, _, command_id))| {
                (command_id.clone(), format!("confirmed-native-{index}"))
            })
            .collect::<Vec<_>>();
        planned.bind_accepted_plan(&plan, &accepted)?;
        planned.mark_bootstrap_confirmed()?;
        let confirmed = GridBridgeState::restore_or_bootstrap(
            Some(planned.checkpoint_bytes()?),
            initial,
            NodeGridRecoveryPolicy::BootstrapWhenAbsent,
        )?;
        assert!(!confirmed.needs_initial_bootstrap());
        assert!(!confirmed.bootstrap_requires_reconciliation());
        Ok(())
    }

    #[test]
    fn signed_grid_surface_is_bijective_and_owner_purpose_exact()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut bridge = GridBridgeState::bootstrap(initial()?)?;
        let _chosen = bridge.install_test_accepted_open_route("chosen-native")?;
        let mut orders = bridge
            .grid
            .owned_orders
            .iter()
            .map(|(key, desired)| {
                let route = bridge.routes.get(key).ok_or("route")?;
                Ok::<_, Box<dyn std::error::Error>>(SignedAccountOrderFact {
                    client_order_id: route.client_order_id.as_str().to_owned(),
                    venue_order_id: route.accepted_venue_order_id.clone(),
                    symbol: bridge.grid.binding.symbol.clone(),
                    family: venue_domain::NativeOrderFamily::UmOrder,
                    side: desired.side,
                    position_side: match desired.key.position {
                        GridPosition::Long => PositionSide::Long,
                        GridPosition::Short => PositionSide::Short,
                    },
                    quantity: desired.quantity,
                    limit_price: Some(desired.price.value()),
                    time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                    created_at_ms: Some(1),
                    reduce_only: desired.reduce_only,
                    owner: Some(owner_for_order(&bridge.grid, desired)),
                    external: false,
                    state: Some(OrderState::New),
                    filled_quantity: Some(Decimal::ZERO),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert!(bridge.signed_desired_matches(&orders));

        let mut wrong_purpose = orders.clone();
        wrong_purpose[0].owner.as_mut().ok_or("owner")?.purpose = OrderPurpose::Protection;
        assert!(!bridge.signed_desired_matches(&wrong_purpose));

        let extra = orders[0].clone();
        orders.push(extra);
        assert!(!bridge.signed_desired_matches(&orders));
        Ok(())
    }

    #[test]
    fn route_checkpoint_accepts_only_the_legacy_empty_object_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let initial = initial()?;
        let bridge = GridBridgeState::bootstrap(initial.clone())?;
        let mut legacy: serde_json::Value = serde_json::from_slice(&bridge.checkpoint_bytes()?)?;
        legacy
            .as_object_mut()
            .ok_or("grid checkpoint object")?
            .insert("routes".to_owned(), serde_json::json!({}));
        let empty_legacy = serde_json::to_vec(&legacy)?;
        assert!(
            GridBridgeState::restore_or_bootstrap(
                Some(empty_legacy),
                initial.clone(),
                NodeGridRecoveryPolicy::RequireExisting,
            )
            .is_ok()
        );
        legacy
            .as_object_mut()
            .ok_or("grid checkpoint object")?
            .insert("routes".to_owned(), serde_json::json!({"old": {}}));
        let nonempty_legacy = serde_json::to_vec(&legacy)?;
        assert!(
            GridBridgeState::restore_or_bootstrap(
                Some(nonempty_legacy),
                initial,
                NodeGridRecoveryPolicy::RequireExisting,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn initial_install_preserves_closing_before_opening_and_refuses_low_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = initial()?;
        state.params.grid_count = 1;
        let mut bridge = GridBridgeState::bootstrap(state)?;
        bridge.mark_bootstrap_attempted()?;
        let inventory = GridInventory {
            private_generation: 2,
            private_observed_at_ms: 10,
            mark_price: Price::new(Decimal::new(100, 0))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        };
        let epoch = GridEpoch {
            epoch: 1,
            anchor_price: Price::new(Decimal::new(100, 0))?,
            step: Price::new(Decimal::ONE)?,
            grid_quantity: Decimal::new(5, 2),
            passive_book_fallback: None,
        };
        let plan = bridge.install_initial_epoch(inventory.clone(), epoch.clone())?;
        assert!(plan.commands.len() >= 4);
        let reduces = plan.commands.iter().take(2).all(
            |command| matches!(command, ExecutionCommand::PlaceLimit(order) if order.reduce_only),
        );
        assert!(reduces);
        let mut low = GridBridgeState::bootstrap(initial()?)?;
        low.mark_bootstrap_attempted()?;
        low.grid.params.grid_count = 1;
        let low_inventory = GridInventory {
            long_quantity: Decimal::ZERO,
            ..inventory
        };
        assert!(low.install_initial_epoch(low_inventory, epoch).is_err());
        Ok(())
    }

    fn bridge_with_accepted_order()
    -> Result<(GridBridgeState, GridOrderKey, GridOrderIntent, String), Box<dyn std::error::Error>>
    {
        let mut state = initial()?;
        state.params.grid_count = 2;
        let mut bridge = GridBridgeState::bootstrap(state)?;
        bridge.mark_bootstrap_attempted()?;
        let plan = bridge.install_initial_epoch(
            GridInventory {
                private_generation: 2,
                private_observed_at_ms: 10,
                mark_price: Price::new(Decimal::new(100, 0))?,
                long_quantity: Decimal::ONE,
                short_quantity: Decimal::ONE,
            },
            GridEpoch {
                epoch: 1,
                anchor_price: Price::new(Decimal::new(100, 0))?,
                step: Price::new(Decimal::ONE)?,
                grid_quantity: Decimal::new(5, 2),
                passive_book_fallback: None,
            },
        )?;
        let accepted = plan
            .accepted_routes
            .iter()
            .enumerate()
            .map(|(index, (_, _, command_id))| {
                (command_id.clone(), format!("native-grid-order-{index}"))
            })
            .collect::<Vec<_>>();
        bridge.bind_accepted_plan(&plan, &accepted)?;
        let (key, route) = bridge
            .routes
            .iter()
            .next()
            .map(|(key, route)| (key.clone(), route.clone()))
            .ok_or("accepted route")?;
        let source = bridge.require_owned(&key)?.clone();
        let native_order_id = route.accepted_venue_order_id.ok_or("native order id")?;
        Ok((bridge, key, source, native_order_id))
    }

    fn owned_fill(
        fill_id: &str,
        order_id: &str,
        source: &GridOrderIntent,
        quantity: Decimal,
        price: Price,
    ) -> Result<Fill, Box<dyn std::error::Error>> {
        Ok(Fill {
            fill_id: fill_id.to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: order_id.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            side: source.side,
            position_side: FieldState::Known(match source.key.position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            }),
            quantity,
            price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(100),
        })
    }

    #[test]
    fn partial_fills_accumulate_across_checkpoint_and_retire_the_completed_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut bridge, key, source, native_order_id) = bridge_with_accepted_order()?;
        let first_quantity = source
            .quantity
            .checked_div(Decimal::new(2, 0))
            .ok_or("first quantity")?;
        let remaining_quantity = source
            .quantity
            .checked_sub(first_quantity)
            .ok_or("remaining quantity")?;
        let first = owned_fill(
            "partial-fill-1",
            &native_order_id,
            &source,
            first_quantity,
            source.price,
        )?;
        assert_eq!(
            bridge.observe_persisted_fill(&first, 9)?,
            GridDecision::Noop
        );
        let checkpoint = bridge.checkpoint_bytes()?;
        let mut decoded: GridBridgeState = serde_json::from_slice(&checkpoint)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        assert_eq!(decoded, bridge);
        decoded.grid.migrate_checkpoint()?;
        decoded.validate()?;
        let mut bridge = GridBridgeState::restore_or_bootstrap(
            Some(checkpoint),
            bridge.grid.clone(),
            NodeGridRecoveryPolicy::RequireExisting,
        )?;
        assert_eq!(
            bridge.observe_persisted_fill(&first, 9)?,
            GridDecision::Noop
        );
        let conflicting_duplicate = owned_fill(
            "partial-fill-1",
            &native_order_id,
            &source,
            source.quantity,
            source.price,
        )?;
        assert!(
            bridge
                .observe_persisted_fill(&conflicting_duplicate, 9)
                .is_err()
        );
        let wrong_price = owned_fill(
            "partial-fill-2",
            &native_order_id,
            &source,
            remaining_quantity,
            Price::new(source.price.value() + Decimal::ONE)?,
        )?;
        assert!(bridge.observe_persisted_fill(&wrong_price, 9).is_err());
        let completion = owned_fill(
            "partial-fill-2",
            &native_order_id,
            &source,
            remaining_quantity,
            source.price,
        )?;
        let decision = bridge.observe_persisted_fill(&completion, 9)?;
        let GridDecision::Actions(actions) = decision else {
            return Err("completed maker fill did not produce a rolling action".into());
        };
        for action in &actions {
            bridge.plan_dispatch(action)?;
        }
        assert!(!bridge.routes.contains_key(&key));
        assert!(!bridge.partial_fills.contains_key(&native_order_id));
        bridge.checkpoint_bytes()?;
        Ok(())
    }
}
