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

/// The complete Grid checkpoint shape used by the production private-fill bridge. A native order
/// id alone is never enough: restart recovery requires the exact key/client/native triad, so it
/// cannot infer a grid level from price, side, or current BBO.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GridBridgeState {
    pub grid: HedgedGridState,
    routes: BTreeMap<GridOrderKey, GridOrderRoute>,
}

pub(crate) struct GridDispatchPlan {
    pub commands: Vec<ExecutionCommand>,
    accepted_routes: Vec<(GridOrderKey, CommandId, CommandId)>,
}

impl GridBridgeState {
    pub(crate) fn bootstrap(grid: HedgedGridState) -> Result<Self, NodeError> {
        let state = Self {
            grid,
            routes: BTreeMap::new(),
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
        serde_json::to_vec(self).map_err(|_| NodeError::ResidentArtifacts)
    }

    /// Signed convergence is based on the reducer's current desired set, never on an order count.
    /// Retired routes are deliberately ignored: a completed rolling cancellation is historical,
    /// while every current desired key must retain its exact client/native/order-shape triad.
    pub(crate) fn signed_desired_matches(&self, orders: &[SignedAccountOrderFact]) -> bool {
        self.grid.owned_orders.iter().all(|(key, desired)| {
            let Some(route) = self.routes.get(key) else {
                return false;
            };
            let Some(native) = route.accepted_venue_order_id.as_deref() else {
                return false;
            };
            orders.iter().any(|actual| {
                actual.client_order_id == route.client_order_id.as_str()
                    && actual.venue_order_id.as_deref() == Some(native)
                    && actual.symbol == self.grid.binding.symbol
                    && actual.side == desired.side
                    && actual.position_side
                        == match desired.key.position {
                            GridPosition::Long => PositionSide::Long,
                            GridPosition::Short => PositionSide::Short,
                        }
                    && actual.quantity == desired.quantity
                    && actual.limit_price == Some(desired.price.value())
                    && actual.reduce_only == desired.reduce_only
                    && matches!(
                        actual.state,
                        Some(OrderState::New | OrderState::PartiallyFilled)
                    )
            })
        })
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
            || fill.quantity > source.quantity
        {
            return Err(GridBridgeError::Evidence);
        }
        let decision = self
            .grid
            .observe_stream_owned_fill(OwnedGridFill {
                fill_id: fill.fill_id.clone(),
                private_generation,
                source_order: key,
                fill_price: fill.price,
                complete: fill.quantity == source.quantity,
                maker: fill.maker.clone(),
            })
            .map_err(GridBridgeError::Reducer)?;
        self.validate()?;
        Ok(decision)
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
        let mut seen_clients = BTreeMap::new();
        let mut seen_native = BTreeMap::new();
        for (key, route) in &self.routes {
            if key != &route.key
                || !self.grid.owned_orders.contains_key(key)
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
        Ok(())
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
    fn initial_install_preserves_closing_before_opening_and_refuses_low_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = initial()?;
        state.params.grid_count = 1;
        let mut bridge = GridBridgeState::bootstrap(state)?;
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
        low.grid.params.grid_count = 1;
        let low_inventory = GridInventory {
            long_quantity: Decimal::ZERO,
            ..inventory
        };
        assert!(low.install_initial_epoch(low_inventory, epoch).is_err());
        Ok(())
    }
}
