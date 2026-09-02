use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    CancelCommand, CommandId, ExecutionCommand, FieldState, Fill, OrderCommand, OrderOwner,
    OrderPurpose, OrderState, PositionSide,
};
use venue_runtime::SignedAccountOrderFact;
use venue_strategies::hedged_grid::{
    GridAction, GridDecision, GridEpoch, GridInventory, GridOrderIntent, GridOrderKey, GridPhase,
    GridPosition, GridTransaction, HedgedGridError, HedgedGridState, OwnedGridFill,
};

use crate::{NodeError, runtime_config::NodeGridRecoveryPolicy};

const MAX_GRID_CHECKPOINT_BYTES: usize = 1_048_576;
const MAX_PARTIAL_FILL_SLICES_PER_ORDER: usize = 256;

pub(super) fn minimum_grid_quantity(
    order_notional: rust_decimal::Decimal,
    anchor: rust_decimal::Decimal,
    step: rust_decimal::Decimal,
    grid_count: u8,
    quantity_step: rust_decimal::Decimal,
) -> Result<rust_decimal::Decimal, NodeError> {
    let outer = step
        .checked_mul(rust_decimal::Decimal::from(grid_count))
        .ok_or(NodeError::ResidentRuntime)?;
    let minimum_open = anchor
        .checked_sub(outer)
        .filter(|value| *value > rust_decimal::Decimal::ZERO)
        .ok_or(NodeError::ResidentRuntime)?;
    order_notional
        .checked_div(minimum_open)
        .and_then(|raw| raw.checked_div(quantity_step))
        .map(|raw_steps| raw_steps.ceil())
        .and_then(|steps| steps.checked_mul(quantity_step))
        .filter(|quantity| *quantity > rust_decimal::Decimal::ZERO)
        .ok_or(NodeError::ResidentRuntime)
}

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GridReconciliationAttempt {
    key: GridOrderKey,
    attempt: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GridStartupReconciliation {
    operation_sequence: u64,
    attempts: Vec<GridReconciliationAttempt>,
    #[serde(default)]
    rebuild_attempted: bool,
}

const TERMINAL_REBUILD_REARM_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SignedGridFillApplication {
    Apply,
    ExactDuplicate,
    Irrelevant,
}

/// The complete Grid checkpoint shape used by the production private-fill bridge. A native order
/// id alone is never enough: restart recovery requires the exact key/client/native triad, so it
/// cannot infer a grid level from price, side, or current BBO.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GridBridgeState {
    pub grid: HedgedGridState,
    /// Adapter-neutral execution precision sealed with the installed epoch. Rolling must use the
    /// same minimum-notional quantity before checkpoint/WAL identities are derived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_profile: Option<GridExecutionProfile>,
    #[serde(default)]
    bootstrap_state: GridBootstrapState,
    /// A completed reset may deliberately leave an uninstalled shape. It is distinct from the
    /// first epoch and receives one separately durable, signed-empty-surface rebuild attempt.
    #[serde(default)]
    reset_rebuild_attempted: bool,
    /// Versioned only to let a checkpoint that failed before the physical boundary under an older
    /// validation implementation take one explicitly confirmed repair retry per repair version.
    #[serde(default)]
    reset_rebuild_attempt_version: u16,
    #[serde(with = "grid_routes")]
    routes: BTreeMap<GridOrderKey, GridOrderRoute>,
    #[serde(default)]
    partial_fills: BTreeMap<String, GridPartialFill>,
    #[serde(default)]
    reconciliation_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    startup_reconciliation: Option<GridStartupReconciliation>,
    /// Checkpoint schema marker for terminally incomplete place-batch recovery. The one-shot
    /// rebuild budget belongs to each `startup_reconciliation` episode, not to this account-wide
    /// compatibility marker.
    #[serde(default)]
    terminal_rebuild_rearm_version: u16,
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
    transaction_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GridExecutionProfile {
    #[serde(with = "rust_decimal::serde::str")]
    quantity_step: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    minimum_quantity: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    maximum_quantity: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    minimum_notional: rust_decimal::Decimal,
}

impl GridExecutionProfile {
    pub(crate) fn new(
        quantity_step: rust_decimal::Decimal,
        minimum_quantity: rust_decimal::Decimal,
        maximum_quantity: rust_decimal::Decimal,
        minimum_notional: rust_decimal::Decimal,
    ) -> Result<Self, GridBridgeError> {
        let profile = Self {
            quantity_step,
            minimum_quantity,
            maximum_quantity,
            minimum_notional,
        };
        profile.validate()?;
        Ok(profile)
    }

    fn normalize_quantity(
        &self,
        quantity: rust_decimal::Decimal,
        price: rust_decimal::Decimal,
    ) -> Result<rust_decimal::Decimal, GridBridgeError> {
        if quantity < self.minimum_quantity
            || quantity > self.maximum_quantity
            || quantity % self.quantity_step != rust_decimal::Decimal::ZERO
            || price <= rust_decimal::Decimal::ZERO
        {
            return Err(GridBridgeError::ExecutionProfile);
        }
        if quantity
            .checked_mul(price)
            .is_some_and(|notional| notional >= self.minimum_notional)
        {
            return Ok(quantity);
        }
        quantity
            .checked_add(self.quantity_step)
            .filter(|value| {
                *value <= self.maximum_quantity
                    && *value % self.quantity_step == rust_decimal::Decimal::ZERO
                    && value
                        .checked_mul(price)
                        .is_some_and(|notional| notional >= self.minimum_notional)
            })
            .ok_or(GridBridgeError::ExecutionProfile)
    }

    fn validate(&self) -> Result<(), GridBridgeError> {
        if self.quantity_step <= rust_decimal::Decimal::ZERO
            || self.minimum_quantity <= rust_decimal::Decimal::ZERO
            || self.maximum_quantity < self.minimum_quantity
            || self.minimum_notional <= rust_decimal::Decimal::ZERO
            || self.minimum_quantity % self.quantity_step != rust_decimal::Decimal::ZERO
        {
            return Err(GridBridgeError::ExecutionProfile);
        }
        Ok(())
    }
}

impl GridBridgeState {
    pub(crate) fn bootstrap(grid: HedgedGridState) -> Result<Self, NodeError> {
        let state = Self {
            grid,
            execution_profile: None,
            bootstrap_state: GridBootstrapState::Eligible,
            reset_rebuild_attempted: false,
            reset_rebuild_attempt_version: 0,
            routes: BTreeMap::new(),
            partial_fills: BTreeMap::new(),
            reconciliation_sequence: 0,
            startup_reconciliation: None,
            terminal_rebuild_rearm_version: 0,
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
            && (!self.reset_rebuild_attempted || self.reset_rebuild_attempt_version < 3)
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

    pub(crate) fn needs_reconciliation_rebuild(&self) -> bool {
        matches!(
            self.bootstrap_state,
            GridBootstrapState::Attempted | GridBootstrapState::Confirmed
        ) && self.grid.phase == GridPhase::ResettingGrid
            && self.grid.epoch.is_some()
            && self.grid.owned_orders.is_empty()
            && self.grid.pending_transactions.is_empty()
            && self.grid.pending_replenishments.is_empty()
            && self.routes.is_empty()
            && self.partial_fills.is_empty()
            && self
                .startup_reconciliation
                .as_ref()
                .is_some_and(|episode| !episode.rebuild_attempted)
    }

    pub(crate) fn has_startup_reconciliation(&self) -> bool {
        self.startup_reconciliation.is_some()
    }

    pub(crate) fn has_unconfirmed_install_surface(&self) -> bool {
        matches!(
            self.bootstrap_state,
            GridBootstrapState::Attempted | GridBootstrapState::Confirmed
        ) && self.grid.phase == GridPhase::Running
            && self.grid.epoch.is_some()
            && self.grid.pending_transactions.is_empty()
            && self.grid.pending_replenishments.is_empty()
            && self.routes.len() == self.grid.owned_orders.len()
            && self
                .grid
                .owned_orders
                .keys()
                .all(|key| self.routes.contains_key(key))
            && (self.bootstrap_state == GridBootstrapState::Attempted
                || self.startup_reconciliation.is_some())
    }

    /// Reconstructs the exact place-only batch whose semantic checkpoint was durable before
    /// dispatch. Host WAL bytes, rather than order count or signed presence alone, classify every
    /// child after a crash.
    pub(crate) fn unconfirmed_install_plan(&self) -> Result<GridDispatchPlan, GridBridgeError> {
        if !self.has_unconfirmed_install_surface() {
            return Err(GridBridgeError::Evidence);
        }
        let mut orders = self.grid.owned_orders.values().cloned().collect::<Vec<_>>();
        orders.sort_by_key(|order| (!order.reduce_only, order.key.clone()));
        let mut commands = Vec::with_capacity(orders.len());
        let mut accepted_routes = Vec::with_capacity(orders.len());
        for order in orders {
            let client = stable_identifier(b"client", &self.grid.binding, &order.key)?;
            let route = self
                .routes
                .get(&order.key)
                .ok_or(GridBridgeError::UnknownOrder)?;
            if route.client_order_id != client {
                return Err(GridBridgeError::RouteConflict);
            }
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
            transaction_id: None,
        })
    }

    pub(crate) fn bind_accepted_install_routes(
        &mut self,
        plan: &GridDispatchPlan,
        accepted: &[(CommandId, String)],
    ) -> Result<(), GridBridgeError> {
        if !self.has_unconfirmed_install_surface() || plan.transaction_id.is_some() {
            return Err(GridBridgeError::Evidence);
        }
        for (key, _, command_id) in &plan.accepted_routes {
            let route = self.routes.get(key).ok_or(GridBridgeError::UnknownOrder)?;
            if let Some(existing) = route.accepted_venue_order_id.as_deref()
                && accepted.iter().find_map(|(accepted_id, native)| {
                    (accepted_id == command_id).then_some(native.as_str())
                }) != Some(existing)
            {
                return Err(GridBridgeError::Evidence);
            }
        }
        for (key, client, command_id) in &plan.accepted_routes {
            let Some(native) = accepted
                .iter()
                .find_map(|(accepted_id, native)| (accepted_id == command_id).then_some(native))
            else {
                continue;
            };
            let route = self.routes.get(key).ok_or(GridBridgeError::UnknownOrder)?;
            if route.client_order_id != *client {
                return Err(GridBridgeError::RouteConflict);
            }
            match route.accepted_venue_order_id.as_deref() {
                Some(existing) if existing == native => {}
                Some(_) => return Err(GridBridgeError::RouteConflict),
                None => self.bind_accepted_native(key, client, native.clone())?,
            }
        }
        self.validate()
    }

    /// Converts a crashed place-only install into the same durable cancellation drain used by a
    /// normal startup. WAL-Accepted routes remain owned; every absent or Rejected child is removed
    /// before the fresh signed surface is compared.
    pub(crate) fn begin_unconfirmed_install_reconciliation(
        &mut self,
    ) -> Result<(), GridBridgeError> {
        if !self.has_unconfirmed_install_surface()
            || self
                .startup_reconciliation
                .as_ref()
                .is_some_and(|episode| !episode.attempts.is_empty())
        {
            return Err(GridBridgeError::Evidence);
        }
        let absent = self
            .routes
            .iter()
            .filter_map(|(key, route)| route.accepted_venue_order_id.is_none().then(|| key.clone()))
            .collect::<Vec<_>>();
        for key in absent {
            self.routes.remove(&key);
            self.grid
                .owned_orders
                .remove(&key)
                .ok_or(GridBridgeError::UnknownOrder)?;
        }
        if !matches!(
            self.grid
                .request_reset(venue_strategies::hedged_grid::GridResetReason::Reconciliation)
                .map_err(GridBridgeError::Reducer)?,
            GridDecision::Actions(actions)
                if actions.iter().all(|action| matches!(action, GridAction::Reset { .. }))
        ) {
            return Err(GridBridgeError::Evidence);
        }
        if self.startup_reconciliation.is_some() {
            let completed_install_attempt = self
                .startup_reconciliation
                .as_ref()
                .is_some_and(|episode| episode.rebuild_attempted && episode.attempts.is_empty());
            if !completed_install_attempt {
                return Err(GridBridgeError::Evidence);
            }
            self.startup_reconciliation = None;
        }
        self.start_reconciliation_episode()?;
        // The episode itself owns the one-shot rebuild bit. A later partial install has a new
        // operation sequence and must be able to drain and rebuild once more; the schema marker
        // is not an account-lifetime attempt budget.
        self.terminal_rebuild_rearm_version = TERMINAL_REBUILD_REARM_VERSION;
        if self.grid.owned_orders.is_empty() {
            self.grid
                .reset_orders_settled()
                .map_err(GridBridgeError::Reducer)?;
        }
        self.validate()
    }

    /// A resident can finish draining a terminally Rejected partial rebuild while retaining that
    /// episode's consumed rebuild bit. Only an exactly empty reducer surface with no cancel
    /// attempt in flight may advance to a fresh episode; the caller separately proves the signed
    /// venue surface empty and Host WAL free of unresolved outcomes before persisting this repair.
    pub(crate) fn rearm_terminally_drained_rebuild(&mut self) -> Result<bool, GridBridgeError> {
        let stranded = matches!(
            self.bootstrap_state,
            GridBootstrapState::Attempted | GridBootstrapState::Confirmed
        ) && self.grid.phase == GridPhase::ResettingGrid
            && self.grid.epoch.is_some()
            && self.grid.owned_orders.is_empty()
            && self.grid.pending_transactions.is_empty()
            && self.grid.pending_replenishments.is_empty()
            && self.routes.is_empty()
            && self.partial_fills.is_empty()
            && self
                .startup_reconciliation
                .as_ref()
                .is_some_and(|episode| episode.rebuild_attempted && episode.attempts.is_empty());
        if !stranded {
            return Ok(false);
        }
        self.startup_reconciliation = None;
        self.start_reconciliation_episode()?;
        self.terminal_rebuild_rearm_version = TERMINAL_REBUILD_REARM_VERSION;
        self.validate()?;
        Ok(true)
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

    /// Binds only WAL-Accepted children from a crashed rolling batch. The caller has already
    /// rejected Prepared/Submitted/Unknown states and checked the exact command bytes. Accepted
    /// replacements remain owned solely so the startup reconciliation lane can signedly cancel
    /// them; no old Place or Cancel is dispatched again.
    pub(crate) fn bind_accepted_pending_routes(
        &mut self,
        accepted: &[(CommandId, String)],
    ) -> Result<(), GridBridgeError> {
        if accepted.is_empty() || self.grid.pending_transactions.is_empty() {
            return Err(GridBridgeError::Evidence);
        }
        let expected = self
            .pending_transaction_command_ids()?
            .into_iter()
            .flat_map(|(_, command_ids)| command_ids)
            .collect::<BTreeSet<_>>();
        if accepted.iter().any(|(command_id, venue_order_id)| {
            !expected.contains(command_id) || venue_order_id.trim().is_empty()
        }) || accepted
            .iter()
            .map(|(command_id, _)| command_id)
            .collect::<BTreeSet<_>>()
            .len()
            != accepted.len()
        {
            return Err(GridBridgeError::Evidence);
        }
        let transactions = self
            .grid
            .pending_transactions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for transaction in transactions {
            for replacement in transaction.places {
                let command_id = stable_identifier(b"place", &self.grid.binding, &replacement.key)?;
                let Some(native) = accepted.iter().find_map(|(accepted_id, native)| {
                    (accepted_id == &command_id).then_some(native)
                }) else {
                    continue;
                };
                let client = stable_identifier(b"client", &self.grid.binding, &replacement.key)?;
                self.bind_accepted_native(&replacement.key, &client, native.clone())?;
            }
        }
        self.validate()
    }

    /// Rolls back only locally projected replacements whose complete command families are proven
    /// terminal in Host WAL. The consumed fills remain durable; exact live cancellation targets
    /// are restored, while WAL-Accepted replacements stay owned only for signed cancellation.
    pub(crate) fn abandon_pending_for_reconciliation(
        &mut self,
        transaction_ids: &[String],
    ) -> Result<(), GridBridgeError> {
        let transactions = self
            .grid
            .pending_transactions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut accepted_replacements = Vec::new();
        for transaction in &transactions {
            for replacement in &transaction.places {
                let route = self
                    .routes
                    .get(&replacement.key)
                    .ok_or(GridBridgeError::UnknownOrder)?;
                if route.accepted_venue_order_id.is_some() {
                    accepted_replacements.push((replacement.clone(), route.clone()));
                }
            }
        }
        if !matches!(
            self.grid
                .abandon_unsubmitted_transactions_for_reconciliation(transaction_ids)
                .map_err(GridBridgeError::Reducer)?,
            GridDecision::Actions(actions)
                if actions.iter().all(|action| matches!(action, GridAction::Reset { .. }))
        ) {
            return Err(GridBridgeError::Evidence);
        }
        for transaction in transactions {
            for replacement in transaction.places {
                self.routes.remove(&replacement.key);
            }
        }
        for (replacement, route) in accepted_replacements {
            self.grid
                .owned_orders
                .insert(replacement.key.clone(), replacement);
            self.routes.insert(route.key.clone(), route);
        }
        self.start_reconciliation_episode()?;
        self.validate()
    }

    pub(crate) fn begin_startup_reconciliation(&mut self) -> Result<(), GridBridgeError> {
        if self.bootstrap_state != GridBootstrapState::Confirmed
            || self.grid.phase != GridPhase::Running
            || !self.grid.pending_transactions.is_empty()
            || self.grid.owned_orders.is_empty()
            || self.routes.is_empty()
        {
            return Err(GridBridgeError::Evidence);
        }
        if !matches!(
            self.grid
                .request_reset(venue_strategies::hedged_grid::GridResetReason::Reconciliation)
                .map_err(GridBridgeError::Reducer)?,
            GridDecision::Actions(actions)
                if actions.iter().all(|action| matches!(action, GridAction::Reset { .. }))
        ) {
            return Err(GridBridgeError::Evidence);
        }
        self.start_reconciliation_episode()?;
        self.validate()
    }

    fn start_reconciliation_episode(&mut self) -> Result<(), GridBridgeError> {
        if self.startup_reconciliation.is_some()
            || self.grid.phase != GridPhase::ResettingGrid
            || !self.grid.pending_transactions.is_empty()
        {
            return Err(GridBridgeError::Evidence);
        }
        self.reconciliation_sequence = self
            .reconciliation_sequence
            .checked_add(1)
            .ok_or(GridBridgeError::Evidence)?;
        self.startup_reconciliation = Some(GridStartupReconciliation {
            operation_sequence: self.reconciliation_sequence,
            attempts: Vec::new(),
            rebuild_attempted: false,
        });
        Ok(())
    }

    pub(crate) fn reconciliation_target(&self) -> Result<Option<GridOrderKey>, GridBridgeError> {
        if self.startup_reconciliation.is_none() || self.grid.phase != GridPhase::ResettingGrid {
            return Err(GridBridgeError::Evidence);
        }
        Ok(self.grid.owned_orders.keys().next().cloned())
    }

    pub(crate) fn reconciliation_attempt(
        &self,
        key: &GridOrderKey,
    ) -> Result<Option<u16>, GridBridgeError> {
        let reconciliation = self
            .startup_reconciliation
            .as_ref()
            .ok_or(GridBridgeError::Evidence)?;
        Ok(reconciliation
            .attempts
            .iter()
            .find(|attempt| &attempt.key == key)
            .map(|attempt| attempt.attempt))
    }

    pub(crate) fn advance_reconciliation_attempt(
        &mut self,
        key: &GridOrderKey,
    ) -> Result<u16, GridBridgeError> {
        if !self.grid.owned_orders.contains_key(key) {
            return Err(GridBridgeError::UnknownOrder);
        }
        let reconciliation = self
            .startup_reconciliation
            .as_mut()
            .ok_or(GridBridgeError::Evidence)?;
        let attempt = if let Some(current) = reconciliation
            .attempts
            .iter_mut()
            .find(|attempt| &attempt.key == key)
        {
            if current.attempt >= 3 {
                return Err(GridBridgeError::Evidence);
            }
            current.attempt = current
                .attempt
                .checked_add(1)
                .ok_or(GridBridgeError::Evidence)?;
            current.attempt
        } else {
            reconciliation.attempts.push(GridReconciliationAttempt {
                key: key.clone(),
                attempt: 1,
            });
            reconciliation
                .attempts
                .sort_by(|left, right| left.key.cmp(&right.key));
            1
        };
        self.validate()?;
        Ok(attempt)
    }

    pub(crate) fn reconciliation_cancel_plan(
        &self,
        key: &GridOrderKey,
        attempt: u16,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        if !matches!(
            self.bootstrap_state,
            GridBootstrapState::Attempted | GridBootstrapState::Confirmed
        ) || self.grid.phase != GridPhase::ResettingGrid
            || !self.grid.pending_transactions.is_empty()
            || self.reconciliation_attempt(key)? != Some(attempt)
        {
            return Err(GridBridgeError::Evidence);
        }
        let order = self.require_owned(key)?;
        let route = self.routes.get(key).ok_or(GridBridgeError::UnknownOrder)?;
        if route.accepted_venue_order_id.is_none() {
            return Err(GridBridgeError::Evidence);
        }
        Ok(GridDispatchPlan {
            commands: vec![ExecutionCommand::Cancel(CancelCommand {
                command_id: reconciliation_cancel_identifier(
                    &self.grid.binding,
                    self.startup_reconciliation
                        .as_ref()
                        .ok_or(GridBridgeError::Evidence)?
                        .operation_sequence,
                    attempt,
                    key,
                )?,
                owner: owner_for_order(&self.grid, order),
                target_client_order_id: route.client_order_id.clone(),
            })],
            accepted_routes: Vec::new(),
            transaction_id: None,
        })
    }

    pub(crate) fn settle_reconciliation_cancel(
        &mut self,
        key: &GridOrderKey,
    ) -> Result<(), GridBridgeError> {
        if self.grid.phase != GridPhase::ResettingGrid
            || !self.grid.pending_transactions.is_empty()
            || self.startup_reconciliation.is_none()
        {
            return Err(GridBridgeError::Evidence);
        }
        let route = self
            .routes
            .remove(key)
            .ok_or(GridBridgeError::UnknownOrder)?;
        if let Some(native) = route.accepted_venue_order_id {
            self.partial_fills.remove(&native);
        }
        self.grid
            .owned_orders
            .remove(key)
            .ok_or(GridBridgeError::UnknownOrder)?;
        let reconciliation = self
            .startup_reconciliation
            .as_mut()
            .ok_or(GridBridgeError::Evidence)?;
        reconciliation
            .attempts
            .retain(|attempt| &attempt.key != key);
        if self.grid.owned_orders.is_empty() {
            self.grid
                .reset_orders_settled()
                .map_err(GridBridgeError::Reducer)?;
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
        self.reset_rebuild_attempt_version = 3;
        self.validate()
    }

    #[cfg(test)]
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

    pub(crate) fn confirm_installed_surface(&mut self) -> Result<(), GridBridgeError> {
        match self.bootstrap_state {
            GridBootstrapState::Attempted | GridBootstrapState::Confirmed
                if self.grid.phase == GridPhase::Running
                    && self.grid.epoch.is_some()
                    && !self.routes.is_empty()
                    && self
                        .routes
                        .values()
                        .all(|route| route.accepted_venue_order_id.is_some()) =>
            {
                self.validate()?;
                self.bootstrap_state = GridBootstrapState::Confirmed;
                self.startup_reconciliation = None;
                self.validate()
            }
            GridBootstrapState::Eligible
            | GridBootstrapState::Attempted
            | GridBootstrapState::Confirmed => Err(GridBridgeError::BootstrapState),
        }
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
        self.signed_orders_match(orders, &self.grid.owned_orders)
    }

    /// During a startup reset the signed venue surface may be a strict subset of the stopped
    /// writer's checkpoint because orders can fill while no private consumer is resident.  Exact
    /// route, owner and immutable order shape still prove every surviving child; only signed
    /// absence is allowed to retire a missing child.  An extra or shape-changed order remains a
    /// hard failure and no physical command is inferred from the snapshot.
    pub(crate) fn settle_signed_absent_reconciliation_orders(
        &mut self,
        orders: &[SignedAccountOrderFact],
    ) -> Result<usize, GridBridgeError> {
        if self.startup_reconciliation.is_none()
            || self.grid.phase != GridPhase::ResettingGrid
            || !self.grid.pending_transactions.is_empty()
        {
            return Err(GridBridgeError::Evidence);
        }
        let mut present = BTreeSet::new();
        for actual in orders {
            let Some(key) = self.grid.owned_orders.iter().find_map(|(key, desired)| {
                self.signed_order_matches(actual, key, desired)
                    .then_some(key.clone())
            }) else {
                return Err(GridBridgeError::Evidence);
            };
            if !present.insert(key) {
                return Err(GridBridgeError::Evidence);
            }
        }
        let absent = self
            .grid
            .owned_orders
            .keys()
            .filter(|key| !present.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in &absent {
            self.settle_reconciliation_cancel(key)?;
        }
        // A burst of signed fills can retire the reducer's last owned orders before previously
        // Accepted startup cancels become visible as absent. Those route projections no longer
        // have a reducer source key, so they cannot be settled by the loop above. They may be
        // discarded only after the same complete signed snapshot proves the whole surface empty.
        let orphaned = if self.grid.owned_orders.is_empty() && orders.is_empty() {
            self.routes
                .keys()
                .filter(|key| !self.grid.owned_orders.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for key in &orphaned {
            self.routes
                .remove(key)
                .ok_or(GridBridgeError::UnknownOrder)?;
        }
        self.validate()?;
        absent
            .len()
            .checked_add(orphaned.len())
            .ok_or(GridBridgeError::Evidence)
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

    /// Returns the checkpoint-derived exchange surface that must exist immediately before the
    /// next reserved rolling batch. Accepted current orders remain live, each pending
    /// transaction contributes its original cancellation target, and its unsubmitted
    /// replacements are deliberately excluded.
    pub(crate) fn expected_pending_signed_surface(
        &self,
    ) -> Result<BTreeMap<CommandId, OrderOwner>, GridBridgeError> {
        self.pending_pre_dispatch_orders()?
            .iter()
            .map(|(key, desired)| {
                let route = self.routes.get(key).ok_or(GridBridgeError::UnknownOrder)?;
                if route.accepted_venue_order_id.is_none() {
                    return Err(GridBridgeError::Evidence);
                }
                Ok((
                    route.client_order_id.clone(),
                    owner_for_order(&self.grid, desired),
                ))
            })
            .collect()
    }

    /// Proves the complete pre-dispatch order shape against the durable checkpoint. A caller may
    /// not derive its expectation from the same signed snapshot it is trying to authorize.
    pub(crate) fn signed_pending_surface_matches(&self, orders: &[SignedAccountOrderFact]) -> bool {
        self.pending_pre_dispatch_orders()
            .is_ok_and(|expected| self.signed_orders_match(orders, &expected))
    }

    fn pending_pre_dispatch_orders(
        &self,
    ) -> Result<BTreeMap<GridOrderKey, GridOrderIntent>, GridBridgeError> {
        self.validate()?;
        let mut expected = BTreeMap::new();
        for (key, desired) in &self.grid.owned_orders {
            let route = self.routes.get(key).ok_or(GridBridgeError::UnknownOrder)?;
            if route.accepted_venue_order_id.is_some() {
                expected.insert(key.clone(), desired.clone());
                continue;
            }
            let reserved_replacement = self
                .grid
                .pending_transactions
                .values()
                .any(|transaction| transaction.places.iter().any(|order| &order.key == key));
            if !reserved_replacement {
                return Err(GridBridgeError::Evidence);
            }
        }
        for transaction in self.grid.pending_transactions.values() {
            let cancelled = transaction
                .cancelled_order
                .as_ref()
                .ok_or(GridBridgeError::Evidence)?;
            if cancelled.key != transaction.cancel
                || self
                    .routes
                    .get(&transaction.cancel)
                    .and_then(|route| route.accepted_venue_order_id.as_ref())
                    .is_none()
                || expected
                    .insert(transaction.cancel.clone(), cancelled.clone())
                    .is_some()
            {
                return Err(GridBridgeError::Evidence);
            }
            for replacement in &transaction.places {
                let route = self
                    .routes
                    .get(&replacement.key)
                    .ok_or(GridBridgeError::UnknownOrder)?;
                if route.accepted_venue_order_id.is_some() {
                    return Err(GridBridgeError::Evidence);
                }
            }
        }
        Ok(expected)
    }

    fn signed_orders_match(
        &self,
        orders: &[SignedAccountOrderFact],
        expected: &BTreeMap<GridOrderKey, GridOrderIntent>,
    ) -> bool {
        orders.len() == expected.len()
            && expected.iter().all(|(key, desired)| {
                orders
                    .iter()
                    .any(|actual| self.signed_order_matches(actual, key, desired))
            })
    }

    fn signed_order_matches(
        &self,
        actual: &SignedAccountOrderFact,
        key: &GridOrderKey,
        desired: &GridOrderIntent,
    ) -> bool {
        let Some(route) = self.routes.get(key) else {
            return false;
        };
        let Some(native) = route.accepted_venue_order_id.as_deref() else {
            return false;
        };
        let expected_filled_quantity = self
            .partial_fills
            .get(native)
            .map(|partial| partial.cumulative_quantity)
            .unwrap_or_default();
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
            && actual.filled_quantity == Some(expected_filled_quantity)
            && matches!(
                actual.state,
                Some(OrderState::New | OrderState::PartiallyFilled)
            )
    }

    /// Classifies a signed overlap before facts append. Exact partial duplicates are checked
    /// against their immutable slice and source order; a new execution of a pending cancellation
    /// target is a conflict because the reserved transaction can no longer be replayed unchanged.
    pub(crate) fn signed_fill_application(
        &self,
        fill: &Fill,
    ) -> Result<SignedGridFillApplication, GridBridgeError> {
        fill.validate().map_err(|_| GridBridgeError::Evidence)?;
        if let Some(record) = self.grid.owned_fill_records.get(&fill.fill_id) {
            if !self.signed_fill_matches_order(fill, &record.source_order)
                || record.maker.is_some_and(|expected| {
                    !matches!(fill.maker, FieldState::Known(actual) if actual == expected)
                })
            {
                return Err(GridBridgeError::Evidence);
            }
            return Ok(SignedGridFillApplication::ExactDuplicate);
        }
        let Some((key, _route)) = self.routes.iter().find(|(_, route)| {
            route.accepted_venue_order_id.as_deref() == Some(fill.order_id.as_str())
        }) else {
            return Ok(SignedGridFillApplication::Irrelevant);
        };
        let pending_cancelled_order = self
            .grid
            .pending_transactions
            .values()
            .find(|transaction| &transaction.cancel == key)
            .and_then(|transaction| transaction.cancelled_order.as_ref());
        let source = self.grid.owned_orders.get(key).or(pending_cancelled_order);
        let Some(source) = source else {
            return Err(GridBridgeError::Evidence);
        };
        if !self.signed_fill_matches_order(fill, source) {
            return Err(GridBridgeError::Evidence);
        }
        if let Some(slice) = self
            .partial_fills
            .get(&fill.order_id)
            .and_then(|partial| partial.fills.get(&fill.fill_id))
        {
            return if slice.quantity == fill.quantity && slice.maker == fill.maker {
                Ok(SignedGridFillApplication::ExactDuplicate)
            } else {
                Err(GridBridgeError::Evidence)
            };
        }
        if pending_cancelled_order.is_some() {
            return Err(GridBridgeError::Evidence);
        }
        Ok(SignedGridFillApplication::Apply)
    }

    /// Classifies a replay for an already retired native order. The caller must obtain
    /// `accepted_command` from Host/WAL by this fill's native order id; price or side alone are
    /// never enough to recover ownership after the live route has been retired.
    pub(crate) fn signed_retired_fill_application(
        &self,
        fill: &Fill,
        accepted_command: &ExecutionCommand,
    ) -> Result<SignedGridFillApplication, GridBridgeError> {
        fill.validate().map_err(|_| GridBridgeError::Evidence)?;
        let mut matched = None;
        for record in self.grid.owned_fill_records.values() {
            let source = &record.source_order;
            let expected = self.place_command_for_order(source)?;
            if &expected != accepted_command {
                continue;
            }
            if matched.replace(record).is_some() {
                return Err(GridBridgeError::Evidence);
            }
        }
        let Some(record) = matched else {
            return Ok(SignedGridFillApplication::Irrelevant);
        };
        if record.maker != Some(true)
            || !record.grid_action_emitted
            || !matches!(fill.maker, FieldState::Known(true))
            || !self.signed_fill_matches_order(fill, &record.source_order)
        {
            return Err(GridBridgeError::Evidence);
        }
        Ok(SignedGridFillApplication::ExactDuplicate)
    }

    fn place_command_for_order(
        &self,
        source: &GridOrderIntent,
    ) -> Result<ExecutionCommand, GridBridgeError> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: Default::default(),
            command_id: stable_identifier(b"place", &self.grid.binding, &source.key)?,
            client_order_id: stable_identifier(b"client", &self.grid.binding, &source.key)?,
            owner: owner_for_order(&self.grid, source),
            side: source.side,
            position_side: match source.key.position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            },
            quantity: source.quantity,
            limit_price: source.price,
            reduce_only: source.reduce_only,
        }))
    }

    fn signed_fill_matches_order(&self, fill: &Fill, source: &GridOrderIntent) -> bool {
        let position = match source.key.position {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        };
        fill.symbol == self.grid.binding.symbol
            && fill.side == source.side
            && matches!(fill.position_side, FieldState::Known(actual) if actual == position)
            && fill.price == source.price
            && fill.quantity <= source.quantity
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
        match self.signed_fill_application(fill)? {
            SignedGridFillApplication::ExactDuplicate => return Ok(GridDecision::Noop),
            SignedGridFillApplication::Irrelevant => {
                return Err(GridBridgeError::UnknownOrder);
            }
            SignedGridFillApplication::Apply => {}
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
        let owned_fill = OwnedGridFill {
            fill_id: fill.fill_id.clone(),
            private_generation,
            source_order: key.clone(),
            fill_price: source.price,
            complete: true,
            maker: aggregate_maker(&completed.fills),
        };
        let decision = match if self.startup_reconciliation.is_some() {
            self.grid
                .retire_owned_fill_during_reset(owned_fill)
                .map(|()| GridDecision::Noop)
        } else {
            self.grid.observe_stream_owned_fill(owned_fill)
        } {
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
        if let Some(reconciliation) = self.startup_reconciliation.as_mut() {
            reconciliation.attempts.retain(|attempt| attempt.key != key);
        }
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
        self.normalize_pending_transaction(transaction)?;
        let transaction = self
            .grid
            .pending_transactions
            .get(&transaction.id)
            .cloned()
            .ok_or(GridBridgeError::Evidence)?;
        self.plan_transaction(&transaction)
    }

    pub(crate) fn set_execution_profile(
        &mut self,
        profile: GridExecutionProfile,
    ) -> Result<(), GridBridgeError> {
        profile.validate()?;
        if !self.grid.pending_transactions.is_empty() {
            return Err(GridBridgeError::ExecutionProfile);
        }
        self.execution_profile = Some(profile);
        self.validate()
    }

    fn normalize_pending_transaction(
        &mut self,
        expected: &GridTransaction,
    ) -> Result<(), GridBridgeError> {
        let profile = self
            .execution_profile
            .clone()
            .ok_or(GridBridgeError::ExecutionProfile)?;
        let transaction = self
            .grid
            .pending_transactions
            .get(&expected.id)
            .cloned()
            .filter(|transaction| transaction == expected)
            .ok_or(GridBridgeError::Evidence)?;
        let mut normalized = transaction.clone();
        for order in &mut normalized.places {
            order.quantity = profile.normalize_quantity(order.quantity, order.price.value())?;
        }
        if normalized == transaction {
            return Ok(());
        }
        for order in &normalized.places {
            let owned = self
                .grid
                .owned_orders
                .get_mut(&order.key)
                .ok_or(GridBridgeError::UnknownOrder)?;
            if owned.key != order.key || owned.price != order.price {
                return Err(GridBridgeError::Evidence);
            }
            owned.quantity = order.quantity;
        }
        self.grid
            .pending_transactions
            .insert(normalized.id.clone(), normalized);
        self.validate()
    }

    /// Rebuilds only the command bytes for reducer transactions already present in the verified
    /// checkpoint. The caller may use these plans only after Host proves every deterministic
    /// command id is absent from WAL and the current signed surface still contains every exact
    /// cancellation target. No new route or transaction identity is allocated here.
    pub(crate) fn pending_dispatch_plans(&self) -> Result<Vec<GridDispatchPlan>, GridBridgeError> {
        self.validate()?;
        self.grid
            .pending_transactions
            .values()
            .map(|transaction| self.transaction_plan_from_routes(transaction))
            .collect()
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

    pub(crate) fn install_rebuilt_epoch(
        &mut self,
        inventory: GridInventory,
        epoch: GridEpoch,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        if !self.needs_reconciliation_rebuild()
            || self
                .grid
                .epoch
                .as_ref()
                .is_none_or(|current| epoch.epoch <= current.epoch)
        {
            return Err(GridBridgeError::BootstrapState);
        }
        if !matches!(
            self.grid
                .observe_inventory(inventory)
                .map_err(GridBridgeError::Reducer)?,
            GridDecision::Noop
        ) {
            return Err(GridBridgeError::Evidence);
        }
        let GridDecision::Actions(actions) = self
            .grid
            .install_epoch(epoch)
            .map_err(GridBridgeError::Reducer)?
        else {
            return Err(GridBridgeError::Evidence);
        };
        self.startup_reconciliation
            .as_mut()
            .ok_or(GridBridgeError::Evidence)?
            .rebuild_attempted = true;
        let plan = self.plan_places(&actions)?;
        self.validate()?;
        Ok(plan)
    }

    pub(crate) fn next_install_epoch(&self) -> Result<u64, GridBridgeError> {
        if self.bootstrap_state == GridBootstrapState::Attempted
            && self.grid.epoch.is_none()
            && self.grid.owned_orders.is_empty()
            && self.routes.is_empty()
        {
            Ok(1)
        } else if self.needs_reconciliation_rebuild() {
            self.grid
                .epoch
                .as_ref()
                .and_then(|epoch| epoch.epoch.checked_add(1))
                .ok_or(GridBridgeError::Evidence)
        } else {
            Err(GridBridgeError::BootstrapState)
        }
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
        if let Some(transaction_id) = plan.transaction_id.as_deref()
            && !matches!(
                self.grid
                    .settle_transaction(transaction_id, true)
                    .map_err(GridBridgeError::Reducer)?,
                GridDecision::Noop
            )
        {
            return Err(GridBridgeError::Evidence);
        }
        self.validate()?;
        Ok(())
    }

    fn plan_transaction(
        &mut self,
        transaction: &GridTransaction,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        for order in &transaction.places {
            let client = stable_identifier(b"client", &self.grid.binding, &order.key)?;
            self.reserve_client_route(order.key.clone(), client)?;
        }
        self.validate()?;
        self.transaction_plan_from_routes(transaction)
    }

    fn transaction_plan_from_routes(
        &self,
        transaction: &GridTransaction,
    ) -> Result<GridDispatchPlan, GridBridgeError> {
        let cancelled_route = self
            .routes
            .get(&transaction.cancel)
            .ok_or(GridBridgeError::UnknownOrder)?;
        if cancelled_route.accepted_venue_order_id.is_none() {
            return Err(GridBridgeError::Evidence);
        }
        let cancelled = cancelled_route.client_order_id.clone();
        let mut commands = Vec::with_capacity(3);
        let mut accepted_routes = Vec::with_capacity(2);
        for order in &transaction.places {
            let route = self
                .routes
                .get(&order.key)
                .ok_or(GridBridgeError::UnknownOrder)?;
            let client = stable_identifier(b"client", &self.grid.binding, &order.key)?;
            if route.client_order_id != client || route.accepted_venue_order_id.is_some() {
                return Err(GridBridgeError::RouteConflict);
            }
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
        Ok(GridDispatchPlan {
            commands,
            accepted_routes,
            transaction_id: Some(transaction.id.clone()),
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
            transaction_id: None,
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
            || self.terminal_rebuild_rearm_version > TERMINAL_REBUILD_REARM_VERSION
            || self
                .execution_profile
                .as_ref()
                .is_some_and(|profile| profile.validate().is_err())
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
        if let Some(reconciliation) = &self.startup_reconciliation {
            if !matches!(
                self.bootstrap_state,
                GridBootstrapState::Attempted | GridBootstrapState::Confirmed
            ) || self.reconciliation_sequence == 0
                || reconciliation.operation_sequence != self.reconciliation_sequence
                || !matches!(
                    self.grid.phase,
                    GridPhase::ResettingGrid | GridPhase::Running
                )
                || (self.grid.phase == GridPhase::Running
                    && (!reconciliation.rebuild_attempted || !reconciliation.attempts.is_empty()))
                || self.grid.epoch.is_none()
                || !self.grid.pending_transactions.is_empty()
                || reconciliation
                    .attempts
                    .windows(2)
                    .any(|pair| pair[0].key >= pair[1].key)
                || reconciliation.attempts.iter().any(|attempt| {
                    attempt.attempt == 0
                        || attempt.attempt > 3
                        || !self.grid.owned_orders.contains_key(&attempt.key)
                })
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

fn reconciliation_cancel_identifier(
    binding: &venue_strategies::hedged_grid::HedgedGridBinding,
    operation_sequence: u64,
    attempt: u16,
    key: &GridOrderKey,
) -> Result<CommandId, GridBridgeError> {
    let encoded = serde_json::to_vec(&(binding, operation_sequence, attempt, key))
        .map_err(|_| GridBridgeError::Evidence)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.grid.reconciliation.cancel.v1");
    digest.update(encoded);
    let raw = format!("gr-{:x}", digest.finalize());
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
    #[error("grid execution precision or minimum notional is invalid")]
    ExecutionProfile,
    #[error("grid reducer rejected the persisted private fill: {0}")]
    Reducer(HedgedGridError),
}

#[cfg(test)]
#[path = "grid_tests.rs"]
mod tests;
