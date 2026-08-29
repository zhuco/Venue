use std::{collections::BTreeMap, path::PathBuf};

use serde::Serialize;
use venue_domain::domain::{
    CancelCommand, CommandId, ExecutionCommand, NativeOrderFamily, OrderOwner,
    is_canonical_trading_account_id,
};
use venue_gateway_api::VenueId;

use crate::{
    AccountCanonicalRootGuard, CommandJournal, CommandJournalError, CommandReceipt, CommandState,
};

/// Immutable account binding for the Owner/native identity projection.
///
/// The canonical-root digest binds this projection to the same account root selected by
/// [`crate::acquire_account_canonical_root`]. It is evidence only: this value does not carry the
/// root lock, a writer lease, a capability, or dispatch authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AccountOwnerRouteScope {
    venue: VenueId,
    trading_account_id: String,
    canonical_root_sha256: String,
}

impl AccountOwnerRouteScope {
    pub fn new(
        venue: VenueId,
        trading_account_id: impl Into<String>,
        canonical_root_sha256: impl Into<String>,
    ) -> Result<Self, OwnerRoutesError> {
        let scope = Self {
            venue,
            trading_account_id: trading_account_id.into(),
            canonical_root_sha256: canonical_root_sha256.into(),
        };
        scope.validate()?;
        Ok(scope)
    }

    pub fn from_canonical_root(
        venue: VenueId,
        trading_account_id: impl Into<String>,
        canonical_root: &AccountCanonicalRootGuard,
    ) -> Result<Self, OwnerRoutesError> {
        Self::new(
            venue,
            trading_account_id,
            canonical_root.canonical_root_sha256(),
        )
    }

    pub const fn venue(&self) -> VenueId {
        self.venue
    }

    pub fn trading_account_id(&self) -> &str {
        &self.trading_account_id
    }

    pub fn canonical_root_sha256(&self) -> &str {
        &self.canonical_root_sha256
    }

    fn validate(&self) -> Result<(), OwnerRoutesError> {
        if !is_canonical_trading_account_id(&self.trading_account_id)
            || !valid_sha256(&self.canonical_root_sha256)
        {
            return Err(OwnerRoutesError::Scope);
        }
        Ok(())
    }

    fn matches_owner(&self, owner: &OrderOwner) -> bool {
        owner.exchange == self.venue.as_str() && owner.account == self.trading_account_id
    }
}

/// Recovered account generation used only to reject stale Owner-route operations.
///
/// The generation is supplied by the caller's durable recovery/writer contract. Advancing this
/// fence never creates a writer lease and never makes a command dispatchable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OwnerRouteFence {
    scope: AccountOwnerRouteScope,
    generation: u64,
}

impl OwnerRouteFence {
    pub fn new(scope: AccountOwnerRouteScope, generation: u64) -> Result<Self, OwnerRoutesError> {
        scope.validate()?;
        if generation == 0 {
            return Err(OwnerRoutesError::Generation);
        }
        Ok(Self { scope, generation })
    }

    pub fn scope(&self) -> &AccountOwnerRouteScope {
        &self.scope
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Family-qualified native client identity. Native identity namespaces never cross families.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NativeOrderRouteKey {
    pub family: NativeOrderFamily,
    pub client_id: CommandId,
}

/// Complete latest projection for one command that can create an exchange-side order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NativeOrderRoute {
    pub command_id: CommandId,
    pub owner: OrderOwner,
    pub key: NativeOrderRouteKey,
    pub venue_order_id: Option<String>,
    pub state: CommandState,
}

/// A cancellation route derived from the durable create reservation, never from caller input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExactCancelRoute {
    pub cancel_command_id: CommandId,
    pub owner: OrderOwner,
    pub target_command_id: CommandId,
    pub target: NativeOrderRouteKey,
    pub target_venue_order_id: Option<String>,
    pub state: CommandState,
}

/// Deterministic restart projection suitable for inclusion in a wider recovery manifest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OwnerRouteProjection {
    pub fence: OwnerRouteFence,
    pub journal_tail_sequence: u64,
    pub routes: Vec<NativeOrderRoute>,
    pub cancels: Vec<ExactCancelRoute>,
    pub unresolved_command_ids: Vec<CommandId>,
}

/// Account-level Owner/native identity contract backed exclusively by [`CommandJournal`].
///
/// This type adds no journal and no mutation authority. It owns the shared command journal so a
/// caller cannot append around the projection, and it rebuilds every route from that journal on
/// restart. Every state-changing method only appends a command fact; dispatch still requires the
/// existing execution, risk, writer, WAL and capability gates.
#[derive(Debug)]
pub struct DurableOwnerRoutes {
    fence: OwnerRouteFence,
    journal: CommandJournal,
    routes: BTreeMap<NativeOrderRouteKey, NativeOrderRoute>,
    command_routes: BTreeMap<CommandId, NativeOrderRouteKey>,
    venue_routes: BTreeMap<(NativeOrderFamily, String), NativeOrderRouteKey>,
    cancel_targets: BTreeMap<CommandId, NativeOrderRouteKey>,
}

impl DurableOwnerRoutes {
    pub fn open(
        journal_path: impl Into<PathBuf>,
        fence: OwnerRouteFence,
    ) -> Result<Self, OwnerRoutesError> {
        fence.scope.validate()?;
        let journal = CommandJournal::open(journal_path)?;
        let mut routes = Self {
            fence,
            journal,
            routes: BTreeMap::new(),
            command_routes: BTreeMap::new(),
            venue_routes: BTreeMap::new(),
            cancel_targets: BTreeMap::new(),
        };
        routes.rebuild_projection()?;
        Ok(routes)
    }

    pub fn fence(&self) -> &OwnerRouteFence {
        &self.fence
    }

    /// Installs a strictly newer recovered generation. Existing UNKNOWN routes remain projected.
    pub fn advance_fence(
        &mut self,
        current: &OwnerRouteFence,
        next: OwnerRouteFence,
    ) -> Result<(), OwnerRoutesError> {
        self.verify_fence(current)?;
        next.scope.validate()?;
        if next.scope != self.fence.scope || next.generation <= self.fence.generation {
            return Err(OwnerRoutesError::Generation);
        }
        self.fence = next;
        Ok(())
    }

    /// Durably reserves a create identity in the shared command journal before any dispatch.
    pub fn reserve_create(
        &mut self,
        fence: &OwnerRouteFence,
        command: ExecutionCommand,
    ) -> Result<NativeOrderRoute, OwnerRoutesError> {
        self.verify_fence(fence)?;
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return Err(OwnerRoutesError::CreateRequired);
        }
        self.verify_owner(command.mutation_owner())?;
        let receipt = self.journal.prepare(command)?.clone();
        self.project_create(&receipt)?;
        self.route_for_command(receipt.command.command_id())
            .cloned()
            .ok_or(OwnerRoutesError::Projection)
    }

    /// Durably reserves one cancellation and returns the family/client/native route of its exact
    /// original target. A caller-provided family mismatch fails before a cancel record is written.
    pub fn reserve_cancel(
        &mut self,
        fence: &OwnerRouteFence,
        command: CancelCommand,
        expected_family: NativeOrderFamily,
    ) -> Result<ExactCancelRoute, OwnerRoutesError> {
        self.verify_fence(fence)?;
        self.verify_owner(&command.owner)?;
        let target = self
            .journal
            .order_identity_by_client_id(&command.target_client_order_id)
            .map(|identity| {
                (
                    identity.owner.clone(),
                    identity.family,
                    identity.client_id.clone(),
                )
            })
            .ok_or(OwnerRoutesError::MissingRoute)?;
        if target.0 != command.owner || target.1 != expected_family {
            return Err(OwnerRoutesError::CancelRoute);
        }
        let key = NativeOrderRouteKey {
            family: target.1,
            client_id: target.2,
        };
        if !self.routes.contains_key(&key) {
            return Err(OwnerRoutesError::Projection);
        }
        let receipt = self.journal.prepare_cancel(command)?.clone();
        if self
            .cancel_targets
            .get(receipt.command.command_id())
            .is_some_and(|existing| existing != &key)
        {
            return Err(OwnerRoutesError::Projection);
        }
        self.cancel_targets
            .insert(receipt.command.command_id().clone(), key);
        self.exact_cancel_route(receipt.command.command_id())
            .ok_or(OwnerRoutesError::Projection)
    }

    pub fn mark_submitted(
        &mut self,
        fence: &OwnerRouteFence,
        command_id: &CommandId,
    ) -> Result<CommandReceipt, OwnerRoutesError> {
        self.transition(fence, command_id, CommandState::Submitted)
    }

    pub fn record_accepted(
        &mut self,
        fence: &OwnerRouteFence,
        command_id: &CommandId,
        venue_order_id: impl Into<String>,
    ) -> Result<CommandReceipt, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let venue_order_id = venue_order_id.into();
        if !valid_native_order_id(&venue_order_id) {
            return Err(OwnerRoutesError::NativeOrderId);
        }
        if let Some(route) = self.route_for_command(command_id) {
            let venue_key = (route.key.family, venue_order_id.clone());
            if self
                .venue_routes
                .get(&venue_key)
                .is_some_and(|existing| existing != &route.key)
            {
                return Err(OwnerRoutesError::NativeOrderConflict);
            }
        }
        self.transition(fence, command_id, CommandState::Accepted { venue_order_id })
    }

    pub fn record_rejected(
        &mut self,
        fence: &OwnerRouteFence,
        command_id: &CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandReceipt, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(OwnerRoutesError::Outcome);
        }
        self.transition(fence, command_id, CommandState::Rejected { reason })
    }

    pub fn record_unknown(
        &mut self,
        fence: &OwnerRouteFence,
        command_id: &CommandId,
        reason: impl Into<String>,
    ) -> Result<CommandReceipt, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(OwnerRoutesError::Outcome);
        }
        self.transition(fence, command_id, CommandState::Unknown { reason })
    }

    pub fn route_by_client_id(
        &self,
        fence: &OwnerRouteFence,
        family: NativeOrderFamily,
        client_id: &CommandId,
    ) -> Result<Option<&NativeOrderRoute>, OwnerRoutesError> {
        self.verify_fence(fence)?;
        Ok(self.routes.get(&NativeOrderRouteKey {
            family,
            client_id: client_id.clone(),
        }))
    }

    pub fn route_by_venue_order_id(
        &self,
        fence: &OwnerRouteFence,
        family: NativeOrderFamily,
        venue_order_id: &str,
    ) -> Result<Option<&NativeOrderRoute>, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let Some(key) = self.venue_routes.get(&(family, venue_order_id.to_owned())) else {
            return Ok(None);
        };
        self.routes
            .get(key)
            .map(Some)
            .ok_or(OwnerRoutesError::Projection)
    }

    pub fn cancel_route(
        &self,
        fence: &OwnerRouteFence,
        cancel_command_id: &CommandId,
    ) -> Result<Option<ExactCancelRoute>, OwnerRoutesError> {
        self.verify_fence(fence)?;
        Ok(self.exact_cancel_route(cancel_command_id))
    }

    pub fn projection(
        &self,
        fence: &OwnerRouteFence,
    ) -> Result<OwnerRouteProjection, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let mut cancels = Vec::with_capacity(self.cancel_targets.len());
        for command_id in self.cancel_targets.keys() {
            cancels.push(
                self.exact_cancel_route(command_id)
                    .ok_or(OwnerRoutesError::Projection)?,
            );
        }
        let journal_tail_sequence = self
            .journal
            .commands()
            .filter_map(|command| self.journal.receipt(command.command_id()))
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0);
        Ok(OwnerRouteProjection {
            fence: self.fence.clone(),
            journal_tail_sequence,
            routes: self.routes.values().cloned().collect(),
            cancels,
            unresolved_command_ids: self.journal.unresolved_command_ids(),
        })
    }

    fn transition(
        &mut self,
        fence: &OwnerRouteFence,
        command_id: &CommandId,
        state: CommandState,
    ) -> Result<CommandReceipt, OwnerRoutesError> {
        self.verify_fence(fence)?;
        let previous = self
            .journal
            .receipt(command_id)
            .cloned()
            .ok_or(OwnerRoutesError::MissingRoute)?;
        if previous.state == state {
            return Ok(previous);
        }
        if previous.state.terminal() {
            return Err(OwnerRoutesError::StateConflict);
        }
        let receipt = self.journal.transition(command_id, state)?.clone();
        if receipt.command.native_client_id().is_some() {
            self.project_create(&receipt)?;
        }
        Ok(receipt)
    }

    fn rebuild_projection(&mut self) -> Result<(), OwnerRoutesError> {
        self.routes.clear();
        self.command_routes.clear();
        self.venue_routes.clear();
        self.cancel_targets.clear();

        let commands = self.journal.commands().cloned().collect::<Vec<_>>();
        for command in &commands {
            self.verify_owner(command.mutation_owner())?;
            if command.native_client_id().is_some() {
                let receipt = self
                    .journal
                    .receipt(command.command_id())
                    .cloned()
                    .ok_or(OwnerRoutesError::Projection)?;
                self.project_create(&receipt)?;
            }
        }
        for command in commands {
            let ExecutionCommand::Cancel(cancel) = command else {
                continue;
            };
            let identity = self
                .journal
                .cancel_target_identity(&cancel.command_id)
                .ok_or(OwnerRoutesError::CancelRoute)?;
            let key = NativeOrderRouteKey {
                family: identity.family,
                client_id: identity.client_id.clone(),
            };
            if identity.owner != &cancel.owner || !self.routes.contains_key(&key) {
                return Err(OwnerRoutesError::CancelRoute);
            }
            if self.cancel_targets.insert(cancel.command_id, key).is_some() {
                return Err(OwnerRoutesError::Projection);
            }
        }
        Ok(())
    }

    fn project_create(&mut self, receipt: &CommandReceipt) -> Result<(), OwnerRoutesError> {
        let owner = receipt
            .command
            .owner()
            .cloned()
            .ok_or(OwnerRoutesError::CreateRequired)?;
        self.verify_owner(&owner)?;
        let key = NativeOrderRouteKey {
            family: receipt
                .command
                .native_order_family()
                .ok_or(OwnerRoutesError::CreateRequired)?,
            client_id: receipt
                .command
                .native_client_id()
                .cloned()
                .ok_or(OwnerRoutesError::CreateRequired)?,
        };
        if self.routes.get(&key).is_some_and(|existing| {
            existing.command_id != *receipt.command.command_id() || existing.owner != owner
        }) {
            return Err(OwnerRoutesError::NativeOrderConflict);
        }
        if self
            .command_routes
            .get(receipt.command.command_id())
            .is_some_and(|existing| existing != &key)
        {
            return Err(OwnerRoutesError::NativeOrderConflict);
        }
        let venue_order_id = match &receipt.state {
            CommandState::Accepted { venue_order_id } => {
                if !valid_native_order_id(venue_order_id) {
                    return Err(OwnerRoutesError::NativeOrderId);
                }
                let venue_key = (key.family, venue_order_id.clone());
                if self
                    .venue_routes
                    .get(&venue_key)
                    .is_some_and(|existing| existing != &key)
                {
                    return Err(OwnerRoutesError::NativeOrderConflict);
                }
                self.venue_routes.insert(venue_key, key.clone());
                Some(venue_order_id.clone())
            }
            CommandState::Prepared
            | CommandState::Submitted
            | CommandState::Rejected { .. }
            | CommandState::Unknown { .. } => None,
        };
        self.routes.insert(
            key.clone(),
            NativeOrderRoute {
                command_id: receipt.command.command_id().clone(),
                owner,
                key: key.clone(),
                venue_order_id,
                state: receipt.state.clone(),
            },
        );
        self.command_routes
            .insert(receipt.command.command_id().clone(), key);
        Ok(())
    }

    fn route_for_command(&self, command_id: &CommandId) -> Option<&NativeOrderRoute> {
        self.command_routes
            .get(command_id)
            .and_then(|key| self.routes.get(key))
    }

    fn exact_cancel_route(&self, cancel_command_id: &CommandId) -> Option<ExactCancelRoute> {
        let target_key = self.cancel_targets.get(cancel_command_id)?;
        let target = self.routes.get(target_key)?;
        let receipt = self.journal.receipt(cancel_command_id)?;
        let cancel = match &receipt.command {
            ExecutionCommand::Cancel(cancel) => cancel,
            ExecutionCommand::PlaceLimit(_)
            | ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => return None,
        };
        Some(ExactCancelRoute {
            cancel_command_id: cancel.command_id.clone(),
            owner: cancel.owner.clone(),
            target_command_id: target.command_id.clone(),
            target: target.key.clone(),
            target_venue_order_id: target.venue_order_id.clone(),
            state: receipt.state.clone(),
        })
    }

    fn verify_owner(&self, owner: &OrderOwner) -> Result<(), OwnerRoutesError> {
        if self.fence.scope.matches_owner(owner) {
            Ok(())
        } else {
            Err(OwnerRoutesError::OwnerScope)
        }
    }

    fn verify_fence(&self, fence: &OwnerRouteFence) -> Result<(), OwnerRoutesError> {
        if &self.fence == fence {
            Ok(())
        } else {
            Err(OwnerRoutesError::StaleFence)
        }
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_native_order_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace())
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerRoutesError {
    #[error("Owner route account scope or canonical-root digest is invalid")]
    Scope,
    #[error("Owner route generation must be non-zero and strictly advance")]
    Generation,
    #[error("Owner route operation used a stale account generation or root fence")]
    StaleFence,
    #[error("command Owner does not match the fenced exchange account")]
    OwnerScope,
    #[error("Owner route create reservation requires an order-creating command")]
    CreateRequired,
    #[error("Owner/native route is missing")]
    MissingRoute,
    #[error("cancel does not name the exact durable Owner/family/client route")]
    CancelRoute,
    #[error("exchange-native order id is invalid")]
    NativeOrderId,
    #[error("exchange-native identity is already bound to a different Owner route")]
    NativeOrderConflict,
    #[error("Owner route outcome reason is empty")]
    Outcome,
    #[error("Owner route terminal state conflicts with the durable outcome")]
    StateConflict,
    #[error("command journal and Owner route projection disagree")]
    Projection,
    #[error(transparent)]
    Journal(#[from] CommandJournalError),
}
