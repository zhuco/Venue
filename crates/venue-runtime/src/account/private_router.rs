use std::collections::BTreeMap;

use crate::{
    domain::{CommandId, DomainEvent, FieldState, NativeOrderFamily, OrderOwner, Symbol},
    execution::AccountExecutionIntent,
    runtime::{
        account::{AccountKey, StrategyInstanceKey, StrategyRegistry},
        strategy::PersistedPrivateFact,
    },
};

const MAX_ORDER_ROUTE_BINDINGS: usize = 100_000;

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderRouteBinding {
    family: NativeOrderFamily,
    command_id: CommandId,
    client_order_id: String,
    venue_order_id: Option<String>,
    owner: OrderOwner,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingEvidenceFacts {
    sequence: u64,
    payload_sha256: String,
    fact_count: u32,
    next_fact_index: u32,
    facts: Vec<PersistedPrivateFact>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FactBatchDecision {
    Pending,
    Complete(Vec<PersistedPrivateFact>),
    Duplicate { pending: bool },
    Gap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateDelivery {
    pub target: StrategyInstanceKey,
    pub fact: PersistedPrivateFact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileScope {
    Account,
    Symbol(Symbol),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileReason {
    PrivateGenerationMismatch,
    PrivateEvidenceGap,
    UnknownOwner,
    IdentityConflict,
    OwnerNoLongerRegistered,
    SymbolMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateReconcileRequest {
    pub scope: ReconcileScope,
    pub reason: ReconcileReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateRouteReport {
    pub deliveries: Vec<PrivateDelivery>,
    pub reconcile: Option<PrivateReconcileRequest>,
    pub duplicate: bool,
    pub pending_batch: bool,
}

impl PrivateRouteReport {
    fn delivered(deliveries: Vec<PrivateDelivery>) -> Self {
        Self {
            deliveries,
            reconcile: None,
            duplicate: false,
            pending_batch: false,
        }
    }

    fn reconcile(scope: ReconcileScope, reason: ReconcileReason) -> Self {
        Self {
            deliveries: Vec::new(),
            reconcile: Some(PrivateReconcileRequest { scope, reason }),
            duplicate: false,
            pending_batch: false,
        }
    }

    fn duplicate(pending_batch: bool) -> Self {
        Self {
            deliveries: Vec::new(),
            reconcile: None,
            duplicate: true,
            pending_batch,
        }
    }

    fn pending() -> Self {
        Self {
            deliveries: Vec::new(),
            reconcile: None,
            duplicate: false,
            pending_batch: true,
        }
    }
}

/// Routes persisted facts by durable order identity. A symbol is never used to guess ownership of
/// an order or fill; it is only a consistency check after Client/venue order identity resolves.
#[derive(Clone, Debug)]
pub(crate) struct PrivateRouter {
    account: AccountKey,
    connection_generation: u64,
    last_evidence_sequence: u64,
    last_evidence_digest: Option<String>,
    last_evidence_fact_count: u32,
    pending_evidence: Option<PendingEvidenceFacts>,
    client_routes: BTreeMap<(NativeOrderFamily, String), OrderRouteBinding>,
    venue_routes: BTreeMap<(NativeOrderFamily, String), OrderRouteBinding>,
}

impl PrivateRouter {
    #[must_use]
    pub fn new(account: AccountKey) -> Self {
        Self {
            account,
            connection_generation: 0,
            last_evidence_sequence: 0,
            last_evidence_digest: None,
            last_evidence_fact_count: 0,
            pending_evidence: None,
            client_routes: BTreeMap::new(),
            venue_routes: BTreeMap::new(),
        }
    }

    pub fn activate_generation(
        &mut self,
        connection_generation: u64,
        evidence_cursor: u64,
    ) -> Result<(), PrivateRouterError> {
        if self.pending_evidence.is_some() {
            return Err(PrivateRouterError::IncompleteEvidenceBatch);
        }
        if connection_generation == 0 || connection_generation <= self.connection_generation {
            return Err(PrivateRouterError::Generation);
        }
        self.connection_generation = connection_generation;
        self.last_evidence_sequence = evidence_cursor;
        self.last_evidence_digest = None;
        self.last_evidence_fact_count = 0;
        Ok(())
    }

    /// Advances an already-active production generation only from a separately verified durable
    /// Actor/facts equality. The router cannot derive this cursor and refuses a gap, regression,
    /// or incomplete batch.
    pub(crate) fn restore_durable_evidence_cursor(
        &mut self,
        expected_current: u64,
        recovered_cursor: u64,
    ) -> Result<(), PrivateRouterError> {
        if self.connection_generation == 0
            || self.pending_evidence.is_some()
            || self.last_evidence_sequence != expected_current
            || recovered_cursor <= expected_current
        {
            return Err(PrivateRouterError::Generation);
        }
        self.last_evidence_sequence = recovered_cursor;
        self.last_evidence_digest = None;
        self.last_evidence_fact_count = 0;
        Ok(())
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn active_generation_for_test(&self) -> u64 {
        self.connection_generation
    }

    pub fn bind_order(
        &mut self,
        family: NativeOrderFamily,
        client_order_id: String,
        venue_order_id: Option<String>,
        command_id: CommandId,
        owner: OrderOwner,
        registry: &StrategyRegistry,
    ) -> Result<(), PrivateRouterError> {
        if client_order_id.trim().is_empty()
            || venue_order_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || owner.validate().is_err()
            || !self.account.matches_owner(&owner)
        {
            return Err(PrivateRouterError::Binding);
        }
        let registration = registry
            .binding_by_instance(&owner.strategy_instance_id)
            .filter(|binding| binding.matches_owner(&owner))
            .ok_or(PrivateRouterError::Binding)?;
        let _ = registration;

        let client_identity = (family, client_order_id.clone());
        let existing = self.client_routes.get(&client_identity);
        if existing.is_some_and(|route| {
            route.owner != owner || route.family != family || route.command_id != command_id
        }) || existing
            .and_then(|route| route.venue_order_id.as_ref())
            .zip(venue_order_id.as_ref())
            .is_some_and(|(left, right)| left != right)
        {
            return Err(PrivateRouterError::Conflict);
        }
        let venue_order_id = venue_order_id
            .or_else(|| existing.and_then(|route| route.venue_order_id.as_ref().cloned()));
        let route = OrderRouteBinding {
            family,
            command_id,
            client_order_id: client_order_id.clone(),
            venue_order_id: venue_order_id.clone(),
            owner,
        };
        if existing.is_none() && self.client_routes.len() >= MAX_ORDER_ROUTE_BINDINGS {
            return Err(PrivateRouterError::Capacity);
        }
        if let Some(venue_order_id) = &venue_order_id {
            let venue_identity = (family, venue_order_id.clone());
            ensure_route_capacity(&self.venue_routes, &venue_identity, &route)?;
        }

        // Both indexes are fully validated before either is changed.
        self.client_routes.insert(client_identity, route.clone());
        if let Some(venue_order_id) = venue_order_id {
            self.venue_routes.insert((family, venue_order_id), route);
        }
        Ok(())
    }

    /// Atomically reserves creation identity or proves an exact cancel target before the command
    /// enters the account lane. A cancel can never infer ownership from symbol or an ID in another
    /// native endpoint family.
    pub(crate) fn reserve_execution_identity(
        &mut self,
        intent: &AccountExecutionIntent,
        registry: &StrategyRegistry,
    ) -> Result<(), PrivateRouterError> {
        match intent.command() {
            crate::domain::ExecutionCommand::Cancel(command) => {
                let (target, _, owner) = self
                    .resolve_order_owner(
                        intent.native_order_family(),
                        Some(command.target_client_order_id.as_str()),
                        None,
                        &command.owner.symbol,
                        registry,
                    )
                    .map_err(|_| PrivateRouterError::CancelTarget)?;
                if target != *intent.target() || owner != command.owner {
                    return Err(PrivateRouterError::CancelTarget);
                }
                Ok(())
            }
            command => {
                let family = command
                    .native_order_family()
                    .ok_or(PrivateRouterError::Binding)?;
                let client_order_id = command
                    .native_client_id()
                    .ok_or(PrivateRouterError::Binding)?;
                if family != intent.native_order_family()
                    || client_order_id != intent.native_client_id()
                {
                    return Err(PrivateRouterError::Binding);
                }
                self.bind_order(
                    family,
                    client_order_id.as_str().to_owned(),
                    None,
                    command.command_id().clone(),
                    command.mutation_owner().clone(),
                    registry,
                )
            }
        }
    }

    /// The legacy migration route is intentionally separate from normal Owner resolution: it is
    /// supplied only by Runtime's sealed Host admission and can authorize one exact Cancel.
    pub(crate) fn reserve_legacy_v1_custody_cancel(
        &self,
        intent: &AccountExecutionIntent,
        route: &venue_execution::LegacyV1CustodyRoute,
    ) -> Result<(), PrivateRouterError> {
        let crate::domain::ExecutionCommand::Cancel(command) = intent.command() else {
            return Err(PrivateRouterError::CancelTarget);
        };
        if !intent.is_legacy_v1_custody_cancel()
            || intent.native_order_family() != route.family
            || command.owner != route.owner
            || command.target_client_order_id != route.client_order_id
            || route.venue_order_id.trim().is_empty()
        {
            return Err(PrivateRouterError::CancelTarget);
        }
        Ok(())
    }

    pub fn bind_venue_order(
        &mut self,
        family: NativeOrderFamily,
        client_order_id: &str,
        venue_order_id: String,
    ) -> Result<(), PrivateRouterError> {
        if venue_order_id.trim().is_empty() {
            return Err(PrivateRouterError::Binding);
        }
        let mut route = self
            .client_routes
            .get(&(family, client_order_id.to_owned()))
            .cloned()
            .ok_or(PrivateRouterError::Binding)?;
        if route
            .venue_order_id
            .as_ref()
            .is_some_and(|existing| existing != &venue_order_id)
        {
            return Err(PrivateRouterError::Conflict);
        }
        route.venue_order_id = Some(venue_order_id.clone());
        let venue_identity = (family, venue_order_id.clone());
        ensure_route_capacity(&self.venue_routes, &venue_identity, &route)?;

        // The client binding already exists and the venue index is checked before either write.
        self.venue_routes.insert(venue_identity, route.clone());
        self.client_routes
            .insert((family, client_order_id.to_owned()), route);
        Ok(())
    }

    pub fn release_instance(&mut self, key: &StrategyInstanceKey) {
        self.client_routes.retain(|_, route| {
            route.owner.strategy_instance_id != key.instance_id || route.owner.symbol != key.symbol
        });
        self.venue_routes.retain(|_, route| {
            route.owner.strategy_instance_id != key.instance_id || route.owner.symbol != key.symbol
        });
    }

    pub(crate) fn recovered_mutation_has_exact_route(
        &self,
        family: NativeOrderFamily,
        client_order_id: &str,
        command_id: &CommandId,
        owner: &OrderOwner,
        is_cancel: bool,
    ) -> bool {
        self.client_routes
            .get(&(family, client_order_id.to_owned()))
            .is_some_and(|route| {
                route.family == family
                    && route.client_order_id == client_order_id
                    && route.owner == *owner
                    && (is_cancel || route.command_id == *command_id)
            })
    }

    pub fn route(
        &mut self,
        fact: PersistedPrivateFact,
        registry: &StrategyRegistry,
    ) -> PrivateRouteReport {
        if fact.evidence().generation() != self.connection_generation {
            return self.reconcile_preserving_pending_batch(
                ReconcileScope::Account,
                ReconcileReason::PrivateGenerationMismatch,
            );
        }
        let facts = match self.advance_fact_cursor(fact) {
            FactBatchDecision::Pending => return PrivateRouteReport::pending(),
            FactBatchDecision::Complete(facts) => facts,
            FactBatchDecision::Duplicate { pending } => {
                return PrivateRouteReport::duplicate(pending);
            }
            FactBatchDecision::Gap => {
                return self.reconcile_preserving_pending_batch(
                    ReconcileScope::Account,
                    ReconcileReason::PrivateEvidenceGap,
                );
            }
        };

        let mut deliveries = Vec::new();
        for fact in &facts {
            let report = self.route_complete_fact(fact.clone(), registry);
            if let Some(reconcile) = report.reconcile {
                return self.reconcile_preserving_pending_batch(reconcile.scope, reconcile.reason);
            }
            deliveries.extend(report.deliveries);
        }
        if !self.commit_fact_cursor(&facts) {
            return self.reconcile_preserving_pending_batch(
                ReconcileScope::Account,
                ReconcileReason::PrivateEvidenceGap,
            );
        }
        PrivateRouteReport::delivered(deliveries)
    }

    fn reconcile_preserving_pending_batch(
        &self,
        scope: ReconcileScope,
        reason: ReconcileReason,
    ) -> PrivateRouteReport {
        let mut report = PrivateRouteReport::reconcile(scope, reason);
        report.pending_batch = self.pending_evidence.is_some();
        report
    }

    fn route_complete_fact(
        &self,
        fact: PersistedPrivateFact,
        registry: &StrategyRegistry,
    ) -> PrivateRouteReport {
        match &fact.record().event {
            DomainEvent::Order(order) => {
                let Some(family) = fact.order_family() else {
                    return PrivateRouteReport::reconcile(
                        ReconcileScope::Account,
                        ReconcileReason::IdentityConflict,
                    );
                };
                let client_order_id = match &order.client_order_id {
                    FieldState::Known(value) => Some(value.as_str()),
                    _ => None,
                };
                self.route_owned_order(
                    family,
                    client_order_id,
                    Some(order.order_id.as_str()),
                    &order.symbol,
                    fact.clone(),
                    registry,
                )
            }
            DomainEvent::Fill(fill) => {
                let Some(family) = fact.order_family() else {
                    return PrivateRouteReport::reconcile(
                        ReconcileScope::Account,
                        ReconcileReason::IdentityConflict,
                    );
                };
                self.route_owned_order(
                    family,
                    None,
                    Some(fill.order_id.as_str()),
                    &fill.symbol,
                    fact.clone(),
                    registry,
                )
            }
            DomainEvent::Position(position) => {
                self.route_symbol_fact(&position.symbol, fact.clone(), registry)
            }
            DomainEvent::Balance(_) | DomainEvent::Funding(_) => {
                let deliveries = registry
                    .active_bindings()
                    .map(|binding| PrivateDelivery {
                        target: binding.key.clone(),
                        fact: fact.clone(),
                    })
                    .collect();
                PrivateRouteReport::delivered(deliveries)
            }
            DomainEvent::Instrument(_) => PrivateRouteReport::reconcile(
                ReconcileScope::Account,
                ReconcileReason::UnknownOwner,
            ),
        }
    }

    pub(crate) fn resolve_order_owner(
        &self,
        family: NativeOrderFamily,
        client_order_id: Option<&str>,
        venue_order_id: Option<&str>,
        symbol: &Symbol,
        registry: &StrategyRegistry,
    ) -> Result<(StrategyInstanceKey, String, OrderOwner), ReconcileReason> {
        let route = match (client_order_id, venue_order_id) {
            (Some(client_order_id), Some(venue_order_id)) => {
                let client_route = self
                    .client_routes
                    .get(&(family, client_order_id.to_owned()));
                let venue_route = self.venue_routes.get(&(family, venue_order_id.to_owned()));
                match (client_route, venue_route) {
                    (Some(left), Some(right))
                        if left == right
                            && left.client_order_id == client_order_id
                            && left.venue_order_id.as_deref() == Some(venue_order_id) =>
                    {
                        left
                    }
                    (None, None) => return Err(ReconcileReason::UnknownOwner),
                    _ => return Err(ReconcileReason::IdentityConflict),
                }
            }
            (Some(client_order_id), None) => self
                .client_routes
                .get(&(family, client_order_id.to_owned()))
                .filter(|route| route.client_order_id == client_order_id)
                .ok_or(ReconcileReason::UnknownOwner)?,
            (None, Some(venue_order_id)) => self
                .venue_routes
                .get(&(family, venue_order_id.to_owned()))
                .filter(|route| route.venue_order_id.as_deref() == Some(venue_order_id))
                .ok_or(ReconcileReason::UnknownOwner)?,
            (None, None) => return Err(ReconcileReason::UnknownOwner),
        };
        if &route.owner.symbol != symbol {
            return Err(ReconcileReason::SymbolMismatch);
        }
        let binding = registry
            .binding_by_instance(&route.owner.strategy_instance_id)
            .filter(|binding| binding.matches_owner(&route.owner))
            .ok_or(ReconcileReason::OwnerNoLongerRegistered)?;
        Ok((
            binding.key.clone(),
            route.client_order_id.clone(),
            route.owner.clone(),
        ))
    }

    fn advance_fact_cursor(&mut self, fact: PersistedPrivateFact) -> FactBatchDecision {
        let sequence = fact.evidence().sequence();
        let payload_sha256 = fact.evidence().payload_sha256().to_owned();
        let fact_index = fact.fact_index();
        let fact_count = fact.fact_count();

        if let Some(pending) = &mut self.pending_evidence {
            if sequence != pending.sequence
                || payload_sha256 != pending.payload_sha256
                || fact_count != pending.fact_count
            {
                return FactBatchDecision::Gap;
            }
            if fact_index < pending.next_fact_index {
                return FactBatchDecision::Duplicate { pending: true };
            }
            if fact_index != pending.next_fact_index {
                return FactBatchDecision::Gap;
            }
            if pending.next_fact_index.checked_add(1) == Some(pending.fact_count) {
                let mut facts = Vec::with_capacity(pending.facts.len().saturating_add(1));
                facts.extend(pending.facts.iter().cloned());
                facts.push(fact);
                return FactBatchDecision::Complete(facts);
            }
            pending.facts.push(fact);
            pending.next_fact_index += 1;
            return FactBatchDecision::Pending;
        }

        if sequence == self.last_evidence_sequence {
            return if self.last_evidence_digest.as_deref() == Some(payload_sha256.as_str())
                && self.last_evidence_fact_count == fact_count
                && fact_index < fact_count
            {
                FactBatchDecision::Duplicate { pending: false }
            } else {
                FactBatchDecision::Gap
            };
        }
        if self.last_evidence_sequence.checked_add(1) != Some(sequence) || fact_index != 0 {
            return FactBatchDecision::Gap;
        }

        if fact_count == 1 {
            FactBatchDecision::Complete(vec![fact])
        } else {
            self.pending_evidence = Some(PendingEvidenceFacts {
                sequence,
                payload_sha256,
                fact_count,
                next_fact_index: 1,
                facts: vec![fact],
            });
            FactBatchDecision::Pending
        }
    }

    /// Commits the consumed evidence boundary only after every normalized fact in the batch has
    /// resolved to its exact destination. On failure, the pending batch remains retryable.
    fn commit_fact_cursor(&mut self, facts: &[PersistedPrivateFact]) -> bool {
        let Some(first) = facts.first() else {
            return false;
        };
        let sequence = first.evidence().sequence();
        let payload_sha256 = first.evidence().payload_sha256();
        let fact_count = first.fact_count();
        if u32::try_from(facts.len()).ok() != Some(fact_count)
            || facts.iter().enumerate().any(|(index, fact)| {
                u32::try_from(index).ok() != Some(fact.fact_index())
                    || fact.evidence().sequence() != sequence
                    || fact.evidence().payload_sha256() != payload_sha256
                    || fact.fact_count() != fact_count
            })
        {
            return false;
        }

        if fact_count == 1 {
            if self.pending_evidence.is_some() {
                return false;
            }
        } else {
            let Some(pending) = self.pending_evidence.as_ref() else {
                return false;
            };
            if pending.sequence != sequence
                || pending.payload_sha256 != payload_sha256
                || pending.fact_count != fact_count
                || pending.next_fact_index.checked_add(1) != Some(fact_count)
                || pending.facts.len().checked_add(1) != Some(facts.len())
            {
                return false;
            }
        }

        self.pending_evidence = None;
        self.last_evidence_sequence = sequence;
        self.last_evidence_digest = Some(payload_sha256.to_owned());
        self.last_evidence_fact_count = fact_count;
        true
    }

    fn route_owned_order(
        &self,
        family: NativeOrderFamily,
        client_order_id: Option<&str>,
        venue_order_id: Option<&str>,
        symbol: &Symbol,
        fact: PersistedPrivateFact,
        registry: &StrategyRegistry,
    ) -> PrivateRouteReport {
        match self.resolve_order_owner(family, client_order_id, venue_order_id, symbol, registry) {
            Ok((target, _, _)) => {
                PrivateRouteReport::delivered(vec![PrivateDelivery { target, fact }])
            }
            Err(reason) => {
                let scope = if reason == ReconcileReason::IdentityConflict {
                    ReconcileScope::Account
                } else {
                    ReconcileScope::Symbol(symbol.clone())
                };
                PrivateRouteReport::reconcile(scope, reason)
            }
        }
    }

    fn route_symbol_fact(
        &self,
        symbol: &Symbol,
        fact: PersistedPrivateFact,
        registry: &StrategyRegistry,
    ) -> PrivateRouteReport {
        let Some(binding) = registry.binding_by_symbol(symbol) else {
            return PrivateRouteReport::reconcile(
                ReconcileScope::Symbol(symbol.clone()),
                ReconcileReason::UnknownOwner,
            );
        };
        PrivateRouteReport::delivered(vec![PrivateDelivery {
            target: binding.key.clone(),
            fact,
        }])
    }
}

fn ensure_route_capacity(
    routes: &BTreeMap<(NativeOrderFamily, String), OrderRouteBinding>,
    identity: &(NativeOrderFamily, String),
    route: &OrderRouteBinding,
) -> Result<(), PrivateRouterError> {
    match routes.get(identity) {
        Some(existing) if existing != route => Err(PrivateRouterError::Conflict),
        Some(_) => Ok(()),
        None if routes.len() >= MAX_ORDER_ROUTE_BINDINGS => Err(PrivateRouterError::Capacity),
        None => Ok(()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PrivateRouterError {
    #[error("private connection generation is invalid or stale")]
    Generation,
    #[error("order route binding is invalid or foreign")]
    Binding,
    #[error("order identity already maps to another owner")]
    Conflict,
    #[error("private order route capacity is exhausted")]
    Capacity,
    #[error("cancel target does not resolve to the exact owner and native order family")]
    CancelTarget,
    #[error("private evidence generation cannot advance with an incomplete fact batch")]
    IncompleteEvidenceBatch,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{
            AccountBalance, DomainEvent, EventHeader, EventId, EventSource, FactRecord, Order,
            OrderPurpose, OrderSide, OrderState, PositionSide,
        },
        runtime::{
            account::{ExchangeId, StrategyBinding, StrategyKind},
            strategy::PersistedPrivateFact,
        },
        storage::{PersistedPrivateEvidence, PrivateEvidence, PrivateEvidenceJournal},
    };

    fn test_account() -> Result<AccountKey, Box<dyn std::error::Error>> {
        Ok(AccountKey::new(ExchangeId::Binance, "portfolio")?)
    }

    fn test_binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding::new(
            StrategyInstanceKey::new(
                test_account()?,
                StrategyKind::HedgedGrid,
                "grid_sol",
                "SOL/USDT".parse()?,
            )?,
            "run_1",
            "config_1",
        )?)
    }

    fn test_owner(binding: &StrategyBinding) -> OrderOwner {
        OrderOwner {
            strategy_instance_id: binding.key.instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.key.account.exchange.as_str().to_owned(),
            account: binding.key.account.account.clone(),
            symbol: binding.key.symbol.clone(),
            purpose: OrderPurpose::Entry,
        }
    }

    fn private_fact(
        evidence: &PersistedPrivateEvidence,
        fact_index: u32,
        fact_count: u32,
    ) -> Result<PersistedPrivateFact, Box<dyn std::error::Error>> {
        let record = FactRecord {
            header: EventHeader {
                schema_version: 1,
                event_id: EventId::new(format!("private_{}_{}", evidence.sequence(), fact_index))?,
                source: EventSource::PrivateAccount,
                source_sequence: Some(evidence.sequence()),
                received_at_ms: evidence.received_at_ms(),
                generation: evidence.generation(),
            },
            event: DomainEvent::Balance(AccountBalance {
                asset: "USDT".parse()?,
                wallet_balance: Decimal::ZERO,
                available_balance: Decimal::ZERO,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            }),
        };
        Ok(PersistedPrivateFact::new_indexed(
            evidence, None, fact_index, fact_count, record,
        )?)
    }

    fn order_fact(
        evidence: &PersistedPrivateEvidence,
        fact_index: u32,
        fact_count: u32,
        binding: &StrategyBinding,
        client_order_id: &str,
        venue_order_id: &str,
    ) -> Result<PersistedPrivateFact, Box<dyn std::error::Error>> {
        let record = FactRecord {
            header: EventHeader {
                schema_version: 1,
                event_id: EventId::new(format!(
                    "private_order_{}_{}",
                    evidence.sequence(),
                    fact_index
                ))?,
                source: EventSource::PrivateAccount,
                source_sequence: Some(evidence.sequence()),
                received_at_ms: evidence.received_at_ms(),
                generation: evidence.generation(),
            },
            event: DomainEvent::Order(Order {
                time_in_force: venue_domain::FieldState::Known(Default::default()),
                order_id: venue_order_id.to_owned(),
                client_order_id: FieldState::Known(client_order_id.to_owned()),
                symbol: binding.key.symbol.clone(),
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                purpose: FieldState::Known(OrderPurpose::Entry),
                state: OrderState::New,
                quantity: Decimal::ONE,
                filled_quantity: Decimal::ZERO,
                limit_price: None,
                average_price: FieldState::Missing,
                reduce_only: false,
            }),
        };
        Ok(PersistedPrivateFact::new_indexed(
            evidence,
            Some(NativeOrderFamily::UmOrder),
            fact_index,
            fact_count,
            record,
        )?)
    }

    #[test]
    fn evidence_cursor_consumes_every_index_and_rejects_a_missing_index()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal =
            PrivateEvidenceJournal::open(directory.path().join("private_evidence.jsonl"))?;
        let first = journal.append_persisted(PrivateEvidence::new(1, 100, "first".to_owned())?)?;
        let second =
            journal.append_persisted(PrivateEvidence::new(1, 101, "second".to_owned())?)?;
        let mut router = PrivateRouter::new(test_account()?);
        router.activate_generation(1, 0)?;

        let first_zero = private_fact(&first, 0, 2)?;
        let first_one = private_fact(&first, 1, 2)?;
        assert_eq!(
            router.advance_fact_cursor(first_zero.clone()),
            FactBatchDecision::Pending
        );
        let completed = router.advance_fact_cursor(first_one.clone());
        let FactBatchDecision::Complete(facts) = completed else {
            return Err("complete evidence batch was not assembled".into());
        };
        assert_eq!(facts, vec![first_zero.clone(), first_one]);
        assert!(router.commit_fact_cursor(&facts));
        assert_eq!(
            router.advance_fact_cursor(first_zero),
            FactBatchDecision::Duplicate { pending: false }
        );
        assert_eq!(
            router.advance_fact_cursor(private_fact(&second, 1, 2)?),
            FactBatchDecision::Gap
        );
        Ok(())
    }

    #[test]
    fn owner_failure_does_not_consume_single_fact_and_retry_succeeds()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal =
            PrivateEvidenceJournal::open(directory.path().join("private_evidence.jsonl"))?;
        let evidence =
            journal.append_persisted(PrivateEvidence::new(1, 100, "order".to_owned())?)?;
        let binding = test_binding()?;
        let mut registry = StrategyRegistry::new(test_account()?);
        registry.register(binding.clone())?;
        let mut router = PrivateRouter::new(test_account()?);
        router.activate_generation(1, 0)?;
        let fact = order_fact(&evidence, 0, 1, &binding, "client_retry", "venue_retry")?;

        let failed = router.route(fact.clone(), &registry);
        assert!(failed.deliveries.is_empty());
        assert_eq!(
            failed.reconcile.map(|request| request.reason),
            Some(ReconcileReason::UnknownOwner)
        );
        assert!(!failed.pending_batch);
        assert_eq!(router.last_evidence_sequence, 0);

        router.bind_order(
            NativeOrderFamily::UmOrder,
            "client_retry".to_owned(),
            Some("venue_retry".to_owned()),
            CommandId::new("cmd_retry")?,
            test_owner(&binding),
            &registry,
        )?;
        let retried = router.route(fact.clone(), &registry);
        assert_eq!(retried.deliveries.len(), 1);
        assert!(retried.reconcile.is_none());
        assert_eq!(router.last_evidence_sequence, 1);

        let duplicate = router.route(fact, &registry);
        assert!(duplicate.duplicate);
        assert!(!duplicate.pending_batch);
        Ok(())
    }

    #[test]
    fn multi_fact_owner_failure_keeps_batch_atomic_and_retryable()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal =
            PrivateEvidenceJournal::open(directory.path().join("private_evidence.jsonl"))?;
        let evidence = journal.append_persisted(PrivateEvidence::new(
            1,
            100,
            "balance and order".to_owned(),
        )?)?;
        let binding = test_binding()?;
        let mut registry = StrategyRegistry::new(test_account()?);
        registry.register(binding.clone())?;
        let mut router = PrivateRouter::new(test_account()?);
        router.activate_generation(1, 0)?;
        let order = order_fact(&evidence, 0, 2, &binding, "client_batch", "venue_batch")?;
        let balance = private_fact(&evidence, 1, 2)?;

        let pending = router.route(order.clone(), &registry);
        assert!(pending.pending_batch);
        assert_eq!(router.last_evidence_sequence, 0);

        let failed = router.route(balance.clone(), &registry);
        assert!(failed.deliveries.is_empty());
        assert_eq!(
            failed.reconcile.map(|request| request.reason),
            Some(ReconcileReason::UnknownOwner)
        );
        assert!(failed.pending_batch);
        assert_eq!(router.last_evidence_sequence, 0);
        assert_eq!(
            router
                .pending_evidence
                .as_ref()
                .map(|pending| pending.next_fact_index),
            Some(1)
        );

        router.bind_order(
            NativeOrderFamily::UmOrder,
            "client_batch".to_owned(),
            Some("venue_batch".to_owned()),
            CommandId::new("cmd_batch")?,
            test_owner(&binding),
            &registry,
        )?;
        let retried = router.route(balance, &registry);
        assert_eq!(retried.deliveries.len(), 2);
        assert!(retried.reconcile.is_none());
        assert_eq!(router.last_evidence_sequence, 1);
        assert!(router.pending_evidence.is_none());

        let duplicate = router.route(order, &registry);
        assert!(duplicate.duplicate);
        assert!(!duplicate.pending_batch);
        Ok(())
    }

    #[test]
    fn generation_cannot_advance_with_an_incomplete_fact_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal =
            PrivateEvidenceJournal::open(directory.path().join("private_evidence.jsonl"))?;
        let evidence =
            journal.append_persisted(PrivateEvidence::new(1, 100, "first".to_owned())?)?;
        let mut router = PrivateRouter::new(test_account()?);
        router.activate_generation(1, 0)?;

        assert_eq!(
            router.advance_fact_cursor(private_fact(&evidence, 0, 2)?),
            FactBatchDecision::Pending
        );
        assert_eq!(
            router.activate_generation(2, 0),
            Err(PrivateRouterError::IncompleteEvidenceBatch)
        );
        Ok(())
    }

    #[test]
    fn combined_order_identities_must_resolve_to_one_exact_route()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = test_binding()?;
        let mut registry = StrategyRegistry::new(test_account()?);
        registry.register(binding.clone())?;
        let mut router = PrivateRouter::new(test_account()?);
        router.bind_order(
            NativeOrderFamily::UmOrder,
            "reserved".to_owned(),
            None,
            CommandId::new("cmd_reserved")?,
            test_owner(&binding),
            &registry,
        )?;
        router.bind_order(
            NativeOrderFamily::UmOrder,
            "reserved".to_owned(),
            Some("reserved_venue".to_owned()),
            CommandId::new("cmd_reserved")?,
            test_owner(&binding),
            &registry,
        )?;
        router.bind_order(
            NativeOrderFamily::UmOrder,
            "client_a".to_owned(),
            Some("venue_a".to_owned()),
            CommandId::new("cmd_a")?,
            test_owner(&binding),
            &registry,
        )?;
        router.bind_order(
            NativeOrderFamily::UmAlgo,
            "client_a".to_owned(),
            Some("venue_a".to_owned()),
            CommandId::new("cmd_algo_a")?,
            test_owner(&binding),
            &registry,
        )?;
        assert!(
            router
                .resolve_order_owner(
                    NativeOrderFamily::UmAlgo,
                    Some("client_a"),
                    Some("venue_a"),
                    &binding.key.symbol,
                    &registry,
                )
                .is_ok()
        );
        router.bind_order(
            NativeOrderFamily::UmOrder,
            "client_b".to_owned(),
            Some("venue_b".to_owned()),
            CommandId::new("cmd_b")?,
            test_owner(&binding),
            &registry,
        )?;

        assert_eq!(
            router.resolve_order_owner(
                NativeOrderFamily::UmOrder,
                Some("client_a"),
                Some("unknown_venue"),
                &binding.key.symbol,
                &registry,
            ),
            Err(ReconcileReason::IdentityConflict)
        );
        assert_eq!(
            router.resolve_order_owner(
                NativeOrderFamily::UmOrder,
                Some("client_a"),
                Some("venue_b"),
                &binding.key.symbol,
                &registry,
            ),
            Err(ReconcileReason::IdentityConflict)
        );

        assert_eq!(
            router.bind_order(
                NativeOrderFamily::UmOrder,
                "client_c".to_owned(),
                Some("venue_b".to_owned()),
                CommandId::new("cmd_c")?,
                test_owner(&binding),
                &registry,
            ),
            Err(PrivateRouterError::Conflict)
        );
        assert_eq!(
            router.resolve_order_owner(
                NativeOrderFamily::UmOrder,
                Some("client_c"),
                None,
                &binding.key.symbol,
                &registry,
            ),
            Err(ReconcileReason::UnknownOwner)
        );
        Ok(())
    }
}
