use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::{
    domain::{
        AccountOrderCapabilityEvidence, AppliedStrategyTurnReceipt, FieldState, NativeOrderFamily,
        Order, OrderPurpose, OrderSide, OrderState, Position, PositionSide, Price,
        StrategyTurnToken, Symbol,
    },
    runtime::{
        account::{
            AccountKey, PrivateRouter, ReconcileReason, StrategyInstanceKey, StrategyRegistry,
            model::validate_config_digest,
        },
        strategy::ReconciliationNotice,
    },
};

const REQUIRED_ORDER_FAMILIES: [NativeOrderFamily; 3] = [
    NativeOrderFamily::UmOrder,
    NativeOrderFamily::UmConditional,
    NativeOrderFamily::UmAlgo,
];

const SEMANTIC_FINGERPRINT_LEN: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderFamilySemanticFingerprint {
    family: NativeOrderFamily,
    digest: String,
}

impl OrderFamilySemanticFingerprint {
    pub(crate) fn verified(
        family: NativeOrderFamily,
        raw: impl Into<String>,
    ) -> Result<Self, AccountReconcilerError> {
        let raw = raw.into();
        if family == NativeOrderFamily::UmOrder
            || raw.len() != SEMANTIC_FINGERPRINT_LEN
            || !raw.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AccountReconcilerError::FamilySemantics);
        }
        Ok(Self {
            family,
            digest: raw.to_ascii_lowercase(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredCheckpointFingerprint(String);

impl DesiredCheckpointFingerprint {
    pub(crate) fn verified(raw: impl Into<String>) -> Result<Self, AccountReconcilerError> {
        let raw = raw.into();
        if raw.len() != SEMANTIC_FINGERPRINT_LEN
            || !raw.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AccountReconcilerError::DesiredAuthority);
        }
        Ok(Self(raw.to_ascii_lowercase()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsupportedOrderFamilyCapabilityReceipt {
    capability_evidence: AccountOrderCapabilityEvidence,
    connection_generation: u64,
    private_generation: u64,
    family: NativeOrderFamily,
}

impl UnsupportedOrderFamilyCapabilityReceipt {
    /// The adapter capability verifier may issue this only for a family that the selected
    /// exchange cannot expose. Current exchange support is deliberately closed and explicit.
    pub(crate) fn verified(
        account: AccountKey,
        connection_generation: u64,
        private_generation: u64,
        family: NativeOrderFamily,
    ) -> Result<Self, AccountReconcilerError> {
        let capability_evidence = AccountOrderCapabilityEvidence::for_account(account);
        if connection_generation == 0
            || private_generation == 0
            || capability_evidence.supports(family)
        {
            return Err(AccountReconcilerError::Capability);
        }
        Ok(Self {
            capability_evidence,
            connection_generation,
            private_generation,
            family,
        })
    }
}

/// One complete, signed native endpoint response. Unsupported endpoints still require an explicit
/// receipt so an adapter cannot turn an omitted read into an empty order set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedOrderFamilySnapshot {
    account: AccountKey,
    capability_evidence: AccountOrderCapabilityEvidence,
    connection_generation: u64,
    family: NativeOrderFamily,
    readback_generation: u64,
    supported: bool,
    orders: Vec<Order>,
    semantic_fingerprints: BTreeMap<String, OrderFamilySemanticFingerprint>,
}

impl SignedOrderFamilySnapshot {
    pub(crate) fn verified_complete(
        account: AccountKey,
        connection_generation: u64,
        family: NativeOrderFamily,
        readback_generation: u64,
        orders: Vec<Order>,
        semantic_fingerprints: BTreeMap<String, OrderFamilySemanticFingerprint>,
    ) -> Result<Self, AccountReconcilerError> {
        let capability_evidence = AccountOrderCapabilityEvidence::for_account(account.clone());
        if connection_generation == 0 || readback_generation == 0 {
            return Err(AccountReconcilerError::Generation);
        }
        if !capability_evidence.supports(family) {
            return Err(AccountReconcilerError::Capability);
        }
        if !signed_family_orders_valid(family, &orders, &semantic_fingerprints) {
            return Err(AccountReconcilerError::FamilySemantics);
        }
        Ok(Self {
            account,
            capability_evidence,
            connection_generation,
            family,
            readback_generation,
            supported: true,
            orders,
            semantic_fingerprints,
        })
    }

    pub(crate) fn verified_unsupported(receipt: UnsupportedOrderFamilyCapabilityReceipt) -> Self {
        Self {
            account: receipt.capability_evidence.account().clone(),
            capability_evidence: receipt.capability_evidence,
            connection_generation: receipt.connection_generation,
            family: receipt.family,
            readback_generation: receipt.private_generation,
            supported: false,
            orders: Vec::new(),
            semantic_fingerprints: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountPositionMode {
    Net,
    Hedge,
}

impl AccountPositionMode {
    fn required_sides(self) -> BTreeSet<PositionSide> {
        match self {
            Self::Net => BTreeSet::from([PositionSide::Net]),
            Self::Hedge => BTreeSet::from([PositionSide::Long, PositionSide::Short]),
        }
    }
}

/// Complete position endpoint result with the signed account mode from the same readback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedPositionSnapshot {
    account: AccountKey,
    connection_generation: u64,
    private_generation: u64,
    mode: AccountPositionMode,
    positions: Vec<Position>,
}

impl SignedPositionSnapshot {
    pub(crate) fn verified_complete(
        account: AccountKey,
        connection_generation: u64,
        private_generation: u64,
        mode: AccountPositionMode,
        positions: Vec<Position>,
    ) -> Result<Self, AccountReconcilerError> {
        let required = mode.required_sides();
        let mut seen = BTreeSet::new();
        if connection_generation == 0
            || private_generation == 0
            || positions.iter().any(|position| {
                !required.contains(&position.side)
                    || (matches!(position.side, PositionSide::Long | PositionSide::Short)
                        && position.quantity.is_sign_negative())
                    || !seen.insert((position.symbol.clone(), position.side))
            })
        {
            return Err(AccountReconcilerError::Position);
        }
        Ok(Self {
            account,
            connection_generation,
            private_generation,
            mode,
            positions,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedOpenOrders {
    account: AccountKey,
    capability_evidence: AccountOrderCapabilityEvidence,
    connection_generation: u64,
    private_generation: u64,
    family_snapshots: BTreeMap<NativeOrderFamily, SignedOrderFamilySnapshot>,
    position_snapshot: SignedPositionSnapshot,
}

impl SignedOpenOrders {
    /// Only an exchange adapter that verified every configured native endpoint may construct this
    /// receipt. The reconciler independently checks the complete canonical family set.
    pub(crate) fn verified(
        account: AccountKey,
        connection_generation: u64,
        private_generation: u64,
        family_snapshots: Vec<SignedOrderFamilySnapshot>,
        position_snapshot: SignedPositionSnapshot,
    ) -> Result<Self, AccountReconcilerError> {
        if connection_generation == 0 || private_generation == 0 {
            return Err(AccountReconcilerError::Generation);
        }
        let capability_evidence = AccountOrderCapabilityEvidence::for_account(account.clone());
        let position_mode = position_snapshot.mode;
        let mut by_family = BTreeMap::new();
        for snapshot in family_snapshots {
            if snapshot.orders.iter().any(|order| {
                !position_mode_accepts_side(position_mode, order.position_side.clone())
            }) {
                return Err(AccountReconcilerError::PositionMode);
            }
            if snapshot.account != account
                || snapshot.capability_evidence != capability_evidence
                || snapshot.connection_generation != connection_generation
                || snapshot.readback_generation != private_generation
                || (!snapshot.supported && !snapshot.orders.is_empty())
                || by_family.insert(snapshot.family, snapshot).is_some()
            {
                return Err(AccountReconcilerError::OrderFamilyCoverage);
            }
        }
        if by_family.keys().copied().collect::<BTreeSet<_>>()
            != BTreeSet::from(REQUIRED_ORDER_FAMILIES)
            || position_snapshot.account != account
            || position_snapshot.connection_generation != connection_generation
            || position_snapshot.private_generation != private_generation
        {
            return Err(AccountReconcilerError::OrderFamilyCoverage);
        }
        Ok(Self {
            account,
            capability_evidence,
            connection_generation,
            private_generation,
            family_snapshots: by_family,
            position_snapshot,
        })
    }

    #[must_use]
    pub(crate) const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub(crate) const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub(crate) const fn capability_evidence(&self) -> &AccountOrderCapabilityEvidence {
        &self.capability_evidence
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredOrder {
    family: NativeOrderFamily,
    client_order_id: String,
    purpose: OrderPurpose,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Option<Decimal>,
    limit_price: Option<Price>,
    reduce_only: bool,
    semantic_fingerprint: Option<OrderFamilySemanticFingerprint>,
}

impl DesiredOrder {
    /// Durable strategy-checkpoint recovery and applied-turn adapters are the only intended
    /// callers. Strategies cannot reduce desired state to a forgeable set of native IDs.
    #[allow(
        clippy::too_many_arguments,
        reason = "desired authority must bind every canonical order semantic"
    )]
    pub(crate) fn verified(
        family: NativeOrderFamily,
        client_order_id: impl Into<String>,
        purpose: OrderPurpose,
        side: OrderSide,
        position_side: PositionSide,
        quantity: Option<Decimal>,
        limit_price: Option<Price>,
        reduce_only: bool,
        semantic_fingerprint: Option<OrderFamilySemanticFingerprint>,
    ) -> Result<Self, AccountReconcilerError> {
        let client_order_id = client_order_id.into();
        if client_order_id.trim().is_empty()
            || !side_matches_purpose(family, purpose, position_side, side, reduce_only)
            || !desired_family_semantics_valid(
                family,
                purpose,
                quantity,
                limit_price,
                reduce_only,
                semantic_fingerprint.as_ref(),
            )
        {
            return Err(AccountReconcilerError::DesiredSemantics);
        }
        Ok(Self {
            family,
            client_order_id,
            purpose,
            side,
            position_side,
            quantity,
            limit_price,
            reduce_only,
            semantic_fingerprint,
        })
    }

    fn identity(&self) -> (NativeOrderFamily, String) {
        (self.family, self.client_order_id.clone())
    }

    fn matches(
        &self,
        family: NativeOrderFamily,
        order: &Order,
        semantic_fingerprint: Option<&OrderFamilySemanticFingerprint>,
    ) -> bool {
        self.family == family
            && order.side == self.side
            && order.position_side == FieldState::Known(self.position_side)
            && order.purpose == FieldState::Known(self.purpose)
            && order.reduce_only == self.reduce_only
            && match family {
                NativeOrderFamily::UmOrder => {
                    self.quantity == Some(order.quantity)
                        && order.limit_price == self.limit_price
                        && self.semantic_fingerprint.is_none()
                        && semantic_fingerprint.is_none()
                }
                NativeOrderFamily::UmConditional => {
                    self.quantity.is_none()
                        && order.limit_price.is_none()
                        && self.semantic_fingerprint.as_ref() == semantic_fingerprint
                }
                NativeOrderFamily::UmAlgo => {
                    self.quantity == Some(order.quantity)
                        && order.limit_price.is_none()
                        && self.semantic_fingerprint.as_ref() == semantic_fingerprint
                }
            }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredDesiredOrdersReceipt {
    authority: StrategyTurnToken,
    checkpoint_fingerprint: DesiredCheckpointFingerprint,
}

impl RecoveredDesiredOrdersReceipt {
    pub(crate) fn verified_checkpoint(
        target: StrategyInstanceKey,
        connection_generation: u64,
        private_generation: u64,
        config_digest: impl Into<String>,
        config_epoch: u64,
        turn_sequence: u64,
        checkpoint_fingerprint: DesiredCheckpointFingerprint,
    ) -> Result<Self, AccountReconcilerError> {
        let authority = StrategyTurnToken::issue(
            target,
            connection_generation,
            private_generation,
            config_digest.into(),
            config_epoch,
            turn_sequence,
        )
        .map_err(|_| AccountReconcilerError::DesiredAuthority)?;
        Ok(Self {
            authority,
            checkpoint_fingerprint,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DesiredOrderSet {
    authority: StrategyTurnToken,
    authority_fingerprint: Option<DesiredCheckpointFingerprint>,
    orders: BTreeMap<(NativeOrderFamily, String), DesiredOrder>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredOrderSets {
    position_mode: AccountPositionMode,
    by_instance: BTreeMap<StrategyInstanceKey, DesiredOrderSet>,
}

impl DesiredOrderSets {
    #[must_use]
    pub(crate) fn new(position_mode: AccountPositionMode) -> Self {
        Self {
            position_mode,
            by_instance: BTreeMap::new(),
        }
    }

    pub(crate) fn set_recovered(
        &mut self,
        receipt: RecoveredDesiredOrdersReceipt,
        orders: impl IntoIterator<Item = DesiredOrder>,
    ) -> Result<(), AccountReconcilerError> {
        let key = receipt.authority.target().clone();
        self.insert_authorized(
            receipt.authority,
            Some(receipt.checkpoint_fingerprint),
            orders,
        )?;
        debug_assert!(self.by_instance.contains_key(&key));
        Ok(())
    }

    fn insert_authorized(
        &mut self,
        authority: StrategyTurnToken,
        authority_fingerprint: Option<DesiredCheckpointFingerprint>,
        orders: impl IntoIterator<Item = DesiredOrder>,
    ) -> Result<(), AccountReconcilerError> {
        if authority.connection_generation() == 0
            || authority.config_epoch() == 0
            || authority.turn_sequence() == 0
            || validate_config_digest(authority.config_digest()).is_err()
        {
            return Err(AccountReconcilerError::DesiredAuthority);
        }
        let mut desired = BTreeMap::new();
        for order in orders {
            if !position_mode_accepts_side(
                self.position_mode,
                FieldState::Known(order.position_side),
            ) {
                return Err(AccountReconcilerError::PositionMode);
            }
            if desired.insert(order.identity(), order).is_some() {
                return Err(AccountReconcilerError::DesiredIdentity);
            }
        }
        if self.by_instance.contains_key(authority.target()) {
            return Err(AccountReconcilerError::DesiredIdentity);
        }
        self.by_instance.insert(
            authority.target().clone(),
            DesiredOrderSet {
                authority,
                authority_fingerprint,
                orders: desired,
            },
        );
        Ok(())
    }

    pub(crate) fn set_from_applied_turn(
        &mut self,
        receipt: &AppliedStrategyTurnReceipt,
        orders: impl IntoIterator<Item = DesiredOrder>,
    ) -> Result<(), AccountReconcilerError> {
        let token = receipt.token();
        self.insert_authorized(token.clone(), None, orders)
    }

    fn bound_get(&self, key: &StrategyInstanceKey) -> Option<&DesiredOrderSet> {
        self.by_instance.get(key)
    }

    #[must_use]
    pub(crate) const fn position_mode(&self) -> AccountPositionMode {
        self.position_mode
    }

    pub(crate) fn verify_authority(
        &self,
        registry: &StrategyRegistry,
        connection_generation: u64,
        private_generation: u64,
    ) -> Result<(), AccountReconcilerError> {
        if connection_generation == 0
            || private_generation == 0
            || self.by_instance.len() != registry.registrations().count()
            || self
                .by_instance
                .keys()
                .any(|key| registry.registration(key).is_none())
            || registry.registrations().any(|registration| {
                self.bound_get(&registration.binding.key)
                    .is_none_or(|desired| {
                        desired.authority.target() != &registration.binding.key
                            || desired.authority.connection_generation() != connection_generation
                            || desired.authority.private_generation() > private_generation
                            || desired.authority.config_digest()
                                != registration.binding.config_digest
                            || desired.authority.config_epoch() != registration.config_epoch
                            || desired.authority.turn_sequence() == 0
                    })
            })
        {
            return Err(AccountReconcilerError::DesiredAuthority);
        }
        Ok(())
    }

    /// Runtime-level authority is stricter than structural snapshot validation: after the first
    /// post-recovery readback, every desired set must be the output of that instance's latest
    /// applied Actor turn. A recovered checkpoint is accepted only for the first reconciliation
    /// of a connection, before any reconciliation notice has been issued.
    pub(crate) fn verify_runtime_authority(
        &self,
        registry: &StrategyRegistry,
        latest_applied: &BTreeMap<StrategyInstanceKey, StrategyTurnToken>,
        connection_generation: u64,
        private_generation: u64,
        allow_recovered_checkpoint: bool,
    ) -> Result<(), AccountReconcilerError> {
        self.verify_authority(registry, connection_generation, private_generation)?;
        for registration in registry.registrations() {
            let desired = self
                .bound_get(&registration.binding.key)
                .ok_or(AccountReconcilerError::DesiredAuthority)?;
            match latest_applied.get(&registration.binding.key) {
                Some(applied) if applied == &desired.authority => {}
                None if allow_recovered_checkpoint && desired.authority_fingerprint.is_some() => {}
                _ => return Err(AccountReconcilerError::DesiredAuthority),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceReconciliation {
    pub target: StrategyInstanceKey,
    pub config_digest: String,
    pub config_epoch: u64,
    pub notice: ReconciliationNotice,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedSignedOrder {
    pub symbol: Symbol,
    pub venue_order_id: String,
    pub reason: ReconcileReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountReconciliationReport {
    pub connection_generation: u64,
    pub private_generation: u64,
    pub instances: Vec<InstanceReconciliation>,
    pub unresolved: Vec<UnresolvedSignedOrder>,
    pub flat_by_symbol: BTreeMap<Symbol, bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SignedActualOrder {
    order: Order,
    semantic_fingerprint: Option<OrderFamilySemanticFingerprint>,
}

/// Allocates signed orders only through the durable owner router and compares full semantics.
pub(crate) fn reconcile_open_orders(
    router: &PrivateRouter,
    registry: &StrategyRegistry,
    capability_evidence: &AccountOrderCapabilityEvidence,
    desired: &DesiredOrderSets,
    signed: SignedOpenOrders,
) -> Result<AccountReconciliationReport, AccountReconcilerError> {
    if signed.connection_generation == 0
        || signed.private_generation == 0
        || signed.account != *registry.account()
    {
        return Err(AccountReconcilerError::Generation);
    }
    if signed.capability_evidence != *capability_evidence {
        return Err(AccountReconcilerError::Capability);
    }
    if desired.position_mode() != signed.position_snapshot.mode {
        return Err(AccountReconcilerError::PositionMode);
    }
    let signed_families: BTreeSet<_> = signed.family_snapshots.keys().copied().collect();
    if signed_families != BTreeSet::from(REQUIRED_ORDER_FAMILIES) {
        return Err(AccountReconcilerError::OrderFamilyCoverage);
    }
    desired.verify_authority(
        registry,
        signed.connection_generation,
        signed.private_generation,
    )?;

    let mut actual_by_instance: BTreeMap<
        StrategyInstanceKey,
        BTreeMap<(NativeOrderFamily, String), SignedActualOrder>,
    > = registry
        .active_bindings()
        .map(|binding| (binding.key.clone(), BTreeMap::new()))
        .collect();
    let mut seen_venue_ids = BTreeSet::new();
    let mut unresolved = Vec::new();
    let registered_symbols: BTreeSet<Symbol> = registry
        .registrations()
        .map(|registration| registration.binding.key.symbol.clone())
        .collect();
    let required_position_sides = signed.position_snapshot.mode.required_sides();
    let expected_position_legs: BTreeSet<(Symbol, PositionSide)> = registered_symbols
        .iter()
        .flat_map(|symbol| {
            required_position_sides
                .iter()
                .map(move |side| (symbol.clone(), *side))
        })
        .collect();
    let mut seen_position_legs = BTreeSet::new();
    let mut flat_by_symbol: BTreeMap<Symbol, bool> = BTreeMap::new();

    for position in signed.position_snapshot.positions {
        if !registered_symbols.contains(&position.symbol)
            || !required_position_sides.contains(&position.side)
            || (matches!(position.side, PositionSide::Long | PositionSide::Short)
                && position.quantity.is_sign_negative())
            || !seen_position_legs.insert((position.symbol.clone(), position.side))
        {
            return Err(AccountReconcilerError::Position);
        }
        flat_by_symbol
            .entry(position.symbol)
            .and_modify(|flat| *flat &= position.quantity.is_zero())
            .or_insert_with(|| position.quantity.is_zero());
    }
    if seen_position_legs != expected_position_legs
        || flat_by_symbol.len() != registered_symbols.len()
    {
        return Err(AccountReconcilerError::PositionCoverage);
    }

    for (family, mut snapshot) in signed.family_snapshots {
        for order in snapshot.orders {
            if !seen_venue_ids.insert((family, order.order_id.clone())) {
                return Err(AccountReconcilerError::Order);
            }
            let semantic_fingerprint = snapshot.semantic_fingerprints.remove(&order.order_id);
            let client_order_id = match &order.client_order_id {
                FieldState::Known(value) => Some(value.as_str()),
                _ => None,
            };
            match router.resolve_order_owner(
                family,
                client_order_id,
                Some(order.order_id.as_str()),
                &order.symbol,
                registry,
            ) {
                Ok((target, stable_client_order_id, owner)) => {
                    if order.purpose != FieldState::Known(owner.purpose) {
                        unresolved.push(UnresolvedSignedOrder {
                            symbol: order.symbol,
                            venue_order_id: order.order_id,
                            reason: ReconcileReason::IdentityConflict,
                        });
                        continue;
                    }
                    let actual = actual_by_instance
                        .get_mut(&target)
                        .ok_or(AccountReconcilerError::Registry)?;
                    if actual
                        .insert(
                            (family, stable_client_order_id),
                            SignedActualOrder {
                                order,
                                semantic_fingerprint,
                            },
                        )
                        .is_some()
                    {
                        return Err(AccountReconcilerError::Order);
                    }
                }
                Err(reason) => unresolved.push(UnresolvedSignedOrder {
                    symbol: order.symbol,
                    venue_order_id: order.order_id,
                    reason,
                }),
            }
        }
    }

    let instances: Vec<InstanceReconciliation> = actual_by_instance
        .into_iter()
        .map(
            |(target, actual)| -> Result<InstanceReconciliation, AccountReconcilerError> {
                let desired_binding = desired
                    .bound_get(&target)
                    .ok_or(AccountReconcilerError::Registry)?;
                let desired_orders = &desired_binding.orders;
                let desired_ids: BTreeSet<_> = desired_orders.keys().cloned().collect();
                let actual_ids: BTreeSet<_> = actual.keys().cloned().collect();
                let missing_client_order_ids = desired_ids
                    .difference(&actual_ids)
                    .map(canonical_identity)
                    .collect();
                let unexpected_client_order_ids = actual_ids
                    .difference(&desired_ids)
                    .map(canonical_identity)
                    .collect();
                let mismatched_client_order_ids = desired_ids
                    .intersection(&actual_ids)
                    .filter(|identity| {
                        let Some(desired) = desired_orders.get(*identity) else {
                            return true;
                        };
                        let Some(actual) = actual.get(*identity) else {
                            return true;
                        };
                        !desired.matches(
                            identity.0,
                            &actual.order,
                            actual.semantic_fingerprint.as_ref(),
                        )
                    })
                    .map(canonical_identity)
                    .collect();
                Ok(InstanceReconciliation {
                    target,
                    config_digest: desired_binding.authority.config_digest().to_owned(),
                    config_epoch: desired_binding.authority.config_epoch(),
                    notice: ReconciliationNotice {
                        private_generation: signed.private_generation,
                        desired_open_orders: desired_orders.len(),
                        actual_open_orders: actual.len(),
                        missing_client_order_ids,
                        unexpected_client_order_ids,
                        mismatched_client_order_ids,
                    },
                })
            },
        )
        .collect::<Result<_, _>>()?;

    Ok(AccountReconciliationReport {
        connection_generation: signed.connection_generation,
        private_generation: signed.private_generation,
        instances,
        unresolved,
        flat_by_symbol,
    })
}

fn canonical_identity(identity: &(NativeOrderFamily, String)) -> String {
    format!("{:?}:{}", identity.0, identity.1)
}

fn signed_family_orders_valid(
    family: NativeOrderFamily,
    orders: &[Order],
    semantic_fingerprints: &BTreeMap<String, OrderFamilySemanticFingerprint>,
) -> bool {
    let mut venue_ids = BTreeSet::new();
    let order_ids: BTreeSet<_> = orders.iter().map(|order| order.order_id.clone()).collect();
    let fingerprint_ids: BTreeSet<_> = semantic_fingerprints.keys().cloned().collect();
    let fingerprints_complete = match family {
        NativeOrderFamily::UmOrder => fingerprint_ids.is_empty(),
        NativeOrderFamily::UmConditional | NativeOrderFamily::UmAlgo => {
            fingerprint_ids == order_ids
                && semantic_fingerprints
                    .values()
                    .all(|fingerprint| fingerprint.family == family)
        }
    };
    fingerprints_complete
        && order_ids.len() == orders.len()
        && orders.iter().all(|order| {
            venue_ids.insert(order.order_id.clone()) && signed_order_semantics_valid(family, order)
        })
}

fn signed_order_semantics_valid(family: NativeOrderFamily, order: &Order) -> bool {
    let (FieldState::Known(position_side), FieldState::Known(purpose)) =
        (order.position_side.clone(), order.purpose.clone())
    else {
        return false;
    };
    if order.order_id.trim().is_empty()
        || !matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
        || !side_matches_purpose(
            family,
            purpose,
            position_side,
            order.side,
            order.reduce_only,
        )
        || order.quantity.is_sign_negative()
        || order.filled_quantity.is_sign_negative()
        || order.filled_quantity > order.quantity
    {
        return false;
    }
    match family {
        NativeOrderFamily::UmOrder => {
            order.validate().is_ok()
                && order.limit_price.is_some()
                && !matches!(purpose, OrderPurpose::ExposureTakeProfit)
        }
        NativeOrderFamily::UmConditional => {
            purpose == OrderPurpose::Protection && order.limit_price.is_none() && !order.reduce_only
        }
        NativeOrderFamily::UmAlgo => {
            matches!(purpose, OrderPurpose::Protection | OrderPurpose::TakeProfit)
                && order.validate().is_ok()
                && order.quantity.is_sign_positive()
                && !order.quantity.is_zero()
                && order.limit_price.is_none()
                && order.reduce_only
        }
    }
}

fn desired_family_semantics_valid(
    family: NativeOrderFamily,
    purpose: OrderPurpose,
    quantity: Option<Decimal>,
    limit_price: Option<Price>,
    reduce_only: bool,
    semantic_fingerprint: Option<&OrderFamilySemanticFingerprint>,
) -> bool {
    match family {
        NativeOrderFamily::UmOrder => {
            quantity.is_some_and(|value| value.is_sign_positive() && !value.is_zero())
                && limit_price.is_some()
                && semantic_fingerprint.is_none()
                && !matches!(purpose, OrderPurpose::ExposureTakeProfit)
                && reduce_only
                    == matches!(
                        purpose,
                        OrderPurpose::Protection | OrderPurpose::TakeProfit | OrderPurpose::Reduce
                    )
        }
        NativeOrderFamily::UmConditional => {
            purpose == OrderPurpose::Protection
                && quantity.is_none()
                && limit_price.is_none()
                && !reduce_only
                && semantic_fingerprint.is_some_and(|fingerprint| fingerprint.family == family)
        }
        NativeOrderFamily::UmAlgo => {
            matches!(purpose, OrderPurpose::Protection | OrderPurpose::TakeProfit)
                && quantity.is_some_and(|value| value.is_sign_positive() && !value.is_zero())
                && limit_price.is_none()
                && reduce_only
                && semantic_fingerprint.is_some_and(|fingerprint| fingerprint.family == family)
        }
    }
}

fn position_mode_accepts_side(
    mode: AccountPositionMode,
    position_side: FieldState<PositionSide>,
) -> bool {
    matches!(
        (mode, position_side),
        (
            AccountPositionMode::Net,
            FieldState::Known(PositionSide::Net)
        ) | (
            AccountPositionMode::Hedge,
            FieldState::Known(PositionSide::Long | PositionSide::Short)
        )
    )
}

const fn side_matches_purpose(
    family: NativeOrderFamily,
    purpose: OrderPurpose,
    position_side: PositionSide,
    side: OrderSide,
    reduce_only: bool,
) -> bool {
    match (purpose, position_side) {
        (OrderPurpose::Entry, PositionSide::Long) => matches!(side, OrderSide::Buy),
        (OrderPurpose::Entry, PositionSide::Short) => matches!(side, OrderSide::Sell),
        (
            OrderPurpose::Protection | OrderPurpose::TakeProfit | OrderPurpose::Reduce,
            PositionSide::Long,
        ) => matches!(side, OrderSide::Sell),
        (
            OrderPurpose::Protection | OrderPurpose::TakeProfit | OrderPurpose::Reduce,
            PositionSide::Short,
        ) => matches!(side, OrderSide::Buy),
        (OrderPurpose::ExposureTakeProfit, PositionSide::Long) => matches!(side, OrderSide::Sell),
        (OrderPurpose::ExposureTakeProfit, PositionSide::Short) => matches!(side, OrderSide::Buy),
        (OrderPurpose::Entry, PositionSide::Net) => {
            matches!(family, NativeOrderFamily::UmOrder) && !reduce_only
        }
        (OrderPurpose::Protection, PositionSide::Net) => {
            reduce_only || matches!(family, NativeOrderFamily::UmConditional)
        }
        (
            OrderPurpose::TakeProfit | OrderPurpose::Reduce | OrderPurpose::ExposureTakeProfit,
            PositionSide::Net,
        ) => reduce_only,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AccountReconcilerError {
    #[error("signed account generation must be positive")]
    Generation,
    #[error("signed order snapshot does not cover every canonical native order family")]
    OrderFamilyCoverage,
    #[error("native order family capability is invalid for this exchange account generation")]
    Capability,
    #[error("native order family semantics or fingerprint coverage is invalid")]
    FamilySemantics,
    #[error("desired and signed orders do not match the signed account position mode")]
    PositionMode,
    #[error("desired client order identity is empty or duplicated")]
    DesiredIdentity,
    #[error("desired order semantics are invalid")]
    DesiredSemantics,
    #[error("desired orders are not bound to a positive current configuration epoch and digest")]
    DesiredBinding,
    #[error("desired orders are not bound to the current account/applied-turn authority")]
    DesiredAuthority,
    #[error("signed open-order set is invalid or duplicated")]
    Order,
    #[error("signed position set contains an invalid, duplicate, or unregistered symbol/side leg")]
    Position,
    #[error(
        "signed position set does not exactly cover every required leg of every strategy symbol"
    )]
    PositionCoverage,
    #[error("signed order resolved outside the active strategy registry")]
    Registry,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::domain::{ExchangeId, StrategyBinding, StrategyKind};

    use super::*;

    fn make_account(exchange: ExchangeId) -> Result<AccountKey, Box<dyn Error>> {
        Ok(AccountKey::new(exchange, "futures_main")?)
    }

    fn strategy_key(account: AccountKey) -> Result<StrategyInstanceKey, Box<dyn Error>> {
        Ok(StrategyInstanceKey::new(
            account,
            StrategyKind::HedgedGrid,
            "grid_1",
            "BTC/USDT".parse()?,
        )?)
    }

    fn semantic_fingerprint(
        family: NativeOrderFamily,
        byte: char,
    ) -> Result<OrderFamilySemanticFingerprint, AccountReconcilerError> {
        OrderFamilySemanticFingerprint::verified(
            family,
            byte.to_string().repeat(SEMANTIC_FINGERPRINT_LEN),
        )
    }

    fn signed_order(
        family: NativeOrderFamily,
        order_id: &str,
        purpose: OrderPurpose,
        position_side: PositionSide,
        side: OrderSide,
    ) -> Result<Order, Box<dyn Error>> {
        let (quantity, limit_price, reduce_only) = match family {
            NativeOrderFamily::UmOrder => (
                Decimal::ONE,
                Some(Price::new(Decimal::new(100, 0))?),
                purpose != OrderPurpose::Entry,
            ),
            NativeOrderFamily::UmConditional => (Decimal::ZERO, None, false),
            NativeOrderFamily::UmAlgo => (Decimal::ONE, None, true),
        };
        Ok(Order {
            order_id: order_id.to_owned(),
            client_order_id: FieldState::Known(format!("client_{order_id}")),
            symbol: "BTC/USDT".parse()?,
            side,
            position_side: FieldState::Known(position_side),
            purpose: FieldState::Known(purpose),
            state: OrderState::New,
            quantity,
            filled_quantity: Decimal::ZERO,
            limit_price,
            average_price: FieldState::Missing,
            reduce_only,
        })
    }

    fn signed_families_with_regular(
        account: &AccountKey,
        connection_generation: u64,
        private_generation: u64,
        regular_orders: Vec<Order>,
    ) -> Result<Vec<SignedOrderFamilySnapshot>, AccountReconcilerError> {
        Ok(vec![
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                connection_generation,
                NativeOrderFamily::UmOrder,
                private_generation,
                regular_orders,
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
                account.clone(),
                connection_generation,
                NativeOrderFamily::UmAlgo,
                private_generation,
                Vec::new(),
                BTreeMap::new(),
            )?,
        ])
    }

    #[test]
    fn unsupported_family_receipt_is_bound_to_account_family_and_generations()
    -> Result<(), Box<dyn Error>> {
        let account = make_account(ExchangeId::Gate)?;
        assert_eq!(
            UnsupportedOrderFamilyCapabilityReceipt::verified(
                account.clone(),
                0,
                9,
                NativeOrderFamily::UmConditional,
            ),
            Err(AccountReconcilerError::Capability)
        );
        assert_eq!(
            UnsupportedOrderFamilyCapabilityReceipt::verified(
                account.clone(),
                4,
                9,
                NativeOrderFamily::UmOrder,
            ),
            Err(AccountReconcilerError::Capability)
        );
        assert_eq!(
            UnsupportedOrderFamilyCapabilityReceipt::verified(
                make_account(ExchangeId::Binance)?,
                4,
                9,
                NativeOrderFamily::UmAlgo,
            ),
            Err(AccountReconcilerError::Capability)
        );
        assert_eq!(
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                4,
                NativeOrderFamily::UmAlgo,
                9,
                Vec::new(),
                BTreeMap::new(),
            ),
            Err(AccountReconcilerError::Capability)
        );

        let regular = SignedOrderFamilySnapshot::verified_complete(
            account.clone(),
            4,
            NativeOrderFamily::UmOrder,
            9,
            Vec::new(),
            BTreeMap::new(),
        )?;
        let conditional = SignedOrderFamilySnapshot::verified_unsupported(
            UnsupportedOrderFamilyCapabilityReceipt::verified(
                account.clone(),
                4,
                9,
                NativeOrderFamily::UmConditional,
            )?,
        );
        let algo = SignedOrderFamilySnapshot::verified_unsupported(
            UnsupportedOrderFamilyCapabilityReceipt::verified(
                account.clone(),
                4,
                9,
                NativeOrderFamily::UmAlgo,
            )?,
        );
        let positions = SignedPositionSnapshot::verified_complete(
            account.clone(),
            4,
            9,
            AccountPositionMode::Hedge,
            Vec::new(),
        )?;
        let signed =
            SignedOpenOrders::verified(account, 4, 9, vec![regular, conditional, algo], positions)?;
        assert_eq!(signed.connection_generation(), 4);
        assert_eq!(signed.private_generation(), 9);
        Ok(())
    }

    #[test]
    fn signed_position_snapshot_derives_and_enforces_mode_legs() -> Result<(), Box<dyn Error>> {
        let account = make_account(ExchangeId::Binance)?;
        let symbol: Symbol = "BTC/USDT".parse()?;
        let long = Position {
            symbol: symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: None,
        };
        let short = Position {
            symbol: symbol.clone(),
            side: PositionSide::Short,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        };
        assert!(
            SignedPositionSnapshot::verified_complete(
                account.clone(),
                2,
                3,
                AccountPositionMode::Hedge,
                vec![long.clone(), short],
            )
            .is_ok()
        );
        assert_eq!(
            SignedPositionSnapshot::verified_complete(
                account.clone(),
                2,
                3,
                AccountPositionMode::Net,
                vec![long.clone()],
            ),
            Err(AccountReconcilerError::Position)
        );
        assert_eq!(
            SignedPositionSnapshot::verified_complete(
                account,
                2,
                3,
                AccountPositionMode::Hedge,
                vec![long.clone(), long],
            ),
            Err(AccountReconcilerError::Position)
        );
        Ok(())
    }

    #[test]
    fn desired_and_signed_orders_are_bound_to_one_account_position_mode()
    -> Result<(), Box<dyn Error>> {
        let account = make_account(ExchangeId::Binance)?;
        let net_order = signed_order(
            NativeOrderFamily::UmOrder,
            "net_entry",
            OrderPurpose::Entry,
            PositionSide::Net,
            OrderSide::Sell,
        )?;
        let net_position = Position {
            symbol: "BTC/USDT".parse()?,
            side: PositionSide::Net,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        };
        let net_positions = SignedPositionSnapshot::verified_complete(
            account.clone(),
            2,
            3,
            AccountPositionMode::Net,
            vec![net_position],
        )?;
        assert!(
            SignedOpenOrders::verified(
                account.clone(),
                2,
                3,
                signed_families_with_regular(&account, 2, 3, vec![net_order.clone()])?,
                net_positions,
            )
            .is_ok()
        );

        let hedge_positions = SignedPositionSnapshot::verified_complete(
            account.clone(),
            2,
            3,
            AccountPositionMode::Hedge,
            Vec::new(),
        )?;
        assert_eq!(
            SignedOpenOrders::verified(
                account.clone(),
                2,
                3,
                signed_families_with_regular(&account, 2, 3, vec![net_order])?,
                hedge_positions,
            ),
            Err(AccountReconcilerError::PositionMode)
        );

        let net_desired = DesiredOrder::verified(
            NativeOrderFamily::UmOrder,
            "net_desired",
            OrderPurpose::Entry,
            OrderSide::Sell,
            PositionSide::Net,
            Some(Decimal::ONE),
            Some(Price::new(Decimal::new(100, 0))?),
            false,
            None,
        )?;
        let closing_net = DesiredOrder::verified(
            NativeOrderFamily::UmOrder,
            "net_reduce",
            OrderPurpose::Reduce,
            OrderSide::Buy,
            PositionSide::Net,
            Some(Decimal::ONE),
            Some(Price::new(Decimal::new(99, 0))?),
            true,
            None,
        )?;
        assert_eq!(
            DesiredOrder::verified(
                NativeOrderFamily::UmOrder,
                "unproven_net_reduce",
                OrderPurpose::Reduce,
                OrderSide::Buy,
                PositionSide::Net,
                Some(Decimal::ONE),
                Some(Price::new(Decimal::new(99, 0))?),
                false,
                None,
            ),
            Err(AccountReconcilerError::DesiredSemantics)
        );
        let key = strategy_key(account.clone())?;
        let token = StrategyTurnToken::issue(key, 2, 3, "config_1".to_owned(), 1, 1)?;
        let applied = AppliedStrategyTurnReceipt::test_persisted(token);
        let mut net_sets = DesiredOrderSets::new(AccountPositionMode::Net);
        net_sets.set_from_applied_turn(&applied, [net_desired.clone(), closing_net])?;
        let mut hedge_sets = DesiredOrderSets::new(AccountPositionMode::Hedge);
        assert_eq!(
            hedge_sets.set_from_applied_turn(&applied, [net_desired]),
            Err(AccountReconcilerError::PositionMode)
        );
        Ok(())
    }

    #[test]
    fn conditional_and_algo_snapshots_require_exact_semantic_fingerprints()
    -> Result<(), Box<dyn Error>> {
        let account = make_account(ExchangeId::Binance)?;
        let conditional = signed_order(
            NativeOrderFamily::UmConditional,
            "conditional_1",
            OrderPurpose::Protection,
            PositionSide::Long,
            OrderSide::Sell,
        )?;
        assert_eq!(
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                4,
                NativeOrderFamily::UmConditional,
                7,
                vec![conditional.clone()],
                BTreeMap::new(),
            ),
            Err(AccountReconcilerError::FamilySemantics)
        );
        let fingerprints = BTreeMap::from([(
            conditional.order_id.clone(),
            semantic_fingerprint(NativeOrderFamily::UmConditional, 'a')?,
        )]);
        assert!(
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                4,
                NativeOrderFamily::UmConditional,
                7,
                vec![conditional],
                fingerprints,
            )
            .is_ok()
        );

        let algo = signed_order(
            NativeOrderFamily::UmAlgo,
            "algo_1",
            OrderPurpose::TakeProfit,
            PositionSide::Short,
            OrderSide::Buy,
        )?;
        let algo_fingerprints = BTreeMap::from([(
            algo.order_id.clone(),
            semantic_fingerprint(NativeOrderFamily::UmAlgo, 'b')?,
        )]);
        assert!(
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                4,
                NativeOrderFamily::UmAlgo,
                7,
                vec![algo.clone()],
                algo_fingerprints,
            )
            .is_ok()
        );
        let mut unsafe_algo = algo.clone();
        unsafe_algo.reduce_only = false;
        assert_eq!(
            SignedOrderFamilySnapshot::verified_complete(
                account.clone(),
                4,
                NativeOrderFamily::UmAlgo,
                7,
                vec![unsafe_algo],
                BTreeMap::from([(
                    algo.order_id.clone(),
                    semantic_fingerprint(NativeOrderFamily::UmAlgo, 'b')?,
                )]),
            ),
            Err(AccountReconcilerError::FamilySemantics)
        );
        let wrong_id = BTreeMap::from([(
            "algo_2".to_owned(),
            semantic_fingerprint(NativeOrderFamily::UmAlgo, 'b')?,
        )]);
        assert_eq!(
            SignedOrderFamilySnapshot::verified_complete(
                account,
                4,
                NativeOrderFamily::UmAlgo,
                7,
                vec![algo],
                wrong_id,
            ),
            Err(AccountReconcilerError::FamilySemantics)
        );
        Ok(())
    }

    #[test]
    fn desired_order_rejects_cross_family_or_direction_semantics() -> Result<(), Box<dyn Error>> {
        let price = Price::new(Decimal::new(100, 0))?;
        assert!(
            DesiredOrder::verified(
                NativeOrderFamily::UmOrder,
                "entry_1",
                OrderPurpose::Entry,
                OrderSide::Buy,
                PositionSide::Long,
                Some(Decimal::ONE),
                Some(price),
                false,
                None,
            )
            .is_ok()
        );
        assert_eq!(
            DesiredOrder::verified(
                NativeOrderFamily::UmOrder,
                "wrong_side",
                OrderPurpose::Entry,
                OrderSide::Sell,
                PositionSide::Long,
                Some(Decimal::ONE),
                Some(price),
                false,
                None,
            ),
            Err(AccountReconcilerError::DesiredSemantics)
        );
        assert_eq!(
            DesiredOrder::verified(
                NativeOrderFamily::UmConditional,
                "missing_trigger_semantics",
                OrderPurpose::Protection,
                OrderSide::Sell,
                PositionSide::Long,
                None,
                None,
                false,
                None,
            ),
            Err(AccountReconcilerError::DesiredSemantics)
        );
        assert!(
            DesiredOrder::verified(
                NativeOrderFamily::UmConditional,
                "conditional_1",
                OrderPurpose::Protection,
                OrderSide::Sell,
                PositionSide::Long,
                None,
                None,
                false,
                Some(semantic_fingerprint(NativeOrderFamily::UmConditional, 'c')?),
            )
            .is_ok()
        );
        assert!(
            DesiredOrder::verified(
                NativeOrderFamily::UmAlgo,
                "algo_1",
                OrderPurpose::TakeProfit,
                OrderSide::Buy,
                PositionSide::Short,
                Some(Decimal::ONE),
                None,
                true,
                Some(semantic_fingerprint(NativeOrderFamily::UmAlgo, 'd')?),
            )
            .is_ok()
        );
        assert_eq!(
            DesiredOrder::verified(
                NativeOrderFamily::UmAlgo,
                "algo_not_reduce_only",
                OrderPurpose::Protection,
                OrderSide::Sell,
                PositionSide::Long,
                Some(Decimal::ONE),
                None,
                false,
                Some(semantic_fingerprint(NativeOrderFamily::UmAlgo, 'e')?),
            ),
            Err(AccountReconcilerError::DesiredSemantics)
        );
        Ok(())
    }

    #[test]
    fn desired_sets_verify_current_applied_or_recovered_authority() -> Result<(), Box<dyn Error>> {
        let account = make_account(ExchangeId::Binance)?;
        let key = strategy_key(account.clone())?;
        let binding = StrategyBinding::new(key.clone(), "run_1", "config_1")?;
        let mut registry = StrategyRegistry::new(account);
        registry.register(binding)?;

        let token = StrategyTurnToken::issue(key.clone(), 4, 6, "config_1".to_owned(), 1, 8)?;
        let applied = AppliedStrategyTurnReceipt::test_persisted(token);
        let mut applied_sets = DesiredOrderSets::new(AccountPositionMode::Hedge);
        applied_sets.set_from_applied_turn(&applied, Vec::<DesiredOrder>::new())?;
        applied_sets.verify_authority(&registry, 4, 6)?;
        assert_eq!(
            applied_sets.verify_authority(&registry, 5, 6),
            Err(AccountReconcilerError::DesiredAuthority)
        );
        assert_eq!(
            applied_sets.verify_authority(&registry, 4, 5),
            Err(AccountReconcilerError::DesiredAuthority)
        );

        let checkpoint = DesiredCheckpointFingerprint::verified("f".repeat(64))?;
        let recovered = RecoveredDesiredOrdersReceipt::verified_checkpoint(
            key, 4, 6, "config_1", 1, 8, checkpoint,
        )?;
        let mut recovered_sets = DesiredOrderSets::new(AccountPositionMode::Hedge);
        recovered_sets.set_recovered(recovered, Vec::<DesiredOrder>::new())?;
        recovered_sets.verify_authority(&registry, 4, 7)?;

        registry.replace_config_digest(
            recovered_sets
                .by_instance
                .keys()
                .next()
                .ok_or(AccountReconcilerError::DesiredAuthority)?,
            "config_2".to_owned(),
        )?;
        assert_eq!(
            recovered_sets.verify_authority(&registry, 4, 7),
            Err(AccountReconcilerError::DesiredAuthority)
        );
        Ok(())
    }
}
