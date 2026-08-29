use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::{
    domain::{
        AccountKey, CommandId, NativeOrderFamily, OrderOwner, StrategyBinding, StrategyInstanceKey,
    },
    execution::AccountExecutionRequest,
    runtime::{account::InstanceLifecycle, strategy::PersistedPrivateFact},
};

const MAX_RECOVERED_PRIVATE_FACTS: u32 = 1_024;

/// Content-addressed roots produced by the recovery adapter after replaying every durable account
/// journal.  Keeping these roots in the opaque snapshot prevents a caller from presenting an
/// unbound collection of otherwise plausible rows as a complete recovery result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryJournalRoots {
    strategy_checkpoint: [u8; 32],
    strategy_checkpoint_boundary: RecoveryJournalBoundary,
    private_evidence: [u8; 32],
    private_evidence_boundary: RecoveryJournalBoundary,
    actor_inbox: [u8; 32],
    actor_inbox_boundary: RecoveryJournalBoundary,
    mutation_wal: [u8; 32],
    mutation_wal_boundary: RecoveryJournalBoundary,
    owner_index: [u8; 32],
    owner_index_boundary: RecoveryJournalBoundary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryJournalBoundary {
    tail_sequence: u64,
    record_count: u64,
}

impl RecoveryJournalBoundary {
    #[allow(dead_code)]
    fn from_persisted_adapter(
        tail_sequence: u64,
        record_count: u64,
    ) -> Result<Self, RecoverySnapshotError> {
        if (tail_sequence == 0) != (record_count == 0) || record_count > tail_sequence {
            return Err(RecoverySnapshotError::JournalBoundary);
        }
        Ok(Self {
            tail_sequence,
            record_count,
        })
    }
}

impl RecoveryJournalRoots {
    #[allow(dead_code)]
    fn from_persisted_adapter_heads(
        strategy_checkpoint: [u8; 32],
        strategy_checkpoint_boundary: RecoveryJournalBoundary,
        private_evidence: [u8; 32],
        private_evidence_boundary: RecoveryJournalBoundary,
        actor_inbox: [u8; 32],
        actor_inbox_boundary: RecoveryJournalBoundary,
        mutation_wal: [u8; 32],
        mutation_wal_boundary: RecoveryJournalBoundary,
        owner_index: [u8; 32],
        owner_index_boundary: RecoveryJournalBoundary,
    ) -> Result<Self, RecoverySnapshotError> {
        if [
            strategy_checkpoint,
            private_evidence,
            actor_inbox,
            mutation_wal,
            owner_index,
        ]
        .iter()
        .any(|root| root.iter().all(|byte| *byte == 0))
        {
            return Err(RecoverySnapshotError::JournalRoot);
        }
        Ok(Self {
            strategy_checkpoint,
            strategy_checkpoint_boundary,
            private_evidence,
            private_evidence_boundary,
            actor_inbox,
            actor_inbox_boundary,
            mutation_wal,
            mutation_wal_boundary,
            owner_index,
            owner_index_boundary,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_verified(
        roots: [[u8; 32]; 5],
        tail_sequences: [u64; 5],
        record_counts: [u64; 5],
    ) -> Result<Self, RecoverySnapshotError> {
        let boundaries = [
            RecoveryJournalBoundary::from_persisted_adapter(tail_sequences[0], record_counts[0])?,
            RecoveryJournalBoundary::from_persisted_adapter(tail_sequences[1], record_counts[1])?,
            RecoveryJournalBoundary::from_persisted_adapter(tail_sequences[2], record_counts[2])?,
            RecoveryJournalBoundary::from_persisted_adapter(tail_sequences[3], record_counts[3])?,
            RecoveryJournalBoundary::from_persisted_adapter(tail_sequences[4], record_counts[4])?,
        ];
        Self::from_persisted_adapter_heads(
            roots[0],
            boundaries[0],
            roots[1],
            boundaries[1],
            roots[2],
            boundaries[2],
            roots[3],
            boundaries[3],
            roots[4],
            boundaries[4],
        )
    }

    pub(super) const fn owner_index(&self) -> [u8; 32] {
        self.owner_index
    }

    pub(super) const fn owner_index_tail_sequence(&self) -> u64 {
        self.owner_index_boundary.tail_sequence
    }

    pub(super) const fn owner_index_record_count(&self) -> u64 {
        self.owner_index_boundary.record_count
    }
}

/// Digest stored by the recovery adapter only after all five journal heads and their replayed
/// projections have been committed as one complete startup manifest. The snapshot recomputes the
/// digest, so a caller cannot silently drop a route, actor delivery, registry row, or UNKNOWN WAL
/// entry while reusing the durable manifest head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryManifestCommitment {
    sha256: [u8; 32],
}

impl RecoveryManifestCommitment {
    #[allow(dead_code)]
    fn from_persisted_adapter_head(sha256: [u8; 32]) -> Result<Self, RecoverySnapshotError> {
        if sha256.iter().all(|byte| *byte == 0) {
            return Err(RecoverySnapshotError::ManifestCommitment);
        }
        Ok(Self { sha256 })
    }
}

/// One ownership index entry recovered from the durable command/order journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredOrderRoute {
    family: NativeOrderFamily,
    command_id: CommandId,
    client_order_id: String,
    venue_order_id: Option<String>,
    owner: OrderOwner,
}

impl RecoveredOrderRoute {
    pub(crate) fn verified(
        family: NativeOrderFamily,
        command_id: CommandId,
        client_order_id: String,
        venue_order_id: Option<String>,
        owner: OrderOwner,
    ) -> Self {
        Self {
            family,
            command_id,
            client_order_id,
            venue_order_id,
            owner,
        }
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        NativeOrderFamily,
        CommandId,
        String,
        Option<String>,
        OrderOwner,
    ) {
        (
            self.family,
            self.command_id,
            self.client_order_id,
            self.venue_order_id,
            self.owner,
        )
    }
}

/// Opaque acknowledgement that one exact Owner route append crossed the durable owner-index
/// boundary. The previous root makes receipts single-use and preserves append ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedOrderRouteAppendReceipt {
    route: RecoveredOrderRoute,
    previous_owner_index_root: [u8; 32],
    next_owner_index_root: [u8; 32],
    append_sequence: u64,
    record_sha256: [u8; 32],
}

impl PersistedOrderRouteAppendReceipt {
    /// Called by the owner-index adapter only after the exact route record and new chain head are
    /// durable. AccountRuntime separately verifies that the receipt extends its installed head.
    #[allow(dead_code)]
    fn persisted_after_append(
        route: RecoveredOrderRoute,
        previous_owner_index_root: [u8; 32],
        append_sequence: u64,
    ) -> Result<Self, RecoverySnapshotError> {
        if append_sequence == 0 || previous_owner_index_root.iter().all(|byte| *byte == 0) {
            return Err(RecoverySnapshotError::OwnerRouteReceipt);
        }
        let record_sha256 = order_route_record_sha256(&route);
        let next_owner_index_root =
            owner_index_append_root(previous_owner_index_root, append_sequence, record_sha256);
        Ok(Self {
            route,
            previous_owner_index_root,
            next_owner_index_root,
            append_sequence,
            record_sha256,
        })
    }

    #[cfg(test)]
    pub(super) fn test_persisted_after_append(
        route: RecoveredOrderRoute,
        previous_owner_index_root: [u8; 32],
        append_sequence: u64,
    ) -> Result<Self, RecoverySnapshotError> {
        Self::persisted_after_append(route, previous_owner_index_root, append_sequence)
    }

    pub(super) fn into_parts(self) -> (RecoveredOrderRoute, [u8; 32], [u8; 32], u64, [u8; 32]) {
        (
            self.route,
            self.previous_owner_index_root,
            self.next_owner_index_root,
            self.append_sequence,
            self.record_sha256,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveredShutdownMode {
    Stop,
    Flatten,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredShutdownState {
    mode: RecoveredShutdownMode,
    fence_connection_generation: u64,
    fence_private_generation: u64,
}

impl RecoveredShutdownState {
    pub(crate) fn verified(
        mode: RecoveredShutdownMode,
        fence_connection_generation: u64,
        fence_private_generation: u64,
    ) -> Result<Self, RecoverySnapshotError> {
        if fence_connection_generation == 0 || fence_private_generation == 0 {
            return Err(RecoverySnapshotError::Shutdown);
        }
        Ok(Self {
            mode,
            fence_connection_generation,
            fence_private_generation,
        })
    }

    pub(super) const fn parts(&self) -> (RecoveredShutdownMode, u64, u64) {
        (
            self.mode,
            self.fence_connection_generation,
            self.fence_private_generation,
        )
    }
}

/// Durable lifecycle/config checkpoint for one registered strategy. Running is recovered as
/// Recovering by the registry; Paused/Stopping/Faulted/NeedsAttention remain fail-closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredStrategyState {
    binding: StrategyBinding,
    config_epoch: u64,
    lifecycle: InstanceLifecycle,
    shutdown: Option<RecoveredShutdownState>,
}

impl RecoveredStrategyState {
    pub(crate) fn verified(
        binding: StrategyBinding,
        config_epoch: u64,
        lifecycle: InstanceLifecycle,
        shutdown: Option<RecoveredShutdownState>,
    ) -> Result<Self, RecoverySnapshotError> {
        if config_epoch == 0 || (lifecycle == InstanceLifecycle::Stopping) != shutdown.is_some() {
            return Err(RecoverySnapshotError::Lifecycle);
        }
        Ok(Self {
            binding,
            config_epoch,
            lifecycle,
            shutdown,
        })
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        StrategyBinding,
        u64,
        InstanceLifecycle,
        Option<RecoveredShutdownState>,
    ) {
        (
            self.binding,
            self.config_epoch,
            self.lifecycle,
            self.shutdown,
        )
    }
}

/// Cursor committed only after every routed actor delivery through this raw evidence sequence has
/// an applied checkpoint receipt. It is intentionally opaque; callers cannot pass journal tail.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveredPrivateCursor {
    sequence: u64,
    generation: u64,
    payload_sha256: Option<String>,
}

/// One not-yet-applied delivery replayed from the durable actor inbox. Raw private evidence is not
/// renormalized under a new connection generation; the exact routed fact is resumed first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredActorInboxEntry {
    target: StrategyInstanceKey,
    fact: PersistedPrivateFact,
}

impl RecoveredActorInboxEntry {
    pub(crate) fn verified(target: StrategyInstanceKey, fact: PersistedPrivateFact) -> Self {
        Self { target, fact }
    }

    pub(super) fn into_parts(self) -> (StrategyInstanceKey, PersistedPrivateFact) {
        (self.target, self.fact)
    }
}

impl RecoveredPrivateCursor {
    pub(crate) fn verified(
        sequence: u64,
        generation: u64,
        payload_sha256: Option<String>,
    ) -> Result<Self, RecoverySnapshotError> {
        let empty = sequence == 0 && generation == 0 && payload_sha256.is_none();
        let populated =
            sequence > 0 && generation > 0 && payload_sha256.as_deref().is_some_and(valid_sha256);
        if !empty && !populated {
            return Err(RecoverySnapshotError::PrivateCursor);
        }
        Ok(Self {
            sequence,
            generation,
            payload_sha256,
        })
    }

    pub(super) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(super) const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Complete delivery manifest for one router-consumed evidence batch whose applied cursor has not
/// yet advanced.  `deliveries` contains every original destination, while `applied` identifies the
/// subset already checkpointed by its Actor. Recovery re-routes the source facts and requires an
/// exact destination-set match before replaying the remaining deliveries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredPrivateBatch {
    facts: Vec<PersistedPrivateFact>,
    deliveries: Vec<RecoveredActorInboxEntry>,
    applied: BTreeSet<(StrategyInstanceKey, u32)>,
}

impl RecoveredPrivateBatch {
    pub(crate) fn verified(
        facts: Vec<PersistedPrivateFact>,
        deliveries: Vec<RecoveredActorInboxEntry>,
        applied: BTreeSet<(StrategyInstanceKey, u32)>,
    ) -> Result<Self, RecoverySnapshotError> {
        let Some(first) = facts.first() else {
            return Err(RecoverySnapshotError::PrivateBatch);
        };
        let sequence = first.evidence().sequence();
        let generation = first.evidence().generation();
        let payload_sha256 = first.evidence().payload_sha256();
        let fact_count = first.fact_count();
        if sequence == 0
            || generation == 0
            || !valid_sha256(payload_sha256)
            || fact_count == 0
            || fact_count > MAX_RECOVERED_PRIVATE_FACTS
            || u32::try_from(facts.len()).ok() != Some(fact_count)
            || facts.iter().enumerate().any(|(index, fact)| {
                u32::try_from(index).ok() != Some(fact.fact_index())
                    || fact.fact_count() != fact_count
                    || fact.evidence().sequence() != sequence
                    || fact.evidence().generation() != generation
                    || fact.evidence().payload_sha256() != payload_sha256
            })
        {
            return Err(RecoverySnapshotError::PrivateBatch);
        }

        let mut delivery_keys = BTreeSet::new();
        for delivery in &deliveries {
            let fact = &delivery.fact;
            if !delivery_keys.insert((delivery.target.clone(), fact.fact_index()))
                || fact.evidence().sequence() != sequence
                || fact.evidence().generation() != generation
                || fact.evidence().payload_sha256() != payload_sha256
                || fact.fact_count() != fact_count
                || facts.get(fact.fact_index() as usize) != Some(fact)
            {
                return Err(RecoverySnapshotError::PrivateBatch);
            }
        }
        if !applied.is_subset(&delivery_keys) {
            return Err(RecoverySnapshotError::PrivateBatch);
        }
        Ok(Self {
            facts,
            deliveries,
            applied,
        })
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Vec<PersistedPrivateFact>,
        Vec<RecoveredActorInboxEntry>,
        BTreeSet<(StrategyInstanceKey, u32)>,
    ) {
        (self.facts, self.deliveries, self.applied)
    }
}

/// Complete result of replaying lifecycle, actor checkpoint, private inbox, mutation and ownership
/// journals. Even an empty account must install this receipt before connectivity becomes Ready.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRecoverySnapshot {
    account: AccountKey,
    journal_roots: RecoveryJournalRoots,
    manifest_commitment: RecoveryManifestCommitment,
    last_connection_generation: u64,
    applied_private_cursor: RecoveredPrivateCursor,
    strategy_states: Vec<RecoveredStrategyState>,
    pending_private_batches: Vec<RecoveredPrivateBatch>,
    routes: Vec<RecoveredOrderRoute>,
    unresolved_mutations: Vec<AccountExecutionRequest>,
}

impl AccountRecoverySnapshot {
    pub(crate) fn verified(
        account: AccountKey,
        journal_roots: RecoveryJournalRoots,
        manifest_commitment: RecoveryManifestCommitment,
        last_connection_generation: u64,
        applied_private_cursor: RecoveredPrivateCursor,
        strategy_states: Vec<RecoveredStrategyState>,
        pending_private_batches: Vec<RecoveredPrivateBatch>,
        routes: Vec<RecoveredOrderRoute>,
        unresolved_mutations: Vec<AccountExecutionRequest>,
    ) -> Result<Self, RecoverySnapshotError> {
        if applied_private_cursor.generation() > last_connection_generation {
            return Err(RecoverySnapshotError::PrivateCursor);
        }
        verify_private_evidence_coverage(
            &journal_roots,
            &applied_private_cursor,
            &pending_private_batches,
        )?;
        let computed_manifest = recovery_manifest_sha256(
            &account,
            &journal_roots,
            last_connection_generation,
            &applied_private_cursor,
            &strategy_states,
            &pending_private_batches,
            &routes,
            &unresolved_mutations,
        )?;
        if manifest_commitment.sha256 != computed_manifest {
            return Err(RecoverySnapshotError::ManifestCommitment);
        }
        Ok(Self {
            account,
            journal_roots,
            manifest_commitment,
            last_connection_generation,
            applied_private_cursor,
            strategy_states,
            pending_private_batches,
            routes,
            unresolved_mutations,
        })
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        AccountKey,
        RecoveryJournalRoots,
        RecoveryManifestCommitment,
        u64,
        RecoveredPrivateCursor,
        Vec<RecoveredStrategyState>,
        Vec<RecoveredPrivateBatch>,
        Vec<RecoveredOrderRoute>,
        Vec<AccountExecutionRequest>,
    ) {
        (
            self.account,
            self.journal_roots,
            self.manifest_commitment,
            self.last_connection_generation,
            self.applied_private_cursor,
            self.strategy_states,
            self.pending_private_batches,
            self.routes,
            self.unresolved_mutations,
        )
    }
}

#[cfg(test)]
impl RecoveryManifestCommitment {
    pub(crate) fn test_for_replayed_state(
        account: &AccountKey,
        journal_roots: &RecoveryJournalRoots,
        last_connection_generation: u64,
        applied_private_cursor: &RecoveredPrivateCursor,
        strategy_states: &[RecoveredStrategyState],
        pending_private_batches: &[RecoveredPrivateBatch],
        routes: &[RecoveredOrderRoute],
        unresolved_mutations: &[AccountExecutionRequest],
    ) -> Result<Self, RecoverySnapshotError> {
        Ok(Self {
            sha256: recovery_manifest_sha256(
                account,
                journal_roots,
                last_connection_generation,
                applied_private_cursor,
                strategy_states,
                pending_private_batches,
                routes,
                unresolved_mutations,
            )?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecoverySnapshotError {
    #[error("recovered lifecycle and shutdown state are inconsistent")]
    Lifecycle,
    #[error("recovered Stop/Flatten fence generations must be positive")]
    Shutdown,
    #[error("recovery journal roots must be nonzero content digests")]
    JournalRoot,
    #[error("recovery journal tail sequence and record count are inconsistent")]
    JournalBoundary,
    #[error("recovered private cursor identity is incomplete or ahead of connectivity")]
    PrivateCursor,
    #[error("recovered private batch manifest is incomplete or internally inconsistent")]
    PrivateBatch,
    #[error("recovered private batches do not exactly cover the durable evidence tail")]
    PrivateManifestBoundary,
    #[error("recovery manifest does not match its durable journal commitment")]
    ManifestCommitment,
    #[error("order route append receipt is not bound to a durable owner-index head")]
    OwnerRouteReceipt,
    #[error("recovery manifest contains a value that cannot be canonically encoded")]
    ManifestEncoding,
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn verify_private_evidence_coverage(
    journal_roots: &RecoveryJournalRoots,
    applied_private_cursor: &RecoveredPrivateCursor,
    batches: &[RecoveredPrivateBatch],
) -> Result<(), RecoverySnapshotError> {
    let mut covered_sequence = applied_private_cursor.sequence;
    for batch in batches {
        let sequence = batch
            .facts
            .first()
            .ok_or(RecoverySnapshotError::PrivateBatch)?
            .evidence()
            .sequence();
        covered_sequence = covered_sequence
            .checked_add(1)
            .ok_or(RecoverySnapshotError::PrivateManifestBoundary)?;
        if sequence != covered_sequence {
            return Err(RecoverySnapshotError::PrivateManifestBoundary);
        }
    }
    if covered_sequence != journal_roots.private_evidence_boundary.tail_sequence {
        return Err(RecoverySnapshotError::PrivateManifestBoundary);
    }
    Ok(())
}

fn recovery_manifest_sha256(
    account: &AccountKey,
    journal_roots: &RecoveryJournalRoots,
    last_connection_generation: u64,
    applied_private_cursor: &RecoveredPrivateCursor,
    strategy_states: &[RecoveredStrategyState],
    pending_private_batches: &[RecoveredPrivateBatch],
    routes: &[RecoveredOrderRoute],
    unresolved_mutations: &[AccountExecutionRequest],
) -> Result<[u8; 32], RecoverySnapshotError> {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-account-recovery-manifest-v1");
    commit_account_key(&mut digest, account);
    commit_bytes(&mut digest, &journal_roots.strategy_checkpoint);
    commit_journal_boundary(&mut digest, journal_roots.strategy_checkpoint_boundary);
    commit_bytes(&mut digest, &journal_roots.private_evidence);
    commit_journal_boundary(&mut digest, journal_roots.private_evidence_boundary);
    commit_bytes(&mut digest, &journal_roots.actor_inbox);
    commit_journal_boundary(&mut digest, journal_roots.actor_inbox_boundary);
    commit_bytes(&mut digest, &journal_roots.mutation_wal);
    commit_journal_boundary(&mut digest, journal_roots.mutation_wal_boundary);
    commit_bytes(&mut digest, &journal_roots.owner_index);
    commit_journal_boundary(&mut digest, journal_roots.owner_index_boundary);
    commit_u64(&mut digest, last_connection_generation);
    commit_u64(&mut digest, applied_private_cursor.sequence);
    commit_u64(&mut digest, applied_private_cursor.generation);
    commit_option_str(
        &mut digest,
        applied_private_cursor.payload_sha256.as_deref(),
    );

    commit_len(&mut digest, strategy_states.len());
    for state in strategy_states {
        commit_strategy_binding(&mut digest, &state.binding);
        commit_u64(&mut digest, state.config_epoch);
        commit_bytes(&mut digest, &[lifecycle_tag(state.lifecycle)]);
        match &state.shutdown {
            Some(shutdown) => {
                commit_bytes(&mut digest, &[1]);
                commit_bytes(
                    &mut digest,
                    &[match shutdown.mode {
                        RecoveredShutdownMode::Stop => 1,
                        RecoveredShutdownMode::Flatten => 2,
                    }],
                );
                commit_u64(&mut digest, shutdown.fence_connection_generation);
                commit_u64(&mut digest, shutdown.fence_private_generation);
            }
            None => commit_bytes(&mut digest, &[0]),
        }
    }
    commit_len(&mut digest, pending_private_batches.len());
    for batch in pending_private_batches {
        commit_len(&mut digest, batch.facts.len());
        for fact in &batch.facts {
            commit_private_fact(&mut digest, fact)?;
        }
        commit_len(&mut digest, batch.deliveries.len());
        for delivery in &batch.deliveries {
            commit_strategy_key(&mut digest, &delivery.target);
            commit_private_fact(&mut digest, &delivery.fact)?;
        }
        commit_len(&mut digest, batch.applied.len());
        for (target, fact_index) in &batch.applied {
            commit_strategy_key(&mut digest, target);
            commit_u64(&mut digest, u64::from(*fact_index));
        }
    }
    commit_len(&mut digest, routes.len());
    for route in routes {
        commit_order_route(&mut digest, route);
    }
    commit_len(&mut digest, unresolved_mutations.len());
    for request in unresolved_mutations {
        let request_commitment = request
            .canonical_recovery_commitment()
            .map_err(|_| RecoverySnapshotError::ManifestEncoding)?;
        commit_bytes(&mut digest, &request_commitment);
    }
    Ok(digest.finalize().into())
}

fn order_route_record_sha256(route: &RecoveredOrderRoute) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-owner-route-record-v1");
    commit_order_route(&mut digest, route);
    digest.finalize().into()
}

fn owner_index_append_root(
    previous_root: [u8; 32],
    append_sequence: u64,
    record_sha256: [u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-owner-index-chain-v1");
    commit_bytes(&mut digest, &previous_root);
    commit_u64(&mut digest, append_sequence);
    commit_bytes(&mut digest, &record_sha256);
    digest.finalize().into()
}

fn commit_private_fact(
    digest: &mut Sha256,
    fact: &PersistedPrivateFact,
) -> Result<(), RecoverySnapshotError> {
    commit_u64(digest, fact.evidence().sequence());
    commit_u64(digest, fact.evidence().generation());
    commit_u64(digest, fact.evidence().received_at_ms());
    commit_str(digest, fact.evidence().payload_sha256());
    commit_u64(digest, u64::from(fact.fact_index()));
    commit_u64(digest, u64::from(fact.fact_count()));
    match fact.order_family() {
        Some(family) => {
            commit_bytes(digest, &[1]);
            commit_bytes(digest, &[native_order_family_tag(family)]);
        }
        None => commit_bytes(digest, &[0]),
    }
    let record =
        serde_json::to_vec(fact.record()).map_err(|_| RecoverySnapshotError::ManifestEncoding)?;
    commit_bytes(digest, &record);
    Ok(())
}

fn commit_order_route(digest: &mut Sha256, route: &RecoveredOrderRoute) {
    commit_bytes(digest, &[native_order_family_tag(route.family)]);
    commit_str(digest, route.command_id.as_str());
    commit_str(digest, &route.client_order_id);
    commit_option_str(digest, route.venue_order_id.as_deref());
    commit_order_owner(digest, &route.owner);
}

fn commit_order_owner(digest: &mut Sha256, owner: &OrderOwner) {
    commit_str(digest, &owner.strategy_instance_id);
    commit_str(digest, &owner.run_id);
    commit_str(digest, &owner.exchange);
    commit_str(digest, &owner.account);
    commit_str(digest, &owner.symbol.to_string());
    commit_bytes(
        digest,
        &[match owner.purpose {
            crate::domain::OrderPurpose::Entry => 1,
            crate::domain::OrderPurpose::Protection => 2,
            crate::domain::OrderPurpose::TakeProfit => 3,
            crate::domain::OrderPurpose::Reduce => 4,
            crate::domain::OrderPurpose::ExposureTakeProfit => 5,
        }],
    );
}

fn commit_strategy_binding(digest: &mut Sha256, binding: &StrategyBinding) {
    commit_strategy_key(digest, &binding.key);
    commit_str(digest, &binding.run_id);
    commit_str(digest, &binding.config_digest);
}

fn commit_strategy_key(digest: &mut Sha256, key: &StrategyInstanceKey) {
    commit_account_key(digest, &key.account);
    commit_bytes(
        digest,
        &[match key.strategy_kind {
            crate::domain::StrategyKind::HedgedGrid => 1,
            crate::domain::StrategyKind::Scalping => 2,
        }],
    );
    commit_str(digest, &key.instance_id);
    commit_str(digest, &key.symbol.to_string());
}

fn commit_account_key(digest: &mut Sha256, account: &AccountKey) {
    commit_str(digest, account.exchange.as_str());
    commit_str(digest, &account.account);
}

const fn lifecycle_tag(lifecycle: InstanceLifecycle) -> u8 {
    match lifecycle {
        InstanceLifecycle::Registered => 1,
        InstanceLifecycle::Recovering => 2,
        InstanceLifecycle::Running => 3,
        InstanceLifecycle::Paused => 4,
        InstanceLifecycle::Stopping => 5,
        InstanceLifecycle::Faulted => 6,
        InstanceLifecycle::NeedsAttention => 7,
    }
}

const fn native_order_family_tag(family: NativeOrderFamily) -> u8 {
    match family {
        NativeOrderFamily::UmOrder => 1,
        NativeOrderFamily::UmConditional => 2,
        NativeOrderFamily::UmAlgo => 3,
    }
}

fn commit_option_str(digest: &mut Sha256, value: Option<&str>) {
    match value {
        Some(value) => {
            commit_bytes(digest, &[1]);
            commit_str(digest, value);
        }
        None => commit_bytes(digest, &[0]),
    }
}

fn commit_str(digest: &mut Sha256, value: &str) {
    commit_bytes(digest, value.as_bytes());
}

fn commit_len(digest: &mut Sha256, value: usize) {
    commit_u64(digest, u64::try_from(value).unwrap_or(u64::MAX));
}

fn commit_journal_boundary(digest: &mut Sha256, boundary: RecoveryJournalBoundary) {
    commit_u64(digest, boundary.tail_sequence);
    commit_u64(digest, boundary.record_count);
}

fn commit_u64(digest: &mut Sha256, value: u64) {
    commit_bytes(digest, &value.to_be_bytes());
}

fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    let len = u64::try_from(value.len()).unwrap_or(u64::MAX);
    digest.update(len.to_be_bytes());
    digest.update(value);
}
