use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use venue_domain::domain::NativeOrderFamily;
use venue_gateway_api::{GatewayBinding, VenueId};

use super::{
    AccountKey, AccountPositionMode, PhysicalRecoveryAuthorityRoots, PhysicalRecoveryManifestError,
    PhysicalRecoveryScope, RecoveryJournalRoots, RecoveryManifestCommitment, StrategyBinding,
};

const DEFAULT_RECOVERY_SESSION_LEASE: Duration = Duration::from_secs(30);
static NEXT_ISSUER_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DurableAuthorityHead {
    root: [u8; 32],
    tail_sequence: u64,
    record_count: u64,
}

impl DurableAuthorityHead {
    fn verified(
        root: [u8; 32],
        tail_sequence: u64,
        record_count: u64,
    ) -> Result<Self, PhysicalRecoverySessionError> {
        if !nonzero_digest(&root)
            || (tail_sequence == 0) != (record_count == 0)
            || record_count > tail_sequence
        {
            return Err(PhysicalRecoverySessionError::DurableRootIncomplete);
        }
        Ok(Self {
            root,
            tail_sequence,
            record_count,
        })
    }

    fn append_only_successor(self, next: Self) -> bool {
        if next.tail_sequence < self.tail_sequence || next.record_count < self.record_count {
            return false;
        }
        let boundary_changed =
            next.tail_sequence != self.tail_sequence || next.record_count != self.record_count;
        if boundary_changed {
            next.root != self.root
        } else {
            next.root == self.root
        }
    }

    fn checkpoint_successor(self, next: Self) -> bool {
        self.append_only_successor(next)
    }
}

/// Complete durable replay heads that must remain stable throughout one physical recovery
/// attempt. Construction is sealed to the account recovery adapter; callers can only retain the
/// opaque commitment carried by a runtime-issued session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRecoveryDurableRoots {
    authority_epoch: u64,
    strategy_checkpoint: DurableAuthorityHead,
    private_evidence: DurableAuthorityHead,
    actor_inbox: DurableAuthorityHead,
    mutation_wal: DurableAuthorityHead,
    owner_index: DurableAuthorityHead,
    replay_manifest_sha256: [u8; 32],
    physical_roots: PhysicalRecoveryAuthorityRoots,
    commitment_sha256: [u8; 32],
}

impl PhysicalRecoveryDurableRoots {
    pub(super) fn from_recovered(
        journal_roots: &RecoveryJournalRoots,
        replay_manifest: &RecoveryManifestCommitment,
        physical_roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<Self, PhysicalRecoverySessionError> {
        let (checkpoint_root, checkpoint_tail, checkpoint_count) =
            journal_roots.strategy_checkpoint_head();
        let (private_root, private_tail, private_count) = journal_roots.private_evidence_head();
        let (inbox_root, inbox_tail, inbox_count) = journal_roots.actor_inbox_head();
        let (wal_root, wal_tail, wal_count) = journal_roots.mutation_wal_head();
        let (owner_root, owner_tail, owner_count) = journal_roots.owner_index_head();
        Self::verified(
            1,
            DurableAuthorityHead::verified(checkpoint_root, checkpoint_tail, checkpoint_count)?,
            DurableAuthorityHead::verified(private_root, private_tail, private_count)?,
            DurableAuthorityHead::verified(inbox_root, inbox_tail, inbox_count)?,
            DurableAuthorityHead::verified(wal_root, wal_tail, wal_count)?,
            DurableAuthorityHead::verified(owner_root, owner_tail, owner_count)?,
            replay_manifest.sha256(),
            physical_roots,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn verified(
        authority_epoch: u64,
        strategy_checkpoint: DurableAuthorityHead,
        private_evidence: DurableAuthorityHead,
        actor_inbox: DurableAuthorityHead,
        mutation_wal: DurableAuthorityHead,
        owner_index: DurableAuthorityHead,
        replay_manifest_sha256: [u8; 32],
        physical_roots: PhysicalRecoveryAuthorityRoots,
    ) -> Result<Self, PhysicalRecoverySessionError> {
        if authority_epoch == 0
            || !nonzero_digest(&replay_manifest_sha256)
            || physical_roots.owner() != &owner_index.root
            || physical_roots.wal() != &mutation_wal.root
            || !nonzero_digest(physical_roots.unknown())
        {
            return Err(PhysicalRecoverySessionError::DurableRootIncomplete);
        }
        let commitment_sha256 = durable_roots_commitment(
            authority_epoch,
            [
                strategy_checkpoint,
                private_evidence,
                actor_inbox,
                mutation_wal,
                owner_index,
            ],
            &replay_manifest_sha256,
            &physical_roots,
        );
        Ok(Self {
            authority_epoch,
            strategy_checkpoint,
            private_evidence,
            actor_inbox,
            mutation_wal,
            owner_index,
            replay_manifest_sha256,
            physical_roots,
            commitment_sha256,
        })
    }

    pub(super) fn refreshed_owner(
        &self,
        owner_root: [u8; 32],
        tail_sequence: u64,
        record_count: u64,
        replay_manifest_sha256: [u8; 32],
    ) -> Result<Self, PhysicalRecoverySessionError> {
        let physical_roots = self
            .physical_roots
            .refreshed_owner(owner_root)
            .map_err(|_| PhysicalRecoverySessionError::DurableRootIncomplete)?;
        Self::verified(
            self.authority_epoch
                .checked_add(1)
                .ok_or(PhysicalRecoverySessionError::EpochExhausted)?,
            self.strategy_checkpoint,
            self.private_evidence,
            self.actor_inbox,
            self.mutation_wal,
            DurableAuthorityHead::verified(owner_root, tail_sequence, record_count)?,
            replay_manifest_sha256,
            physical_roots,
        )
    }

    pub(super) fn monotonic_successor_of(&self, previous: &Self) -> bool {
        let Some(expected_epoch) = previous.authority_epoch.checked_add(1) else {
            return false;
        };
        let any_head_changed = self.strategy_checkpoint != previous.strategy_checkpoint
            || self.private_evidence != previous.private_evidence
            || self.actor_inbox != previous.actor_inbox
            || self.mutation_wal != previous.mutation_wal
            || self.owner_index != previous.owner_index
            || self.physical_roots != previous.physical_roots;
        let unknown_changed = !self
            .physical_roots
            .same_unknown_authority(&previous.physical_roots);
        self.authority_epoch == expected_epoch
            && previous
                .strategy_checkpoint
                .checkpoint_successor(self.strategy_checkpoint)
            && previous
                .private_evidence
                .append_only_successor(self.private_evidence)
            && previous.actor_inbox.append_only_successor(self.actor_inbox)
            && previous
                .mutation_wal
                .append_only_successor(self.mutation_wal)
            && previous.owner_index.append_only_successor(self.owner_index)
            && self.physical_roots.owner() == &self.owner_index.root
            && self.physical_roots.wal() == &self.mutation_wal.root
            && nonzero_digest(self.physical_roots.unknown())
            && (!unknown_changed || self.mutation_wal != previous.mutation_wal)
            && if any_head_changed {
                self.replay_manifest_sha256 != previous.replay_manifest_sha256
            } else {
                self.replay_manifest_sha256 == previous.replay_manifest_sha256
            }
    }

    #[must_use]
    pub const fn authority_epoch(&self) -> u64 {
        self.authority_epoch
    }

    #[must_use]
    pub const fn commitment_sha256(&self) -> &[u8; 32] {
        &self.commitment_sha256
    }

    pub(super) const fn physical_roots(&self) -> &PhysicalRecoveryAuthorityRoots {
        &self.physical_roots
    }
}

/// Opaque runtime-issued authority for exactly one bounded physical recovery attempt. It contains
/// no credentials, network handle, writer lease, mutation permit, or capability promotion proof.
#[derive(Clone)]
pub struct PhysicalRecoverySession {
    issuer_seal: Arc<PhysicalRecoverySessionSeal>,
    session_id: [u8; 32],
    attempt_id: u64,
    session_epoch: u64,
    expected_private_generation: u64,
    runtime_authority_sha256: [u8; 32],
    durable_roots: PhysicalRecoveryDurableRoots,
    session_binding_sha256: [u8; 32],
    scope: PhysicalRecoveryScope,
    expires_at: Instant,
}

impl fmt::Debug for PhysicalRecoverySession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhysicalRecoverySession")
            .field("attempt_id", &self.attempt_id)
            .field("session_epoch", &self.session_epoch)
            .field("connection_generation", &self.scope.connection_generation())
            .field("private_generation", &self.expected_private_generation)
            .field("binding", self.scope.binding())
            .field("account_universe", &self.scope.account_universe())
            .field(
                "durable_authority_epoch",
                &self.durable_roots.authority_epoch,
            )
            .finish_non_exhaustive()
    }
}

impl PhysicalRecoverySession {
    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.scope.connection_generation()
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.expected_private_generation
    }

    #[must_use]
    pub const fn scope(&self) -> &PhysicalRecoveryScope {
        &self.scope
    }

    #[must_use]
    pub const fn durable_roots(&self) -> &PhysicalRecoveryDurableRoots {
        &self.durable_roots
    }

    pub(super) fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub(super) const fn runtime_authority_sha256(&self) -> &[u8; 32] {
        &self.runtime_authority_sha256
    }

    pub(super) fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.issuer_seal, &other.issuer_seal)
            && self.session_id == other.session_id
            && self.attempt_id == other.attempt_id
            && self.session_epoch == other.session_epoch
            && self.session_binding_sha256 == other.session_binding_sha256
            && self.scope == other.scope
            && self.durable_roots == other.durable_roots
            && self.expected_private_generation == other.expected_private_generation
            && self.runtime_authority_sha256 == other.runtime_authority_sha256
            && self.expires_at == other.expires_at
    }
}

/// Sealed receipt returned only after the durable adapter has replayed all checkpoint/journal
/// heads again. Runtime validates the old session before accepting the complete next snapshot.
#[derive(Clone, Debug)]
pub struct PhysicalRecoveryRootRefresh {
    session_id: [u8; 32],
    attempt_id: u64,
    session_epoch: u64,
    roots: PhysicalRecoveryDurableRoots,
}

impl PhysicalRecoveryRootRefresh {
    #[allow(dead_code)]
    #[cfg(test)]
    pub(super) fn after_complete_replay(
        session: &PhysicalRecoverySession,
        roots: PhysicalRecoveryDurableRoots,
    ) -> Self {
        Self {
            session_id: session.session_id,
            attempt_id: session.attempt_id,
            session_epoch: session.session_epoch,
            roots,
        }
    }

    #[cfg(test)]
    pub(super) fn test_complete_replay(
        session: &PhysicalRecoverySession,
    ) -> Result<Self, PhysicalRecoverySessionError> {
        let previous = session.durable_roots();
        let roots = PhysicalRecoveryDurableRoots::verified(
            previous
                .authority_epoch
                .checked_add(1)
                .ok_or(PhysicalRecoverySessionError::EpochExhausted)?,
            previous.strategy_checkpoint,
            previous.private_evidence,
            previous.actor_inbox,
            previous.mutation_wal,
            previous.owner_index,
            previous.replay_manifest_sha256,
            previous.physical_roots.clone(),
        )?;
        Ok(Self::after_complete_replay(session, roots))
    }
}

pub(super) struct PhysicalRecoverySessionIssuer {
    seal: Arc<PhysicalRecoverySessionSeal>,
    issuer_nonce: u64,
    next_attempt_id: u64,
}

struct PhysicalRecoverySessionSeal;

impl fmt::Debug for PhysicalRecoverySessionIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhysicalRecoverySessionIssuer")
            .field("next_attempt_id", &self.next_attempt_id)
            .finish_non_exhaustive()
    }
}

pub(super) struct PhysicalRecoverySessionParameters {
    pub binding: GatewayBinding,
    pub account: AccountKey,
    pub registrations: Vec<(StrategyBinding, u64)>,
    pub position_mode: AccountPositionMode,
    pub family_support: BTreeMap<NativeOrderFamily, bool>,
    pub profile_version: u64,
    pub connection_generation: u64,
    pub recovered_private_generation: u64,
    pub expected_private_generation: u64,
    pub runtime_authority_sha256: [u8; 32],
    pub durable_roots: PhysicalRecoveryDurableRoots,
}

impl PhysicalRecoverySessionIssuer {
    pub(super) fn new(_account: &AccountKey) -> Self {
        let issuer_nonce = NEXT_ISSUER_NONCE.fetch_add(1, Ordering::Relaxed);
        Self {
            seal: Arc::new(PhysicalRecoverySessionSeal),
            issuer_nonce,
            next_attempt_id: 1,
        }
    }

    pub(super) fn issue(
        &mut self,
        parameters: PhysicalRecoverySessionParameters,
    ) -> Result<PhysicalRecoverySession, PhysicalRecoverySessionError> {
        self.issue_with_lease(parameters, DEFAULT_RECOVERY_SESSION_LEASE)
    }

    fn issue_with_lease(
        &mut self,
        parameters: PhysicalRecoverySessionParameters,
        lease: Duration,
    ) -> Result<PhysicalRecoverySession, PhysicalRecoverySessionError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(PhysicalRecoverySessionError::AttemptExhausted)?;
        let session_id = session_id(self.issuer_nonce, attempt_id, &parameters.account);
        self.issue_epoch(
            parameters,
            session_id,
            attempt_id,
            1,
            Instant::now() + lease,
        )
    }

    pub(super) fn refresh(
        &self,
        previous: &PhysicalRecoverySession,
        parameters: PhysicalRecoverySessionParameters,
    ) -> Result<PhysicalRecoverySession, PhysicalRecoverySessionError> {
        let session_epoch = previous
            .session_epoch
            .checked_add(1)
            .ok_or(PhysicalRecoverySessionError::EpochExhausted)?;
        self.issue_epoch(
            parameters,
            previous.session_id,
            previous.attempt_id,
            session_epoch,
            previous.expires_at,
        )
    }

    fn issue_epoch(
        &self,
        parameters: PhysicalRecoverySessionParameters,
        session_id: [u8; 32],
        attempt_id: u64,
        session_epoch: u64,
        expires_at: Instant,
    ) -> Result<PhysicalRecoverySession, PhysicalRecoverySessionError> {
        if parameters.registrations.is_empty()
            || parameters.expected_private_generation <= parameters.recovered_private_generation
        {
            return Err(PhysicalRecoverySessionError::AccountUniverseIncomplete);
        }
        let session_binding_sha256 =
            session_binding_commitment(&session_id, attempt_id, session_epoch, &parameters);
        let scope = PhysicalRecoveryScope::verified_account_session(
            parameters.binding,
            parameters.account,
            parameters.registrations,
            parameters.position_mode,
            parameters.family_support,
            parameters.profile_version,
            parameters.connection_generation,
            parameters.recovered_private_generation,
            parameters.durable_roots.physical_roots.clone(),
            session_binding_sha256,
        )
        .map_err(PhysicalRecoverySessionError::Scope)?;
        Ok(PhysicalRecoverySession {
            issuer_seal: Arc::clone(&self.seal),
            session_id,
            attempt_id,
            session_epoch,
            expected_private_generation: parameters.expected_private_generation,
            runtime_authority_sha256: parameters.runtime_authority_sha256,
            durable_roots: parameters.durable_roots,
            session_binding_sha256,
            scope,
            expires_at,
        })
    }

    pub(super) fn authenticates(&self, session: &PhysicalRecoverySession) -> bool {
        Arc::ptr_eq(&self.seal, &session.issuer_seal)
    }

    #[cfg(test)]
    pub(super) fn issue_expired_for_test(
        &mut self,
        parameters: PhysicalRecoverySessionParameters,
    ) -> Result<PhysicalRecoverySession, PhysicalRecoverySessionError> {
        self.issue_with_lease(parameters, Duration::ZERO)
    }
}

impl PhysicalRecoveryRootRefresh {
    pub(super) fn matches(&self, session: &PhysicalRecoverySession) -> bool {
        self.session_id == session.session_id
            && self.attempt_id == session.attempt_id
            && self.session_epoch == session.session_epoch
    }

    pub(super) const fn roots(&self) -> &PhysicalRecoveryDurableRoots {
        &self.roots
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PhysicalRecoverySessionError {
    #[error("durable recovery roots are incomplete or internally inconsistent")]
    DurableRootIncomplete,
    #[error("physical recovery account universe is empty or incomplete")]
    AccountUniverseIncomplete,
    #[error("physical recovery session attempt counter is exhausted")]
    AttemptExhausted,
    #[error("physical recovery authority epoch counter is exhausted")]
    EpochExhausted,
    #[error("physical recovery session scope is invalid: {0}")]
    Scope(PhysicalRecoveryManifestError),
}

fn durable_roots_commitment(
    authority_epoch: u64,
    heads: [DurableAuthorityHead; 5],
    replay_manifest_sha256: &[u8; 32],
    physical_roots: &PhysicalRecoveryAuthorityRoots,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-durable-roots-v1");
    commit_u64(&mut digest, authority_epoch);
    for head in heads {
        commit_bytes(&mut digest, &head.root);
        commit_u64(&mut digest, head.tail_sequence);
        commit_u64(&mut digest, head.record_count);
    }
    commit_bytes(&mut digest, replay_manifest_sha256);
    commit_bytes(&mut digest, physical_roots.owner());
    commit_bytes(&mut digest, physical_roots.wal());
    commit_bytes(&mut digest, physical_roots.unknown());
    digest.finalize().into()
}

fn session_id(issuer_nonce: u64, attempt_id: u64, account: &AccountKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-session-id-v1");
    commit_u64(&mut digest, issuer_nonce);
    commit_u64(&mut digest, attempt_id);
    commit_bytes(&mut digest, &[venue_tag(account.exchange)]);
    commit_str(&mut digest, &account.account);
    digest.finalize().into()
}

fn session_binding_commitment(
    session_id: &[u8; 32],
    attempt_id: u64,
    session_epoch: u64,
    parameters: &PhysicalRecoverySessionParameters,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    commit_bytes(&mut digest, b"venue-physical-recovery-session-binding-v1");
    commit_bytes(&mut digest, session_id);
    commit_u64(&mut digest, attempt_id);
    commit_u64(&mut digest, session_epoch);
    commit_u64(&mut digest, parameters.connection_generation);
    commit_u64(&mut digest, parameters.recovered_private_generation);
    commit_u64(&mut digest, parameters.expected_private_generation);
    commit_bytes(&mut digest, &parameters.runtime_authority_sha256);
    commit_bytes(&mut digest, parameters.durable_roots.commitment_sha256());
    digest.finalize().into()
}

const fn venue_tag(venue: VenueId) -> u8 {
    match venue {
        VenueId::Binance => 1,
        VenueId::Bitget => 2,
        VenueId::Bybit => 3,
        VenueId::Gate => 4,
        VenueId::Hyperliquid => 5,
        VenueId::Okx => 6,
    }
}

fn nonzero_digest(value: &[u8; 32]) -> bool {
    value.iter().any(|byte| *byte != 0)
}

fn commit_str(digest: &mut Sha256, value: &str) {
    commit_bytes(digest, value.as_bytes());
}

fn commit_u64(digest: &mut Sha256, value: u64) {
    commit_bytes(digest, &value.to_be_bytes());
}

fn commit_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}
