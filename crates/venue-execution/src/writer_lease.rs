use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::Symbol;

pub const WRITER_LEASE_TTL_MS: u64 = 10_000;

/// The authority identity is deliberately narrower than an account: a lease cannot move across
/// exchange, account, normalized symbol, or strategy owner scope.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterScope {
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub owner_scope: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterSession {
    pub scope: WriterScope,
    pub token: String,
    pub generation: u64,
    pub revision: u64,
    pub readback_generation: u64,
    pub valid_until_ms: u64,
}

/// A signed/readback-derived flat proof. It is evidence only and may be consumed exactly once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlatReceipt {
    pub receipt_id: String,
    pub predecessor: WriterSession,
    pub scope: WriterScope,
    pub readback_generation: u64,
    pub summary_sha256: String,
}

/// A protected proof may preserve the predecessor in protection-only state but can never grant
/// a successor active writer authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectedReceipt {
    pub predecessor: WriterSession,
    pub scope: WriterScope,
    pub readback_generation: u64,
    pub summary_sha256: String,
}

/// A verified non-flat executable handoff. Fencing consumes the predecessor's entry authority;
/// only the named successor digest can activate the next writer generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutableHandoffReceipt {
    pub receipt_id: String,
    pub predecessor: WriterSession,
    pub scope: WriterScope,
    pub readback_generation: u64,
    pub handoff_sha256: String,
    pub successor_executable_sha256: String,
}

#[derive(Clone, Debug)]
pub struct WriterLeaseAuthority {
    path: PathBuf,
    scope: WriterScope,
}

/// Retains the OS-exclusive `.lock` for the duration of a gateway call.
#[derive(Debug)]
pub struct DispatchGuard {
    _lock: OsLock,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableState {
    schema_version: u16,
    revision: u64,
    scope: WriterScope,
    next_generation: u64,
    active: Option<DurableSession>,
    protection_only: bool,
    consumed_flat_receipts: BTreeSet<String>,
    #[serde(default)]
    pending_executable_handoff: Option<DurableExecutableHandoffFence>,
    #[serde(default)]
    consumed_executable_handoffs: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableExecutableHandoffFence {
    receipt_id: String,
    predecessor: DurableSession,
    readback_generation: u64,
    handoff_sha256: String,
    successor_executable_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableSession {
    token: String,
    generation: u64,
    revision: u64,
    readback_generation: u64,
    valid_until_ms: u64,
}

#[derive(Debug)]
struct OsLock {
    file: File,
}

impl Drop for OsLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl WriterLeaseAuthority {
    pub fn open(path: impl Into<PathBuf>, scope: WriterScope) -> Result<Self, WriterLeaseError> {
        let path = path.into();
        validate_scope(&scope)?;
        if !path.is_absolute() {
            return Err(WriterLeaseError::AuthorityPath);
        }
        let authority = Self { path, scope };
        if let Some(state) = authority.read_recovered()? {
            authority.validate_state(&state)?;
        }
        Ok(authority)
    }

    pub fn register_initial(
        &self,
        now_ms: u64,
        readback_generation: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        if readback_generation == 0 {
            return Err(WriterLeaseError::Generation);
        }
        let lock = self.lock()?;
        let mut state = self.load_or_initial()?;
        if state.pending_executable_handoff.is_some() {
            return Err(WriterLeaseError::HandoffPending);
        }
        if state.active.is_some() {
            return Err(WriterLeaseError::WriterExists);
        }
        let next_revision = increment(state.revision)?;
        let generation = state.next_generation;
        state.next_generation = increment(generation)?;
        let session = DurableSession {
            token: session_token(
                &self.path,
                &self.scope,
                generation,
                next_revision,
                readback_generation,
                now_ms,
            )?,
            generation,
            revision: next_revision,
            readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.active = Some(session.clone());
        state.protection_only = false;
        self.persist_locked(&lock, &state)?;
        Ok(public_session(&self.scope, session))
    }

    /// Renewal is a strict compare-and-swap: an expired lease, stale revision, or any identity
    /// mismatch is fenced rather than extended.
    pub fn renew(
        &self,
        session: &WriterSession,
        now_ms: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        self.require_scope(&session.scope)?;
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_session(current, session, now_ms)?;
        if state.protection_only {
            return Err(WriterLeaseError::ProtectionOnly);
        }
        let next_revision = increment(state.revision)?;
        let updated = DurableSession {
            token: current.token.clone(),
            generation: current.generation,
            revision: next_revision,
            readback_generation: current.readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.active = Some(updated.clone());
        self.persist_locked(&lock, &state)?;
        Ok(public_session(&self.scope, updated))
    }

    /// Reopens the exact expired writer identity after a strictly newer private readback. This is
    /// not a takeover: scope, generation and durable predecessor must all match, and the caller
    /// still holds this authority's OS lock while the state is replaced. It serves symbol actors
    /// whose signed account reconciliation can legitimately outlive the short lease.
    pub fn recover_same_scope_after_readback(
        &self,
        predecessor: &WriterSession,
        readback_generation: u64,
        now_ms: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        self.require_scope(&predecessor.scope)?;
        if readback_generation <= predecessor.readback_generation || now_ms == 0 {
            return Err(WriterLeaseError::Receipt);
        }
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if state.protection_only {
            return Err(WriterLeaseError::ProtectionOnly);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, predecessor)?;
        if current.valid_until_ms > now_ms {
            return Err(WriterLeaseError::Fenced);
        }
        let next_revision = increment(state.revision)?;
        let updated = DurableSession {
            token: session_token(
                &self.path,
                &self.scope,
                current.generation,
                next_revision,
                readback_generation,
                now_ms,
            )?,
            generation: current.generation,
            revision: next_revision,
            readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.active = Some(updated.clone());
        self.persist_locked(&lock, &state)?;
        Ok(public_session(&self.scope, updated))
    }

    /// Extends only the protected predecessor's maintenance window. Unlike an entry renewal it
    /// may recover an expired lease, but it requires the exact durable predecessor and never
    /// changes the `protection_only` fence or grants an entry-capable dispatch.
    pub fn renew_protection(
        &self,
        session: &WriterSession,
        now_ms: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        self.require_scope(&session.scope)?;
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if !state.protection_only {
            return Err(WriterLeaseError::NotProtectionOnly);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, session)?;
        let next_revision = increment(state.revision)?;
        let updated = DurableSession {
            token: current.token.clone(),
            generation: current.generation,
            revision: next_revision,
            readback_generation: current.readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.active = Some(updated.clone());
        self.persist_locked(&lock, &state)?;
        Ok(public_session(&self.scope, updated))
    }

    /// Re-reads the durable fence while holding the same OS lock that remains alive in the
    /// returned guard. A second writer cannot pass this guard until the caller drops it.
    pub fn dispatch_guard(
        &self,
        session: &WriterSession,
        now_ms: u64,
    ) -> Result<DispatchGuard, WriterLeaseError> {
        self.require_scope(&session.scope)?;
        let lock = self.lock()?;
        let state = self.require_state()?;
        if state.protection_only {
            return Err(WriterLeaseError::ProtectionOnly);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_session(current, session, now_ms)?;
        Ok(DispatchGuard { _lock: lock })
    }

    /// Holds the exact durable writer identity for a resident mutation batch without treating
    /// the short recovery TTL as a hot-path deadline. TTL expiry never elects a replacement, so
    /// scope/token/revision plus this OS-exclusive lock still preserve one physical writer. A
    /// restarted resident must continue through its normal private-readback recovery before it
    /// can obtain the current session.
    pub fn persistent_dispatch_guard(
        &self,
        session: &WriterSession,
    ) -> Result<DispatchGuard, WriterLeaseError> {
        self.require_scope(&session.scope)?;
        let lock = self.lock()?;
        let state = self.require_state()?;
        if state.protection_only {
            return Err(WriterLeaseError::ProtectionOnly);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, session)?;
        Ok(DispatchGuard { _lock: lock })
    }

    /// Keeps the predecessor able to maintain or flatten already-open risk after takeover has
    /// fenced every entry-capable dispatch. It never re-enables ordinary mutation authority.
    pub fn protection_dispatch_guard(
        &self,
        session: &WriterSession,
        now_ms: u64,
    ) -> Result<DispatchGuard, WriterLeaseError> {
        self.require_scope(&session.scope)?;
        let lock = self.lock()?;
        let state = self.require_state()?;
        if !state.protection_only {
            return Err(WriterLeaseError::NotProtectionOnly);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_session(current, session, now_ms)?;
        Ok(DispatchGuard { _lock: lock })
    }

    /// TTL expiry never elects a replacement. Only this exact, newer flat receipt can consume
    /// the predecessor and activate the requested successor.
    pub fn consume_flat_receipt(
        &self,
        successor_scope: &WriterScope,
        receipt: &FlatReceipt,
        now_ms: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        self.require_scope(successor_scope)?;
        validate_receipt_id(&receipt.receipt_id)?;
        if receipt.scope != self.scope || receipt.predecessor.scope != self.scope {
            return Err(WriterLeaseError::Scope);
        }
        if receipt.readback_generation <= receipt.predecessor.readback_generation
            || !valid_summary(&receipt.summary_sha256)
        {
            return Err(WriterLeaseError::Receipt);
        }
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if state.consumed_flat_receipts.contains(&receipt.receipt_id) {
            return Err(WriterLeaseError::ReceiptConsumed);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, &receipt.predecessor)?;
        let next_revision = increment(state.revision)?;
        let generation = state.next_generation;
        state.next_generation = increment(generation)?;
        let successor = DurableSession {
            token: session_token(
                &self.path,
                successor_scope,
                generation,
                next_revision,
                receipt.readback_generation,
                now_ms,
            )?,
            generation,
            revision: next_revision,
            readback_generation: receipt.readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.scope = successor_scope.clone();
        state.active = Some(successor.clone());
        state.protection_only = false;
        state
            .consumed_flat_receipts
            .insert(receipt.receipt_id.clone());
        self.persist_locked(&lock, &state)?;
        Ok(public_session(successor_scope, successor))
    }

    /// Retires the exact active writer after a newer signed readback proves the whole scope flat.
    /// The durable authority remains in place, so a later run receives the next generation rather
    /// than creating an unrelated lock in a run-local directory.
    pub fn retire_flat(&self, receipt: &FlatReceipt) -> Result<(), WriterLeaseError> {
        validate_receipt_id(&receipt.receipt_id)?;
        if receipt.scope != self.scope
            || receipt.predecessor.scope != self.scope
            || receipt.readback_generation <= receipt.predecessor.readback_generation
            || !valid_summary(&receipt.summary_sha256)
        {
            return Err(WriterLeaseError::Receipt);
        }
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if state.consumed_flat_receipts.contains(&receipt.receipt_id) {
            return Err(WriterLeaseError::ReceiptConsumed);
        }
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, &receipt.predecessor)?;
        state.revision = increment(state.revision)?;
        state.active = None;
        state.protection_only = false;
        state
            .consumed_flat_receipts
            .insert(receipt.receipt_id.clone());
        self.persist_locked(&lock, &state)
    }

    /// A protected receipt changes no writer identity. It only records that the predecessor may
    /// remain in its protection-only path; successor activation still requires a Flat receipt.
    pub fn retain_protected_predecessor(
        &self,
        receipt: &ProtectedReceipt,
    ) -> Result<(), WriterLeaseError> {
        self.require_scope(&receipt.scope)?;
        if receipt.predecessor.scope != self.scope
            || receipt.readback_generation <= receipt.predecessor.readback_generation
            || !valid_summary(&receipt.summary_sha256)
        {
            return Err(WriterLeaseError::Receipt);
        }
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        let current = state.active.as_ref().ok_or(WriterLeaseError::NoWriter)?;
        require_exact_session(current, &receipt.predecessor)?;
        state.revision = increment(state.revision)?;
        state.protection_only = true;
        if let Some(active) = state.active.as_mut() {
            active.revision = state.revision;
            active.readback_generation = receipt.readback_generation;
        }
        self.persist_locked(&lock, &state)
    }

    /// Permanently fences this exact predecessor before a non-flat executable handoff. The
    /// handoff receipt must be bound to the final signed private generation. It may equal the
    /// exact predecessor session's watermark only when that session was already refreshed from
    /// that signed snapshot; fencing removes the active session so an old binary cannot renew,
    /// recover, or dispatch.
    pub fn fence_for_executable_handoff(
        &self,
        receipt: &ExecutableHandoffReceipt,
    ) -> Result<(), WriterLeaseError> {
        validate_executable_handoff_receipt(receipt, &self.scope)?;
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if state.pending_executable_handoff.is_some() {
            return Err(WriterLeaseError::HandoffPending);
        }
        if state
            .consumed_executable_handoffs
            .contains(&receipt.receipt_id)
        {
            return Err(WriterLeaseError::HandoffReceiptConsumed);
        }
        let current = state
            .active
            .as_ref()
            .ok_or(WriterLeaseError::NoWriter)?
            .clone();
        require_exact_session(&current, &receipt.predecessor)?;
        state.revision = increment(state.revision)?;
        state.active = None;
        state.protection_only = false;
        state.pending_executable_handoff = Some(DurableExecutableHandoffFence {
            receipt_id: receipt.receipt_id.clone(),
            predecessor: current,
            readback_generation: receipt.readback_generation,
            handoff_sha256: receipt.handoff_sha256.clone(),
            successor_executable_sha256: receipt.successor_executable_sha256.clone(),
        });
        self.persist_locked(&lock, &state)
    }

    /// Activates the one fenced successor. The caller supplies the digest measured from the
    /// running executable; a mismatched or replayed handoff stays fenced.
    pub fn activate_executable_handoff_successor(
        &self,
        receipt: &ExecutableHandoffReceipt,
        current_executable_sha256: &str,
        now_ms: u64,
    ) -> Result<WriterSession, WriterLeaseError> {
        validate_executable_handoff_receipt(receipt, &self.scope)?;
        if current_executable_sha256 != receipt.successor_executable_sha256
            || !valid_summary(current_executable_sha256)
        {
            return Err(WriterLeaseError::Receipt);
        }
        let lock = self.lock()?;
        let mut state = self.require_state()?;
        if state
            .consumed_executable_handoffs
            .contains(&receipt.receipt_id)
        {
            return Err(WriterLeaseError::HandoffReceiptConsumed);
        }
        let fence = state
            .pending_executable_handoff
            .as_ref()
            .ok_or(WriterLeaseError::HandoffPending)?;
        if fence.receipt_id != receipt.receipt_id
            || fence.predecessor != durable_session(&receipt.predecessor)
            || fence.readback_generation != receipt.readback_generation
            || fence.handoff_sha256 != receipt.handoff_sha256
            || fence.successor_executable_sha256 != receipt.successor_executable_sha256
            || state.active.is_some()
            || state.protection_only
        {
            return Err(WriterLeaseError::Receipt);
        }
        let next_revision = increment(state.revision)?;
        let generation = state.next_generation;
        state.next_generation = increment(generation)?;
        let successor = DurableSession {
            token: session_token(
                &self.path,
                &self.scope,
                generation,
                next_revision,
                receipt.readback_generation,
                now_ms,
            )?,
            generation,
            revision: next_revision,
            readback_generation: receipt.readback_generation,
            valid_until_ms: lease_until(now_ms)?,
        };
        state.revision = next_revision;
        state.active = Some(successor.clone());
        state.pending_executable_handoff = None;
        state
            .consumed_executable_handoffs
            .insert(receipt.receipt_id.clone());
        self.persist_locked(&lock, &state)?;
        Ok(public_session(&self.scope, successor))
    }

    pub fn active_session(&self) -> Result<Option<WriterSession>, WriterLeaseError> {
        let state = self.read_recovered()?;
        match state {
            Some(state) => {
                self.validate_state(&state)?;
                Ok(state
                    .active
                    .map(|active| public_session(&state.scope, active)))
            }
            None => Ok(None),
        }
    }

    /// Returns the exact active writer only when it still owns ordinary entry-capable authority.
    /// Read-only handoff validation uses this to reject a protection-only predecessor without
    /// renewing, replacing, or otherwise mutating the durable writer state.
    pub fn active_entry_session(&self) -> Result<Option<WriterSession>, WriterLeaseError> {
        let state = self.read_recovered()?;
        match state {
            Some(state) => {
                self.validate_state(&state)?;
                if state.protection_only {
                    return Err(WriterLeaseError::ProtectionOnly);
                }
                Ok(state
                    .active
                    .map(|active| public_session(&state.scope, active)))
            }
            None => Ok(None),
        }
    }

    fn require_scope(&self, scope: &WriterScope) -> Result<(), WriterLeaseError> {
        if scope != &self.scope {
            return Err(WriterLeaseError::Scope);
        }
        Ok(())
    }

    fn load_or_initial(&self) -> Result<DurableState, WriterLeaseError> {
        match self.read_recovered()? {
            Some(state) => {
                self.validate_state(&state)?;
                Ok(state)
            }
            None => Ok(DurableState {
                schema_version: 1,
                revision: 0,
                scope: self.scope.clone(),
                next_generation: 1,
                active: None,
                protection_only: false,
                consumed_flat_receipts: BTreeSet::new(),
                pending_executable_handoff: None,
                consumed_executable_handoffs: BTreeSet::new(),
            }),
        }
    }

    fn require_state(&self) -> Result<DurableState, WriterLeaseError> {
        self.read_recovered()?
            .ok_or(WriterLeaseError::NoWriter)
            .and_then(|state| {
                self.validate_state(&state)?;
                Ok(state)
            })
    }

    fn validate_state(&self, state: &DurableState) -> Result<(), WriterLeaseError> {
        if state.schema_version != 1 || state.scope != self.scope || state.next_generation == 0 {
            return Err(WriterLeaseError::CorruptAuthority);
        }
        if let Some(handoff) = &state.pending_executable_handoff
            && (state.active.is_some()
                || state.protection_only
                || handoff.receipt_id.trim().is_empty()
                || handoff.predecessor.token.is_empty()
                || handoff.predecessor.generation == 0
                || handoff.predecessor.readback_generation == 0
                || handoff.readback_generation < handoff.predecessor.readback_generation
                || !valid_summary(&handoff.handoff_sha256)
                || !valid_summary(&handoff.successor_executable_sha256))
        {
            return Err(WriterLeaseError::CorruptAuthority);
        }
        if let Some(active) = &state.active
            && (active.token.is_empty()
                || active.generation == 0
                || active.revision != state.revision
                || active.readback_generation == 0)
        {
            return Err(WriterLeaseError::CorruptAuthority);
        }
        Ok(())
    }

    fn lock(&self) -> Result<OsLock, WriterLeaseError> {
        let parent = self.path.parent().ok_or(WriterLeaseError::AuthorityPath)?;
        fs::create_dir_all(parent).map_err(WriterLeaseError::Io)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(sibling(&self.path, ".lock"))
            .map_err(WriterLeaseError::Io)?;
        let mut last_error = None;
        for attempt in 0..100 {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(OsLock { file }),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 99 {
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        }
        Err(WriterLeaseError::Lock(last_error.unwrap_or_else(|| {
            std::io::Error::other("writer lock unavailable")
        })))
    }

    fn persist_locked(&self, lock: &OsLock, state: &DurableState) -> Result<(), WriterLeaseError> {
        lock.file.metadata().map_err(WriterLeaseError::Lock)?;
        persist_state(&self.path, state)
    }

    fn read_recovered(&self) -> Result<Option<DurableState>, WriterLeaseError> {
        let candidates = [
            self.path.clone(),
            sibling(&self.path, ".backup"),
            sibling(&self.path, ".next"),
        ];
        let mut decoded = Vec::new();
        let mut present = false;
        for candidate in candidates {
            match fs::read(candidate) {
                Ok(bytes) => {
                    present = true;
                    if let Ok(state) = decode_state(&bytes) {
                        decoded.push(state);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => present = true,
            }
        }
        if let Some(state) = decoded.into_iter().max_by_key(|state| state.revision) {
            return Ok(Some(state));
        }
        if present {
            Err(WriterLeaseError::CorruptAuthority)
        } else {
            Ok(None)
        }
    }
}

fn validate_scope(scope: &WriterScope) -> Result<(), WriterLeaseError> {
    if scope.exchange.trim().is_empty()
        || scope.account.trim().is_empty()
        || scope.owner_scope.trim().is_empty()
    {
        return Err(WriterLeaseError::Scope);
    }
    Ok(())
}

fn validate_receipt_id(receipt_id: &str) -> Result<(), WriterLeaseError> {
    if receipt_id.trim().is_empty() {
        Err(WriterLeaseError::Receipt)
    } else {
        Ok(())
    }
}

fn valid_summary(summary: &str) -> bool {
    summary.len() == 64 && summary.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_executable_handoff_receipt(
    receipt: &ExecutableHandoffReceipt,
    scope: &WriterScope,
) -> Result<(), WriterLeaseError> {
    validate_receipt_id(&receipt.receipt_id)?;
    if receipt.scope != *scope
        || receipt.predecessor.scope != *scope
        || receipt.readback_generation < receipt.predecessor.readback_generation
        || !valid_summary(&receipt.handoff_sha256)
        || !valid_summary(&receipt.successor_executable_sha256)
    {
        return Err(WriterLeaseError::Receipt);
    }
    Ok(())
}

fn durable_session(session: &WriterSession) -> DurableSession {
    DurableSession {
        token: session.token.clone(),
        generation: session.generation,
        revision: session.revision,
        readback_generation: session.readback_generation,
        valid_until_ms: session.valid_until_ms,
    }
}

fn require_session(
    durable: &DurableSession,
    session: &WriterSession,
    now_ms: u64,
) -> Result<(), WriterLeaseError> {
    require_exact_session(durable, session)?;
    if durable.valid_until_ms <= now_ms {
        return Err(WriterLeaseError::Expired);
    }
    Ok(())
}

fn require_exact_session(
    durable: &DurableSession,
    session: &WriterSession,
) -> Result<(), WriterLeaseError> {
    if durable.token != session.token
        || durable.generation != session.generation
        || durable.revision != session.revision
        || durable.readback_generation != session.readback_generation
        || durable.valid_until_ms != session.valid_until_ms
    {
        return Err(WriterLeaseError::Fenced);
    }
    Ok(())
}

fn public_session(scope: &WriterScope, session: DurableSession) -> WriterSession {
    WriterSession {
        scope: scope.clone(),
        token: session.token,
        generation: session.generation,
        revision: session.revision,
        readback_generation: session.readback_generation,
        valid_until_ms: session.valid_until_ms,
    }
}

fn increment(value: u64) -> Result<u64, WriterLeaseError> {
    value.checked_add(1).ok_or(WriterLeaseError::Generation)
}

fn lease_until(now_ms: u64) -> Result<u64, WriterLeaseError> {
    if now_ms == 0 {
        return Err(WriterLeaseError::Generation);
    }
    now_ms
        .checked_add(WRITER_LEASE_TTL_MS)
        .ok_or(WriterLeaseError::Generation)
}

fn session_token(
    authority_path: &Path,
    scope: &WriterScope,
    generation: u64,
    revision: u64,
    readback_generation: u64,
    now_ms: u64,
) -> Result<String, WriterLeaseError> {
    let encoded = serde_json::to_vec(&(
        authority_path,
        scope,
        generation,
        revision,
        readback_generation,
        now_ms,
        std::process::id(),
    ))
    .map_err(WriterLeaseError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn decode_state(bytes: &[u8]) -> Result<DurableState, WriterLeaseError> {
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Err(WriterLeaseError::CorruptAuthority);
    }
    serde_json::from_slice(bytes).map_err(|_| WriterLeaseError::CorruptAuthority)
}

fn persist_state(path: &Path, state: &DurableState) -> Result<(), WriterLeaseError> {
    let parent = path.parent().ok_or(WriterLeaseError::AuthorityPath)?;
    fs::create_dir_all(parent).map_err(WriterLeaseError::Io)?;
    let mut encoded = serde_json::to_vec(state).map_err(WriterLeaseError::Encode)?;
    encoded.push(b'\n');
    let next = sibling(path, ".next");
    let backup = sibling(path, ".backup");
    write_synced(&next, &encoded)?;
    if path.exists() {
        fs::copy(path, &backup).map_err(WriterLeaseError::Io)?;
        sync_file(&backup)?;
    }
    fs::rename(&next, path)
        .or_else(|_| {
            fs::copy(&next, path)?;
            fs::remove_file(&next)
        })
        .map_err(WriterLeaseError::Io)?;
    sync_file(path)?;
    write_synced(&backup, &encoded)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), WriterLeaseError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(WriterLeaseError::Io)?;
    file.write_all(bytes).map_err(WriterLeaseError::Io)?;
    file.sync_all().map_err(WriterLeaseError::Io)
}

fn sync_file(path: &Path) -> Result<(), WriterLeaseError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(WriterLeaseError::Io)
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name: std::ffi::OsString = path
        .file_name()
        .map_or_else(|| "authority".into(), Into::into);
    name.push(suffix);
    path.with_file_name(name)
}

#[derive(Debug, thiserror::Error)]
pub enum WriterLeaseError {
    #[error("writer authority path must be absolute and have a parent")]
    AuthorityPath,
    #[error("writer scope is incomplete or differs from the authority")]
    Scope,
    #[error("writer authority is corrupt or every recovery snapshot is invalid")]
    CorruptAuthority,
    #[error("writer lease is currently held by another process")]
    Lock(#[source] std::io::Error),
    #[error("writer authority I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("writer authority serialization failed: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("an active writer already exists; TTL cannot elect a replacement")]
    WriterExists,
    #[error("no active writer exists")]
    NoWriter,
    #[error("writer session has expired")]
    Expired,
    #[error("writer session token, generation, revision, or TTL was fenced")]
    Fenced,
    #[error("predecessor is restricted to protection-only handling")]
    ProtectionOnly,
    #[error("writer is not restricted to the protection-only path")]
    NotProtectionOnly,
    #[error("writer generation or revision is exhausted")]
    Generation,
    #[error("takeover receipt is incomplete or inconsistent")]
    Receipt,
    #[error("flat takeover receipt was already consumed")]
    ReceiptConsumed,
    #[error("an executable handoff is fenced and requires its exact successor activation")]
    HandoffPending,
    #[error("executable handoff receipt was already consumed")]
    HandoffReceiptConsumed,
}
