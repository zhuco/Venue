use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use super::private_session::{PrivateSessionBinding, PrivateSessionState};

const PRIVATE_SESSION_STATE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PrivateSessionSnapshot {
    schema_version: u16,
    binding: PrivateSessionBinding,
    revision: u64,
    generation: u64,
    state: PrivateSessionState,
    evidence_sequence: u64,
}

impl PrivateSessionSnapshot {
    fn validate(&self, binding: &PrivateSessionBinding) -> Result<(), DurablePrivateSessionError> {
        if self.schema_version != PRIVATE_SESSION_STATE_SCHEMA_VERSION
            || &self.binding != binding
            || self.revision == 0
            || self.generation == 0
        {
            return Err(DurablePrivateSessionError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct DurablePrivateSessionStore {
    path: PathBuf,
    lock_path: PathBuf,
    binding: PrivateSessionBinding,
}

impl DurablePrivateSessionStore {
    pub(super) fn open(path: impl Into<PathBuf>, binding: PrivateSessionBinding) -> Self {
        let path = path.into();
        let lock_path = sibling(&path, ".lock");
        Self {
            path,
            lock_path,
            binding,
        }
    }

    pub(super) fn recover_and_fence(
        self,
        evidence_sequence: u64,
    ) -> Result<DurablePrivateSessionState, DurablePrivateSessionError> {
        let lock = acquire_lock(&self.lock_path)?;
        let current = load(&self.path)?;
        let snapshot = match current {
            Some(current) => {
                current.validate(&self.binding)?;
                PrivateSessionSnapshot {
                    schema_version: PRIVATE_SESSION_STATE_SCHEMA_VERSION,
                    binding: self.binding.clone(),
                    revision: increment(current.revision)?,
                    generation: increment(current.generation)?,
                    state: PrivateSessionState::Reconnecting,
                    evidence_sequence: evidence_sequence.max(current.evidence_sequence),
                }
            }
            None => PrivateSessionSnapshot {
                schema_version: PRIVATE_SESSION_STATE_SCHEMA_VERSION,
                binding: self.binding.clone(),
                revision: 1,
                generation: 1,
                state: PrivateSessionState::Reconnecting,
                evidence_sequence,
            },
        };
        save(&self.path, &snapshot)?;
        drop(lock);
        Ok(DurablePrivateSessionState {
            store: self,
            snapshot,
        })
    }

    fn acquire(
        &self,
        expected: &PrivateSessionSnapshot,
    ) -> Result<PrivateSessionStateGuard, DurablePrivateSessionError> {
        let lock = acquire_lock(&self.lock_path)?;
        let current = load(&self.path)?.ok_or(DurablePrivateSessionError::Missing)?;
        current.validate(&self.binding)?;
        if &current != expected {
            return Err(DurablePrivateSessionError::StaleWorker);
        }
        Ok(PrivateSessionStateGuard {
            path: self.path.clone(),
            lock,
            current,
        })
    }
}

#[derive(Debug)]
pub(super) struct DurablePrivateSessionState {
    store: DurablePrivateSessionStore,
    snapshot: PrivateSessionSnapshot,
}

impl DurablePrivateSessionState {
    pub(super) fn snapshot(&self) -> DurablePrivateSessionSnapshotRef {
        DurablePrivateSessionSnapshotRef {
            generation: self.snapshot.generation,
            state: self.snapshot.state,
        }
    }

    pub(super) fn begin_transition(
        &self,
    ) -> Result<PrivateSessionStateGuard, DurablePrivateSessionError> {
        self.store.acquire(&self.snapshot)
    }

    pub(super) fn finish_transition(
        &mut self,
        guard: PrivateSessionStateGuard,
        generation: u64,
        state: PrivateSessionState,
        evidence_sequence: u64,
    ) -> Result<(), DurablePrivateSessionError> {
        self.snapshot = guard.commit(generation, state, evidence_sequence)?;
        Ok(())
    }

    pub(super) fn commit(
        &mut self,
        generation: u64,
        state: PrivateSessionState,
        evidence_sequence: u64,
    ) -> Result<(), DurablePrivateSessionError> {
        let guard = self.begin_transition()?;
        self.finish_transition(guard, generation, state, evidence_sequence)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DurablePrivateSessionSnapshotRef {
    pub(super) generation: u64,
    pub(super) state: PrivateSessionState,
}

pub(super) struct PrivateSessionStateGuard {
    path: PathBuf,
    lock: File,
    current: PrivateSessionSnapshot,
}

impl PrivateSessionStateGuard {
    fn commit(
        self,
        generation: u64,
        state: PrivateSessionState,
        evidence_sequence: u64,
    ) -> Result<PrivateSessionSnapshot, DurablePrivateSessionError> {
        if generation < self.current.generation
            || generation > increment(self.current.generation)?
            || evidence_sequence < self.current.evidence_sequence
        {
            return Err(DurablePrivateSessionError::Regression);
        }
        let next = PrivateSessionSnapshot {
            schema_version: PRIVATE_SESSION_STATE_SCHEMA_VERSION,
            binding: self.current.binding.clone(),
            revision: increment(self.current.revision)?,
            generation,
            state,
            evidence_sequence,
        };
        save(&self.path, &next)?;
        drop(self.lock);
        Ok(next)
    }
}

fn acquire_lock(path: &Path) -> Result<File, DurablePrivateSessionError> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .map_err(|source| io(path, source))?;
    file.lock_exclusive().map_err(|source| io(path, source))?;
    Ok(file)
}

fn load(path: &Path) -> Result<Option<PrivateSessionSnapshot>, DurablePrivateSessionError> {
    let mut found = false;
    let mut candidates = Vec::new();
    for candidate_path in [
        path.to_path_buf(),
        sibling(path, ".next"),
        sibling(path, ".backup"),
    ] {
        match fs::read(&candidate_path) {
            Ok(bytes) => {
                found = true;
                if let Ok(snapshot) = serde_json::from_slice::<PrivateSessionSnapshot>(&bytes) {
                    candidates.push(snapshot);
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io(&candidate_path, source)),
        }
    }
    if candidates.is_empty() {
        return if found {
            Err(DurablePrivateSessionError::Invalid)
        } else {
            Ok(None)
        };
    }
    candidates.sort_by_key(|snapshot| snapshot.revision);
    let selected = candidates
        .last()
        .cloned()
        .ok_or(DurablePrivateSessionError::Invalid)?;
    if candidates
        .iter()
        .any(|candidate| candidate.revision == selected.revision && candidate != &selected)
    {
        return Err(DurablePrivateSessionError::Invalid);
    }
    Ok(Some(selected))
}

fn save(path: &Path, snapshot: &PrivateSessionSnapshot) -> Result<(), DurablePrivateSessionError> {
    snapshot.validate(&snapshot.binding)?;
    let mut encoded = serde_json::to_vec(snapshot).map_err(DurablePrivateSessionError::Encode)?;
    encoded.push(b'\n');
    let next = sibling(path, ".next");
    let backup = sibling(path, ".backup");
    write_synced(&next, &encoded)?;
    if path.exists() {
        fs::copy(path, &backup).map_err(|source| io(&backup, source))?;
        sync_file(&backup)?;
    }
    fs::rename(&next, path)
        .or_else(|_| {
            fs::copy(&next, path)?;
            fs::remove_file(&next)
        })
        .map_err(|source| io(path, source))?;
    sync_file(path)?;
    write_synced(&backup, &encoded)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), DurablePrivateSessionError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|source| io(path, source))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| io(path, source))
}

fn sync_file(path: &Path) -> Result<(), DurablePrivateSessionError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io(path, source))
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn increment(value: u64) -> Result<u64, DurablePrivateSessionError> {
    value
        .checked_add(1)
        .ok_or(DurablePrivateSessionError::Generation)
}

fn io(path: &Path, source: std::io::Error) -> DurablePrivateSessionError {
    DurablePrivateSessionError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum DurablePrivateSessionError {
    #[error("private-session state is invalid or bound to another account")]
    Invalid,
    #[error("private-session state is missing")]
    Missing,
    #[error("private-session generation or evidence watermark regressed")]
    Regression,
    #[error("private-session generation is exhausted")]
    Generation,
    #[error("a stale private-session worker cannot overwrite the current state")]
    StaleWorker,
    #[error("private-session state I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("private-session state encoding failed: {0}")]
    Encode(serde_json::Error),
}
