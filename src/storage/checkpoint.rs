use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::storage::{StorageError, TradingFacts};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub journal_sequence: u64,
    pub facts: TradingFacts,
}

#[derive(Debug)]
pub struct CheckpointStore {
    path: PathBuf,
}

impl CheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, StorageError> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(StorageError::Decode),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(StorageError::Io {
                path: self.path.clone(),
                source,
            }),
        }
    }

    pub fn save(&self, checkpoint: &Checkpoint) -> Result<(), StorageError> {
        save_json_atomic(&self.path, checkpoint)
    }
}

/// Atomic JSON persistence for a non-authoritative business projection. Physical trading facts
/// remain in the journal and `TradingFacts`; strategy snapshots may only cache rebuildable state.
#[derive(Debug)]
pub struct ProjectionStore {
    path: PathBuf,
}

impl ProjectionStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load<T: for<'de> Deserialize<'de>>(&self) -> Result<Option<T>, StorageError> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(StorageError::Decode),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(StorageError::Io {
                path: self.path.clone(),
                source,
            }),
        }
    }

    pub fn save<T: Serialize>(&self, projection: &T) -> Result<(), StorageError> {
        save_json_atomic(&self.path, projection)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

fn save_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), StorageError> {
    let encoded = serde_json::to_vec(value).map_err(StorageError::Encode)?;
    let temporary = temporary_path(path);
    let result = (|| {
        let mut file = File::create(&temporary).map_err(|source| StorageError::Io {
            path: temporary.clone(),
            source,
        })?;
        file.write_all(&encoded)
            .map_err(|source| StorageError::Io {
                path: temporary.clone(),
                source,
            })?;
        file.sync_all().map_err(|source| StorageError::Io {
            path: temporary.clone(),
            source,
        })?;
        fs::rename(&temporary, path).map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_os_string();
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    temporary.push(format!(".tmp.{}.{sequence}", std::process::id()));
    PathBuf::from(temporary)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), StorageError> {
    let parent = path.parent().ok_or_else(|| StorageError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "atomic storage path has no parent directory",
        ),
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StorageError::Io {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), StorageError> {
    Ok(())
}
