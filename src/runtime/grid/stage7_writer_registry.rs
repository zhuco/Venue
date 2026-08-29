use std::path::Path;

use crate::execution::WriterScope;

#[cfg(not(test))]
pub(super) type Stage7CanonicalRootGuard = venue_execution::AccountCanonicalRootGuard;
pub(super) type Stage7WriterRegistryError = venue_execution::AccountCanonicalRootError;

#[cfg(test)]
pub(super) struct Stage7CanonicalRootGuard {
    canonical_root_sha256: String,
}

#[cfg(test)]
impl Stage7CanonicalRootGuard {
    pub(super) fn canonical_root_sha256(&self) -> &str {
        &self.canonical_root_sha256
    }
}

#[cfg(not(test))]
pub(super) fn acquire(
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    venue_execution::acquire_account_canonical_root(scope, artifacts_root)
}

#[cfg(test)]
pub(super) fn acquire(
    _scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    use sha2::{Digest, Sha256};

    if !artifacts_root.is_absolute() {
        return Err(Stage7WriterRegistryError::AbsolutePath);
    }
    std::fs::create_dir_all(artifacts_root).map_err(|source| Stage7WriterRegistryError::Io {
        path: artifacts_root.to_path_buf(),
        source,
    })?;
    let canonical =
        std::fs::canonicalize(artifacts_root).map_err(|source| Stage7WriterRegistryError::Io {
            path: artifacts_root.to_path_buf(),
            source,
        })?;
    let encoded = canonical
        .to_str()
        .ok_or(Stage7WriterRegistryError::PathEncoding)?;
    let digest = Sha256::digest(encoded.as_bytes());
    let canonical_root_sha256 = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(Stage7CanonicalRootGuard {
        canonical_root_sha256,
    })
}
