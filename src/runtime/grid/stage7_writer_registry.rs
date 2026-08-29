use std::path::Path;

use crate::execution::WriterScope;

pub(super) type Stage7CanonicalRootGuard = venue_execution::AccountCanonicalRootGuard;
pub(super) type Stage7WriterRegistryError = venue_execution::AccountCanonicalRootError;

#[cfg(not(test))]
pub(super) fn acquire(
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    venue_execution::acquire_account_canonical_root(scope, artifacts_root)
}

#[cfg(test)]
pub(super) fn acquire(
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    venue_execution::acquire_account_canonical_root_in(
        &artifacts_root.join(".stage7_writer_registry_test"),
        scope,
        artifacts_root,
    )
}
