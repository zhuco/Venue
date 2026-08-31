use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use venue_domain::domain::{OrderOwner, Symbol, is_canonical_trading_account_id};
use venue_gateway_api::VenueId;

use crate::WriterScope;

const REGISTRY_SCHEMA_VERSION: u16 = 2;
const REGISTRY_DIRECTORY: &str = "stage7_writer_roots";

/// Explicit, deployment-supplied link to a frozen Stage-7 v1 writer scope. The legacy account
/// is a product scope (not the new trading UUID), so Runtime must never attempt to derive it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyV1WriterPredecessor {
    pub exchange: VenueId,
    /// The one canonical unified account that may consume this frozen legacy product scope.
    /// This is deployment-supplied and checked again by `AccountMutationHost`; a predecessor
    /// record is never a transferable authority for a different account.
    pub successor_trading_account_id: String,
    pub legacy_product_account: String,
    pub legacy_symbol: Symbol,
    pub legacy_owner_scope: String,
    /// The legacy WAL owner is not the new canonical account. These deployment-supplied values
    /// are compared to every preserved WAL Owner before it may enter the cancellation-only route.
    pub legacy_strategy_instance_id: String,
    pub legacy_run_id: String,
    pub legacy_artifacts_root: PathBuf,
    pub legacy_lock_sha256: String,
    pub legacy_lock_path: PathBuf,
    pub handoff_sha256: String,
}

/// Retains the exact frozen writer lock through the lifetime of the unified account Host.
#[derive(Debug)]
pub struct LegacyV1WriterGuard {
    _lock: File,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableLegacyV1Handoff {
    schema_version: u16,
    scope_sha256: String,
    scope: WriterScope,
    #[serde(default)]
    legacy_strategy_instance_id: String,
    #[serde(default)]
    legacy_run_id: String,
    canonical_artifacts_root: String,
    canonical_root_sha256: String,
    entry_sha256: String,
}

#[derive(Serialize)]
struct LegacyV1EntryDigest<'a> {
    schema_version: u16,
    scope_sha256: &'a str,
    scope: &'a WriterScope,
    legacy_strategy_instance_id: &'a str,
    legacy_run_id: &'a str,
    canonical_artifacts_root: &'a str,
    canonical_root_sha256: &'a str,
}

#[derive(Serialize)]
struct LegacyV1EntryDigestV1<'a> {
    schema_version: u16,
    scope_sha256: &'a str,
    scope: &'a WriterScope,
    canonical_artifacts_root: &'a str,
    canonical_root_sha256: &'a str,
}

impl LegacyV1WriterPredecessor {
    pub fn validate(&self) -> Result<(), Stage7WriterRegistryError> {
        validate_legacy_predecessor(self, &legacy_v1_registry_root()?)
    }

    pub fn acquire(&self) -> Result<LegacyV1WriterGuard, Stage7WriterRegistryError> {
        acquire_legacy_predecessor(self, &legacy_v1_registry_root()?)
    }

    #[must_use]
    pub fn matches_legacy_owner(&self, owner: &OrderOwner) -> bool {
        owner.exchange == self.exchange.as_str()
            && owner.account == self.legacy_product_account
            && owner.symbol == self.legacy_symbol
            && owner.strategy_instance_id == self.legacy_strategy_instance_id
            && owner.run_id == self.legacy_run_id
    }
}

fn validate_legacy_predecessor(
    predecessor: &LegacyV1WriterPredecessor,
    registry_root: &Path,
) -> Result<(), Stage7WriterRegistryError> {
    if !is_canonical_trading_account_id(&predecessor.successor_trading_account_id)
        || predecessor.legacy_product_account.trim().is_empty()
        || predecessor.legacy_owner_scope.trim().is_empty()
        || !valid_owner_identity(&predecessor.legacy_strategy_instance_id)
        || !valid_owner_identity(&predecessor.legacy_run_id)
        || !predecessor.legacy_artifacts_root.is_absolute()
        || !valid_sha256(&predecessor.legacy_lock_sha256)
        || !valid_sha256(&predecessor.handoff_sha256)
    {
        return Err(Stage7WriterRegistryError::LegacyPredecessor);
    }
    let expected = registry_root.join(format!("{}.lock", predecessor.legacy_lock_sha256));
    if predecessor.legacy_lock_path != expected {
        return Err(Stage7WriterRegistryError::LegacyPredecessor);
    }
    let canonical_root = fs::canonicalize(&predecessor.legacy_artifacts_root)
        .map_err(|source| io_error(&predecessor.legacy_artifacts_root, source))?;
    let canonical_root = canonical_root
        .to_str()
        .ok_or(Stage7WriterRegistryError::PathEncoding)?
        .to_owned();
    let handoff_path = registry_root.join(format!("{}.json", predecessor.legacy_lock_sha256));
    let encoded = fs::read(&handoff_path).map_err(|source| io_error(&handoff_path, source))?;
    if digest_bytes(&encoded) != predecessor.handoff_sha256 {
        return Err(Stage7WriterRegistryError::LegacyPredecessor);
    }
    let handoff = serde_json::from_slice::<DurableLegacyV1Handoff>(&encoded)
        .map_err(Stage7WriterRegistryError::Decode)?;
    let expected_scope = WriterScope {
        exchange: predecessor.exchange.as_str().to_owned(),
        account: predecessor.legacy_product_account.clone(),
        symbol: predecessor.legacy_symbol.clone(),
        owner_scope: predecessor.legacy_owner_scope.clone(),
    };
    let scope_sha256 = digest_json(&expected_scope)?;
    let owner_identity_matches = handoff.legacy_strategy_instance_id
        == predecessor.legacy_strategy_instance_id
        && handoff.legacy_run_id == predecessor.legacy_run_id;
    if !matches!(handoff.schema_version, 1 | 2)
        || handoff.scope != expected_scope
        || handoff.scope_sha256 != scope_sha256
        || handoff.scope_sha256 != predecessor.legacy_lock_sha256
        || handoff.canonical_artifacts_root != canonical_root
        || handoff.canonical_root_sha256 != digest_bytes(canonical_root.as_bytes())
        || (handoff.schema_version == 1
            && (!handoff.legacy_strategy_instance_id.is_empty()
                || !handoff.legacy_run_id.is_empty()))
        || (handoff.schema_version == 2 && !owner_identity_matches)
        || handoff.entry_sha256 != legacy_v1_entry_digest(&handoff)?
    {
        return Err(Stage7WriterRegistryError::LegacyPredecessor);
    }
    Ok(())
}

fn valid_owner_identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn legacy_v1_entry_digest(
    entry: &DurableLegacyV1Handoff,
) -> Result<String, Stage7WriterRegistryError> {
    match entry.schema_version {
        1 => digest_json(&LegacyV1EntryDigestV1 {
            schema_version: entry.schema_version,
            scope_sha256: &entry.scope_sha256,
            scope: &entry.scope,
            canonical_artifacts_root: &entry.canonical_artifacts_root,
            canonical_root_sha256: &entry.canonical_root_sha256,
        }),
        2 => digest_json(&LegacyV1EntryDigest {
            schema_version: entry.schema_version,
            scope_sha256: &entry.scope_sha256,
            scope: &entry.scope,
            legacy_strategy_instance_id: &entry.legacy_strategy_instance_id,
            legacy_run_id: &entry.legacy_run_id,
            canonical_artifacts_root: &entry.canonical_artifacts_root,
            canonical_root_sha256: &entry.canonical_root_sha256,
        }),
        _ => Err(Stage7WriterRegistryError::LegacyPredecessor),
    }
}

fn acquire_legacy_predecessor(
    predecessor: &LegacyV1WriterPredecessor,
    registry_root: &Path,
) -> Result<LegacyV1WriterGuard, Stage7WriterRegistryError> {
    validate_legacy_predecessor(predecessor, registry_root)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&predecessor.legacy_lock_path)
        .map_err(|source| io_error(&predecessor.legacy_lock_path, source))?;
    lock.try_lock_exclusive()
        .map_err(|source| Stage7WriterRegistryError::Lock {
            path: predecessor.legacy_lock_path.clone(),
            source,
        })?;
    Ok(LegacyV1WriterGuard { _lock: lock })
}

/// Holds the machine-wide scope lock for the complete lifetime of one account writer entry.
/// The durable entry remains after this guard is dropped, so a crash cannot make another root
/// canonical for the same scope.
#[derive(Debug)]
pub struct Stage7CanonicalRootGuard {
    lock: File,
    canonical_root_sha256: String,
}

impl Stage7CanonicalRootGuard {
    pub fn canonical_root_sha256(&self) -> &str {
        &self.canonical_root_sha256
    }
}

pub type AccountCanonicalRootGuard = Stage7CanonicalRootGuard;

impl Drop for Stage7CanonicalRootGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableCanonicalRoot {
    schema_version: u16,
    account_sha256: String,
    account: CanonicalAccountKey,
    first_scope: WriterScope,
    canonical_artifacts_root: String,
    canonical_root_sha256: String,
    entry_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CanonicalAccountKey {
    exchange: VenueId,
    account: String,
}

#[derive(Serialize)]
struct CanonicalRootDigest<'a> {
    schema_version: u16,
    account_sha256: &'a str,
    account: &'a CanonicalAccountKey,
    first_scope: &'a WriterScope,
    canonical_artifacts_root: &'a str,
    canonical_root_sha256: &'a str,
}

pub fn acquire_account_canonical_root(
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    let registry_root = machine_registry_root()?;

    acquire_in(&registry_root, scope, artifacts_root)
}

fn acquire_in(
    registry_root: &Path,
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<Stage7CanonicalRootGuard, Stage7WriterRegistryError> {
    let account = account_key(scope)?;
    if !registry_root.is_absolute() || !artifacts_root.is_absolute() {
        return Err(Stage7WriterRegistryError::AbsolutePath);
    }

    create_directory(registry_root)?;
    create_directory(artifacts_root)?;
    let canonical_registry_root = canonicalize(registry_root)?;
    let canonical_artifacts_root = canonicalize(artifacts_root)?;
    let canonical_artifacts_root = canonical_artifacts_root
        .to_str()
        .ok_or(Stage7WriterRegistryError::PathEncoding)?
        .to_owned();

    let account_sha256 = digest_json(&account)?;
    let entry_path = canonical_registry_root.join(format!("{account_sha256}.json"));
    let lock_path = canonical_registry_root.join(format!("{account_sha256}.lock"));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| io_error(&lock_path, source))?;
    lock.try_lock_exclusive()
        .map_err(|source| Stage7WriterRegistryError::Lock {
            path: lock_path.clone(),
            source,
        })?;
    let canonical_root_sha256 = digest_bytes(canonical_artifacts_root.as_bytes());
    let guard = Stage7CanonicalRootGuard {
        lock,
        canonical_root_sha256: canonical_root_sha256.clone(),
    };

    match fs::read(&entry_path) {
        Ok(encoded) => {
            let existing = serde_json::from_slice::<DurableCanonicalRoot>(&encoded)
                .map_err(Stage7WriterRegistryError::Decode)?;
            validate_entry(&existing, &account, &account_sha256)?;
            if existing.canonical_artifacts_root != canonical_artifacts_root {
                return Err(Stage7WriterRegistryError::CanonicalRootConflict {
                    registered: existing.canonical_artifacts_root,
                    requested: canonical_artifacts_root,
                });
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            let mut entry = DurableCanonicalRoot {
                schema_version: REGISTRY_SCHEMA_VERSION,
                account_sha256,
                account,
                first_scope: scope.clone(),
                canonical_artifacts_root,
                canonical_root_sha256,
                entry_sha256: String::new(),
            };
            entry.entry_sha256 = entry_digest(&entry)?;
            persist_new_entry(&entry_path, &entry)?;
        }
        Err(source) => return Err(io_error(&entry_path, source)),
    }

    Ok(guard)
}

fn account_key(scope: &WriterScope) -> Result<CanonicalAccountKey, Stage7WriterRegistryError> {
    if scope.exchange.trim().is_empty()
        || !is_canonical_trading_account_id(&scope.account)
        || scope.owner_scope.trim().is_empty()
    {
        return Err(Stage7WriterRegistryError::Scope);
    }
    let exchange = scope
        .exchange
        .parse::<VenueId>()
        .map_err(|_| Stage7WriterRegistryError::Scope)?;
    Ok(CanonicalAccountKey {
        exchange,
        account: scope.account.clone(),
    })
}

fn validate_entry(
    entry: &DurableCanonicalRoot,
    account: &CanonicalAccountKey,
    account_sha256: &str,
) -> Result<(), Stage7WriterRegistryError> {
    if entry.schema_version != REGISTRY_SCHEMA_VERSION
        || entry.account != *account
        || entry.account_sha256 != account_sha256
        || account_key(&entry.first_scope)? != *account
        || entry.canonical_artifacts_root.is_empty()
        || entry.canonical_root_sha256 != digest_bytes(entry.canonical_artifacts_root.as_bytes())
        || entry.entry_sha256 != entry_digest(entry)?
    {
        return Err(Stage7WriterRegistryError::Corrupt);
    }
    Ok(())
}

fn entry_digest(entry: &DurableCanonicalRoot) -> Result<String, Stage7WriterRegistryError> {
    digest_json(&CanonicalRootDigest {
        schema_version: entry.schema_version,
        account_sha256: &entry.account_sha256,
        account: &entry.account,
        first_scope: &entry.first_scope,
        canonical_artifacts_root: &entry.canonical_artifacts_root,
        canonical_root_sha256: &entry.canonical_root_sha256,
    })
}

fn persist_new_entry(
    path: &Path,
    entry: &DurableCanonicalRoot,
) -> Result<(), Stage7WriterRegistryError> {
    let encoded = serde_json::to_vec(entry).map_err(Stage7WriterRegistryError::Encode)?;
    let temporary = sibling(path, ".tmp");
    let mut file = File::create(&temporary).map_err(|source| io_error(&temporary, source))?;
    file.write_all(&encoded)
        .map_err(|source| io_error(&temporary, source))?;
    file.sync_all()
        .map_err(|source| io_error(&temporary, source))?;
    fs::rename(&temporary, path).map_err(|source| io_error(path, source))?;
    sync_parent(path)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), Stage7WriterRegistryError> {
    let parent = path
        .parent()
        .ok_or(Stage7WriterRegistryError::AbsolutePath)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error(parent, source))
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), Stage7WriterRegistryError> {
    Ok(())
}

fn create_directory(path: &Path) -> Result<(), Stage7WriterRegistryError> {
    fs::create_dir_all(path).map_err(|source| io_error(path, source))
}

fn canonicalize(path: &Path) -> Result<PathBuf, Stage7WriterRegistryError> {
    fs::canonicalize(path).map_err(|source| io_error(path, source))
}

fn digest_json<T: Serialize + ?Sized>(value: &T) -> Result<String, Stage7WriterRegistryError> {
    serde_json::to_vec(value)
        .map(|encoded| digest_bytes(&encoded))
        .map_err(Stage7WriterRegistryError::Encode)
}

fn digest_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut sibling = path.as_os_str().to_os_string();
    sibling.push(suffix);
    PathBuf::from(sibling)
}

fn io_error(path: &Path, source: std::io::Error) -> Stage7WriterRegistryError {
    Stage7WriterRegistryError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn machine_registry_root() -> Result<PathBuf, Stage7WriterRegistryError> {
    #[cfg(windows)]
    let base = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
    #[cfg(not(windows))]
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".local").join("state"))
        });

    let base = base.ok_or(Stage7WriterRegistryError::MachineState)?;
    if !base.is_absolute() {
        return Err(Stage7WriterRegistryError::MachineState);
    }
    #[cfg(windows)]
    let application_directory = "Venue";
    #[cfg(not(windows))]
    let application_directory = "venue";
    Ok(base
        .join(application_directory)
        .join(REGISTRY_DIRECTORY)
        .join("v2"))
}

fn legacy_v1_registry_root() -> Result<PathBuf, Stage7WriterRegistryError> {
    let v2 = machine_registry_root()?;
    v2.parent()
        .map(|root| root.join("v1"))
        .ok_or(Stage7WriterRegistryError::MachineState)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum Stage7WriterRegistryError {
    #[error("stage-7 canonical writer registry requires absolute paths")]
    AbsolutePath,
    #[error("stage-7 canonical writer registry cannot locate machine-local state")]
    MachineState,
    #[error("account-level canonical writer registry scope is invalid")]
    Scope,
    #[error("stage-7 canonical writer registry path cannot be encoded")]
    PathEncoding,
    #[error("legacy Stage-7 predecessor handoff or exact v1 lock scope is invalid")]
    LegacyPredecessor,
    #[error("stage-7 canonical writer registry entry is corrupt")]
    Corrupt,
    #[error(
        "trading account is already bound to canonical root {registered}; requested {requested}"
    )]
    CanonicalRootConflict {
        registered: String,
        requested: String,
    },
    #[error("stage-7 canonical writer registry lock failed for {path}: {source}")]
    Lock {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("stage-7 canonical writer registry I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("stage-7 canonical writer registry encode failed: {0}")]
    Encode(serde_json::Error),
    #[error("stage-7 canonical writer registry decode failed: {0}")]
    Decode(serde_json::Error),
}

pub type AccountCanonicalRootError = Stage7WriterRegistryError;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    fn scope(exchange: &str) -> Result<WriterScope, Box<dyn std::error::Error>> {
        Ok(WriterScope {
            exchange: exchange.to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
        })
    }

    fn legacy_predecessor(
        registry: &Path,
        artifacts_root: &Path,
        valid_entry: bool,
    ) -> Result<LegacyV1WriterPredecessor, Box<dyn std::error::Error>> {
        fs::create_dir_all(registry)?;
        fs::create_dir_all(artifacts_root)?;
        let lock_sha256 = "a".repeat(64);
        let lock_path = registry.join(format!("{lock_sha256}.lock"));
        File::create(&lock_path)?;
        let mut predecessor = LegacyV1WriterPredecessor {
            exchange: VenueId::Gate,
            successor_trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            legacy_product_account: "usdt_futures".to_owned(),
            legacy_symbol: "DOGE/USDT".parse()?,
            legacy_owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
            legacy_strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            legacy_run_id: "primary".to_owned(),
            legacy_artifacts_root: fs::canonicalize(artifacts_root)?,
            legacy_lock_sha256: lock_sha256,
            legacy_lock_path: lock_path,
            handoff_sha256: "0".repeat(64),
        };
        let canonical = predecessor
            .legacy_artifacts_root
            .to_str()
            .ok_or("legacy root encoding")?
            .to_owned();
        let scope = WriterScope {
            exchange: predecessor.exchange.as_str().to_owned(),
            account: predecessor.legacy_product_account.clone(),
            symbol: predecessor.legacy_symbol.clone(),
            owner_scope: predecessor.legacy_owner_scope.clone(),
        };
        predecessor.legacy_lock_sha256 = digest_json(&scope)?;
        predecessor.legacy_lock_path =
            registry.join(format!("{}.lock", predecessor.legacy_lock_sha256));
        File::create(&predecessor.legacy_lock_path)?;
        let canonical_root_sha256 = digest_bytes(canonical.as_bytes());
        let mut entry = DurableLegacyV1Handoff {
            schema_version: 2,
            scope_sha256: predecessor.legacy_lock_sha256.clone(),
            scope,
            legacy_strategy_instance_id: predecessor.legacy_strategy_instance_id.clone(),
            legacy_run_id: predecessor.legacy_run_id.clone(),
            canonical_artifacts_root: canonical,
            canonical_root_sha256,
            entry_sha256: String::new(),
        };
        entry.entry_sha256 = legacy_v1_entry_digest(&entry)?;
        if !valid_entry {
            entry.entry_sha256 = "0".repeat(64);
        }
        let handoff = serde_json::json!({
            "schema_version": 2,
            "scope_sha256": entry.scope_sha256,
            "scope": entry.scope,
            "legacy_strategy_instance_id": entry.legacy_strategy_instance_id,
            "legacy_run_id": entry.legacy_run_id,
            "canonical_artifacts_root": entry.canonical_artifacts_root,
            "canonical_root_sha256": entry.canonical_root_sha256,
            "entry_sha256": entry.entry_sha256,
        });
        let encoded = serde_json::to_vec(&handoff)?;
        predecessor.handoff_sha256 = digest_bytes(&encoded);
        fs::write(
            registry.join(format!("{}.json", predecessor.legacy_lock_sha256)),
            encoded,
        )?;
        Ok(predecessor)
    }

    #[test]
    fn explicit_legacy_predecessor_handoff_binds_root_scope_hash_and_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("v1");
        let predecessor = legacy_predecessor(&registry, &temporary.path().join("legacy"), true)?;
        let first = acquire_legacy_predecessor(&predecessor, &registry)?;
        assert!(matches!(
            acquire_legacy_predecessor(&predecessor, &registry),
            Err(Stage7WriterRegistryError::Lock { .. })
        ));
        drop(first);
        assert!(acquire_legacy_predecessor(&predecessor, &registry).is_ok());
        Ok(())
    }

    #[test]
    fn legacy_owner_match_never_substitutes_the_successor_account()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("v1");
        let predecessor = legacy_predecessor(&registry, &temporary.path().join("legacy"), true)?;
        let owner = OrderOwner {
            strategy_instance_id: predecessor.legacy_strategy_instance_id.clone(),
            run_id: predecessor.legacy_run_id.clone(),
            exchange: predecessor.exchange.as_str().to_owned(),
            account: predecessor.legacy_product_account.clone(),
            symbol: predecessor.legacy_symbol.clone(),
            purpose: venue_domain::domain::OrderPurpose::Reduce,
        };
        assert!(predecessor.matches_legacy_owner(&owner));
        let mut successor_owner = owner.clone();
        successor_owner.account = predecessor.successor_trading_account_id.clone();
        assert!(!predecessor.matches_legacy_owner(&successor_owner));
        let mut different_run = owner;
        different_run.run_id = "other-run".to_owned();
        assert!(!predecessor.matches_legacy_owner(&different_run));
        Ok(())
    }

    #[test]
    fn legacy_predecessor_rejects_handoff_with_a_different_owner_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("v1");
        let predecessor = legacy_predecessor(&registry, &temporary.path().join("legacy"), true)?;
        let mut different_owner = predecessor.clone();
        different_owner.legacy_run_id = "other-run".to_owned();
        assert!(matches!(
            acquire_legacy_predecessor(&different_owner, &registry),
            Err(Stage7WriterRegistryError::LegacyPredecessor)
        ));
        let mut different_instance = predecessor;
        different_instance.legacy_strategy_instance_id = "other-instance".to_owned();
        assert!(matches!(
            acquire_legacy_predecessor(&different_instance, &registry),
            Err(Stage7WriterRegistryError::LegacyPredecessor)
        ));
        Ok(())
    }

    #[test]
    fn schema_one_handoff_remains_lock_compatible_until_import_derives_the_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("v1");
        let mut predecessor =
            legacy_predecessor(&registry, &temporary.path().join("legacy"), true)?;
        let canonical = predecessor
            .legacy_artifacts_root
            .to_str()
            .ok_or("legacy root encoding")?
            .to_owned();
        let scope = WriterScope {
            exchange: predecessor.exchange.as_str().to_owned(),
            account: predecessor.legacy_product_account.clone(),
            symbol: predecessor.legacy_symbol.clone(),
            owner_scope: predecessor.legacy_owner_scope.clone(),
        };
        let scope_sha256 = digest_json(&scope)?;
        let entry_sha256 = digest_json(&LegacyV1EntryDigestV1 {
            schema_version: 1,
            scope_sha256: &scope_sha256,
            scope: &scope,
            canonical_artifacts_root: &canonical,
            canonical_root_sha256: &digest_bytes(canonical.as_bytes()),
        })?;
        let encoded = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "scope_sha256": scope_sha256,
            "scope": scope,
            "canonical_artifacts_root": canonical,
            "canonical_root_sha256": digest_bytes(predecessor.legacy_artifacts_root.to_string_lossy().as_bytes()),
            "entry_sha256": entry_sha256,
        }))?;
        predecessor.handoff_sha256 = digest_bytes(&encoded);
        fs::write(
            registry.join(format!("{}.json", predecessor.legacy_lock_sha256)),
            encoded,
        )?;
        assert!(acquire_legacy_predecessor(&predecessor, &registry).is_ok());
        Ok(())
    }

    #[test]
    fn legacy_predecessor_rejects_tampered_handoff_and_lock_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("v1");
        let predecessor = legacy_predecessor(&registry, &temporary.path().join("legacy"), false)?;
        assert!(matches!(
            acquire_legacy_predecessor(&predecessor, &registry),
            Err(Stage7WriterRegistryError::LegacyPredecessor)
        ));
        let valid = legacy_predecessor(&registry, &temporary.path().join("valid"), true)?;
        let mut wrong_path = valid.clone();
        wrong_path.legacy_lock_path = registry.join("b".repeat(64) + ".lock");
        assert!(matches!(
            acquire_legacy_predecessor(&wrong_path, &registry),
            Err(Stage7WriterRegistryError::LegacyPredecessor)
        ));
        Ok(())
    }

    #[test]
    fn schema_two_account_identity_and_registry_filename_remain_compatible()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let root = temporary.path().join("gate");
        let writer_scope = scope("gate")?;
        let account = account_key(&writer_scope)?;
        let account_sha256 = digest_json(&account)?;

        assert_eq!(
            serde_json::to_string(&account)?,
            "{\"exchange\":\"gate\",\"account\":\"00000000-0000-4000-8000-000000000001\"}"
        );
        assert_eq!(
            account_sha256,
            "6bc09225c216a36f4bb9a530ddaea12a02be4620bbe258c212fabfba51fd7b29"
        );

        let guard = acquire_in(&registry, &writer_scope, &root)?;
        assert!(registry.join(format!("{account_sha256}.json")).is_file());
        drop(guard);
        Ok(())
    }

    #[test]
    fn canonical_root_survives_guard_drop_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let root = temporary.path().join("gate");
        let writer_scope = scope("gate")?;

        let first = acquire_in(&registry, &writer_scope, &root)?;
        drop(first);
        let reopened = acquire_in(&registry, &writer_scope, &root)?;
        drop(reopened);

        let entries = fs::read_dir(&registry)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == "json")
            })
            .count();
        assert_eq!(entries, 1);
        Ok(())
    }

    #[test]
    fn different_root_is_rejected_after_the_first_writer_exits()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let first_root = temporary.path().join("first");
        let second_root = temporary.path().join("second");
        let writer_scope = scope("gate")?;

        let first = acquire_in(&registry, &writer_scope, &first_root)?;
        drop(first);
        let rejected = acquire_in(&registry, &writer_scope, &second_root);

        assert!(matches!(
            rejected,
            Err(Stage7WriterRegistryError::CanonicalRootConflict { .. })
        ));
        Ok(())
    }

    #[test]
    fn different_symbol_and_owner_still_share_one_account_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let first_root = temporary.path().join("first");
        let second_root = temporary.path().join("second");
        let first_scope = scope("gate")?;
        let mut second_scope = first_scope.clone();
        second_scope.symbol = "SOL/USDT".parse()?;
        second_scope.owner_scope = "scalping_sol_usdt_primary".to_owned();

        let first = acquire_in(&registry, &first_scope, &first_root)?;
        drop(first);
        let rejected = acquire_in(&registry, &second_scope, &second_root);

        assert!(matches!(
            rejected,
            Err(Stage7WriterRegistryError::CanonicalRootConflict { .. })
        ));
        Ok(())
    }

    #[test]
    fn different_accounts_hold_independent_machine_locks() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let first_scope = scope("gate")?;
        let mut second_scope = first_scope.clone();
        second_scope.account = "00000000-0000-4000-8000-000000000002".to_owned();
        let first = acquire_in(&registry, &first_scope, &temporary.path().join("first"))?;
        let second = acquire_in(&registry, &second_scope, &temporary.path().join("second"))?;
        drop(second);
        drop(first);
        Ok(())
    }

    #[test]
    fn concurrent_different_roots_cannot_both_register() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let roots = [
            temporary.path().join("left"),
            temporary.path().join("right"),
        ];
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for root in roots {
            let registry = registry.clone();
            let barrier = Arc::clone(&barrier);
            let writer_scope = scope("gate")?;
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                acquire_in(&registry, &writer_scope, &root).is_ok()
            }));
        }

        let mut successes = 0;
        for handle in handles {
            if handle
                .join()
                .map_err(|_| "canonical root registration thread panicked")?
            {
                successes += 1;
            }
        }
        assert_eq!(successes, 1);
        Ok(())
    }

    #[test]
    fn different_exchanges_hold_independent_machine_locks() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let gate = acquire_in(&registry, &scope("gate")?, &temporary.path().join("gate"))?;
        let bitget = acquire_in(
            &registry,
            &scope("bitget")?,
            &temporary.path().join("bitget"),
        )?;

        drop(bitget);
        drop(gate);
        Ok(())
    }

    #[test]
    fn test_and_live_named_roots_cannot_split_one_canonical_account()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let test_root = temporary.path().join("TEST");
        let live_root = temporary.path().join("LIVE");
        let writer_scope = scope("gate")?;

        drop(acquire_in(&registry, &writer_scope, &test_root)?);
        assert!(matches!(
            acquire_in(&registry, &writer_scope, &live_root),
            Err(Stage7WriterRegistryError::CanonicalRootConflict { .. })
        ));
        Ok(())
    }

    #[test]
    fn canonical_entry_hash_fork_is_never_repaired_or_rebound()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let registry = temporary.path().join("registry");
        let root = temporary.path().join("gate");
        let writer_scope = scope("gate")?;
        let account_sha256 = digest_json(&account_key(&writer_scope)?)?;
        drop(acquire_in(&registry, &writer_scope, &root)?);

        let entry_path = registry.join(format!("{account_sha256}.json"));
        let mut entry: DurableCanonicalRoot = serde_json::from_slice(&fs::read(&entry_path)?)?;
        entry.entry_sha256 = "0".repeat(64);
        fs::write(&entry_path, serde_json::to_vec(&entry)?)?;
        assert!(matches!(
            acquire_in(&registry, &writer_scope, &root),
            Err(Stage7WriterRegistryError::Corrupt)
        ));
        Ok(())
    }
}
