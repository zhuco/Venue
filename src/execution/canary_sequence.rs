use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::is_canonical_trading_account_id,
    execution::{CanaryEvidenceRecovery, CanaryEvidenceStage, CanaryTerminalState},
};

const SCHEMA_VERSION: u16 = 1;
const SOL_SYMBOL: &str = "SOL/USDT";
const BNB_SYMBOL: &str = "BNB/USDT";
const REQUIRED_EXCHANGE: &str = "binance";
const RECEIPT_FILE: &str = "sol_to_bnb_canary_receipt.json";
const LOCK_FILE: &str = "sol_to_bnb_canary_receipt.lock";

/// Immutable local identity for the one SOL-to-BNB promotion campaign. It deliberately does not
/// duplicate `CanaryEvidenceBinding`: the latter remains the authoritative evidence scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryCampaignBinding {
    pub exchange: String,
    pub account: String,
    pub release_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SolCompletionReceipt {
    pub schema_version: u16,
    pub binding: CanaryCampaignBinding,
    pub header_binding_sha256: String,
    pub terminal_record_sha256: String,
    pub protection_custody_sha256: String,
    pub receipt_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BnbMutationRequest {
    pub binding: CanaryCampaignBinding,
    pub symbol: String,
    pub mutation_id: String,
    pub mutation_sha256: String,
}

/// Pure local authorization material. It intentionally cannot submit an order or call a venue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BnbMutationPermit {
    pub mutation_id: String,
    pub mutation_sha256: String,
    pub header_binding_sha256: String,
    pub receipt_sha256: String,
}

#[derive(Clone, Debug)]
pub struct CanarySequenceGate {
    root: PathBuf,
    binding: CanaryCampaignBinding,
}

impl CanarySequenceGate {
    /// Opens an absolute, campaign-local artifact root. The root is canonicalized before any
    /// receipt path is constructed, so caller-controlled traversal never reaches another scope.
    pub fn open(
        artifacts_root: impl Into<PathBuf>,
        binding: CanaryCampaignBinding,
    ) -> Result<Self, CanarySequenceError> {
        validate_campaign_binding(&binding)?;
        let requested = artifacts_root.into();
        if !requested.is_absolute() {
            return Err(CanarySequenceError::Path);
        }
        fs::create_dir_all(&requested).map_err(CanarySequenceError::Io)?;
        let root = fs::canonicalize(&requested).map_err(CanarySequenceError::Io)?;
        if !root.is_dir() {
            return Err(CanarySequenceError::Path);
        }
        Ok(Self { root, binding })
    }

    pub fn receipt_path(&self) -> PathBuf {
        self.root.join(RECEIPT_FILE)
    }

    /// Fences a second SOL protection attempt once this campaign already has a durable completion
    /// receipt. The same symbol writer lease closes the check-to-dispatch race between SOL runs.
    pub fn ensure_sol_pending(&self) -> Result<(), CanarySequenceError> {
        let _lock = self.lock()?;
        if self.load_primary()?.is_some() {
            return Err(CanarySequenceError::ReceiptConflict);
        }
        Ok(())
    }

    /// Creates the sole durable SOL completion receipt from the already-recovered authoritative
    /// journal. The caller supplies no synthetic chain: header, stages, and terminal are all
    /// validated by `CanaryEvidenceJournal` recovery before reaching this boundary.
    pub fn complete_sol_protection(
        &self,
        recovery: &CanaryEvidenceRecovery,
    ) -> Result<SolCompletionReceipt, CanarySequenceError> {
        let receipt = receipt_for(&self.binding, recovery)?;
        let _lock = self.lock()?;
        self.persist_create_once(&receipt)
    }

    /// Revalidates the exact same recovered SOL journal before yielding BNB-only permit data.
    /// Receipt contents, campaign binding, and evidence summaries must all agree.
    pub fn permit_bnb_mutation(
        &self,
        request: &BnbMutationRequest,
        recovery: &CanaryEvidenceRecovery,
    ) -> Result<BnbMutationPermit, CanarySequenceError> {
        validate_bnb_request(&self.binding, request)?;
        let expected = receipt_for(&self.binding, recovery)?;
        let _lock = self.lock()?;
        let receipt = self
            .load_primary()?
            .ok_or(CanarySequenceError::ReceiptMissing)?;
        validate_receipt(&receipt, &self.binding)?;
        if receipt != expected {
            return Err(CanarySequenceError::ReceiptMismatch);
        }
        Ok(BnbMutationPermit {
            mutation_id: request.mutation_id.clone(),
            mutation_sha256: request.mutation_sha256.clone(),
            header_binding_sha256: receipt.header_binding_sha256,
            receipt_sha256: receipt.receipt_sha256,
        })
    }

    fn lock(&self) -> Result<OsLock, CanarySequenceError> {
        let path = self.safe_child(LOCK_FILE)?;
        reject_link(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(CanarySequenceError::Io)?;
        let mut last_error = None;
        for _ in 0..100 {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(OsLock { file }),
                Err(error) => {
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        Err(CanarySequenceError::Lock(last_error.unwrap_or_else(|| {
            std::io::Error::other("canary sequence lock unavailable")
        })))
    }

    fn persist_create_once(
        &self,
        receipt: &SolCompletionReceipt,
    ) -> Result<SolCompletionReceipt, CanarySequenceError> {
        if let Some(existing) = self.load_primary()? {
            return same_or_conflict(existing, receipt);
        }
        let pending = self.safe_child(&format!("{RECEIPT_FILE}.next"))?;
        if let Some(existing) = self.load(&pending)? {
            let recovered = same_or_conflict(existing, receipt)?;
            self.promote_pending(&pending, &recovered)?;
            return Ok(recovered);
        }
        let encoded = serde_json::to_vec(receipt).map_err(CanarySequenceError::Encode)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)
            .map_err(CanarySequenceError::Io)?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(CanarySequenceError::Io)?;
        self.promote_pending(&pending, receipt)?;
        Ok(receipt.clone())
    }

    fn promote_pending(
        &self,
        pending: &Path,
        expected: &SolCompletionReceipt,
    ) -> Result<(), CanarySequenceError> {
        let primary = self.safe_child(RECEIPT_FILE)?;
        match fs::hard_link(pending, &primary) {
            Ok(()) => {
                let _ = fs::remove_file(pending);
                Ok(())
            }
            Err(_) => match self.load_primary()? {
                Some(existing) if existing == *expected => Ok(()),
                Some(_) => Err(CanarySequenceError::ReceiptConflict),
                None => Err(CanarySequenceError::Recovery),
            },
        }
    }

    fn load_primary(&self) -> Result<Option<SolCompletionReceipt>, CanarySequenceError> {
        self.load(&self.safe_child(RECEIPT_FILE)?)
    }

    fn load(&self, path: &Path) -> Result<Option<SolCompletionReceipt>, CanarySequenceError> {
        reject_link(path)?;
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(CanarySequenceError::Io(error)),
        };
        let receipt = serde_json::from_slice(&bytes).map_err(CanarySequenceError::Decode)?;
        validate_receipt(&receipt, &self.binding)?;
        Ok(Some(receipt))
    }

    fn safe_child(&self, name: &str) -> Result<PathBuf, CanarySequenceError> {
        if name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err(CanarySequenceError::Path);
        }
        let path = self.root.join(name);
        if path.parent() != Some(self.root.as_path()) || !path.starts_with(&self.root) {
            return Err(CanarySequenceError::Path);
        }
        Ok(path)
    }
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

fn receipt_for(
    binding: &CanaryCampaignBinding,
    recovery: &CanaryEvidenceRecovery,
) -> Result<SolCompletionReceipt, CanarySequenceError> {
    let custody = validate_sol_recovery(binding, recovery)?;
    let terminal = recovery
        .terminal
        .as_ref()
        .ok_or(CanarySequenceError::Evidence)?;
    let mut receipt = SolCompletionReceipt {
        schema_version: SCHEMA_VERSION,
        binding: binding.clone(),
        header_binding_sha256: recovery.header.binding_sha256.clone(),
        terminal_record_sha256: terminal.record_sha256.clone(),
        protection_custody_sha256: custody,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt_digest(&receipt)?;
    Ok(receipt)
}

fn validate_sol_recovery(
    expected: &CanaryCampaignBinding,
    recovery: &CanaryEvidenceRecovery,
) -> Result<String, CanarySequenceError> {
    validate_campaign_binding(expected)?;
    let header = &recovery.header;
    if header.binding.exchange != expected.exchange
        || header.binding.account != expected.account
        || header.binding.release_id != expected.release_id
        || header.binding.symbol.to_string() != SOL_SYMBOL
        || !valid_sha256(&header.binding_sha256)
    {
        return Err(CanarySequenceError::Binding);
    }
    let terminal = recovery
        .terminal
        .as_ref()
        .ok_or(CanarySequenceError::Evidence)?;
    if !matches!(&terminal.terminal, CanaryTerminalState::Flat { .. })
        || terminal.binding_sha256 != header.binding_sha256
        || !valid_sha256(&terminal.record_sha256)
    {
        return Err(CanarySequenceError::Evidence);
    }
    let custody = recovery
        .stages
        .iter()
        .filter(|stage| stage.name == "protection_custody")
        .map(stage_custody_digest)
        .collect::<Result<Vec<_>, _>>()?;
    if custody.len() != 1 {
        return Err(CanarySequenceError::Evidence);
    }
    Ok(custody[0].clone())
}

fn stage_custody_digest(stage: &CanaryEvidenceStage) -> Result<String, CanarySequenceError> {
    if stage.evidence.len() != 1 || !stage.evidence.contains_key("custody") {
        return Err(CanarySequenceError::Evidence);
    }
    let custody = stage
        .evidence
        .get("custody")
        .ok_or(CanarySequenceError::Evidence)?;
    if !valid_sha256(custody) || !valid_sha256(&stage.record_sha256) {
        return Err(CanarySequenceError::Evidence);
    }
    Ok(custody.clone())
}

fn validate_receipt(
    receipt: &SolCompletionReceipt,
    expected: &CanaryCampaignBinding,
) -> Result<(), CanarySequenceError> {
    if receipt.schema_version != SCHEMA_VERSION
        || receipt.binding != *expected
        || !valid_sha256(&receipt.header_binding_sha256)
        || !valid_sha256(&receipt.terminal_record_sha256)
        || !valid_sha256(&receipt.protection_custody_sha256)
        || !valid_sha256(&receipt.receipt_sha256)
        || receipt.receipt_sha256 != receipt_digest(receipt)?
    {
        return Err(CanarySequenceError::ReceiptCorrupt);
    }
    Ok(())
}

fn validate_bnb_request(
    expected: &CanaryCampaignBinding,
    request: &BnbMutationRequest,
) -> Result<(), CanarySequenceError> {
    if request.binding != *expected
        || request.symbol != BNB_SYMBOL
        || request.mutation_id.trim().is_empty()
        || !valid_sha256(&request.mutation_sha256)
    {
        return Err(CanarySequenceError::Binding);
    }
    Ok(())
}

fn validate_campaign_binding(binding: &CanaryCampaignBinding) -> Result<(), CanarySequenceError> {
    if binding.exchange != REQUIRED_EXCHANGE
        || !is_canonical_trading_account_id(&binding.account)
        || binding.release_id.trim().is_empty()
    {
        return Err(CanarySequenceError::Binding);
    }
    Ok(())
}

fn same_or_conflict(
    existing: SolCompletionReceipt,
    expected: &SolCompletionReceipt,
) -> Result<SolCompletionReceipt, CanarySequenceError> {
    if existing == *expected {
        Ok(existing)
    } else {
        Err(CanarySequenceError::ReceiptConflict)
    }
}

fn reject_link(path: &Path) -> Result<(), CanarySequenceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(CanarySequenceError::Path)
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CanarySequenceError::Io(error)),
    }
}

fn receipt_digest(receipt: &SolCompletionReceipt) -> Result<String, CanarySequenceError> {
    digest_json(&(
        "venue.canary.sequence.receipt.v2",
        receipt.schema_version,
        &receipt.binding,
        &receipt.header_binding_sha256,
        &receipt.terminal_record_sha256,
        &receipt.protection_custody_sha256,
    ))
}

fn digest_json(value: &impl Serialize) -> Result<String, CanarySequenceError> {
    let encoded = serde_json::to_vec(value).map_err(CanarySequenceError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum CanarySequenceError {
    #[error("canary sequence binding is invalid or does not match the SOL campaign")]
    Binding,
    #[error("SOL evidence is missing, non-terminal, corrupt, or lacks exact protection custody")]
    Evidence,
    #[error("canary sequence artifact path is invalid or escapes its root")]
    Path,
    #[error("SOL completion receipt is absent")]
    ReceiptMissing,
    #[error("SOL completion receipt is corrupt")]
    ReceiptCorrupt,
    #[error("SOL completion receipt differs from the create-once campaign receipt")]
    ReceiptConflict,
    #[error("SOL completion receipt does not match the supplied evidence")]
    ReceiptMismatch,
    #[error("canary sequence recovery found no promotable receipt")]
    Recovery,
    #[error("canary sequence OS lock failed: {0}")]
    Lock(#[source] std::io::Error),
    #[error("canary sequence I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("canary sequence encoding failed: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("canary sequence decoding failed: {0}")]
    Decode(#[source] serde_json::Error),
}
