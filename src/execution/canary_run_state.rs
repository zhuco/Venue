use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::domain::{OrderCommand, OrderPurpose, PositionSide, Symbol};

use super::WriterSession;

pub const MAX_UNPROTECTED_MS: u64 = 1_500;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryRunBinding {
    pub canary_id: String,
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub owner_scope: String,
    pub release_id: String,
    pub position_side: PositionSide,
    pub writer_generation: u64,
    pub readback_generation: u64,
    pub valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "snake_case",
    tag = "state",
    content = "detail"
)]
pub enum CanaryRunPhase {
    Prepared,
    EntrySubmitted {
        command_sha256: String,
    },
    FilledUnprotected {
        fill_sha256: String,
        deadline_ms: u64,
    },
    ProtectionSubmitted {
        command_sha256: String,
        installed_at_ms: u64,
    },
    Protected {
        custody_sha256: String,
    },
    EmergencyFlattening {
        control_sha256: String,
    },
    Flat {
        readback_sha256: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableRun {
    schema_version: u16,
    revision: u64,
    binding: CanaryRunBinding,
    phase: CanaryRunPhase,
    observed_at_ms: u64,
    frozen: bool,
    #[serde(default)]
    recovery_receipt_sha256: Option<String>,
}

#[derive(Debug)]
pub struct CanaryRunState {
    path: PathBuf,
    durable: DurableRun,
}

impl CanaryRunState {
    pub fn create_new(
        path: impl Into<PathBuf>,
        binding: CanaryRunBinding,
        now_ms: u64,
    ) -> Result<Self, CanaryRunStateError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(CanaryRunStateError::Path);
        }
        validate_binding(&binding, now_ms)?;
        let durable = DurableRun {
            schema_version: 1,
            revision: 1,
            binding,
            phase: CanaryRunPhase::Prepared,
            observed_at_ms: now_ms,
            frozen: false,
            recovery_receipt_sha256: None,
        };
        persist_new(&path, &durable)?;
        Ok(Self { path, durable })
    }

    pub fn recover(
        path: impl Into<PathBuf>,
        expected: &CanaryRunBinding,
        now_ms: u64,
    ) -> Result<Self, CanaryRunStateError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(CanaryRunStateError::Path);
        }
        let durable = recover_highest(&path)?;
        if durable.binding != *expected || durable.schema_version != 1 {
            return Err(CanaryRunStateError::Binding);
        }
        let mut state = Self { path, durable };
        if now_ms < state.durable.observed_at_ms {
            state.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        if state.expired(now_ms) {
            state.freeze()?;
        }
        Ok(state)
    }

    /// Opens an existing run from its own durable binding. This is recovery-only discovery: it
    /// grants no writer authority and never permits a new entry.
    pub fn recover_existing(
        path: impl Into<PathBuf>,
        now_ms: u64,
    ) -> Result<Self, CanaryRunStateError> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(CanaryRunStateError::Path);
        }
        let durable = recover_highest(&path)?;
        validate_binding_identity(&durable.binding)?;
        let mut state = Self { path, durable };
        if now_ms < state.durable.observed_at_ms {
            state.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        if state.expired(now_ms) {
            state.freeze()?;
        }
        Ok(state)
    }

    pub fn phase(&self) -> &CanaryRunPhase {
        &self.durable.phase
    }
    pub fn is_frozen(&self) -> bool {
        self.durable.frozen
    }
    pub fn binding(&self) -> &CanaryRunBinding {
        &self.durable.binding
    }
    pub const fn revision(&self) -> u64 {
        self.durable.revision
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self.durable.phase, CanaryRunPhase::Flat { .. })
    }
    pub fn recovery_receipt_sha256(&self) -> Option<&str> {
        self.durable.recovery_receipt_sha256.as_deref()
    }

    /// Rechecks the exact writer and hedge entry scope before the WAL is allowed to advance.
    pub fn validate_entry_context(
        &mut self,
        writer: &WriterSession,
        command: &OrderCommand,
        now_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        if self.durable.frozen || !matches!(self.durable.phase, CanaryRunPhase::Prepared) {
            return Err(CanaryRunStateError::Frozen);
        }
        if now_ms < self.durable.observed_at_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        let binding = &self.durable.binding;
        if writer.scope.exchange != binding.exchange
            || writer.scope.account != binding.account
            || writer.scope.symbol != binding.symbol
            || writer.scope.owner_scope != binding.owner_scope
            || writer.generation != binding.writer_generation
            || writer.readback_generation != binding.readback_generation
            || writer.valid_until_ms <= now_ms
            || command.owner.exchange != binding.exchange
            || command.owner.account != binding.account
            || command.owner.symbol != binding.symbol
            || command.owner.purpose != OrderPurpose::Entry
            || command.position_side != binding.position_side
        {
            return Err(CanaryRunStateError::Binding);
        }
        if now_ms >= binding.valid_until_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Expired);
        }
        Ok(())
    }

    pub fn entry_submitted(
        &mut self,
        command_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        self.advance(
            CanaryRunPhase::EntrySubmitted { command_sha256 },
            observed_at_ms,
        )
    }
    pub fn filled_unprotected(
        &mut self,
        fill_sha256: String,
        observed_at_ms: u64,
    ) -> Result<u64, CanaryRunStateError> {
        let deadline_ms = self
            .durable
            .observed_at_ms
            .checked_add(MAX_UNPROTECTED_MS)
            .ok_or(CanaryRunStateError::Clock)?;
        self.advance(
            CanaryRunPhase::FilledUnprotected {
                fill_sha256,
                deadline_ms,
            },
            observed_at_ms,
        )?;
        Ok(deadline_ms)
    }
    pub fn protected(
        &mut self,
        custody_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        self.advance(CanaryRunPhase::Protected { custody_sha256 }, observed_at_ms)
    }
    pub fn protection_submitted(
        &mut self,
        command_sha256: String,
        installed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        self.advance(
            CanaryRunPhase::ProtectionSubmitted {
                command_sha256,
                installed_at_ms,
            },
            installed_at_ms,
        )
    }
    pub fn emergency_flattening(
        &mut self,
        control_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        self.safety_advance(
            CanaryRunPhase::EmergencyFlattening { control_sha256 },
            observed_at_ms,
        )
    }
    pub fn flat(
        &mut self,
        readback_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        self.safety_advance(CanaryRunPhase::Flat { readback_sha256 }, observed_at_ms)
    }

    /// Recovery may seal any unfinished phase only after a separate, durable recovery receipt has
    /// proven two exact clean account snapshots. This path never creates or renews writer power.
    pub fn seal_recovered_flat(
        &mut self,
        recovery_receipt_sha256: String,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        if self.is_terminal()
            || observed_at_ms < self.durable.observed_at_ms
            || !valid_summary(&recovery_receipt_sha256)
        {
            return Err(CanaryRunStateError::Transition);
        }
        self.durable.revision = self
            .durable
            .revision
            .checked_add(1)
            .ok_or(CanaryRunStateError::Revision)?;
        self.durable.phase = CanaryRunPhase::Flat {
            readback_sha256: recovery_receipt_sha256.clone(),
        };
        self.durable.observed_at_ms = observed_at_ms;
        self.durable.frozen = false;
        self.durable.recovery_receipt_sha256 = Some(recovery_receipt_sha256);
        persist_replace(&self.path, &self.durable)
    }

    /// Upgrades a receipt-backed Flat state written by the first recovery schema. The exact Flat
    /// summary must already equal the validated receipt hash, so this cannot relabel normal Flat.
    pub fn bind_existing_recovery_receipt(
        &mut self,
        recovery_receipt_sha256: String,
    ) -> Result<(), CanaryRunStateError> {
        if self.durable.recovery_receipt_sha256.is_some()
            || !matches!(
                &self.durable.phase,
                CanaryRunPhase::Flat { readback_sha256 }
                    if readback_sha256 == &recovery_receipt_sha256
            )
            || !valid_summary(&recovery_receipt_sha256)
        {
            return Err(CanaryRunStateError::Transition);
        }
        self.durable.revision = self
            .durable
            .revision
            .checked_add(1)
            .ok_or(CanaryRunStateError::Revision)?;
        self.durable.recovery_receipt_sha256 = Some(recovery_receipt_sha256);
        persist_replace(&self.path, &self.durable)
    }

    pub fn require_unprotected_before(&mut self, now_ms: u64) -> Result<(), CanaryRunStateError> {
        if now_ms < self.durable.observed_at_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        if self.expired(now_ms) {
            self.freeze()?;
            return Err(CanaryRunStateError::Expired);
        }
        Ok(())
    }

    /// UNKNOWN is never translated into a lifecycle transition; it permanently fences this run
    /// until an external reconciliation creates a new, separately bound authority.
    pub fn freeze_unknown(&mut self) -> Result<(), CanaryRunStateError> {
        self.freeze()
    }

    fn advance(
        &mut self,
        next: CanaryRunPhase,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        if self.durable.frozen {
            return Err(CanaryRunStateError::Frozen);
        }
        if observed_at_ms < self.durable.observed_at_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        if observed_at_ms >= self.durable.binding.valid_until_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Expired);
        }
        if self.expired(observed_at_ms) {
            self.freeze()?;
            return Err(CanaryRunStateError::Expired);
        }
        if !allowed(&self.durable.phase, &next) || !summary_present(&next) {
            return Err(CanaryRunStateError::Transition);
        }
        self.durable.revision = self
            .durable
            .revision
            .checked_add(1)
            .ok_or(CanaryRunStateError::Revision)?;
        self.durable.phase = next;
        self.durable.observed_at_ms = observed_at_ms;
        persist_replace(&self.path, &self.durable)
    }

    fn expired(&self, now_ms: u64) -> bool {
        match self.durable.phase {
            CanaryRunPhase::FilledUnprotected { deadline_ms, .. } => now_ms >= deadline_ms,
            _ => false,
        }
    }
    /// A frozen or expired run remains authorized to reduce exposure, but never to re-enter.
    fn safety_advance(
        &mut self,
        next: CanaryRunPhase,
        observed_at_ms: u64,
    ) -> Result<(), CanaryRunStateError> {
        if observed_at_ms < self.durable.observed_at_ms {
            self.freeze()?;
            return Err(CanaryRunStateError::Clock);
        }
        if !allowed(&self.durable.phase, &next) || !summary_present(&next) {
            return Err(CanaryRunStateError::Transition);
        }
        self.durable.revision = self
            .durable
            .revision
            .checked_add(1)
            .ok_or(CanaryRunStateError::Revision)?;
        self.durable.phase = next;
        self.durable.observed_at_ms = observed_at_ms;
        persist_replace(&self.path, &self.durable)
    }
    fn freeze(&mut self) -> Result<(), CanaryRunStateError> {
        if !self.durable.frozen {
            self.durable.revision = self
                .durable
                .revision
                .checked_add(1)
                .ok_or(CanaryRunStateError::Revision)?;
            self.durable.frozen = true;
            persist_replace(&self.path, &self.durable)?;
        }
        Ok(())
    }
}

fn validate_binding(binding: &CanaryRunBinding, now_ms: u64) -> Result<(), CanaryRunStateError> {
    validate_binding_identity(binding)?;
    if binding.valid_until_ms <= now_ms {
        return Err(CanaryRunStateError::Binding);
    }
    Ok(())
}
fn validate_binding_identity(binding: &CanaryRunBinding) -> Result<(), CanaryRunStateError> {
    if [
        binding.canary_id.as_str(),
        binding.exchange.as_str(),
        binding.account.as_str(),
        binding.owner_scope.as_str(),
        binding.release_id.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
        || binding.position_side == PositionSide::Net
        || binding.writer_generation == 0
        || binding.readback_generation == 0
        || binding.valid_until_ms == 0
    {
        return Err(CanaryRunStateError::Binding);
    }
    Ok(())
}
fn allowed(current: &CanaryRunPhase, next: &CanaryRunPhase) -> bool {
    matches!(
        (current, next),
        (
            CanaryRunPhase::Prepared,
            CanaryRunPhase::EntrySubmitted { .. }
        ) | (
            CanaryRunPhase::EntrySubmitted { .. },
            CanaryRunPhase::FilledUnprotected { .. } | CanaryRunPhase::EmergencyFlattening { .. }
        ) | (
            CanaryRunPhase::FilledUnprotected { .. },
            CanaryRunPhase::ProtectionSubmitted { .. }
                | CanaryRunPhase::Protected { .. }
                | CanaryRunPhase::EmergencyFlattening { .. }
        ) | (
            CanaryRunPhase::ProtectionSubmitted { .. },
            CanaryRunPhase::Protected { .. } | CanaryRunPhase::EmergencyFlattening { .. }
        ) | (
            CanaryRunPhase::EmergencyFlattening { .. },
            CanaryRunPhase::Flat { .. }
        ) | (
            CanaryRunPhase::EntrySubmitted { .. },
            CanaryRunPhase::Flat { .. }
        ) | (
            CanaryRunPhase::Protected { .. },
            CanaryRunPhase::EmergencyFlattening { .. }
        )
    )
}
fn summary_present(phase: &CanaryRunPhase) -> bool {
    match phase {
        CanaryRunPhase::Protected { custody_sha256 } => valid_summary(custody_sha256),
        CanaryRunPhase::Flat { readback_sha256 } => valid_summary(readback_sha256),
        CanaryRunPhase::EntrySubmitted { command_sha256 } => valid_summary(command_sha256),
        CanaryRunPhase::FilledUnprotected { fill_sha256, .. } => valid_summary(fill_sha256),
        CanaryRunPhase::ProtectionSubmitted { command_sha256, .. } => valid_summary(command_sha256),
        CanaryRunPhase::EmergencyFlattening { control_sha256 } => valid_summary(control_sha256),
        CanaryRunPhase::Prepared => true,
    }
}
fn valid_summary(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn recover_highest(path: &Path) -> Result<DurableRun, CanaryRunStateError> {
    let mut values = Vec::new();
    for candidate in [
        path.to_path_buf(),
        sibling(path, ".backup"),
        sibling(path, ".next"),
    ] {
        match fs::read(candidate) {
            Ok(bytes) => {
                if let Ok(value) = serde_json::from_slice::<DurableRun>(&bytes) {
                    values.push(value)
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
    values
        .into_iter()
        .max_by_key(|value| value.revision)
        .ok_or(CanaryRunStateError::Recovery)
}
fn persist_new(path: &Path, durable: &DurableRun) -> Result<(), CanaryRunStateError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(CanaryRunStateError::Io)?;
    let bytes = serde_json::to_vec(durable).map_err(CanaryRunStateError::Encode)?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(CanaryRunStateError::Io)
}
fn persist_replace(path: &Path, durable: &DurableRun) -> Result<(), CanaryRunStateError> {
    let bytes = serde_json::to_vec(durable).map_err(CanaryRunStateError::Encode)?;
    let next = sibling(path, ".next");
    {
        let mut file = fs::File::create(&next).map_err(CanaryRunStateError::Io)?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(CanaryRunStateError::Io)?;
    }
    let backup = sibling(path, ".backup");
    if path.exists() {
        fs::copy(path, &backup).map_err(CanaryRunStateError::Io)?;
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&backup)
            .and_then(|file| file.sync_all())
            .map_err(CanaryRunStateError::Io)?;
    }
    let mut primary = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(CanaryRunStateError::Io)?;
    primary
        .write_all(&bytes)
        .and_then(|_| primary.sync_all())
        .map_err(CanaryRunStateError::Io)?;
    fs::remove_file(next).map_err(CanaryRunStateError::Io)
}
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name: std::ffi::OsString = path.file_name().map_or_else(|| "canary".into(), Into::into);
    name.push(suffix);
    path.with_file_name(name)
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryRunStateError {
    #[error("canary run path must be absolute")]
    Path,
    #[error("canary run binding is invalid")]
    Binding,
    #[error("canary run clock regressed")]
    Clock,
    #[error("canary run evidence or unprotected deadline expired")]
    Expired,
    #[error("canary run is frozen")]
    Frozen,
    #[error("canary run transition or evidence summary is invalid")]
    Transition,
    #[error("canary run revision exhausted")]
    Revision,
    #[error("canary run recovery failed closed")]
    Recovery,
    #[error("canary run I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("canary run encoding failed: {0}")]
    Encode(#[source] serde_json::Error),
}
