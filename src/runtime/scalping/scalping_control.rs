use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;

use crate::{
    config::{BinanceAccountBinding, Config},
    controller::{
        CONTROL_SCHEMA_VERSION, ControlTarget, InstanceControlRecord, InstanceControlStore,
    },
    strategy::scalping::StrategyBinding,
};

pub const MAX_STAGE6_ENTRY_AUTHORITY_TTL_MS: u64 = 15 * 60 * 1_000;
const CONTROL_LOCK_FILE: &str = "controller.json.lock";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingControlRequest {
    pub artifacts_root: PathBuf,
    pub binding_path: PathBuf,
    pub target: ControlTarget,
    pub command_id: String,
    pub idempotency_key: String,
    pub entry_expires_at_ms: Option<u64>,
    pub confirm_entry_authority: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingControlReport {
    pub target: ControlTarget,
    pub revision: u64,
    pub changed: bool,
    pub entry_expires_at_ms: Option<u64>,
}

/// The only Stage 6 controller writer. It records a bounded intent to run but never contacts an
/// exchange; the resident still requires fresh private authority and its separate Live confirmation.
pub fn apply_scalping_control(
    config: &Config,
    request: ScalpingControlRequest,
) -> Result<ScalpingControlReport, ScalpingControlError> {
    validate_request(&request)?;
    let binding = load_binding(&request.binding_path)?;
    validate_binding(config, &binding)?;
    let now_ms = wall_clock_ms()?;
    validate_authority_window(&request, now_ms)?;

    fs::create_dir_all(&request.artifacts_root).map_err(|source| ScalpingControlError::Io {
        path: request.artifacts_root.clone(),
        source,
    })?;
    let _lock = acquire_lock(&request.artifacts_root)?;
    let store = InstanceControlStore::new(request.artifacts_root.join("controller.json"));
    let existing = store.load()?;

    if let Some(existing) = &existing {
        if existing.binding != binding {
            return Err(ScalpingControlError::BindingConflict);
        }
        if existing.command_id == request.command_id
            || existing.idempotency_key == request.idempotency_key
        {
            if existing.target == request.target
                && existing.command_id == request.command_id
                && existing.idempotency_key == request.idempotency_key
                && existing.safety_deadline_ms == request.entry_expires_at_ms
            {
                return Ok(ScalpingControlReport {
                    target: existing.target,
                    revision: existing.revision,
                    changed: false,
                    entry_expires_at_ms: existing.safety_deadline_ms,
                });
            }
            return Err(ScalpingControlError::CommandIdentity);
        }
    }

    let expected_revision = existing.as_ref().map(|record| record.revision);
    let revision = expected_revision
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(ScalpingControlError::Revision)?;
    let record = InstanceControlRecord {
        schema_version: CONTROL_SCHEMA_VERSION,
        binding,
        target: request.target,
        command_id: request.command_id,
        idempotency_key: request.idempotency_key,
        safety_deadline_ms: request.entry_expires_at_ms,
        revision,
    };
    store.save(&record, expected_revision)?;
    Ok(ScalpingControlReport {
        target: record.target,
        revision,
        changed: true,
        entry_expires_at_ms: record.safety_deadline_ms,
    })
}

fn validate_request(request: &ScalpingControlRequest) -> Result<(), ScalpingControlError> {
    if !request.artifacts_root.is_absolute()
        || !request.binding_path.is_absolute()
        || request.command_id.trim().is_empty()
        || request.idempotency_key.trim().is_empty()
    {
        return Err(ScalpingControlError::Request);
    }
    Ok(())
}

fn validate_authority_window(
    request: &ScalpingControlRequest,
    now_ms: u64,
) -> Result<(), ScalpingControlError> {
    if request.target != ControlTarget::Running {
        return request
            .entry_expires_at_ms
            .is_none()
            .then_some(())
            .ok_or(ScalpingControlError::UnexpectedDeadline);
    }
    if !request.confirm_entry_authority {
        return Err(ScalpingControlError::Confirmation);
    }
    let deadline = request
        .entry_expires_at_ms
        .ok_or(ScalpingControlError::Deadline)?;
    let maximum = now_ms
        .checked_add(MAX_STAGE6_ENTRY_AUTHORITY_TTL_MS)
        .ok_or(ScalpingControlError::Clock)?;
    if deadline <= now_ms || deadline > maximum {
        return Err(ScalpingControlError::Deadline);
    }
    Ok(())
}

fn load_binding(path: &Path) -> Result<StrategyBinding, ScalpingControlError> {
    let bytes = fs::read(path).map_err(|source| ScalpingControlError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(ScalpingControlError::BindingDecode)
}

fn validate_binding(
    config: &Config,
    binding: &StrategyBinding,
) -> Result<(), ScalpingControlError> {
    if binding.validate().is_err()
        || binding.exchange != "binance"
        || binding.account != config.trading_account_id
        || binding.symbol != config.symbol
        || binding.risk_budget.asset.as_str() != "USDT"
        || config.binance.as_ref().is_none_or(|binding| {
            binding.account_binding != BinanceAccountBinding::PortfolioMarginUm
        })
    {
        return Err(ScalpingControlError::Binding);
    }
    Ok(())
}

fn acquire_lock(artifacts_root: &Path) -> Result<ControlLock, ScalpingControlError> {
    let path = artifacts_root.join(CONTROL_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|source| ScalpingControlError::Io {
            path: path.clone(),
            source,
        })?;
    file.try_lock_exclusive()
        .map_err(|_| ScalpingControlError::Busy)?;
    Ok(ControlLock { file })
}

fn wall_clock_ms() -> Result<u64, ScalpingControlError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ScalpingControlError::Clock)
        .and_then(|duration| {
            u64::try_from(duration.as_millis()).map_err(|_| ScalpingControlError::Clock)
        })
}

#[derive(Debug)]
struct ControlLock {
    file: File,
}

impl Drop for ControlLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingControlError {
    #[error(
        "controller requires absolute artifact and binding paths plus nonempty command identities"
    )]
    Request,
    #[error("controller binding is invalid or differs from the configured Binance deployment")]
    Binding,
    #[error("controller root is already bound to a different strategy instance")]
    BindingConflict,
    #[error("controller command ID or idempotency key was reused with different content")]
    CommandIdentity,
    #[error("running controller target requires --confirm-entry-authority")]
    Confirmation,
    #[error("running controller target requires an unexpired entry deadline within 15 minutes")]
    Deadline,
    #[error("a non-running controller target must not contain an entry deadline")]
    UnexpectedDeadline,
    #[error("controller clock or revision overflowed")]
    Clock,
    #[error("controller revision overflowed")]
    Revision,
    #[error("another controller writer currently owns this artifact root")]
    Busy,
    #[error("controller binding JSON is invalid: {0}")]
    BindingDecode(serde_json::Error),
    #[error("controller filesystem failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("controller persistence failed: {0}")]
    Store(#[from] crate::controller::ControlError),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        config::Config,
        controller::{ControlTarget, InstanceControlStore},
    };

    use super::*;

    fn config(directory: &Path) -> Result<Config, Box<dyn std::error::Error>> {
        let path = directory.join("venue.toml");
        fs::write(
            &path,
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[binance]\naccount_binding = 'portfolio_margin_um'",
        )?;
        Ok(Config::load(path)?)
    }

    fn binding() -> StrategyBinding {
        serde_json::from_str(
            r#"{"strategy_kind":"scalping","strategy_instance_id":"stage6","run_id":"run-1","exchange":"binance","account":"00000000-0000-4000-8000-000000000001","symbol":"SOL/USDT","parameter_release_id":"stage6-v1","owner_scope":"stage6:run-1","risk_budget":{"asset":"USDT","value":"5"}}"#,
        )
        .unwrap_or_else(|error| panic!("test binding must decode: {error}"))
    }

    fn request(root: PathBuf, binding_path: PathBuf, deadline: u64) -> ScalpingControlRequest {
        ScalpingControlRequest {
            artifacts_root: root,
            binding_path,
            target: ControlTarget::Running,
            command_id: "run-1".to_owned(),
            idempotency_key: "run-1-key".to_owned(),
            entry_expires_at_ms: Some(deadline),
            confirm_entry_authority: true,
        }
    }

    #[test]
    fn running_is_durable_bounded_and_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let config = config(directory.path())?;
        let root = directory.path().join("artifacts");
        let binding_path = directory.path().join("binding.json");
        fs::write(&binding_path, serde_json::to_vec(&binding())?)?;
        let deadline = wall_clock_ms()? + 1_000;
        let first = apply_scalping_control(
            &config,
            request(root.clone(), binding_path.clone(), deadline),
        )?;
        let second =
            apply_scalping_control(&config, request(root.clone(), binding_path, deadline))?;
        assert_eq!(first.revision, 1);
        assert!(first.changed);
        assert_eq!(second.revision, 1);
        assert!(!second.changed);
        assert_eq!(
            InstanceControlStore::new(root.join("controller.json"))
                .load()?
                .map(|record| record.target),
            Some(ControlTarget::Running)
        );
        Ok(())
    }

    #[test]
    fn running_refuses_missing_confirmation_or_unbounded_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let config = config(directory.path())?;
        let root = directory.path().join("artifacts");
        let binding_path = directory.path().join("binding.json");
        fs::write(&binding_path, serde_json::to_vec(&binding())?)?;
        let mut missing_confirmation =
            request(root.clone(), binding_path.clone(), wall_clock_ms()? + 1_000);
        missing_confirmation.confirm_entry_authority = false;
        assert!(matches!(
            apply_scalping_control(&config, missing_confirmation),
            Err(ScalpingControlError::Confirmation)
        ));
        let expired = request(root, binding_path, wall_clock_ms()?);
        assert!(matches!(
            apply_scalping_control(&config, expired),
            Err(ScalpingControlError::Deadline)
        ));
        Ok(())
    }
}
