use std::path::Path;

use crate::{storage::ProjectionStore, strategy::scalping::StrategyBinding};
use serde::{Deserialize, Serialize};

pub use venue_strategies::scalping::ControlTarget;

pub const CONTROL_SCHEMA_VERSION: u16 = 1;

/// The controller-owned durable lifecycle record. It contains no exchange client and only emits
/// a read-only authorization consumed by strategy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InstanceControlRecord {
    pub schema_version: u16,
    pub binding: StrategyBinding,
    pub target: ControlTarget,
    pub command_id: String,
    pub idempotency_key: String,
    pub safety_deadline_ms: Option<u64>,
    pub revision: u64,
}

impl InstanceControlRecord {
    pub fn validate(&self) -> Result<(), ControlError> {
        self.binding.validate().map_err(|_| ControlError::Record)?;
        if self.schema_version != CONTROL_SCHEMA_VERSION
            || self.command_id.trim().is_empty()
            || self.idempotency_key.trim().is_empty()
            || self.revision == 0
        {
            return Err(ControlError::Record);
        }
        Ok(())
    }

    pub fn binding_digest(&self) -> String {
        digest_binding(&self.binding)
    }

    pub fn authorize(&self, authority: &ControlAuthority, now_ms: u64) -> EntryAuthorization {
        let blocked = if self.target != ControlTarget::Running {
            Some(ControlBlock::Target)
        } else if authority.parameter_release_id != self.binding.parameter_release_id {
            Some(ControlBlock::Release)
        } else if !authority.private_snapshot_ready {
            Some(ControlBlock::PrivateSnapshot)
        } else if authority.execution_unknown {
            Some(ControlBlock::ExecutionUnknown)
        } else if !authority.protection_complete {
            Some(ControlBlock::Protection)
        } else if authority.owner_conflict {
            Some(ControlBlock::OwnerConflict)
        } else if authority.generation == 0 {
            Some(ControlBlock::AuthorityGeneration)
        } else if self
            .safety_deadline_ms
            .is_some_and(|deadline| deadline <= now_ms)
        {
            Some(ControlBlock::Deadline)
        } else {
            None
        };
        EntryAuthorization {
            binding_digest: self.binding_digest(),
            parameter_release_id: self.binding.parameter_release_id.clone(),
            revision: self.revision,
            authority_generation: authority.generation,
            issued_at_ms: now_ms,
            expires_at_ms: self.safety_deadline_ms.unwrap_or(u64::MAX),
            allowed: blocked.is_none(),
            block: blocked,
        }
    }
}

/// Anonymous authoritative facts required before controller can let strategy evaluate entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlAuthority {
    pub generation: u64,
    pub parameter_release_id: String,
    pub private_snapshot_ready: bool,
    pub execution_unknown: bool,
    pub protection_complete: bool,
    pub owner_conflict: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlBlock {
    Target,
    Release,
    PrivateSnapshot,
    ExecutionUnknown,
    Protection,
    OwnerConflict,
    Deadline,
    AuthorityGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryAuthorization {
    pub(crate) binding_digest: String,
    pub(crate) parameter_release_id: String,
    pub(crate) revision: u64,
    pub(crate) authority_generation: u64,
    pub(crate) issued_at_ms: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) allowed: bool,
    pub(crate) block: Option<ControlBlock>,
}

impl EntryAuthorization {
    pub fn is_allowed(&self) -> bool {
        self.allowed
    }
    pub const fn authority_generation(&self) -> u64 {
        self.authority_generation
    }
    pub fn block(&self) -> Option<ControlBlock> {
        self.block
    }
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    pub const fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
    pub fn is_valid_at(&self, now_ms: u64) -> bool {
        self.allowed && now_ms >= self.issued_at_ms && now_ms < self.expires_at_ms
    }
    pub(crate) fn matches(&self, binding: &StrategyBinding) -> bool {
        self.allowed
            && self.parameter_release_id == binding.parameter_release_id
            && self.binding_digest == digest_binding(binding)
            && self.revision > 0
            && self.authority_generation > 0
    }

    pub(crate) fn matches_at(&self, binding: &StrategyBinding, now_ms: u64) -> bool {
        self.matches(binding) && self.is_valid_at(now_ms)
    }
}

#[derive(Debug)]
pub struct InstanceControlStore {
    store: ProjectionStore,
}

impl InstanceControlStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            store: ProjectionStore::new(path.as_ref().to_path_buf()),
        }
    }

    pub fn load(&self) -> Result<Option<InstanceControlRecord>, ControlError> {
        let record: Option<InstanceControlRecord> =
            self.store.load().map_err(ControlError::Store)?;
        if let Some(record) = &record {
            record.validate()?;
        }
        Ok(record)
    }

    /// A record can only replace its direct predecessor, preventing a stale controller from
    /// silently overwriting a newer target during recovery.
    pub fn save(
        &self,
        record: &InstanceControlRecord,
        expected_previous_revision: Option<u64>,
    ) -> Result<(), ControlError> {
        record.validate()?;
        let current = self.load()?;
        if current.as_ref().map(|value| value.revision) != expected_previous_revision {
            return Err(ControlError::Revision);
        }
        if record.revision != expected_previous_revision.unwrap_or(0).saturating_add(1) {
            return Err(ControlError::Revision);
        }
        self.store.save(record).map_err(ControlError::Store)
    }
}

pub(crate) fn digest_binding(binding: &StrategyBinding) -> String {
    binding.digest()
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("instance control record is invalid")]
    Record,
    #[error("instance control revision is stale or non-sequential")]
    Revision,
    #[error("instance control projection storage failed: {0}")]
    Store(crate::storage::StorageError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        domain::{Amount, Asset},
        strategy::scalping::StrategyBinding,
    };

    use super::*;

    fn record(
        revision: u64,
        target: ControlTarget,
    ) -> Result<InstanceControlRecord, Box<dyn std::error::Error>> {
        Ok(InstanceControlRecord {
            schema_version: CONTROL_SCHEMA_VERSION,
            binding: StrategyBinding {
                strategy_kind: crate::strategy::scalping::StrategyKind::Scalping,
                strategy_instance_id: "scalping_primary".to_owned(),
                run_id: "shadow_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "BTC/USDT".parse()?,
                parameter_release_id: "scalping-shadow-v1".to_owned(),
                owner_scope: "scalping_primary:shadow_1".to_owned(),
                risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
            },
            target,
            command_id: format!("control_{revision}"),
            idempotency_key: format!("control_key_{revision}"),
            safety_deadline_ms: None,
            revision,
        })
    }

    #[test]
    fn only_current_running_control_with_fresh_authority_can_authorize_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let running = record(1, ControlTarget::Running)?;
        let authority = ControlAuthority {
            generation: 1,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            private_snapshot_ready: true,
            execution_unknown: false,
            protection_complete: true,
            owner_conflict: false,
        };
        assert!(running.authorize(&authority, 1).is_allowed());
        let stopped = record(1, ControlTarget::StopAndProtect)?;
        assert_eq!(
            stopped.authorize(&authority, 1).block(),
            Some(ControlBlock::Target)
        );
        Ok(())
    }

    #[test]
    fn control_store_rejects_stale_revisions() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let store = InstanceControlStore::new(directory.path().join("control.json"));
        let first = record(1, ControlTarget::Running)?;
        store.save(&first, None)?;
        assert!(matches!(
            store.save(&first, None),
            Err(ControlError::Revision)
        ));
        let second = record(2, ControlTarget::StopAndProtect)?;
        store.save(&second, Some(1))?;
        assert_eq!(store.load()?.ok_or("missing control")?, second);
        Ok(())
    }
}
