use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{storage::ProjectionStore, strategy::scalping::StrategyBinding};

use super::{
    ControlAuthority, ControlBlock, ControlError, ControlTarget, EntryAuthorization,
    InstanceControlStore,
};

pub const SCALPING_CONTROLLER_SOURCE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalpingControllerBlock {
    Missing,
    Binding,
    Target,
    Release,
    Revision,
    Authority(ControlBlock),
    Deadline,
    Generation,
    RecoveryGeneration,
}

/// One controller input for a resident turn. A blocked update carries no authorization; only a
/// durable non-Running target or an untrusted/missing record requests a semantic stop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingControllerUpdate {
    authorization: Option<EntryAuthorization>,
    control: Option<ControlTarget>,
    block: Option<ScalpingControllerBlock>,
    revision: Option<u64>,
}

impl ScalpingControllerUpdate {
    #[must_use]
    pub fn authorization(&self) -> Option<&EntryAuthorization> {
        self.authorization.as_ref()
    }

    #[must_use]
    pub const fn control(&self) -> Option<ControlTarget> {
        self.control
    }

    #[must_use]
    pub const fn block(&self) -> Option<ScalpingControllerBlock> {
        self.block
    }

    #[must_use]
    pub const fn revision(&self) -> Option<u64> {
        self.revision
    }

    fn authorized(authorization: EntryAuthorization, revision: u64) -> Self {
        Self {
            authorization: Some(authorization),
            control: Some(ControlTarget::Running),
            block: None,
            revision: Some(revision),
        }
    }

    fn fenced(
        block: ScalpingControllerBlock,
        control: Option<ControlTarget>,
        revision: Option<u64>,
    ) -> Self {
        Self {
            authorization: None,
            control,
            block: Some(block),
            revision,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ScalpingControllerCursor {
    schema_version: u16,
    binding_digest: String,
    parameter_release_id: String,
    control_revision: u64,
    authority_generation: u64,
    target: ControlTarget,
    authorization_expires_at_ms: u64,
}

impl ScalpingControllerCursor {
    fn validate(&self, binding: &StrategyBinding) -> Result<(), ScalpingControllerSourceError> {
        if self.schema_version != SCALPING_CONTROLLER_SOURCE_SCHEMA_VERSION
            || self.binding_digest != binding.digest()
            || self.parameter_release_id != binding.parameter_release_id
            || self.control_revision == 0
            || (self.target == ControlTarget::Running && self.authorization_expires_at_ms == 0)
        {
            return Err(ScalpingControllerSourceError::State);
        }
        Ok(())
    }
}

/// Reads the controller-owned record and persists only the resident's authorization cursor. It
/// owns no controller command endpoint, authority producer, clock, worker, or mutation path.
#[derive(Debug)]
pub struct ScalpingControllerSource {
    control: InstanceControlStore,
    cursor_store: ProjectionStore,
    binding: StrategyBinding,
    cursor: Option<ScalpingControllerCursor>,
    recovery_generation_required: bool,
    poisoned: bool,
}

impl ScalpingControllerSource {
    pub fn open(
        control_path: impl AsRef<Path>,
        cursor_path: impl AsRef<Path>,
        binding: StrategyBinding,
    ) -> Result<Self, ScalpingControllerSourceError> {
        binding
            .validate()
            .map_err(|_| ScalpingControllerSourceError::Binding)?;
        let control_path = control_path.as_ref();
        let cursor_path = cursor_path.as_ref();
        if !control_path.is_absolute() || !cursor_path.is_absolute() || control_path == cursor_path
        {
            return Err(ScalpingControllerSourceError::Path);
        }
        let cursor_store = ProjectionStore::new(cursor_path.to_path_buf());
        let cursor: Option<ScalpingControllerCursor> = cursor_store.load()?;
        if let Some(cursor) = &cursor {
            cursor.validate(&binding)?;
        }
        let recovery_generation_required = cursor.is_some();
        Ok(Self {
            control: InstanceControlStore::new(control_path),
            cursor_store,
            binding,
            cursor,
            recovery_generation_required,
            poisoned: false,
        })
    }

    pub fn observe(
        &mut self,
        authority: Option<&ControlAuthority>,
        now_ms: u64,
    ) -> Result<ScalpingControllerUpdate, ScalpingControllerSourceError> {
        if self.poisoned {
            return Err(ScalpingControllerSourceError::Poisoned);
        }
        if now_ms == 0 {
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Deadline,
                Some(ControlTarget::StopAndProtect),
                None,
            ));
        }
        let Some(record) = self.control.load()? else {
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Missing,
                Some(ControlTarget::StopAndProtect),
                None,
            ));
        };
        if record.binding != self.binding {
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Binding,
                Some(ControlTarget::StopAndProtect),
                Some(record.revision),
            ));
        }
        if self.cursor.as_ref().is_some_and(|cursor| {
            record.revision < cursor.control_revision
                || (record.revision == cursor.control_revision
                    && (record.target != cursor.target
                        || record.safety_deadline_ms.unwrap_or_default()
                            != cursor.authorization_expires_at_ms))
        }) {
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Revision,
                Some(ControlTarget::StopAndProtect),
                Some(record.revision),
            ));
        }

        let observed_generation = authority.map_or_else(
            || {
                self.cursor
                    .as_ref()
                    .map_or(0, |cursor| cursor.authority_generation)
            },
            |authority| authority.generation,
        );
        let deadline = record.safety_deadline_ms.unwrap_or_default();
        if record.target != ControlTarget::Running {
            self.persist_cursor(
                record.revision,
                observed_generation,
                record.target,
                deadline,
            )?;
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Target,
                Some(record.target),
                Some(record.revision),
            ));
        }
        if deadline == 0 || deadline <= now_ms {
            self.persist_cursor(
                record.revision,
                observed_generation,
                record.target,
                deadline,
            )?;
            // Expiry closes entry immediately, but a complete current authority is sufficient for
            // a later operator renewal: the control timeout did not mutate the exchange account.
            // A missing authority keeps the recovery fence, and process reopen sets it again.
            self.recovery_generation_required = authority.is_none();
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Deadline,
                Some(ControlTarget::StopAndProtect),
                Some(record.revision),
            ));
        }
        let Some(authority) = authority else {
            self.persist_cursor(
                record.revision,
                observed_generation,
                record.target,
                deadline,
            )?;
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Authority(ControlBlock::PrivateSnapshot),
                None,
                Some(record.revision),
            ));
        };
        if authority.parameter_release_id != self.binding.parameter_release_id {
            self.persist_cursor(
                record.revision,
                authority.generation,
                record.target,
                deadline,
            )?;
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Release,
                Some(ControlTarget::StopAndProtect),
                Some(record.revision),
            ));
        }

        if let Some(cursor) = &self.cursor {
            if authority.generation < cursor.authority_generation {
                self.recovery_generation_required = true;
                return Ok(ScalpingControllerUpdate::fenced(
                    ScalpingControllerBlock::Generation,
                    None,
                    Some(record.revision),
                ));
            }
            if self.recovery_generation_required
                && authority.generation <= cursor.authority_generation
            {
                self.recovery_generation_required = true;
                return Ok(ScalpingControllerUpdate::fenced(
                    ScalpingControllerBlock::RecoveryGeneration,
                    None,
                    Some(record.revision),
                ));
            }
        }

        let authorization = record.authorize(authority, now_ms);
        if !authorization.is_allowed() {
            let block = authorization
                .block()
                .unwrap_or(ControlBlock::AuthorityGeneration);
            self.persist_cursor(
                record.revision,
                authority.generation,
                record.target,
                deadline,
            )?;
            self.recovery_generation_required = true;
            return Ok(ScalpingControllerUpdate::fenced(
                ScalpingControllerBlock::Authority(block),
                None,
                Some(record.revision),
            ));
        }
        self.persist_cursor(
            record.revision,
            authority.generation,
            record.target,
            deadline,
        )?;
        self.recovery_generation_required = false;
        Ok(ScalpingControllerUpdate::authorized(
            authorization,
            record.revision,
        ))
    }

    fn persist_cursor(
        &mut self,
        control_revision: u64,
        authority_generation: u64,
        target: ControlTarget,
        authorization_expires_at_ms: u64,
    ) -> Result<(), ScalpingControllerSourceError> {
        let cursor = ScalpingControllerCursor {
            schema_version: SCALPING_CONTROLLER_SOURCE_SCHEMA_VERSION,
            binding_digest: self.binding.digest(),
            parameter_release_id: self.binding.parameter_release_id.clone(),
            control_revision,
            authority_generation,
            target,
            authorization_expires_at_ms,
        };
        if self.cursor.as_ref() != Some(&cursor)
            && let Err(error) = self.cursor_store.save(&cursor)
        {
            self.poisoned = true;
            return Err(error.into());
        }
        self.cursor = Some(cursor);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingControllerSourceError {
    #[error("scalping controller source requires distinct absolute durable paths")]
    Path,
    #[error("scalping controller source binding is invalid")]
    Binding,
    #[error("scalping controller source cursor is incompatible with its binding")]
    State,
    #[error("scalping controller source is poisoned after a failed durable save")]
    Poisoned,
    #[error("scalping controller record failed: {0}")]
    Control(#[from] ControlError),
    #[error("scalping controller cursor storage failed: {0}")]
    Storage(#[from] crate::storage::StorageError),
}
