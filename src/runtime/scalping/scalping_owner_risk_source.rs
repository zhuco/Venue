use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    storage::{ProjectionStore, ScalpingRiskCursor, ScalpingRiskFact, StorageError},
    strategy::scalping::{RiskUnit, StrategyBinding},
};

use super::{
    BoundRiskRevaluation, RiskProofClock, RiskRevaluationProducer, RiskRevaluationProducerError,
};

pub const SCALPING_OWNER_RISK_SOURCE_SCHEMA_VERSION: u16 = 1;

/// The durable fail-closed state of one owner-risk page source. This is deliberately not a second
/// risk journal: it records only the binding, logical unit and a permanent local fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalpingOwnerRiskSourceFenceReason {
    Cursor,
    DurableCursor,
    Producer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingOwnerRiskSourceCheckpoint {
    pub schema_version: u16,
    pub binding: StrategyBinding,
    pub risk_unit: RiskUnit,
    pub fenced_reason: Option<ScalpingOwnerRiskSourceFenceReason>,
    pub state_digest: String,
}

/// Returns the non-authoritative source-fence checkpoint paired with an owner-risk journal.
/// The checkpoint has no facts or cursor and cannot be used to reconstruct a risk replay.
#[must_use]
pub fn scalping_owner_risk_source_checkpoint_path(journal_path: impl AsRef<Path>) -> PathBuf {
    let mut path = journal_path.as_ref().as_os_str().to_os_string();
    path.push(".source.json");
    PathBuf::from(path)
}

/// One upstream page of already-valued owner risk. This boundary accepts no account, fill, quote,
/// or exchange-native data: its facts must already satisfy the logical-risk journal contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingOwnerRiskPage {
    /// The durable cursor used by the upstream reader. A generation replacement may restart from
    /// `None`, matching the frozen runner's full revaluation retry.
    pub requested_after: Option<ScalpingRiskCursor>,
    pub facts: Vec<ScalpingRiskFact>,
    pub cursor: ScalpingRiskCursor,
}

/// One bounded source turn. `Proof` and `PendingProof` can be placed directly in the resident
/// runtime's existing `risk` command slot; their durable proof payload is boxed to keep this
/// control enum small. This source never calls the host itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalpingOwnerRiskTurn {
    Idle {
        resume_after: Option<ScalpingRiskCursor>,
    },
    PendingProof {
        proof: Box<BoundRiskRevaluation>,
    },
    PageCommitted {
        resume_after: ScalpingRiskCursor,
    },
    Proof {
        proof: Box<BoundRiskRevaluation>,
        resume_after: ScalpingRiskCursor,
    },
}

/// One-instance, synchronous owner-risk page coordinator. Its caller owns fetching, scheduling,
/// and host delivery; this type only accepts one supplied page per turn and commits it through the
/// existing durable producer.
#[derive(Debug)]
pub struct ScalpingOwnerRiskSource {
    producer: Option<RiskRevaluationProducer>,
    durable_cursor: Option<ScalpingRiskCursor>,
    store: ProjectionStore,
    checkpoint: ScalpingOwnerRiskSourceCheckpoint,
    fenced: bool,
}

impl ScalpingOwnerRiskSource {
    pub fn open(
        path: impl Into<PathBuf>,
        binding: &StrategyBinding,
        risk_unit: RiskUnit,
    ) -> Result<Self, ScalpingOwnerRiskSourceError> {
        binding
            .validate()
            .map_err(|_| ScalpingOwnerRiskSourceError::Binding)?;
        if risk_unit.as_str().is_empty() {
            return Err(ScalpingOwnerRiskSourceError::Binding);
        }
        let journal_path = path.into();
        let store = ProjectionStore::new(scalping_owner_risk_source_checkpoint_path(&journal_path));
        let checkpoint = match store.load::<ScalpingOwnerRiskSourceCheckpoint>()? {
            Some(checkpoint) => {
                validate_checkpoint(&checkpoint, binding, &risk_unit)?;
                checkpoint
            }
            None => seal_checkpoint(ScalpingOwnerRiskSourceCheckpoint {
                schema_version: SCALPING_OWNER_RISK_SOURCE_SCHEMA_VERSION,
                binding: binding.clone(),
                risk_unit: risk_unit.clone(),
                fenced_reason: None,
                state_digest: String::new(),
            })?,
        };
        let mut source = Self {
            producer: None,
            durable_cursor: None,
            store,
            fenced: checkpoint.fenced_reason.is_some(),
            checkpoint,
        };
        if source.fenced {
            return Ok(source);
        }
        let producer = match RiskRevaluationProducer::open(journal_path, binding, risk_unit) {
            Ok(producer) => producer,
            Err(error) => {
                return source
                    .fence_open(ScalpingOwnerRiskSourceFenceReason::Producer, error.into());
            }
        };
        let durable_cursor = match producer.recover_durable_cursor() {
            Ok(cursor) => cursor,
            Err(error) => {
                return source
                    .fence_open(ScalpingOwnerRiskSourceFenceReason::Producer, error.into());
            }
        };
        source.producer = Some(producer);
        source.durable_cursor = durable_cursor;
        Ok(source)
    }

    #[must_use]
    pub const fn is_fenced(&self) -> bool {
        self.fenced
    }

    #[must_use]
    pub fn resume_after(&self) -> Option<&ScalpingRiskCursor> {
        self.durable_cursor.as_ref()
    }

    /// First checks the caller's durable applied-proof point. If an unapplied proof already exists,
    /// it is returned without consuming a new page. Otherwise this turn commits exactly one page;
    /// intermediate pages only advance the durable cursor, while a terminal page returns one proof.
    pub fn drive_turn(
        &mut self,
        clock: RiskProofClock,
        last_applied_proof_id: Option<&str>,
        page: Option<ScalpingOwnerRiskPage>,
    ) -> Result<ScalpingOwnerRiskTurn, ScalpingOwnerRiskSourceError> {
        if self.fenced {
            return Err(ScalpingOwnerRiskSourceError::Fenced);
        }
        let pending = match self
            .producer
            .as_ref()
            .ok_or(ScalpingOwnerRiskSourceError::Fenced)?
            .recover_complete(clock, last_applied_proof_id)
        {
            Ok(pending) => pending,
            Err(error) => return self.fence(error.into()),
        };
        if let Some(proof) = pending {
            return Ok(ScalpingOwnerRiskTurn::PendingProof {
                proof: Box::new(proof),
            });
        }
        let Some(page) = page else {
            return Ok(ScalpingOwnerRiskTurn::Idle {
                resume_after: self.durable_cursor.clone(),
            });
        };
        if let Err(error) = self.validate_requested_after(&page) {
            return self.fence(error);
        }
        let proof = match self
            .producer
            .as_mut()
            .ok_or(ScalpingOwnerRiskSourceError::Fenced)?
            .commit_page(clock, page.facts, page.cursor)
        {
            Ok(proof) => proof,
            Err(error) => return self.fence(error.into()),
        };
        let durable_cursor = match self
            .producer
            .as_ref()
            .ok_or(ScalpingOwnerRiskSourceError::Fenced)?
            .recover_durable_cursor()
        {
            Ok(Some(cursor)) => cursor,
            Ok(None) => return self.fence(ScalpingOwnerRiskSourceError::DurableCursor),
            Err(error) => return self.fence(error.into()),
        };
        self.durable_cursor = Some(durable_cursor.clone());
        Ok(match proof {
            Some(proof) => ScalpingOwnerRiskTurn::Proof {
                proof: Box::new(proof),
                resume_after: durable_cursor,
            },
            None => ScalpingOwnerRiskTurn::PageCommitted {
                resume_after: durable_cursor,
            },
        })
    }

    fn validate_requested_after(
        &self,
        page: &ScalpingOwnerRiskPage,
    ) -> Result<(), ScalpingOwnerRiskSourceError> {
        let Some(durable_cursor) = &self.durable_cursor else {
            return if page.requested_after.is_none() {
                Ok(())
            } else {
                Err(ScalpingOwnerRiskSourceError::Cursor)
            };
        };
        if page.cursor == *durable_cursor {
            return Ok(());
        }
        if page.requested_after.as_ref() == Some(durable_cursor) {
            return Ok(());
        }
        if page.requested_after.is_none()
            && page.cursor.binding.valuation_generation
                > durable_cursor.binding.valuation_generation
        {
            return Ok(());
        }
        Err(ScalpingOwnerRiskSourceError::Cursor)
    }

    fn fence<T>(
        &mut self,
        error: ScalpingOwnerRiskSourceError,
    ) -> Result<T, ScalpingOwnerRiskSourceError> {
        self.fenced = true;
        self.persist_fence(fence_reason(&error))?;
        Err(error)
    }

    fn fence_open<T>(
        &mut self,
        reason: ScalpingOwnerRiskSourceFenceReason,
        error: ScalpingOwnerRiskSourceError,
    ) -> Result<T, ScalpingOwnerRiskSourceError> {
        self.fenced = true;
        self.persist_fence(reason)?;
        Err(error)
    }

    fn persist_fence(
        &mut self,
        reason: ScalpingOwnerRiskSourceFenceReason,
    ) -> Result<(), ScalpingOwnerRiskSourceError> {
        let mut checkpoint = self.checkpoint.clone();
        checkpoint.fenced_reason = Some(reason);
        let sealed = seal_checkpoint(checkpoint)?;
        self.store.save(&sealed)?;
        self.checkpoint = sealed;
        Ok(())
    }
}

fn fence_reason(error: &ScalpingOwnerRiskSourceError) -> ScalpingOwnerRiskSourceFenceReason {
    match error {
        ScalpingOwnerRiskSourceError::Cursor => ScalpingOwnerRiskSourceFenceReason::Cursor,
        ScalpingOwnerRiskSourceError::DurableCursor => {
            ScalpingOwnerRiskSourceFenceReason::DurableCursor
        }
        ScalpingOwnerRiskSourceError::Producer(_) => ScalpingOwnerRiskSourceFenceReason::Producer,
        ScalpingOwnerRiskSourceError::Fenced
        | ScalpingOwnerRiskSourceError::Binding
        | ScalpingOwnerRiskSourceError::Checkpoint
        | ScalpingOwnerRiskSourceError::Encode(_)
        | ScalpingOwnerRiskSourceError::Storage(_) => ScalpingOwnerRiskSourceFenceReason::Producer,
    }
}

fn validate_checkpoint(
    checkpoint: &ScalpingOwnerRiskSourceCheckpoint,
    binding: &StrategyBinding,
    risk_unit: &RiskUnit,
) -> Result<(), ScalpingOwnerRiskSourceError> {
    if checkpoint.schema_version != SCALPING_OWNER_RISK_SOURCE_SCHEMA_VERSION
        || checkpoint.binding != *binding
        || checkpoint.risk_unit != *risk_unit
        || checkpoint.state_digest != seal_checkpoint(checkpoint.clone())?.state_digest
    {
        return Err(ScalpingOwnerRiskSourceError::Checkpoint);
    }
    Ok(())
}

fn seal_checkpoint(
    mut checkpoint: ScalpingOwnerRiskSourceCheckpoint,
) -> Result<ScalpingOwnerRiskSourceCheckpoint, ScalpingOwnerRiskSourceError> {
    checkpoint.state_digest.clear();
    let encoded = serde_json::to_vec(&checkpoint).map_err(ScalpingOwnerRiskSourceError::Encode)?;
    checkpoint.state_digest = format!("{:x}", Sha256::digest(encoded));
    Ok(checkpoint)
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingOwnerRiskSourceError {
    #[error("owner-risk source binding or logical risk unit is invalid")]
    Binding,
    #[error("owner-risk page was not requested from the durable cursor")]
    Cursor,
    #[error("owner-risk producer committed a page without a durable cursor")]
    DurableCursor,
    #[error("owner-risk source is fenced after a rejected page or recovery proof")]
    Fenced,
    #[error("owner-risk source fence checkpoint is invalid or tampered")]
    Checkpoint,
    #[error("owner-risk source checkpoint encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("owner-risk source checkpoint storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("owner-risk producer rejected its durable logical replay: {0}")]
    Producer(#[from] RiskRevaluationProducerError),
}
