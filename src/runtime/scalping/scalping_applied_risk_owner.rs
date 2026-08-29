use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    storage::{ProjectionStore, ScalpingRiskBinding, StorageError},
    strategy::scalping::{RiskUnit, StrategyBinding, risk_revaluation_digest},
};

use super::{AppliedRiskReceipt, BoundRiskRevaluation, ScalpingCoordinatorCheckpoint};

pub const SCALPING_APPLIED_RISK_OWNER_SCHEMA_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedRiskFenceReason {
    Binding,
    Proof,
    CursorRegression,
    Equivocation,
    UnknownHostProof,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppliedRiskOwnerCheckpoint {
    pub schema_version: u16,
    pub binding: StrategyBinding,
    pub risk_unit: RiskUnit,
    pub last_ack: Option<AppliedRiskReceipt>,
    pub fenced_reason: Option<AppliedRiskFenceReason>,
    pub state_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppliedRiskOwnerTurn {
    ApplyRequired,
    Persisted(AppliedRiskReceipt),
    Duplicate(AppliedRiskReceipt),
}

/// Durable acknowledgment owner for one Shadow host's applied logical-risk stream. It neither
/// values risk nor applies a proof: a receipt is saved only after the supplied host checkpoint
/// contains the exact proof id/cursor and the strategy risk projection confirms its generation.
#[derive(Debug)]
pub struct ScalpingAppliedRiskOwner {
    expected_binding: StrategyBinding,
    expected_risk_unit: RiskUnit,
    store: ProjectionStore,
    checkpoint: AppliedRiskOwnerCheckpoint,
    fenced: bool,
}

impl ScalpingAppliedRiskOwner {
    pub fn open_or_restore(
        path: impl Into<PathBuf>,
        expected_binding: StrategyBinding,
        expected_risk_unit: RiskUnit,
    ) -> Result<Self, ScalpingAppliedRiskOwnerError> {
        expected_binding
            .validate()
            .map_err(|_| ScalpingAppliedRiskOwnerError::Binding)?;
        if expected_risk_unit.as_str().is_empty() {
            return Err(ScalpingAppliedRiskOwnerError::Binding);
        }
        let store = ProjectionStore::new(path.into());
        let checkpoint = match store.load::<AppliedRiskOwnerCheckpoint>()? {
            Some(checkpoint) => {
                validate_checkpoint(&checkpoint, &expected_binding, &expected_risk_unit)?;
                checkpoint
            }
            None => seal_checkpoint(AppliedRiskOwnerCheckpoint {
                schema_version: SCALPING_APPLIED_RISK_OWNER_SCHEMA_VERSION,
                binding: expected_binding.clone(),
                risk_unit: expected_risk_unit.clone(),
                last_ack: None,
                fenced_reason: None,
                state_digest: String::new(),
            })?,
        };
        let fenced = checkpoint.fenced_reason.is_some();
        Ok(Self {
            expected_binding,
            expected_risk_unit,
            store,
            checkpoint,
            fenced,
        })
    }

    #[must_use]
    pub const fn checkpoint(&self) -> &AppliedRiskOwnerCheckpoint {
        &self.checkpoint
    }

    #[must_use]
    pub fn last_ack(&self) -> Option<&AppliedRiskReceipt> {
        self.checkpoint.last_ack.as_ref()
    }

    #[must_use]
    pub fn last_ack_proof_id(&self) -> Option<&str> {
        self.last_ack().map(|receipt| receipt.proof_id.as_str())
    }

    #[must_use]
    pub const fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Examines exactly one proof. `ApplyRequired` is the only result that authorizes the caller
    /// to invoke the resident risk cycle. `Persisted` means the host checkpoint already proves the
    /// application point, including the crash window where no prior receipt exists.
    pub fn turn(
        &mut self,
        host: &ScalpingCoordinatorCheckpoint,
        bound: &BoundRiskRevaluation,
    ) -> Result<AppliedRiskOwnerTurn, ScalpingAppliedRiskOwnerError> {
        if self.fenced {
            return Err(ScalpingAppliedRiskOwnerError::Fenced);
        }
        if host.strategy.binding_digest != self.expected_binding.digest() {
            return self.fence(
                AppliedRiskFenceReason::Binding,
                ScalpingAppliedRiskOwnerError::Binding,
            );
        }
        let receipt = match self.receipt_for(bound) {
            Ok(receipt) => receipt,
            Err(ScalpingAppliedRiskOwnerError::Binding) => {
                return self.fence(
                    AppliedRiskFenceReason::Binding,
                    ScalpingAppliedRiskOwnerError::Binding,
                );
            }
            Err(error) => {
                return self.fence(AppliedRiskFenceReason::Proof, error);
            }
        };
        if let Some(last) = self.checkpoint.last_ack.as_ref() {
            if receipt.cursor_sequence < last.cursor_sequence {
                return self.fence(
                    AppliedRiskFenceReason::CursorRegression,
                    ScalpingAppliedRiskOwnerError::CursorRegression,
                );
            }
            if receipt.cursor_sequence == last.cursor_sequence && receipt != *last {
                return self.fence(
                    AppliedRiskFenceReason::Equivocation,
                    ScalpingAppliedRiskOwnerError::Equivocation,
                );
            }
            if receipt.cursor_sequence > last.cursor_sequence
                && (receipt.proof_id == last.proof_id
                    || receipt.target_generation < last.target_generation
                    || receipt.valuation_generation < last.valuation_generation)
            {
                return self.fence(
                    AppliedRiskFenceReason::CursorRegression,
                    ScalpingAppliedRiskOwnerError::CursorRegression,
                );
            }
        }

        let host_point = match host_point(host) {
            Ok(point) => point,
            Err(error) => {
                return self.fence(AppliedRiskFenceReason::UnknownHostProof, error);
            }
        };
        if self.checkpoint.last_ack.as_ref() == Some(&receipt) {
            if host_confirms_receipt(host, host_point, &receipt) {
                return Ok(AppliedRiskOwnerTurn::Duplicate(receipt));
            }
            return self.fence(
                AppliedRiskFenceReason::UnknownHostProof,
                ScalpingAppliedRiskOwnerError::UnknownHostProof,
            );
        }

        let prior_host_point = self
            .checkpoint
            .last_ack
            .as_ref()
            .map(|last| (last.cursor_sequence, last.proof_id.as_str()));
        if host_confirms_receipt(host, host_point, &receipt) {
            self.persist_receipt(receipt.clone())?;
            return Ok(AppliedRiskOwnerTurn::Persisted(receipt));
        }
        if host_point == prior_host_point {
            if let Some(last) = self.checkpoint.last_ack.as_ref()
                && !host_confirms_receipt(host, host_point, last)
            {
                return self.fence(
                    AppliedRiskFenceReason::UnknownHostProof,
                    ScalpingAppliedRiskOwnerError::UnknownHostProof,
                );
            }
            return Ok(AppliedRiskOwnerTurn::ApplyRequired);
        }
        self.fence(
            AppliedRiskFenceReason::UnknownHostProof,
            ScalpingAppliedRiskOwnerError::UnknownHostProof,
        )
    }

    fn receipt_for(
        &self,
        bound: &BoundRiskRevaluation,
    ) -> Result<AppliedRiskReceipt, ScalpingAppliedRiskOwnerError> {
        validate_risk_binding(
            &self.expected_binding,
            &self.expected_risk_unit,
            &bound.binding,
        )?;
        if bound.cursor_sequence == 0
            || bound.proof.proof_id.trim().is_empty()
            || bound.proof.target_generation == 0
            || bound.proof.target_generation != bound.binding.valuation_generation
            || bound.proof.risk_unit != self.expected_risk_unit
            || bound.proof.complete_through_ms == 0
            || bound.proof.window_start_ms > bound.proof.complete_through_ms
        {
            return Err(ScalpingAppliedRiskOwnerError::Proof);
        }
        Ok(AppliedRiskReceipt {
            binding: self.expected_binding.clone(),
            proof_id: bound.proof.proof_id.clone(),
            cursor_sequence: bound.cursor_sequence,
            risk_revaluation_digest: risk_revaluation_digest(&bound.proof)
                .map_err(|_| ScalpingAppliedRiskOwnerError::Proof)?,
            target_generation: bound.proof.target_generation,
            valuation_generation: bound.binding.valuation_generation,
        })
    }

    fn persist_receipt(
        &mut self,
        receipt: AppliedRiskReceipt,
    ) -> Result<(), ScalpingAppliedRiskOwnerError> {
        let mut next = self.checkpoint.clone();
        next.last_ack = Some(receipt);
        let sealed = seal_checkpoint(next)?;
        self.store.save(&sealed)?;
        self.checkpoint = sealed;
        Ok(())
    }

    fn fence<T>(
        &mut self,
        reason: AppliedRiskFenceReason,
        error: ScalpingAppliedRiskOwnerError,
    ) -> Result<T, ScalpingAppliedRiskOwnerError> {
        self.fenced = true;
        let mut next = self.checkpoint.clone();
        next.fenced_reason = Some(reason);
        let sealed = seal_checkpoint(next)?;
        self.store.save(&sealed)?;
        self.checkpoint = sealed;
        Err(error)
    }
}

fn host_confirms_receipt(
    host: &ScalpingCoordinatorCheckpoint,
    host_point: Option<(u64, &str)>,
    receipt: &AppliedRiskReceipt,
) -> bool {
    host_point == Some((receipt.cursor_sequence, receipt.proof_id.as_str()))
        && host.strategy.risk.valuation_generation == Some(receipt.target_generation)
        && host.strategy.risk.last_revaluation_id.as_deref() == Some(receipt.proof_id.as_str())
}

fn host_point(
    checkpoint: &ScalpingCoordinatorCheckpoint,
) -> Result<Option<(u64, &str)>, ScalpingAppliedRiskOwnerError> {
    match (
        checkpoint.last_risk_cursor_sequence,
        checkpoint.last_risk_proof_id.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(sequence), Some(proof_id)) if sequence > 0 && !proof_id.trim().is_empty() => {
            Ok(Some((sequence, proof_id)))
        }
        _ => Err(ScalpingAppliedRiskOwnerError::UnknownHostProof),
    }
}

fn validate_risk_binding(
    expected: &StrategyBinding,
    risk_unit: &RiskUnit,
    actual: &ScalpingRiskBinding,
) -> Result<(), ScalpingAppliedRiskOwnerError> {
    if actual.exchange != expected.exchange
        || actual.account != expected.account
        || actual.owner_scope != expected.owner_scope
        || actual.strategy_instance_id != expected.strategy_instance_id
        || actual.run_id != expected.run_id
        || actual.parameter_release_id != expected.parameter_release_id
        || actual.symbol != expected.symbol
        || actual.risk_unit != *risk_unit
        || actual.valuation_generation == 0
    {
        return Err(ScalpingAppliedRiskOwnerError::Binding);
    }
    Ok(())
}

fn validate_checkpoint(
    checkpoint: &AppliedRiskOwnerCheckpoint,
    expected_binding: &StrategyBinding,
    expected_risk_unit: &RiskUnit,
) -> Result<(), ScalpingAppliedRiskOwnerError> {
    if checkpoint.schema_version != SCALPING_APPLIED_RISK_OWNER_SCHEMA_VERSION
        || checkpoint.binding != *expected_binding
        || checkpoint.risk_unit != *expected_risk_unit
        || checkpoint.state_digest != seal_checkpoint(checkpoint.clone())?.state_digest
        || checkpoint.last_ack.as_ref().is_some_and(|receipt| {
            receipt.binding != *expected_binding
                || receipt.proof_id.trim().is_empty()
                || receipt.cursor_sequence == 0
                || !digest_is_valid(&receipt.risk_revaluation_digest)
                || receipt.target_generation == 0
                || receipt.target_generation != receipt.valuation_generation
        })
    {
        return Err(ScalpingAppliedRiskOwnerError::Checkpoint);
    }
    Ok(())
}

fn seal_checkpoint(
    mut checkpoint: AppliedRiskOwnerCheckpoint,
) -> Result<AppliedRiskOwnerCheckpoint, ScalpingAppliedRiskOwnerError> {
    checkpoint.state_digest.clear();
    let encoded = serde_json::to_vec(&checkpoint).map_err(ScalpingAppliedRiskOwnerError::Encode)?;
    checkpoint.state_digest = format!("{:x}", Sha256::digest(encoded));
    Ok(checkpoint)
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingAppliedRiskOwnerError {
    #[error("applied-risk owner binding or logical risk unit does not match")]
    Binding,
    #[error("applied-risk proof identity or generation is invalid")]
    Proof,
    #[error("applied-risk cursor regressed or reused a proof identity")]
    CursorRegression,
    #[error("applied-risk cursor was reused with different content")]
    Equivocation,
    #[error("host checkpoint contains an unknown applied-risk proof")]
    UnknownHostProof,
    #[error("applied-risk owner checkpoint is invalid or corrupt")]
    Checkpoint,
    #[error("applied-risk owner is fenced after a rejected transition")]
    Fenced,
    #[error("applied-risk owner identity encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("applied-risk owner storage failed: {0}")]
    Storage(#[from] StorageError),
}
