use super::ControlResidentLoopError;
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    ControlSnapshot, CopyExecutionEvidence, CopyPlanningFact, ExecutionFactsSnapshot,
};

pub(super) fn projection_digest_for<T: serde::Serialize>(
    label: &str,
    value: &T,
) -> Result<[u8; 32], ControlResidentLoopError> {
    let encoded =
        serde_json::to_vec(value).map_err(|_| ControlResidentLoopError::ProjectionEncoding)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.signed-projection.v1");
    digest.update(label.as_bytes());
    digest.update(encoded);
    Ok(digest.finalize().into())
}

pub(super) fn envelope_digest(
    snapshot: &ControlSnapshot,
    facts: &ExecutionFactsSnapshot,
    copy_execution_evidence: &[CopyExecutionEvidence],
    copy_planning_facts: &[CopyPlanningFact],
    sequence: u64,
    previous_digest: [u8; 32],
) -> Result<[u8; 32], ControlResidentLoopError> {
    let mut digest = Sha256::new();
    digest.update(b"venue.node.projection-envelope.v1");
    digest.update(sequence.to_le_bytes());
    digest.update(previous_digest);
    digest.update(
        serde_json::to_vec(snapshot).map_err(|_| ControlResidentLoopError::ProjectionEncoding)?,
    );
    digest.update(
        serde_json::to_vec(facts).map_err(|_| ControlResidentLoopError::ProjectionEncoding)?,
    );
    digest.update(
        serde_json::to_vec(copy_execution_evidence)
            .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?,
    );
    digest.update(
        serde_json::to_vec(copy_planning_facts)
            .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?,
    );
    Ok(digest.finalize().into())
}
