mod calibration;
mod checkpoint;
mod engine;
mod evidence;

pub use crate::controller::ControlTarget;
pub use calibration::{
    CALIBRATION_SCHEMA_VERSION, CalibrationBook, CalibrationCostPriors, CalibrationKey,
    CalibrationManifest, CalibrationProjection, CalibrationSlice, RESEARCH_EVIDENCE_SCHEMA_VERSION,
    ResearchCheckStatus, ResearchEvidence, ResearchSliceEvidence,
};
pub(crate) use checkpoint::SCALPING_CHECKPOINT_SCHEMA;
pub use checkpoint::{ScalpingCheckpoint, ScalpingCheckpointStore};
pub use engine::ScalpingStrategy;
pub use evidence::{
    CalibrationEvidence, CandidateEvidenceBundle, CostEvidence, EvidenceIdentity, RiskEvidence,
    join_candidate_evidence, risk_revaluation_digest,
};
pub use venue_strategies::scalping::*;
