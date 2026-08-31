mod calibration;
mod checkpoint;
mod evidence;

pub use calibration::{
    CALIBRATION_SCHEMA_VERSION, CalibrationBook, CalibrationCostPriors, CalibrationKey,
    CalibrationManifest, CalibrationProjection, CalibrationSlice, RESEARCH_EVIDENCE_SCHEMA_VERSION,
    ResearchCheckStatus, ResearchEvidence, ResearchSliceEvidence,
};
pub use checkpoint::ScalpingCheckpointStore;
pub use evidence::{
    CalibrationEvidence, CandidateEvidenceBundle, CostEvidence, EvidenceIdentity, RiskEvidence,
    join_candidate_evidence, risk_revaluation_digest,
};
pub use venue_strategies::scalping::*;

impl venue_strategies::scalping::LifecycleAuthorization for crate::controller::EntryAuthorization {
    fn is_allowed(&self) -> bool {
        self.is_allowed()
    }

    fn matches_at(&self, binding: &StrategyBinding, decision_at_ms: u64) -> bool {
        self.matches_at(binding, decision_at_ms)
    }

    fn revision(&self) -> u64 {
        self.revision()
    }

    fn authority_generation(&self) -> u64 {
        self.authority_generation()
    }
}
