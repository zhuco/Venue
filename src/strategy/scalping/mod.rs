mod calibration;
mod candidate_memory;
mod checkpoint;
mod engine;
mod evidence;
mod model;
mod risk;

pub use crate::controller::ControlTarget;
pub use calibration::{
    CALIBRATION_SCHEMA_VERSION, CalibrationBook, CalibrationCostPriors, CalibrationKey,
    CalibrationManifest, CalibrationProjection, CalibrationSlice, RESEARCH_EVIDENCE_SCHEMA_VERSION,
    ResearchCheckStatus, ResearchEvidence, ResearchSliceEvidence,
};
pub use candidate_memory::{BreakoutCursor, CandidateMemoryState, SeenCandidate};
pub(crate) use checkpoint::SCALPING_CHECKPOINT_SCHEMA;
pub use checkpoint::{ScalpingCheckpoint, ScalpingCheckpointStore};
pub use engine::ScalpingStrategy;
pub use evidence::{
    CalibrationEvidence, CandidateEvidenceBundle, CostEvidence, EvidenceIdentity, RiskEvidence,
    join_candidate_evidence, risk_revaluation_digest,
};
pub use model::{
    ArmedEpisodeFaultDeadline, BlockingReason, CandidateCosts, CandidateEvidence,
    CandidatePreparation, DeadlineFired, Direction, EntryStyle, EpisodeAction, EpisodeExitReason,
    EpisodeFaultKind, EpisodeProjection, EpisodeState, ExitDistancePolicy, ExitTemplate, Expert,
    ExposureState, FaultProjection, FaultRecoveryAuthorization, FaultScope, FillSlice,
    MarketRegime, NoopReason, OutcomeProbabilities, PHASE8_ATR14_PARAMETER_RELEASE_ID,
    ProtectionState, RiskLimit, RiskPlan, RiskUnit, SafetyDeadline, SafetyProjection,
    ScalpingDecision, ScalpingError, ScalpingParams, ScalpingState, SemanticIntent,
    SemanticPurpose, StrategyBinding, StrategyKind,
};
pub use risk::{RiskFact, RiskGate, RiskLedgerState, RiskRevaluation, RiskSnapshot};
