mod exposure_repair;
mod fill_driver;
mod rebuild;
mod risk_snapshot;
mod shadow_evidence;

pub(crate) use exposure_repair::{ExposureLadderRepairPlan, plan_same_anchor_exposure_repair};
pub(crate) use fill_driver::{
    GridFillApplication, GridFillProjection, GridFillRoute, TerminalExecutionError,
    apply_owned_grid_fill, route_grid_fill, terminal_owned_execution,
};
pub(crate) use rebuild::{epoch_at_anchor, epoch_at_midpoint};
#[allow(unused_imports)]
pub(crate) use risk_snapshot::{
    BindingRiskSnapshot, ExposureReductionAudit, ExposureReductionPending, ExposureRuntimeSettings,
    MarketReductionPlan, RiskSnapshotRuntimeError, append_reduction_audit_once,
    associate_reduction_fill, plan_market_reduction, reduction_audit_for_episode,
    select_binding_risk_snapshot, summarize_reduction_fills,
};
#[allow(unused_imports)]
pub(crate) use shadow_evidence::{
    EXPOSURE_SHADOW_EVIDENCE_FILE, ExposureShadowDecision, ExposureShadowEvidence,
    ExposureShadowEvidenceJournal, ExposureShadowReason, RawRiskEvidenceRef, build_shadow_evidence,
};

#[cfg(test)]
#[path = "exposure_runtime_tests.rs"]
mod exposure_runtime_tests;
