use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{CalibrationKey, CalibrationManifest, valid_sha256};
use crate::strategy::scalping::ScalpingError;

pub const RESEARCH_EVIDENCE_SCHEMA_VERSION: u32 = 1;

/// Per-slice out-of-sample evidence sealed into a calibration release. Raw datasets remain
/// outside the strategy artifact and are referenced only by release-wide digests.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchSliceEvidence {
    pub key: CalibrationKey,
    pub sample_count: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub after_cost_ev_lower_bps: Decimal,
    pub fill_calibration: ResearchCheckStatus,
    pub cost_calibration: ResearchCheckStatus,
    pub markout_calibration: ResearchCheckStatus,
    pub stress_budget: ResearchCheckStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchCheckStatus {
    Passed,
    Failed,
}

/// Release-wide dataset provenance and preregistered acceptance evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchEvidence {
    pub schema_version: u32,
    pub dataset_digest: String,
    pub preregistration_digest: String,
    pub evidence_cursor_ms: u64,
    pub approved_for_live: bool,
    pub slices: Vec<ResearchSliceEvidence>,
}

impl ResearchEvidence {
    pub(super) fn validate_for(
        &self,
        calibration: &CalibrationManifest,
    ) -> Result<(), ScalpingError> {
        if self.schema_version != RESEARCH_EVIDENCE_SCHEMA_VERSION
            || !valid_sha256(&self.dataset_digest)
            || !valid_sha256(&self.preregistration_digest)
            || self.evidence_cursor_ms == 0
            || self.slices.is_empty()
        {
            return Err(ScalpingError::Evidence);
        }
        let mut evidence = BTreeMap::new();
        for item in &self.slices {
            if item.sample_count == 0 || evidence.insert(item.key.clone(), item).is_some() {
                return Err(ScalpingError::Evidence);
            }
        }
        let mut calibration_keys = BTreeMap::new();
        for slice in &calibration.slices {
            if calibration_keys.insert(slice.key.clone(), ()).is_some() {
                return Err(ScalpingError::Evidence);
            }
        }
        if evidence.len() != calibration_keys.len() {
            return Err(ScalpingError::Evidence);
        }
        for slice in &calibration.slices {
            let item = evidence.get(&slice.key).ok_or(ScalpingError::Evidence)?;
            if item.sample_count != slice.sample_count
                || slice.evidence_cursor_ms > self.evidence_cursor_ms
            {
                return Err(ScalpingError::Evidence);
            }
            if slice.live_approved
                && (!self.approved_for_live
                    || item.after_cost_ev_lower_bps <= Decimal::ZERO
                    || item.fill_calibration != ResearchCheckStatus::Passed
                    || item.cost_calibration != ResearchCheckStatus::Passed
                    || item.markout_calibration != ResearchCheckStatus::Passed
                    || item.stress_budget != ResearchCheckStatus::Passed)
            {
                return Err(ScalpingError::Evidence);
            }
        }
        Ok(())
    }
}
