mod research;

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use research::{
    RESEARCH_EVIDENCE_SCHEMA_VERSION, ResearchCheckStatus, ResearchEvidence, ResearchSliceEvidence,
};

use crate::{
    domain::Symbol,
    strategy::scalping::{
        CalibrationEvidence, CandidatePreparation, Direction, EntryStyle, EvidenceIdentity, Expert,
        FillSlice, MarketRegime, OutcomeProbabilities, ScalpingError, ScalpingParams,
        SemanticIntent, StrategyBinding,
    },
};

pub const CALIBRATION_SCHEMA_VERSION: u32 = 1;

/// Identity of one independently calibrated decision slice.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationKey {
    pub symbol: Symbol,
    pub expert: Expert,
    pub regime: MarketRegime,
    pub direction: Direction,
    pub entry_style: EntryStyle,
}

impl CalibrationKey {
    #[must_use]
    pub fn from_candidate(preparation: &CandidatePreparation, candidate: &SemanticIntent) -> Self {
        Self {
            symbol: candidate.symbol.clone(),
            expert: candidate.expert,
            regime: preparation.market_regime,
            direction: candidate.direction,
            entry_style: candidate.entry_style,
        }
    }
}

/// Alpha-owned statistical evidence for one candidate family. Current executable costs and risk
/// authority are intentionally absent and remain separate evidence owners.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationSlice {
    pub key: CalibrationKey,
    pub release_id: String,
    pub model_version: String,
    pub artifact_digest: String,
    pub model_generation: u64,
    pub evidence_cursor_ms: u64,
    pub valid_from_ms: u64,
    pub valid_until_ms: u64,
    pub sample_count: u64,
    pub live_approved: bool,
    pub fill_distribution: Vec<FillSlice>,
    pub outcomes: OutcomeProbabilities,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub stop_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub other_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub nonfill_cancel_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub opportunity_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub ev_sigma_bps: Decimal,
}

/// Legacy Alpha-owned cost priors that accompany a calibrated fill distribution. Current
/// entry/exit/funding costs remain absent and must be supplied by a contemporaneous cost owner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationCostPriors {
    #[serde(with = "rust_decimal::serde::str")]
    pub nonfill_cancel_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub opportunity_cost_bps: Decimal,
}

/// Pure projection of one exact immutable slice. It is intentionally not a complete evidence
/// bundle and carries no current executable cost or risk decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationProjection {
    pub evidence: CalibrationEvidence,
    pub cost_priors: CalibrationCostPriors,
}

impl CalibrationSlice {
    /// Validates the deployment pins, evidence time, probability mass, PnL signs and costs of one
    /// exact lookup. Live use additionally requires the sealed research approval.
    pub fn validate_for(
        &self,
        binding: &StrategyBinding,
        params: &ScalpingParams,
        key: &CalibrationKey,
        watermark_ms: u64,
        live: bool,
    ) -> Result<(), ScalpingError> {
        let fill_probability = self
            .fill_distribution
            .iter()
            .map(|slice| slice.probability)
            .sum::<Decimal>();
        let outcome_probability = self.outcomes.target + self.outcomes.stop + self.outcomes.other;
        if &self.key != key
            || self.key.symbol != binding.symbol
            || self.release_id != binding.parameter_release_id
            || self.model_version != params.calibration_model_version
            || !valid_sha256(&self.artifact_digest)
            || self.artifact_digest != params.calibration_model_digest
            || self.model_generation == 0
            || self.evidence_cursor_ms == 0
            || self.evidence_cursor_ms > watermark_ms
            || self.valid_from_ms == 0
            || self.valid_from_ms > watermark_ms
            || self.valid_until_ms < watermark_ms
            || self.valid_until_ms < self.valid_from_ms
            || self.sample_count == 0
            || (live && !self.live_approved)
        {
            return Err(ScalpingError::Evidence);
        }
        if self.fill_distribution.is_empty()
            || fill_probability != Decimal::ONE
            || !self
                .fill_distribution
                .iter()
                .any(|slice| slice.fill_ratio > Decimal::ZERO)
            || self.fill_distribution.iter().any(|slice| {
                slice.fill_ratio < Decimal::ZERO
                    || slice.fill_ratio > Decimal::ONE
                    || slice.probability < Decimal::ZERO
                    || slice.probability > Decimal::ONE
            })
            || outcome_probability != Decimal::ONE
            || [
                self.outcomes.target,
                self.outcomes.stop,
                self.outcomes.other,
            ]
            .iter()
            .any(|probability| *probability < Decimal::ZERO || *probability > Decimal::ONE)
            || self.target_pnl_bps <= Decimal::ZERO
            || self.stop_pnl_bps >= Decimal::ZERO
            || self.nonfill_cancel_cost_bps < Decimal::ZERO
            || self.opportunity_cost_bps < Decimal::ZERO
            || self.ev_sigma_bps < Decimal::ZERO
        {
            return Err(ScalpingError::Evidence);
        }
        Ok(())
    }
}

/// Immutable content-addressed calibration release.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalibrationManifest {
    pub schema_version: u32,
    pub release_id: String,
    pub model_version: String,
    pub artifact_digest: String,
    pub research: ResearchEvidence,
    pub slices: Vec<CalibrationSlice>,
}

impl CalibrationManifest {
    /// Seals research output with the deployment digest. Existing manifest and slice digest fields
    /// are excluded from the hash, and both slice lists use canonical key order.
    pub fn seal(mut self) -> Result<Self, ScalpingError> {
        let digest = manifest_digest(&self)?;
        self.artifact_digest.clone_from(&digest);
        for slice in &mut self.slices {
            slice.artifact_digest.clone_from(&digest);
        }
        Ok(self)
    }
}

/// Validated read-only lookup for one deployment-bound calibration release.
#[derive(Clone, Debug)]
pub struct CalibrationBook {
    manifest: CalibrationManifest,
    binding: StrategyBinding,
    params: ScalpingParams,
    slices: BTreeMap<CalibrationKey, CalibrationSlice>,
}

impl CalibrationBook {
    /// Parses and verifies one immutable artifact. The binding release ID and the parameter model
    /// version/digest are the deployment pins; duplicate keys fail closed.
    pub fn from_json(
        bytes: &[u8],
        binding: &StrategyBinding,
        params: &ScalpingParams,
    ) -> Result<Self, ScalpingError> {
        binding.validate()?;
        params.validate_for(binding)?;
        let manifest: CalibrationManifest =
            serde_json::from_slice(bytes).map_err(|_| ScalpingError::Evidence)?;
        if manifest.schema_version != CALIBRATION_SCHEMA_VERSION
            || manifest.release_id != binding.parameter_release_id
            || manifest.model_version != params.calibration_model_version
            || manifest.artifact_digest != params.calibration_model_digest
            || manifest.artifact_digest != manifest_digest(&manifest)?
        {
            return Err(ScalpingError::Evidence);
        }
        manifest.research.validate_for(&manifest)?;
        let mut slices = BTreeMap::new();
        for slice in &manifest.slices {
            if slice.release_id != manifest.release_id
                || slice.model_version != manifest.model_version
                || slice.artifact_digest != manifest.artifact_digest
                || slices.insert(slice.key.clone(), slice.clone()).is_some()
            {
                return Err(ScalpingError::Evidence);
            }
        }
        if slices.is_empty() {
            return Err(ScalpingError::Evidence);
        }
        Ok(Self {
            manifest,
            binding: binding.clone(),
            params: params.clone(),
            slices,
        })
    }

    #[must_use]
    pub fn artifact_digest(&self) -> &str {
        &self.manifest.artifact_digest
    }

    #[must_use]
    pub fn research_dataset_digest(&self) -> &str {
        &self.manifest.research.dataset_digest
    }

    #[must_use]
    pub fn preregistration_digest(&self) -> &str {
        &self.manifest.research.preregistration_digest
    }

    pub fn get(&self, key: &CalibrationKey) -> Result<CalibrationSlice, ScalpingError> {
        self.slices.get(key).cloned().ok_or(ScalpingError::Evidence)
    }

    /// Looks up and validates only the slice named by the preparation regime and candidate's
    /// canonical symbol/expert/direction/style identity.
    pub fn lookup(
        &self,
        preparation: &CandidatePreparation,
        candidate: &SemanticIntent,
        live: bool,
    ) -> Result<CalibrationSlice, ScalpingError> {
        let key = CalibrationKey::from_candidate(preparation, candidate);
        let slice = self.get(&key)?;
        slice.validate_for(
            &self.binding,
            &self.params,
            &key,
            preparation.watermark_ms,
            live,
        )?;
        Ok(slice)
    }

    /// Projects an exact slice into root calibration evidence and its two legacy cost priors.
    /// Identity is supplied by the caller; this helper creates no current costs or risk evidence
    /// and performs no persistence.
    pub fn project_evidence(
        &self,
        preparation: &CandidatePreparation,
        candidate: &SemanticIntent,
        identity: EvidenceIdentity,
        live: bool,
    ) -> Result<CalibrationProjection, ScalpingError> {
        if !preparation
            .candidates
            .iter()
            .any(|prepared| prepared == candidate)
        {
            return Err(ScalpingError::Evidence);
        }
        let slice = self.lookup(preparation, candidate, live)?;
        let valid_until_ms = preparation
            .valid_until_ms
            .min(candidate.valid_until_ms)
            .min(slice.valid_until_ms);
        if identity.schema_version == 0
            || identity.evidence_id.trim().is_empty()
            || identity.candidate_id != candidate.intent_id
            || identity.preparation_id != preparation.preparation_id
            || identity.binding_digest != preparation.binding_digest
            || identity.frame_generation != preparation.frame_generation
            || identity.watermark_ms != preparation.watermark_ms
            || identity.producer_generation != slice.model_generation
            || identity.release_digest != self.manifest.artifact_digest
            || identity.valid_until_ms != valid_until_ms
        {
            return Err(ScalpingError::Evidence);
        }
        Ok(CalibrationProjection {
            evidence: CalibrationEvidence {
                identity,
                model_version: slice.model_version,
                fill_distribution: slice.fill_distribution,
                outcomes: slice.outcomes,
                target_pnl_bps: slice.target_pnl_bps,
                stop_pnl_bps: slice.stop_pnl_bps,
                other_pnl_bps: slice.other_pnl_bps,
                uncertainty_bps: slice.ev_sigma_bps,
            },
            cost_priors: CalibrationCostPriors {
                nonfill_cancel_cost_bps: slice.nonfill_cancel_cost_bps,
                opportunity_cost_bps: slice.opportunity_cost_bps,
            },
        })
    }
}

fn manifest_digest(manifest: &CalibrationManifest) -> Result<String, ScalpingError> {
    let mut slices = manifest.slices.clone();
    slices.sort_by(|left, right| left.key.cmp(&right.key));
    for slice in &mut slices {
        slice.artifact_digest.clear();
    }
    let mut research = manifest.research.clone();
    research
        .slices
        .sort_by(|left, right| left.key.cmp(&right.key));
    let bytes = serde_json::to_vec(&(
        manifest.schema_version,
        &manifest.release_id,
        &manifest.model_version,
        research,
        slices,
    ))
    .map_err(|_| ScalpingError::Evidence)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
