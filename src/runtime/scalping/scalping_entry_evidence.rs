use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    execution::{
        ScalpingBoundLimits, ScalpingEntryQuote, ScalpingEntryQuoteError, ScalpingPrivateAdmission,
        ScalpingQuoteAuthority, validate_scalping_entry_quote,
    },
    runtime::BoundRiskRevaluation,
    strategy::scalping::{
        CalibrationProjection, CandidateEvidence, CandidateEvidenceBundle, CandidatePreparation,
        CostEvidence, EntryStyle, EvidenceIdentity, RiskEvidence, ScalpingError, SemanticIntent,
        StrategyBinding, join_candidate_evidence, risk_revaluation_digest,
    },
};

pub const SCALPING_ENTRY_EVIDENCE_SCHEMA_VERSION: u16 = 1;

/// Identity emitted by the durable applied-risk owner only after its bound host application point
/// is persistent. This projector verifies exact equality but does not recreate that persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppliedRiskReceipt {
    pub binding: StrategyBinding,
    pub proof_id: String,
    pub cursor_sequence: u64,
    pub risk_revaluation_digest: String,
    pub target_generation: u64,
    pub valuation_generation: u64,
}

/// Pure result. The caller remains responsible for persisting `bundle` before consuming
/// `candidate`; this module owns neither that journal nor a mutation path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingEntryEvidenceProjection {
    pub bundle: CandidateEvidenceBundle,
    pub candidate: CandidateEvidence,
}

#[allow(clippy::too_many_arguments)]
pub fn project_scalping_entry_evidence(
    binding: &StrategyBinding,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    calibration: &CalibrationProjection,
    limits: &ScalpingBoundLimits,
    private: &ScalpingPrivateAdmission,
    quote_authority: &ScalpingQuoteAuthority,
    quote: &ScalpingEntryQuote,
    applied_risk: &BoundRiskRevaluation,
    applied_risk_receipt: &AppliedRiskReceipt,
    observed_at_ms: u64,
) -> Result<ScalpingEntryEvidenceProjection, ScalpingEntryEvidenceError> {
    binding
        .validate()
        .map_err(|_| ScalpingEntryEvidenceError::Binding)?;
    if preparation.binding_digest != binding.digest() || candidate.symbol != binding.symbol {
        return Err(ScalpingEntryEvidenceError::Binding);
    }
    validate_scalping_entry_quote(
        preparation,
        candidate,
        limits,
        private,
        quote_authority,
        quote,
        observed_at_ms,
    )?;
    validate_calibration(preparation, candidate, calibration, observed_at_ms)?;
    validate_applied_risk(
        binding,
        preparation,
        limits,
        private,
        quote,
        applied_risk,
        applied_risk_receipt,
    )?;

    let valid_until_ms = preparation
        .valid_until_ms
        .min(candidate.valid_until_ms)
        .min(calibration.evidence.identity.valid_until_ms)
        .min(quote.valid_until_ms);
    let cost_identity = evidence_identity(
        "cost",
        preparation,
        candidate,
        quote.generation,
        &quote.quote_release_digest,
        valid_until_ms,
        &quote.quote_id,
    );
    let risk_identity = evidence_identity(
        "risk",
        preparation,
        candidate,
        applied_risk.proof.target_generation,
        &applied_risk_receipt.risk_revaluation_digest,
        valid_until_ms,
        &applied_risk.proof.proof_id,
    );
    let entry_cost_bps = match candidate.entry_style {
        EntryStyle::PassiveMaker => quote.maker_fee_bps + quote.entry_slippage_impact_bps,
        EntryStyle::MarketableLimit => {
            quote.taker_fee_bps + quote.spread_cross_bps + quote.entry_slippage_impact_bps
        }
    };
    let exit_cost_bps = quote.taker_fee_bps
        + quote.urgent_exit_spread_cross_bps
        + quote.urgent_exit_slippage_impact_bps;
    let costs = CostEvidence {
        identity: cost_identity,
        entry_cost_bps,
        exit_cost_bps,
        funding_cost_bps: quote.funding_bps,
        nonfill_cost_bps: calibration.cost_priors.nonfill_cancel_cost_bps,
        opportunity_cost_bps: calibration.cost_priors.opportunity_cost_bps,
    };
    let risk = RiskEvidence {
        identity: risk_identity,
        policy_digest: applied_risk_receipt.risk_revaluation_digest.clone(),
        worst_loss: quote.worst_loss.limit.clone(),
        admissible: true,
    };
    let bundle = CandidateEvidenceBundle {
        calibration: calibration.evidence.clone(),
        costs,
        risk,
    };
    let projected = join_candidate_evidence(preparation, candidate, &bundle, observed_at_ms)?;
    Ok(ScalpingEntryEvidenceProjection {
        bundle,
        candidate: projected,
    })
}

fn validate_calibration(
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    calibration: &CalibrationProjection,
    observed_at_ms: u64,
) -> Result<(), ScalpingEntryEvidenceError> {
    let identity = &calibration.evidence.identity;
    if identity.schema_version == 0
        || identity.evidence_id.trim().is_empty()
        || identity.candidate_id != candidate.intent_id
        || identity.preparation_id != preparation.preparation_id
        || identity.binding_digest != preparation.binding_digest
        || identity.frame_generation != preparation.frame_generation
        || identity.watermark_ms != preparation.watermark_ms
        || identity.producer_generation == 0
        || !digest_is_valid(&identity.release_digest)
        || identity.valid_until_ms < observed_at_ms
        || calibration.cost_priors.nonfill_cancel_cost_bps < Decimal::ZERO
        || calibration.cost_priors.opportunity_cost_bps < Decimal::ZERO
    {
        return Err(ScalpingEntryEvidenceError::Calibration);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_applied_risk(
    binding: &StrategyBinding,
    preparation: &CandidatePreparation,
    limits: &ScalpingBoundLimits,
    private: &ScalpingPrivateAdmission,
    quote: &ScalpingEntryQuote,
    applied: &BoundRiskRevaluation,
    receipt: &AppliedRiskReceipt,
) -> Result<(), ScalpingEntryEvidenceError> {
    let proof = &applied.proof;
    let source = &applied.binding;
    let actual_digest = risk_revaluation_digest(proof)?;
    if receipt.binding != *binding
        || receipt.proof_id.trim().is_empty()
        || receipt.proof_id != proof.proof_id
        || receipt.cursor_sequence == 0
        || receipt.cursor_sequence != applied.cursor_sequence
        || !digest_is_valid(&receipt.risk_revaluation_digest)
        || actual_digest != receipt.risk_revaluation_digest
        || receipt.target_generation != proof.target_generation
        || receipt.valuation_generation != source.valuation_generation
        || receipt.target_generation != receipt.valuation_generation
        || applied.cursor_sequence == 0
        || source.exchange != binding.exchange
        || source.account != binding.account
        || source.owner_scope != binding.owner_scope
        || source.strategy_instance_id != binding.strategy_instance_id
        || source.run_id != binding.run_id
        || source.parameter_release_id != binding.parameter_release_id
        || source.symbol != binding.symbol
        || source.risk_unit != limits.risk_per_episode.limit.unit
        || source.valuation_generation != proof.target_generation
        || proof.proof_id.trim().is_empty()
        || proof.target_generation == 0
        || proof.target_generation != quote.worst_loss.generation
        || proof.risk_unit != quote.worst_loss.limit.unit
        || proof.window_start_ms == 0
        || proof.window_start_ms > proof.complete_through_ms
        || proof.complete_through_ms < preparation.watermark_ms
        || proof.complete_through_ms < private.observed_at_ms
    {
        return Err(ScalpingEntryEvidenceError::RiskProof);
    }
    Ok(())
}

fn evidence_identity(
    owner: &str,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    producer_generation: u64,
    release_digest: &str,
    valid_until_ms: u64,
    source_id: &str,
) -> EvidenceIdentity {
    let mut digest = Sha256::new();
    for field in [
        owner.as_bytes(),
        source_id.as_bytes(),
        preparation.preparation_id.as_bytes(),
        candidate.intent_id.as_bytes(),
        preparation.binding_digest.as_bytes(),
        release_digest.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    for value in [
        preparation.frame_generation,
        preparation.watermark_ms,
        producer_generation,
        valid_until_ms,
    ] {
        digest.update(value.to_be_bytes());
    }
    EvidenceIdentity {
        schema_version: SCALPING_ENTRY_EVIDENCE_SCHEMA_VERSION,
        evidence_id: format!("scalping-{owner}-{:x}", digest.finalize()),
        candidate_id: candidate.intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        producer_generation,
        release_digest: release_digest.to_owned(),
        valid_until_ms,
    }
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingEntryEvidenceError {
    #[error("scalping entry evidence binding is invalid")]
    Binding,
    #[error("scalping calibration projection is not bound to the exact candidate")]
    Calibration,
    #[error("scalping risk proof is not the exact applied bound revaluation")]
    RiskProof,
    #[error("scalping quote validation failed: {0}")]
    Quote(#[from] ScalpingEntryQuoteError),
    #[error("scalping candidate evidence projection failed: {0}")]
    Evidence(#[from] ScalpingError),
}
