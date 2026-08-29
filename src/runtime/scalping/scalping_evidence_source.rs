use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use crate::{
    storage::{ScalpingEvidenceError, ScalpingEvidenceJournal},
    strategy::scalping::{
        CandidateEvidence, CandidateEvidenceBundle, CandidatePreparation, RiskRevaluation,
        ScalpingError, join_candidate_evidence, risk_revaluation_digest,
    },
};

/// Immutable, read-only view of externally produced Shadow evidence. Opening the source verifies
/// the append-only journal once; it never manufactures calibration, costs, or risk evidence.
#[derive(Debug)]
pub struct ScalpingEvidenceSource {
    by_preparation: BTreeMap<String, Vec<CandidateEvidenceBundle>>,
}

impl ScalpingEvidenceSource {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ScalpingEvidenceSourceError> {
        let journal = ScalpingEvidenceJournal::open(path)?;
        Self::from_journal(&journal)
    }

    pub fn from_journal(
        journal: &ScalpingEvidenceJournal,
    ) -> Result<Self, ScalpingEvidenceSourceError> {
        let mut by_preparation = BTreeMap::<String, Vec<CandidateEvidenceBundle>>::new();
        for record in journal.recover()? {
            validate_bundle_identity(&record.bundle)?;
            by_preparation
                .entry(record.bundle.calibration.identity.preparation_id.clone())
                .or_default()
                .push(record.bundle);
        }
        validate_bundle_ambiguity(&by_preparation)?;
        Ok(Self { by_preparation })
    }

    /// Joins only evidence bound to the exact prepared candidate and complete logical-risk proof.
    /// Missing evidence returns an empty/partial set so the strategy remains fenced. Two matching
    /// bundles for one candidate are ambiguous and therefore reject the whole turn.
    pub fn join(
        &self,
        preparation: &CandidatePreparation,
        risk_revaluation: &RiskRevaluation,
        observed_at_ms: u64,
    ) -> Result<Vec<CandidateEvidence>, ScalpingEvidenceSourceError> {
        if observed_at_ms == 0 || preparation.preparation_id.trim().is_empty() {
            return Err(ScalpingEvidenceSourceError::Identity);
        }
        let risk_digest = risk_revaluation_digest(risk_revaluation)?;
        let Some(bundles) = self.by_preparation.get(&preparation.preparation_id) else {
            return Ok(Vec::new());
        };

        let mut joined = Vec::new();
        for candidate in &preparation.candidates {
            let mut matching = bundles.iter().filter(|bundle| {
                bundle.calibration.identity.candidate_id == candidate.intent_id
                    && bundle.risk.identity.release_digest == risk_digest
                    && bundle.risk.identity.producer_generation
                        == risk_revaluation.target_generation
            });
            let Some(bundle) = matching.next() else {
                continue;
            };
            if matching.next().is_some() {
                return Err(ScalpingEvidenceSourceError::Ambiguous);
            }
            joined.push(join_candidate_evidence(
                preparation,
                candidate,
                bundle,
                observed_at_ms,
            )?);
        }

        if bundles.iter().any(|bundle| {
            bundle.risk.identity.release_digest == risk_digest
                && bundle.risk.identity.producer_generation == risk_revaluation.target_generation
                && !preparation.candidates.iter().any(|candidate| {
                    candidate.intent_id == bundle.calibration.identity.candidate_id
                })
        }) {
            return Err(ScalpingEvidenceSourceError::Identity);
        }
        Ok(joined)
    }

    #[must_use]
    pub fn is_retryable_error(error: &ScalpingEvidenceSourceError) -> bool {
        matches!(
            error,
            ScalpingEvidenceSourceError::Journal(ScalpingEvidenceError::Io { .. })
                | ScalpingEvidenceSourceError::Strategy(_)
        )
    }
}

fn validate_bundle_ambiguity(
    by_preparation: &BTreeMap<String, Vec<CandidateEvidenceBundle>>,
) -> Result<(), ScalpingEvidenceSourceError> {
    let mut identities = BTreeSet::new();
    for (preparation_id, bundles) in by_preparation {
        for bundle in bundles {
            let identity = (
                preparation_id.clone(),
                bundle.calibration.identity.candidate_id.clone(),
                bundle.risk.identity.release_digest.clone(),
                bundle.risk.identity.producer_generation,
            );
            if !identities.insert(identity) {
                return Err(ScalpingEvidenceSourceError::Ambiguous);
            }
        }
    }
    Ok(())
}

fn validate_bundle_identity(
    bundle: &CandidateEvidenceBundle,
) -> Result<(), ScalpingEvidenceSourceError> {
    let expected = &bundle.calibration.identity;
    if expected.schema_version == 0
        || expected.candidate_id.trim().is_empty()
        || expected.preparation_id.trim().is_empty()
        || expected.binding_digest.trim().is_empty()
        || expected.frame_generation == 0
        || expected.watermark_ms == 0
        || expected.valid_until_ms == 0
    {
        return Err(ScalpingEvidenceSourceError::Identity);
    }
    for identity in [&bundle.costs.identity, &bundle.risk.identity] {
        if identity.schema_version != expected.schema_version
            || identity.candidate_id != expected.candidate_id
            || identity.preparation_id != expected.preparation_id
            || identity.binding_digest != expected.binding_digest
            || identity.frame_generation != expected.frame_generation
            || identity.watermark_ms != expected.watermark_ms
            || identity.valid_until_ms == 0
        {
            return Err(ScalpingEvidenceSourceError::Identity);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingEvidenceSourceError {
    #[error("resident evidence journal failed: {0}")]
    Journal(#[from] ScalpingEvidenceError),
    #[error("resident evidence bundle identity is incomplete or cross-bound")]
    Identity,
    #[error("resident evidence has multiple projections for the same candidate and risk proof")]
    Ambiguous,
    #[error("resident evidence join rejected an incompatible projection: {0}")]
    Strategy(#[from] ScalpingError),
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        domain::{Amount, Asset, Price},
        storage::ScalpingEvidenceJournal,
        strategy::scalping::{
            CalibrationEvidence, CostEvidence, Direction, EntryStyle, EvidenceIdentity,
            ExitTemplate, Expert, FillSlice, MarketRegime, OutcomeProbabilities, RiskEvidence,
            RiskLimit, RiskPlan, RiskUnit, SemanticIntent, SemanticPurpose,
        },
    };

    use super::*;

    fn preparation() -> Result<CandidatePreparation, Box<dyn std::error::Error>> {
        let unit = RiskUnit::new("risk")?;
        let quote = Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0));
        let candidate = SemanticIntent {
            intent_id: "candidate-1".to_owned(),
            symbol: "SOL/USDT".parse()?,
            direction: Direction::Long,
            purpose: SemanticPurpose::Entry,
            expert: Expert::RangeFade,
            entry_style: EntryStyle::PassiveMaker,
            exit_template: ExitTemplate::FairValue,
            attempt_cap: 1,
            max_reprices: 1,
            risk_plan: RiskPlan {
                risk_per_episode: RiskLimit::new(unit.clone(), Decimal::ONE),
                quote_cap: quote.clone(),
                max_episode_loss: RiskLimit::new(unit, Decimal::ONE),
            },
            target_quote: quote,
            reference_price: Price::new(Decimal::new(100, 0))?,
            max_slippage_bps: Decimal::ONE,
            valid_until_ms: 200,
            entry_ttl_ms: 1_000,
            hard_stop_distance_bps: Decimal::ONE,
            target_distance_bps: Decimal::ONE,
            max_hold_ms: 1_000,
            max_unprotected_ms: 100,
            requires_server_protection: true,
            opportunity_key: "range".to_owned(),
            breakout_cursor: None,
            idempotency_seed: "seed".to_owned(),
        };
        Ok(CandidatePreparation {
            preparation_id: "preparation-1".to_owned(),
            binding_digest: "b".repeat(64),
            controller_revision: 1,
            authority_generation: 1,
            market_regime: MarketRegime::Range,
            frame_generation: 3,
            watermark_ms: 100,
            valid_until_ms: 200,
            candidates: vec![candidate],
        })
    }

    fn proof() -> Result<RiskRevaluation, Box<dyn std::error::Error>> {
        Ok(RiskRevaluation {
            proof_id: "proof-1".to_owned(),
            target_generation: 7,
            risk_unit: RiskUnit::new("risk")?,
            window_start_ms: 1,
            complete_through_ms: 100,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        })
    }

    fn identity(
        kind: &str,
        preparation: &CandidatePreparation,
        risk_digest: &str,
    ) -> EvidenceIdentity {
        EvidenceIdentity {
            schema_version: 1,
            evidence_id: format!("{kind}-1"),
            candidate_id: preparation.candidates[0].intent_id.clone(),
            preparation_id: preparation.preparation_id.clone(),
            binding_digest: preparation.binding_digest.clone(),
            frame_generation: preparation.frame_generation,
            watermark_ms: preparation.watermark_ms,
            producer_generation: if kind == "risk" { 7 } else { 1 },
            release_digest: if kind == "risk" {
                risk_digest.to_owned()
            } else {
                "a".repeat(64)
            },
            valid_until_ms: preparation.valid_until_ms,
        }
    }

    fn bundle(
        preparation: &CandidatePreparation,
        proof: &RiskRevaluation,
    ) -> Result<CandidateEvidenceBundle, Box<dyn std::error::Error>> {
        let risk_digest = risk_revaluation_digest(proof)?;
        Ok(CandidateEvidenceBundle {
            calibration: CalibrationEvidence {
                identity: identity("calibration", preparation, &risk_digest),
                model_version: "calibration-v1".to_owned(),
                fill_distribution: vec![FillSlice {
                    fill_ratio: Decimal::ONE,
                    probability: Decimal::ONE,
                }],
                outcomes: OutcomeProbabilities {
                    target: Decimal::ONE,
                    stop: Decimal::ZERO,
                    other: Decimal::ZERO,
                },
                target_pnl_bps: Decimal::new(10, 0),
                stop_pnl_bps: -Decimal::ONE,
                other_pnl_bps: Decimal::ZERO,
                uncertainty_bps: Decimal::ONE,
            },
            costs: CostEvidence {
                identity: identity("cost", preparation, &risk_digest),
                entry_cost_bps: Decimal::ONE,
                exit_cost_bps: Decimal::ONE,
                funding_cost_bps: Decimal::ZERO,
                nonfill_cost_bps: Decimal::ZERO,
                opportunity_cost_bps: Decimal::ZERO,
            },
            risk: RiskEvidence {
                identity: identity("risk", preparation, &risk_digest),
                policy_digest: "c".repeat(64),
                worst_loss: preparation.candidates[0].risk_plan.risk_per_episode.clone(),
                admissible: true,
            },
        })
    }

    #[test]
    fn exact_preparation_and_risk_proof_join_one_bundle() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let path = directory.path().join("evidence.jsonl");
        let preparation = preparation()?;
        let proof = proof()?;
        ScalpingEvidenceJournal::open(&path)?.append(bundle(&preparation, &proof)?)?;

        let joined = ScalpingEvidenceSource::open(path)?.join(&preparation, &proof, 100)?;
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].candidate_id, preparation.candidates[0].intent_id);
        Ok(())
    }

    #[test]
    fn absent_or_different_risk_proof_returns_no_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("evidence.jsonl");
        let preparation = preparation()?;
        let proof = proof()?;
        ScalpingEvidenceJournal::open(&path)?.append(bundle(&preparation, &proof)?)?;
        let mut other = proof.clone();
        other.proof_id = "proof-2".to_owned();

        assert!(
            ScalpingEvidenceSource::open(path)?
                .join(&preparation, &other, 100)?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn ambiguous_or_cross_bound_bundle_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("ambiguous.jsonl");
        let preparation = preparation()?;
        let proof = proof()?;
        let first = bundle(&preparation, &proof)?;
        let mut second = first.clone();
        second.calibration.identity.evidence_id = "calibration-2".to_owned();
        second.costs.identity.evidence_id = "cost-2".to_owned();
        second.risk.identity.evidence_id = "risk-2".to_owned();
        let mut journal = ScalpingEvidenceJournal::open(&path)?;
        journal.append(first)?;
        journal.append(second)?;
        assert!(matches!(
            ScalpingEvidenceSource::open(&path),
            Err(ScalpingEvidenceSourceError::Ambiguous)
        ));

        let crossed_path = directory.path().join("crossed.jsonl");
        let mut crossed = bundle(&preparation, &proof)?;
        crossed.costs.identity.preparation_id = "other-preparation".to_owned();
        ScalpingEvidenceJournal::open(&crossed_path)?.append(crossed)?;
        assert!(matches!(
            ScalpingEvidenceSource::open(crossed_path),
            Err(ScalpingEvidenceSourceError::Identity)
        ));
        Ok(())
    }
}
