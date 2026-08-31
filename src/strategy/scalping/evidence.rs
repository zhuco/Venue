use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::strategy::scalping::{
    CandidateCosts, CandidateEvidence, CandidatePreparation, FillSlice, OutcomeProbabilities,
    RiskLimit, RiskRevaluation, ScalpingError, SemanticIntent,
};

/// Identity shared by one independently generated evidence projection. It binds an observation to
/// the exact prepared candidate, without importing an execution or venue type into strategy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvidenceIdentity {
    pub schema_version: u16,
    pub evidence_id: String,
    pub candidate_id: String,
    pub preparation_id: String,
    pub binding_digest: String,
    pub frame_generation: u64,
    pub watermark_ms: u64,
    pub producer_generation: u64,
    pub release_digest: String,
    pub valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CalibrationEvidence {
    pub identity: EvidenceIdentity,
    pub model_version: String,
    pub fill_distribution: Vec<FillSlice>,
    pub outcomes: OutcomeProbabilities,
    #[serde(with = "rust_decimal::serde::str")]
    pub target_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub stop_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub other_pnl_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub uncertainty_bps: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CostEvidence {
    pub identity: EvidenceIdentity,
    #[serde(with = "rust_decimal::serde::str")]
    pub entry_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub exit_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub nonfill_cost_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub opportunity_cost_bps: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskEvidence {
    pub identity: EvidenceIdentity,
    pub policy_digest: String,
    pub worst_loss: RiskLimit,
    pub admissible: bool,
}

/// The three evidence owners remain separate until this explicit pure join at the strategy
/// boundary. A missing bundle is not representable as a zero-cost or zero-risk substitution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CandidateEvidenceBundle {
    pub calibration: CalibrationEvidence,
    pub costs: CostEvidence,
    pub risk: RiskEvidence,
}

/// Stable content identity used to bind candidate risk evidence to the exact complete logical
/// risk revaluation admitted by the Shadow host. This is an audit digest, not an authorization.
pub fn risk_revaluation_digest(proof: &RiskRevaluation) -> Result<String, ScalpingError> {
    let encoded = serde_json::to_vec(proof).map_err(|_| ScalpingError::Evidence)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub fn join_candidate_evidence(
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    bundle: &CandidateEvidenceBundle,
    observed_at_ms: u64,
) -> Result<CandidateEvidence, ScalpingError> {
    for identity in [
        &bundle.calibration.identity,
        &bundle.costs.identity,
        &bundle.risk.identity,
    ] {
        validate_identity(identity, preparation, candidate, observed_at_ms)?;
    }
    if bundle.calibration.model_version.trim().is_empty()
        || !digest_is_valid(&bundle.risk.policy_digest)
        || !candidate
            .risk_plan
            .admits_worst_loss(&bundle.risk.worst_loss)
        || bundle.calibration.uncertainty_bps < Decimal::ZERO
    {
        return Err(ScalpingError::Evidence);
    }
    let costs = CandidateCosts {
        entry_cost_bps: bundle.costs.entry_cost_bps,
        exit_cost_bps: bundle.costs.exit_cost_bps,
        funding_cost_bps: bundle.costs.funding_cost_bps,
        nonfill_cost_bps: bundle.costs.nonfill_cost_bps,
        opportunity_cost_bps: bundle.costs.opportunity_cost_bps,
    };
    let score = venue_strategies::scalping::score_candidate_evidence(
        &bundle.calibration.fill_distribution,
        &bundle.calibration.outcomes,
        bundle.calibration.target_pnl_bps,
        bundle.calibration.stop_pnl_bps,
        bundle.calibration.other_pnl_bps,
        &costs,
    )?;
    Ok(CandidateEvidence {
        candidate_id: candidate.intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        valid_until_ms: preparation
            .valid_until_ms
            .min(candidate.valid_until_ms)
            .min(bundle.calibration.identity.valid_until_ms)
            .min(bundle.costs.identity.valid_until_ms)
            .min(bundle.risk.identity.valid_until_ms),
        calibration_model_version: bundle.calibration.model_version.clone(),
        calibration_digest: bundle.calibration.identity.release_digest.clone(),
        cost_digest: bundle.costs.identity.release_digest.clone(),
        risk_digest: bundle.risk.identity.release_digest.clone(),
        worst_loss: bundle.risk.worst_loss.clone(),
        fill_probability: score.fill_probability,
        fill_distribution: bundle.calibration.fill_distribution.clone(),
        outcomes: bundle.calibration.outcomes.clone(),
        costs,
        target_pnl_bps: bundle.calibration.target_pnl_bps,
        stop_pnl_bps: bundle.calibration.stop_pnl_bps,
        other_pnl_bps: bundle.calibration.other_pnl_bps,
        outcome_expected_value_bps: score.outcome_expected_value_bps,
        net_expected_value_bps: score.net_expected_value_bps,
        uncertainty_bps: bundle.calibration.uncertainty_bps,
        admissible: bundle.risk.admissible,
    })
}

fn validate_identity(
    identity: &EvidenceIdentity,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    observed_at_ms: u64,
) -> Result<(), ScalpingError> {
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
    {
        return Err(ScalpingError::Evidence);
    }
    Ok(())
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::{
        domain::{Amount, Asset, Price},
        strategy::scalping::{
            Direction, EntryStyle, ExitTemplate, Expert, RiskLimit, RiskPlan, RiskUnit,
            SemanticPurpose, StrategyBinding,
        },
    };

    use super::*;

    fn preparation() -> Result<(CandidatePreparation, SemanticIntent), Box<dyn std::error::Error>> {
        let candidate = SemanticIntent {
            intent_id: "candidate-1".to_owned(),
            symbol: "BTC/USDT".parse()?,
            direction: Direction::Long,
            purpose: SemanticPurpose::Entry,
            expert: Expert::RangeFade,
            entry_style: EntryStyle::PassiveMaker,
            exit_template: ExitTemplate::FairValue,
            attempt_cap: 1,
            max_reprices: 1,
            risk_plan: RiskPlan {
                risk_per_episode: RiskLimit::new(RiskUnit::new("risk")?, Decimal::ONE),
                quote_cap: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
                max_episode_loss: RiskLimit::new(RiskUnit::new("risk")?, Decimal::new(5, 0)),
            },
            target_quote: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
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
        let binding = StrategyBinding {
            strategy_kind: crate::strategy::scalping::StrategyKind::Scalping,
            strategy_instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: candidate.symbol.clone(),
            parameter_release_id: "release".to_owned(),
            owner_scope: "owner".to_owned(),
            risk_budget: candidate.target_quote.clone(),
        };
        Ok((
            CandidatePreparation {
                preparation_id: "preparation-1".to_owned(),
                binding_digest: venue_strategies::scalping::digest_strategy_binding(&binding),
                controller_revision: 1,
                authority_generation: 1,
                market_regime: crate::strategy::scalping::MarketRegime::Range,
                frame_generation: 3,
                watermark_ms: 100,
                valid_until_ms: 200,
                candidates: vec![candidate.clone()],
            },
            candidate,
        ))
    }

    fn identity(
        kind: &str,
        preparation: &CandidatePreparation,
        candidate: &SemanticIntent,
    ) -> EvidenceIdentity {
        EvidenceIdentity {
            schema_version: 1,
            evidence_id: format!("{kind}-1"),
            candidate_id: candidate.intent_id.clone(),
            preparation_id: preparation.preparation_id.clone(),
            binding_digest: preparation.binding_digest.clone(),
            frame_generation: preparation.frame_generation,
            watermark_ms: preparation.watermark_ms,
            producer_generation: 1,
            release_digest: "a".repeat(64),
            valid_until_ms: 150,
        }
    }

    fn bundle(
        preparation: &CandidatePreparation,
        candidate: &SemanticIntent,
    ) -> CandidateEvidenceBundle {
        CandidateEvidenceBundle {
            calibration: CalibrationEvidence {
                identity: identity("calibration", preparation, candidate),
                model_version: "scalping-shadow-calibration-v1".to_owned(),
                fill_distribution: vec![
                    FillSlice {
                        fill_ratio: Decimal::ZERO,
                        probability: Decimal::new(2, 1),
                    },
                    FillSlice {
                        fill_ratio: Decimal::ONE,
                        probability: Decimal::new(8, 1),
                    },
                ],
                outcomes: OutcomeProbabilities {
                    target: Decimal::ONE,
                    stop: Decimal::ZERO,
                    other: Decimal::ZERO,
                },
                target_pnl_bps: Decimal::new(10, 0),
                stop_pnl_bps: Decimal::new(-1, 0),
                other_pnl_bps: Decimal::ZERO,
                uncertainty_bps: Decimal::ONE,
            },
            costs: CostEvidence {
                identity: identity("cost", preparation, candidate),
                entry_cost_bps: Decimal::ONE,
                exit_cost_bps: Decimal::ONE,
                funding_cost_bps: Decimal::ZERO,
                nonfill_cost_bps: Decimal::ONE,
                opportunity_cost_bps: Decimal::ONE,
            },
            risk: RiskEvidence {
                identity: identity("risk", preparation, candidate),
                policy_digest: "b".repeat(64),
                worst_loss: candidate.risk_plan.risk_per_episode.clone(),
                admissible: true,
            },
        }
    }

    #[test]
    fn join_computes_after_cost_expected_value_and_the_earliest_ttl()
    -> Result<(), Box<dyn std::error::Error>> {
        let (preparation, candidate) = preparation()?;
        let evidence = join_candidate_evidence(
            &preparation,
            &candidate,
            &bundle(&preparation, &candidate),
            100,
        )?;
        assert_eq!(evidence.valid_until_ms, 150);
        assert_eq!(evidence.fill_probability, Decimal::new(8, 1));
        assert_eq!(evidence.net_expected_value_bps, Decimal::new(44, 1));
        Ok(())
    }

    #[test]
    fn join_rejects_worst_loss_that_exceeds_or_mixes_the_frozen_risk_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let (preparation, candidate) = preparation()?;
        let mut over_limit = bundle(&preparation, &candidate);
        over_limit.risk.worst_loss.value = Decimal::new(11, 1);
        assert!(join_candidate_evidence(&preparation, &candidate, &over_limit, 100).is_err());

        let mut wrong_unit = bundle(&preparation, &candidate);
        wrong_unit.risk.worst_loss.unit = RiskUnit::new("another-risk")?;
        assert!(join_candidate_evidence(&preparation, &candidate, &wrong_unit, 100).is_err());
        Ok(())
    }

    #[test]
    fn mismatched_or_expired_identity_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let (preparation, candidate) = preparation()?;
        let mut mismatched = bundle(&preparation, &candidate);
        mismatched.costs.identity.frame_generation = 4;
        assert!(matches!(
            join_candidate_evidence(&preparation, &candidate, &mismatched, 100),
            Err(ScalpingError::Evidence)
        ));
        let mut expired = bundle(&preparation, &candidate);
        expired.risk.identity.valid_until_ms = 99;
        assert!(matches!(
            join_candidate_evidence(&preparation, &candidate, &expired, 100),
            Err(ScalpingError::Evidence)
        ));
        Ok(())
    }
}
