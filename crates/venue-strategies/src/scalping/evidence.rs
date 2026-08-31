use rust_decimal::Decimal;

use super::{CandidateCosts, CandidateEvidence, FillSlice, OutcomeProbabilities, ScalpingError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidenceScore {
    pub fill_probability: Decimal,
    pub outcome_expected_value_bps: Decimal,
    pub net_expected_value_bps: Decimal,
}

pub fn robust_value(
    evidence: &CandidateEvidence,
    uncertainty_multiplier: Decimal,
) -> Result<Decimal, ScalpingError> {
    if uncertainty_multiplier <= Decimal::ZERO || evidence.uncertainty_bps < Decimal::ZERO {
        return Err(ScalpingError::Evidence);
    }
    let score = score_candidate_evidence(
        &evidence.fill_distribution,
        &evidence.outcomes,
        evidence.target_pnl_bps,
        evidence.stop_pnl_bps,
        evidence.other_pnl_bps,
        &evidence.costs,
    )?;
    if evidence.fill_probability != score.fill_probability
        || evidence.outcome_expected_value_bps != score.outcome_expected_value_bps
        || evidence.net_expected_value_bps != score.net_expected_value_bps
    {
        return Err(ScalpingError::Evidence);
    }
    Ok(score.net_expected_value_bps - uncertainty_multiplier * evidence.uncertainty_bps)
}

pub fn score_candidate_evidence(
    fill_distribution: &[FillSlice],
    outcomes: &OutcomeProbabilities,
    target_pnl_bps: Decimal,
    stop_pnl_bps: Decimal,
    other_pnl_bps: Decimal,
    costs: &CandidateCosts,
) -> Result<EvidenceScore, ScalpingError> {
    let fill_mass = fill_distribution
        .iter()
        .map(|slice| slice.probability)
        .sum::<Decimal>();
    let outcome_mass = outcomes.target + outcomes.stop + outcomes.other;
    if fill_distribution.is_empty()
        || fill_mass != Decimal::ONE
        || !fill_distribution
            .iter()
            .any(|slice| slice.fill_ratio > Decimal::ZERO)
        || fill_distribution.iter().any(|slice| {
            slice.fill_ratio < Decimal::ZERO
                || slice.fill_ratio > Decimal::ONE
                || slice.probability < Decimal::ZERO
                || slice.probability > Decimal::ONE
        })
        || outcome_mass != Decimal::ONE
        || [outcomes.target, outcomes.stop, outcomes.other]
            .iter()
            .any(|probability| *probability < Decimal::ZERO || *probability > Decimal::ONE)
        || target_pnl_bps <= Decimal::ZERO
        || stop_pnl_bps >= Decimal::ZERO
        || [
            costs.entry_cost_bps,
            costs.exit_cost_bps,
            costs.nonfill_cost_bps,
            costs.opportunity_cost_bps,
        ]
        .iter()
        .any(|cost| *cost < Decimal::ZERO)
    {
        return Err(ScalpingError::Evidence);
    }
    let outcome_expected_value_bps = outcomes.target * target_pnl_bps
        + outcomes.stop * stop_pnl_bps
        + outcomes.other * other_pnl_bps;
    let filled_expected_value_bps = outcome_expected_value_bps
        - costs.entry_cost_bps
        - costs.exit_cost_bps
        - costs.funding_cost_bps;
    let fill_probability = fill_distribution
        .iter()
        .filter(|slice| slice.fill_ratio > Decimal::ZERO)
        .map(|slice| slice.probability)
        .sum();
    let net_expected_value_bps = fill_distribution
        .iter()
        .map(|slice| slice.fill_ratio * slice.probability * filled_expected_value_bps)
        .sum::<Decimal>()
        - costs.nonfill_cost_bps
        - costs.opportunity_cost_bps;
    Ok(EvidenceScore {
        fill_probability,
        outcome_expected_value_bps,
        net_expected_value_bps,
    })
}
