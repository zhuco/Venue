use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::{Amount, Price, Symbol},
    strategy::scalping::{
        CandidatePreparation, Direction, EntryStyle, ExposureState, ProtectionState, RiskLimit,
        SafetyProjection, SemanticIntent,
    },
};

pub const SCALPING_ENTRY_QUOTE_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingBoundRiskLimit {
    pub limit: RiskLimit,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingBoundQuoteAmount {
    pub amount: Amount,
    pub generation: u64,
}

/// Core-owned logical limits. They contain no venue quantity or order representation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingBoundLimits {
    pub risk_per_episode: ScalpingBoundRiskLimit,
    pub quote_cap: ScalpingBoundQuoteAmount,
    pub max_episode_loss: ScalpingBoundRiskLimit,
    pub worst_loss_at_quote_cap: ScalpingBoundRiskLimit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingBoundExposure {
    #[serde(with = "rust_decimal::serde::str")]
    pub value: Decimal,
    pub unit: String,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingAdmissionFacts {
    pub fact_id: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub private_snapshot_ready: bool,
    pub execution_unknown: bool,
    pub owner_conflict: bool,
    pub entry_terminal: bool,
    pub residual_protection: ScalpingBoundExposure,
    pub protection_gap: ScalpingBoundExposure,
    pub open_permission_generation: u64,
}

/// The exact private projection against which Core produced an admission fact.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingPrivateAdmission {
    pub fact_id: String,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub safety: SafetyProjection,
}

/// Identity copied from an externally persisted Core quote receipt. Validation proves equality
/// with this receipt identity; it does not establish the receipt's storage provenance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingQuoteAuthority {
    pub quote_id: String,
    pub quote_content_digest: String,
    pub quote_release_digest: String,
    pub quote_generation: u64,
    pub capability_generation: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_funding_abs_bps: Decimal,
    pub max_private_stale_ms: u64,
}

/// Read-only Core quote. It deliberately excludes quantity, order identity and native fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingEntryQuote {
    pub schema_version: u16,
    pub quote_id: String,
    pub quote_release_digest: String,
    pub binding_digest: String,
    pub symbol: Symbol,
    pub direction: Direction,
    pub entry_style: EntryStyle,
    pub target_quote: Amount,
    pub bound_limits_generation: u64,
    pub generation: u64,
    pub capability_generation: u64,
    pub valid_until_ms: u64,
    pub admission: ScalpingAdmissionFacts,
    #[serde(with = "rust_decimal::serde::str")]
    pub maker_fee_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub taker_fee_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub spread_cross_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub entry_slippage_impact_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub urgent_exit_spread_cross_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub urgent_exit_slippage_impact_bps: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub funding_bps: Decimal,
    pub price_tick: Price,
    pub max_executable_price: Price,
    pub worst_loss: ScalpingBoundRiskLimit,
}

pub fn scalping_entry_quote_digest(
    quote: &ScalpingEntryQuote,
) -> Result<String, ScalpingEntryQuoteError> {
    let encoded = serde_json::to_vec(quote).map_err(|_| ScalpingEntryQuoteError::Encoding)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub fn validate_scalping_bound_limits(
    candidate: &SemanticIntent,
    limits: &ScalpingBoundLimits,
) -> Result<(), ScalpingEntryQuoteError> {
    let generation = limits.risk_per_episode.generation;
    let common_generation = generation > 0
        && limits.quote_cap.generation == generation
        && limits.max_episode_loss.generation == generation
        && limits.worst_loss_at_quote_cap.generation == generation;
    let risk_unit = &limits.risk_per_episode.limit.unit;
    let common_risk_unit = limits.max_episode_loss.limit.unit == *risk_unit
        && limits.worst_loss_at_quote_cap.limit.unit == *risk_unit;
    let quote_not_expanded = limits.quote_cap.amount.asset == candidate.target_quote.asset
        && limits.quote_cap.amount.asset == candidate.risk_plan.quote_cap.asset
        && limits.quote_cap.amount.value <= candidate.target_quote.value
        && limits.quote_cap.amount.value <= candidate.risk_plan.quote_cap.value;
    let risk_not_expanded = *risk_unit == candidate.risk_plan.risk_per_episode.unit
        && limits.risk_per_episode.limit.value <= candidate.risk_plan.risk_per_episode.value
        && limits.max_episode_loss.limit.unit == candidate.risk_plan.max_episode_loss.unit
        && limits.max_episode_loss.limit.value <= candidate.risk_plan.max_episode_loss.value;
    if !common_generation
        || !common_risk_unit
        || !quote_not_expanded
        || !risk_not_expanded
        || limits.risk_per_episode.limit.value <= Decimal::ZERO
        || limits.quote_cap.amount.value <= Decimal::ZERO
        || limits.max_episode_loss.limit.value <= Decimal::ZERO
        || limits.worst_loss_at_quote_cap.limit.value < Decimal::ZERO
        || limits.worst_loss_at_quote_cap.limit.value > limits.risk_per_episode.limit.value
        || limits.worst_loss_at_quote_cap.limit.value > limits.max_episode_loss.limit.value
    {
        return Err(ScalpingEntryQuoteError::BoundLimits);
    }
    Ok(())
}

pub fn validate_scalping_entry_quote(
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    limits: &ScalpingBoundLimits,
    private: &ScalpingPrivateAdmission,
    authority: &ScalpingQuoteAuthority,
    quote: &ScalpingEntryQuote,
    observed_at_ms: u64,
) -> Result<(), ScalpingEntryQuoteError> {
    validate_scalping_bound_limits(candidate, limits)?;
    if preparation.preparation_id.trim().is_empty()
        || preparation.binding_digest.trim().is_empty()
        || preparation.frame_generation == 0
        || preparation.watermark_ms == 0
        || preparation.authority_generation == 0
        || candidate.intent_id.trim().is_empty()
        || !preparation.candidates.iter().any(|item| item == candidate)
        || quote.schema_version != SCALPING_ENTRY_QUOTE_SCHEMA_VERSION
        || quote.quote_id.trim().is_empty()
        || observed_at_ms == 0
        || quote.valid_until_ms == 0
        || authority.quote_id.trim().is_empty()
        || !digest_is_valid(&authority.quote_content_digest)
        || authority.quote_generation == 0
        || authority.capability_generation == 0
        || authority.max_funding_abs_bps < Decimal::ZERO
        || authority.max_private_stale_ms == 0
        || !digest_is_valid(&authority.quote_release_digest)
        || !digest_is_valid(&quote.quote_release_digest)
        || quote.quote_id != authority.quote_id
        || scalping_entry_quote_digest(quote)? != authority.quote_content_digest
        || quote.quote_release_digest != authority.quote_release_digest
        || quote.binding_digest != preparation.binding_digest
        || quote.symbol != candidate.symbol
        || quote.direction != candidate.direction
        || quote.entry_style != candidate.entry_style
        || quote.target_quote != limits.quote_cap.amount
        || quote.bound_limits_generation != limits.risk_per_episode.generation
        || quote.generation != authority.quote_generation
        || quote.capability_generation != authority.capability_generation
        || quote.valid_until_ms < observed_at_ms
        || observed_at_ms < preparation.watermark_ms
        || private.observed_at_ms < preparation.watermark_ms
        || private.observed_at_ms > observed_at_ms
        || observed_at_ms.saturating_sub(private.observed_at_ms) > authority.max_private_stale_ms
        || preparation.valid_until_ms < observed_at_ms
        || candidate.valid_until_ms < observed_at_ms
    {
        return Err(ScalpingEntryQuoteError::Identity);
    }
    validate_private(private, quote)?;
    validate_costs_and_price(candidate, authority, quote)?;
    let worst_loss = &quote.worst_loss;
    if worst_loss.generation != limits.risk_per_episode.generation
        || worst_loss.limit.unit != limits.risk_per_episode.limit.unit
        || worst_loss.limit.value <= Decimal::ZERO
        || worst_loss.limit.value > limits.risk_per_episode.limit.value
        || worst_loss.limit.value > limits.max_episode_loss.limit.value
        || worst_loss.limit.value > candidate.risk_plan.risk_per_episode.value
        || worst_loss.limit.value > candidate.risk_plan.max_episode_loss.value
    {
        return Err(ScalpingEntryQuoteError::WorstLoss);
    }
    Ok(())
}

fn validate_private(
    private: &ScalpingPrivateAdmission,
    quote: &ScalpingEntryQuote,
) -> Result<(), ScalpingEntryQuoteError> {
    let facts = &quote.admission;
    let safe = &private.safety;
    let exposure = &facts.residual_protection;
    let gap = &facts.protection_gap;
    if private.fact_id.trim().is_empty()
        || private.generation == 0
        || private.observed_at_ms == 0
        || facts.fact_id != private.fact_id
        || facts.generation != private.generation
        || facts.observed_at_ms != private.observed_at_ms
        || quote.capability_generation != private.generation
        || facts.open_permission_generation != private.generation
        || !safe.private_snapshot_ready
        || safe.exposure != ExposureState::Flat
        || safe.execution_unknown
        || safe.protection != ProtectionState::Complete
        || safe.owner_conflict
        || !safe.risk_budget_available
        || facts.private_snapshot_ready != safe.private_snapshot_ready
        || facts.execution_unknown != safe.execution_unknown
        || facts.owner_conflict != safe.owner_conflict
        || !facts.entry_terminal
        || exposure.value != Decimal::ZERO
        || gap.value != Decimal::ZERO
        || exposure.generation == 0
        || exposure.generation != facts.generation
        || gap.generation != exposure.generation
        || exposure.unit.trim().is_empty()
        || gap.unit != exposure.unit
    {
        return Err(ScalpingEntryQuoteError::Admission);
    }
    Ok(())
}

fn validate_costs_and_price(
    candidate: &SemanticIntent,
    authority: &ScalpingQuoteAuthority,
    quote: &ScalpingEntryQuote,
) -> Result<(), ScalpingEntryQuoteError> {
    if candidate.max_slippage_bps <= Decimal::ZERO
        || candidate.max_slippage_bps >= Decimal::new(10_000, 0)
        || quote.funding_bps.abs() > authority.max_funding_abs_bps
        || [
            quote.maker_fee_bps,
            quote.taker_fee_bps,
            quote.spread_cross_bps,
            quote.entry_slippage_impact_bps,
            quote.urgent_exit_spread_cross_bps,
            quote.urgent_exit_slippage_impact_bps,
        ]
        .iter()
        .any(|value| *value < Decimal::ZERO)
        || quote.max_executable_price.value() % quote.price_tick.value() != Decimal::ZERO
    {
        return Err(ScalpingEntryQuoteError::CostsOrPrice);
    }
    let reference = candidate.reference_price.value();
    let tick = quote.price_tick.value();
    let slippage = candidate.max_slippage_bps / Decimal::new(10_000, 0);
    let within_boundary = match candidate.entry_style {
        EntryStyle::PassiveMaker => quote.max_executable_price == candidate.reference_price,
        EntryStyle::MarketableLimit => match candidate.direction {
            Direction::Long => {
                let boundary = (reference * (Decimal::ONE + slippage) / tick).floor() * tick;
                quote.max_executable_price.value() <= boundary
            }
            Direction::Short => {
                let boundary = (reference * (Decimal::ONE - slippage) / tick).ceil() * tick;
                boundary > Decimal::ZERO && quote.max_executable_price.value() >= boundary
            }
        },
    };
    if !within_boundary {
        return Err(ScalpingEntryQuoteError::CostsOrPrice);
    }
    Ok(())
}

fn digest_is_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ScalpingEntryQuoteError {
    #[error("Core-bound scalping limits are invalid or expand the semantic risk plan")]
    BoundLimits,
    #[error("scalping quote identity, generation, binding, or TTL is invalid")]
    Identity,
    #[error("scalping quote admission is not the exact safe private fact")]
    Admission,
    #[error("scalping quote cost or executable price is invalid")]
    CostsOrPrice,
    #[error("scalping quote worst loss is invalid for its bound risk plan")]
    WorstLoss,
    #[error("scalping quote content identity could not be encoded")]
    Encoding,
}
