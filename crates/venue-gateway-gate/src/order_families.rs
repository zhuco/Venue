use sha2::{Digest, Sha256};
use venue_gateway_api::GatewayBinding;

use crate::{
    GateContractRules, GateGatewayBinding, GateRegularOrdersReadback, collect_regular_order_pages,
};

pub const GATE_STAGE7_ORDER_PROFILE_VERSION: u64 = 1;

/// Immutable scope for one authenticated Gate Stage 7 order-family collection attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateStage7OrderFamilyScope {
    pub binding: GatewayBinding,
    pub profile_version: u64,
    pub attempt: u64,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateStage7UnsupportedOrderFamily {
    Conditional,
    Algo,
}

/// Explicit profile evidence. It is not an empty page and contains no native-order projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateStage7UnsupportedEvidence {
    pub family: GateStage7UnsupportedOrderFamily,
    pub profile_version: u64,
}

impl GateStage7UnsupportedEvidence {
    #[must_use]
    pub const fn conditional(profile_version: u64) -> Self {
        Self {
            family: GateStage7UnsupportedOrderFamily::Conditional,
            profile_version,
        }
    }

    #[must_use]
    pub const fn algo(profile_version: u64) -> Self {
        Self {
            family: GateStage7UnsupportedOrderFamily::Algo,
            profile_version,
        }
    }

    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self.family {
            GateStage7UnsupportedOrderFamily::Conditional => {
                "Gate Stage 7 regular-only profile has no conditional-order read or mutation surface"
            }
            GateStage7UnsupportedOrderFamily::Algo => {
                "Gate Stage 7 regular-only profile has no algo-order read or mutation surface"
            }
        }
    }
}

/// Exactly one item must be supplied for each canonical family. The unsupported variants cannot
/// carry pages, preventing an empty payload from being presented as negative venue evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GateStage7OrderFamilyEvidence {
    Regular(GateRegularOrdersReadback),
    Unsupported(GateStage7UnsupportedEvidence),
}

/// A fully checked readback candidate. This type deliberately exposes no capability or mutation
/// authorization method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateStage7OrderFamilyCandidate {
    scope: GateStage7OrderFamilyScope,
    regular: GateRegularOrdersReadback,
    conditional: GateStage7UnsupportedEvidence,
    algo: GateStage7UnsupportedEvidence,
    regular_payload_digest: [u8; 32],
}

impl GateStage7OrderFamilyCandidate {
    #[must_use]
    pub const fn scope(&self) -> &GateStage7OrderFamilyScope {
        &self.scope
    }

    #[must_use]
    pub const fn regular(&self) -> &GateRegularOrdersReadback {
        &self.regular
    }

    #[must_use]
    pub const fn conditional(&self) -> GateStage7UnsupportedEvidence {
        self.conditional
    }

    #[must_use]
    pub const fn algo(&self) -> GateStage7UnsupportedEvidence {
        self.algo
    }

    #[must_use]
    pub const fn regular_payload_digest(&self) -> [u8; 32] {
        self.regular_payload_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateStage7OrderFamilyError {
    #[error("Gate Stage 7 order-family binding does not match the Gate contract rules")]
    Scope,
    #[error("Gate Stage 7 order-family execution profile is unsupported")]
    Profile,
    #[error("Gate Stage 7 order-family attempt or generation is invalid")]
    Generation,
    #[error("Gate Stage 7 order-family evidence is stale, future-dated, or malformed")]
    Freshness,
    #[error("Gate Stage 7 order-family evidence is missing a canonical family")]
    MissingFamily,
    #[error("Gate Stage 7 order-family evidence repeats a canonical family")]
    DuplicateFamily,
    #[error("Gate Stage 7 regular-order projection does not exactly replay its raw pages")]
    RegularProjection,
    #[error("Gate Stage 7 unsupported-family evidence does not match the execution profile")]
    UnsupportedEvidence,
}

pub fn validate_stage7_order_families<I>(
    scope: GateStage7OrderFamilyScope,
    rules: &GateContractRules,
    validated_at_ms: u64,
    evidence: I,
) -> Result<GateStage7OrderFamilyCandidate, GateStage7OrderFamilyError>
where
    I: IntoIterator<Item = GateStage7OrderFamilyEvidence>,
{
    let binding = GateGatewayBinding::new(scope.binding.clone())
        .map_err(|_| GateStage7OrderFamilyError::Scope)?;
    if binding.gateway_binding().symbol != rules.instrument.symbol
        || rules.instrument.validate().is_err()
        || rules.native_symbol.trim().is_empty()
    {
        return Err(GateStage7OrderFamilyError::Scope);
    }
    if scope.profile_version != GATE_STAGE7_ORDER_PROFILE_VERSION {
        return Err(GateStage7OrderFamilyError::Profile);
    }
    if scope.attempt == 0 || scope.generation == 0 {
        return Err(GateStage7OrderFamilyError::Generation);
    }
    if scope.observed_at_ms == 0
        || scope.expires_at_ms <= scope.observed_at_ms
        || validated_at_ms < scope.observed_at_ms
        || validated_at_ms >= scope.expires_at_ms
    {
        return Err(GateStage7OrderFamilyError::Freshness);
    }

    let mut regular = None;
    let mut conditional = None;
    let mut algo = None;
    for item in evidence {
        match item {
            GateStage7OrderFamilyEvidence::Regular(value) => {
                if regular.replace(value).is_some() {
                    return Err(GateStage7OrderFamilyError::DuplicateFamily);
                }
            }
            GateStage7OrderFamilyEvidence::Unsupported(value) => {
                if value.profile_version != scope.profile_version {
                    return Err(GateStage7OrderFamilyError::UnsupportedEvidence);
                }
                let slot = match value.family {
                    GateStage7UnsupportedOrderFamily::Conditional => &mut conditional,
                    GateStage7UnsupportedOrderFamily::Algo => &mut algo,
                };
                if slot.replace(value).is_some() {
                    return Err(GateStage7OrderFamilyError::DuplicateFamily);
                }
            }
        }
    }
    let regular = regular.ok_or(GateStage7OrderFamilyError::MissingFamily)?;
    let conditional = conditional.ok_or(GateStage7OrderFamilyError::MissingFamily)?;
    let algo = algo.ok_or(GateStage7OrderFamilyError::MissingFamily)?;

    let replayed = collect_regular_order_pages(
        regular.raw_payloads.iter().map(String::as_str),
        &scope.binding.symbol,
        rules,
    )
    .map_err(|_| GateStage7OrderFamilyError::RegularProjection)?;
    if replayed != regular {
        return Err(GateStage7OrderFamilyError::RegularProjection);
    }
    let regular_payload_digest = digest_pages(&regular.raw_payloads);
    Ok(GateStage7OrderFamilyCandidate {
        scope,
        regular,
        conditional,
        algo,
        regular_payload_digest,
    })
}

fn digest_pages(pages: &[String]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for page in pages {
        digest.update(u64::try_from(page.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(page.as_bytes());
    }
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Amount, Instrument, MarketKind, Price, Symbol};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::collect_regular_order_pages;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn rules() -> Result<GateContractRules, Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        Ok(GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                settlement_asset: Some("USDT".parse()?),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
                symbol,
                market: MarketKind::LinearPerpetual,
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        })
    }

    fn scope(
        venue: VenueId,
        symbol: Symbol,
    ) -> Result<GateStage7OrderFamilyScope, Box<dyn std::error::Error>> {
        Ok(GateStage7OrderFamilyScope {
            binding: GatewayBinding::new(venue, GatewayMode::Live, ACCOUNT, symbol)?,
            profile_version: GATE_STAGE7_ORDER_PROFILE_VERSION,
            attempt: 11,
            generation: 19,
            observed_at_ms: 1_000,
            expires_at_ms: 2_000,
        })
    }

    fn regular(
        rules: &GateContractRules,
    ) -> Result<GateRegularOrdersReadback, Box<dyn std::error::Error>> {
        Ok(collect_regular_order_pages(
            [include_str!("../tests/fixtures/regular_orders.json")],
            &rules.instrument.symbol,
            rules,
        )?)
    }

    fn complete_evidence(regular: GateRegularOrdersReadback) -> [GateStage7OrderFamilyEvidence; 3] {
        [
            GateStage7OrderFamilyEvidence::Regular(regular),
            GateStage7OrderFamilyEvidence::Unsupported(GateStage7UnsupportedEvidence::conditional(
                GATE_STAGE7_ORDER_PROFILE_VERSION,
            )),
            GateStage7OrderFamilyEvidence::Unsupported(GateStage7UnsupportedEvidence::algo(
                GATE_STAGE7_ORDER_PROFILE_VERSION,
            )),
        ]
    }

    #[test]
    fn candidate_binds_scope_and_all_three_families_without_granting_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let scope = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        let candidate = validate_stage7_order_families(
            scope.clone(),
            &rules,
            1_500,
            complete_evidence(regular(&rules)?),
        )?;
        assert_eq!(candidate.scope(), &scope);
        assert_eq!(candidate.regular().orders.len(), 2);
        assert_eq!(
            candidate.conditional().family,
            GateStage7UnsupportedOrderFamily::Conditional
        );
        assert_eq!(
            candidate.algo().family,
            GateStage7UnsupportedOrderFamily::Algo
        );
        assert_ne!(candidate.regular_payload_digest(), [0; 32]);
        assert!(
            candidate
                .conditional()
                .reason()
                .contains("no conditional-order")
        );
        Ok(())
    }

    #[test]
    fn raw_pages_must_replay_to_the_exact_regular_projection()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let scope = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        let mut projection_tampered = regular(&rules)?;
        projection_tampered.orders[0].quantity += Decimal::ONE;
        assert_eq!(
            validate_stage7_order_families(
                scope.clone(),
                &rules,
                1_500,
                complete_evidence(projection_tampered)
            ),
            Err(GateStage7OrderFamilyError::RegularProjection)
        );

        let mut raw_tampered = regular(&rules)?;
        raw_tampered.raw_payloads[0] = raw_tampered.raw_payloads[0].replace("9001", "9011");
        assert_eq!(
            validate_stage7_order_families(scope, &rules, 1_500, complete_evidence(raw_tampered)),
            Err(GateStage7OrderFamilyError::RegularProjection)
        );
        Ok(())
    }

    #[test]
    fn missing_duplicate_and_wrong_profile_family_evidence_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let scope = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        assert_eq!(
            validate_stage7_order_families(
                scope.clone(),
                &rules,
                1_500,
                [
                    GateStage7OrderFamilyEvidence::Regular(regular(&rules)?),
                    GateStage7OrderFamilyEvidence::Unsupported(
                        GateStage7UnsupportedEvidence::conditional(
                            GATE_STAGE7_ORDER_PROFILE_VERSION
                        )
                    )
                ]
            ),
            Err(GateStage7OrderFamilyError::MissingFamily)
        );
        assert_eq!(
            validate_stage7_order_families(
                scope.clone(),
                &rules,
                1_500,
                [
                    GateStage7OrderFamilyEvidence::Regular(regular(&rules)?),
                    GateStage7OrderFamilyEvidence::Regular(regular(&rules)?),
                    GateStage7OrderFamilyEvidence::Unsupported(
                        GateStage7UnsupportedEvidence::conditional(
                            GATE_STAGE7_ORDER_PROFILE_VERSION
                        )
                    ),
                    GateStage7OrderFamilyEvidence::Unsupported(
                        GateStage7UnsupportedEvidence::algo(GATE_STAGE7_ORDER_PROFILE_VERSION)
                    )
                ]
            ),
            Err(GateStage7OrderFamilyError::DuplicateFamily)
        );
        let mut wrong_profile = complete_evidence(regular(&rules)?);
        wrong_profile[1] = GateStage7OrderFamilyEvidence::Unsupported(
            GateStage7UnsupportedEvidence::conditional(2),
        );
        assert_eq!(
            validate_stage7_order_families(scope, &rules, 1_500, wrong_profile),
            Err(GateStage7OrderFamilyError::UnsupportedEvidence)
        );
        Ok(())
    }

    #[test]
    fn binding_profile_generation_and_freshness_are_exact() -> Result<(), Box<dyn std::error::Error>>
    {
        let rules = rules()?;
        let wrong_venue = scope(VenueId::Bybit, rules.instrument.symbol.clone())?;
        assert_eq!(
            validate_stage7_order_families(
                wrong_venue,
                &rules,
                1_500,
                complete_evidence(regular(&rules)?)
            ),
            Err(GateStage7OrderFamilyError::Scope)
        );

        let mut invalid = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        invalid.profile_version = 2;
        assert_eq!(
            validate_stage7_order_families(
                invalid,
                &rules,
                1_500,
                complete_evidence(regular(&rules)?)
            ),
            Err(GateStage7OrderFamilyError::Profile)
        );
        let mut invalid = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        invalid.generation = 0;
        assert_eq!(
            validate_stage7_order_families(
                invalid,
                &rules,
                1_500,
                complete_evidence(regular(&rules)?)
            ),
            Err(GateStage7OrderFamilyError::Generation)
        );
        let expired = scope(VenueId::Gate, rules.instrument.symbol.clone())?;
        assert_eq!(
            validate_stage7_order_families(
                expired,
                &rules,
                2_000,
                complete_evidence(regular(&rules)?)
            ),
            Err(GateStage7OrderFamilyError::Freshness)
        );
        Ok(())
    }
}
