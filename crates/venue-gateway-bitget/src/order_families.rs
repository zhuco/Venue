//! Regular-only UTA execution profile with explicit canonical unsupported-family evidence.

use sha2::{Digest, Sha256};
use venue_gateway_api::GatewayBinding;

use crate::{
    instrument::BitgetInstrumentRules,
    private::{
        BitgetPrivateFace, BitgetPrivateGenerationCandidate, BitgetPrivateSurface,
        complete_private_turn, parse_account_face, parse_fill_page, parse_positions_face,
        parse_regular_order_page, parse_settings_face,
    },
};

pub const BITGET_ORDER_PROFILE_VERSION: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetOrderFamilyScope {
    pub binding: GatewayBinding,
    pub profile_version: u64,
    pub attempt_id: u64,
    pub generation: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitgetUnsupportedOrderFamily {
    Conditional,
    Algo,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitgetUnsupportedEvidence {
    pub family: BitgetUnsupportedOrderFamily,
    pub profile_version: u64,
}

impl BitgetUnsupportedEvidence {
    #[must_use]
    pub const fn conditional(profile_version: u64) -> Self {
        Self {
            family: BitgetUnsupportedOrderFamily::Conditional,
            profile_version,
        }
    }

    #[must_use]
    pub const fn algo(profile_version: u64) -> Self {
        Self {
            family: BitgetUnsupportedOrderFamily::Algo,
            profile_version,
        }
    }

    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self.family {
            BitgetUnsupportedOrderFamily::Conditional => {
                "Bitget UTA regular-only profile has no conditional-order read or mutation surface"
            }
            BitgetUnsupportedOrderFamily::Algo => {
                "Bitget UTA regular-only profile has no algo-order read or mutation surface"
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitgetOrderFamilyEvidence {
    Regular(Box<BitgetPrivateGenerationCandidate>),
    Unsupported(BitgetUnsupportedEvidence),
}

/// Complete five-face candidate plus all three canonical order-family declarations.
/// It grants no writer, WAL, capability, or dispatch authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetOrderFamilyCandidate {
    scope: BitgetOrderFamilyScope,
    private: BitgetPrivateGenerationCandidate,
    conditional: BitgetUnsupportedEvidence,
    algo: BitgetUnsupportedEvidence,
    regular_payload_digest: [u8; 32],
}

impl BitgetOrderFamilyCandidate {
    #[must_use]
    pub const fn scope(&self) -> &BitgetOrderFamilyScope {
        &self.scope
    }

    #[must_use]
    pub const fn private(&self) -> &BitgetPrivateGenerationCandidate {
        &self.private
    }

    #[must_use]
    pub const fn conditional(&self) -> BitgetUnsupportedEvidence {
        self.conditional
    }

    #[must_use]
    pub const fn algo(&self) -> BitgetUnsupportedEvidence {
        self.algo
    }

    #[must_use]
    pub const fn regular_payload_digest(&self) -> [u8; 32] {
        self.regular_payload_digest
    }
}

pub fn validate_order_families<I>(
    scope: BitgetOrderFamilyScope,
    rules: &BitgetInstrumentRules,
    validated_at_ms: u64,
    evidence: I,
) -> Result<BitgetOrderFamilyCandidate, BitgetOrderFamilyError>
where
    I: IntoIterator<Item = BitgetOrderFamilyEvidence>,
{
    rules
        .raw
        .validate()
        .map_err(|_| BitgetOrderFamilyError::Scope)?;
    if scope.binding != rules.raw.binding
        || scope.binding.symbol != *rules.canonical_symbol()
        || scope.generation != rules.snapshot.metadata.instrument.generation
    {
        return Err(BitgetOrderFamilyError::Scope);
    }
    if scope.profile_version != BITGET_ORDER_PROFILE_VERSION {
        return Err(BitgetOrderFamilyError::Profile);
    }
    if scope.attempt_id == 0 || scope.generation == 0 {
        return Err(BitgetOrderFamilyError::Generation);
    }
    if scope.observed_at_ms == 0
        || scope.expires_at_ms <= scope.observed_at_ms
        || validated_at_ms < scope.observed_at_ms
        || validated_at_ms >= scope.expires_at_ms
    {
        return Err(BitgetOrderFamilyError::Freshness);
    }

    let mut regular = None;
    let mut conditional = None;
    let mut algo = None;
    for item in evidence {
        match item {
            BitgetOrderFamilyEvidence::Regular(value) => {
                if regular.replace(value).is_some() {
                    return Err(BitgetOrderFamilyError::DuplicateFamily);
                }
            }
            BitgetOrderFamilyEvidence::Unsupported(value) => {
                if value.profile_version != scope.profile_version {
                    return Err(BitgetOrderFamilyError::UnsupportedEvidence);
                }
                let slot = match value.family {
                    BitgetUnsupportedOrderFamily::Conditional => &mut conditional,
                    BitgetUnsupportedOrderFamily::Algo => &mut algo,
                };
                if slot.replace(value).is_some() {
                    return Err(BitgetOrderFamilyError::DuplicateFamily);
                }
            }
        }
    }
    let regular = *regular.ok_or(BitgetOrderFamilyError::MissingFamily)?;
    let conditional = conditional.ok_or(BitgetOrderFamilyError::MissingFamily)?;
    let algo = algo.ok_or(BitgetOrderFamilyError::MissingFamily)?;
    if regular.binding != scope.binding
        || regular.attempt_id != scope.attempt_id
        || regular.generation != scope.generation
        || regular.observed_at_ms != scope.observed_at_ms
    {
        return Err(BitgetOrderFamilyError::Scope);
    }
    let replayed = replay_private_candidate(&regular)?;
    if replayed != regular {
        return Err(BitgetOrderFamilyError::Projection);
    }
    let regular_payload_digest = digest_regular_pages(&regular);
    Ok(BitgetOrderFamilyCandidate {
        scope,
        private: regular,
        conditional,
        algo,
        regular_payload_digest,
    })
}

fn replay_private_candidate(
    candidate: &BitgetPrivateGenerationCandidate,
) -> Result<BitgetPrivateGenerationCandidate, BitgetOrderFamilyError> {
    let mut account = Vec::new();
    let mut settings = Vec::new();
    let mut positions = Vec::new();
    let mut orders = Vec::new();
    let mut fills = Vec::new();
    for raw in &candidate.raw_pages {
        match raw.surface {
            BitgetPrivateSurface::Account => account.push(parse_account_face(raw.clone())),
            BitgetPrivateSurface::Settings => settings.push(parse_settings_face(raw.clone())),
            BitgetPrivateSurface::Positions => positions.push(parse_positions_face(raw.clone())),
            BitgetPrivateSurface::RegularOrders => {
                orders.push(parse_regular_order_page(raw.clone()))
            }
            BitgetPrivateSurface::Fills => fills.push(parse_fill_page(raw.clone())),
        }
    }
    let account = exactly_one(account)?;
    let settings = exactly_one(settings)?;
    let positions = exactly_one(positions)?;
    let orders = orders
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| BitgetOrderFamilyError::Projection)?;
    let fills = fills
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| BitgetOrderFamilyError::Projection)?;
    complete_private_turn(vec![
        BitgetPrivateFace::Account(account),
        BitgetPrivateFace::Settings(settings),
        BitgetPrivateFace::Positions(positions),
        BitgetPrivateFace::RegularOrders(orders),
        BitgetPrivateFace::Fills(fills),
    ])
    .map_err(|_| BitgetOrderFamilyError::Projection)
}

fn exactly_one<T, E>(values: Vec<Result<T, E>>) -> Result<T, BitgetOrderFamilyError> {
    let mut values = values.into_iter();
    let Some(value) = values.next() else {
        return Err(BitgetOrderFamilyError::Projection);
    };
    if values.next().is_some() {
        return Err(BitgetOrderFamilyError::Projection);
    }
    value.map_err(|_| BitgetOrderFamilyError::Projection)
}

fn digest_regular_pages(candidate: &BitgetPrivateGenerationCandidate) -> [u8; 32] {
    let mut digest = Sha256::new();
    for raw in candidate
        .raw_pages
        .iter()
        .filter(|raw| raw.surface == BitgetPrivateSurface::RegularOrders)
    {
        digest.update(
            u64::try_from(raw.payload.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(raw.payload.as_bytes());
    }
    digest.finalize().into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetOrderFamilyError {
    #[error("Bitget order-family scope does not match the instrument and five-face readback")]
    Scope,
    #[error("Bitget order-family execution profile is unsupported")]
    Profile,
    #[error("Bitget order-family attempt or generation is invalid")]
    Generation,
    #[error("Bitget order-family evidence is stale or future-dated")]
    Freshness,
    #[error("Bitget order-family evidence is missing a canonical family")]
    MissingFamily,
    #[error("Bitget order-family evidence repeats a canonical family")]
    DuplicateFamily,
    #[error("Bitget regular/five-face projection does not replay its raw evidence")]
    Projection,
    #[error("Bitget unsupported-family evidence does not match the execution profile")]
    UnsupportedEvidence,
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;
    use crate::{
        instrument::{BitgetRawInstrumentPayload, parse_instrument_rules},
        private::{
            BitgetPrivateFace, BitgetPrivateSurface, BitgetRawPrivatePage, complete_private_turn,
            parse_account_face, parse_fill_page, parse_positions_face, parse_regular_order_page,
            parse_settings_face,
        },
    };

    fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn rules() -> Result<BitgetInstrumentRules, Box<dyn std::error::Error>> {
        let raw = BitgetRawInstrumentPayload::new(
            binding()?,
            7,
            50,
            1_000,
            include_str!("../tests/fixtures/bitget_uta_btcusdt_instrument.json").to_owned(),
        )?;
        Ok(parse_instrument_rules(raw, 60)?)
    }

    fn raw(
        surface: BitgetPrivateSurface,
        data: serde_json::Value,
    ) -> Result<BitgetRawPrivatePage, Box<dyn std::error::Error>> {
        Ok(BitgetRawPrivatePage::new_with_generation(
            surface,
            binding()?,
            9,
            7,
            0,
            None,
            (surface == BitgetPrivateSurface::Fills).then_some(10),
            100,
            json!({"code":"00000", "data":data}).to_string(),
        )?)
    }

    fn private_candidate() -> Result<BitgetPrivateGenerationCandidate, Box<dyn std::error::Error>> {
        Ok(complete_private_turn(vec![
            BitgetPrivateFace::Account(parse_account_face(raw(
                BitgetPrivateSurface::Account,
                json!({
                    "imr":"0", "mmr":"0",
                    "assets":[{"coin":"USDT", "balance":"20", "available":"20"}]
                }),
            )?)?),
            BitgetPrivateFace::Settings(parse_settings_face(raw(
                BitgetPrivateSurface::Settings,
                json!({"holdMode":"hedge_mode"}),
            )?)?),
            BitgetPrivateFace::Positions(parse_positions_face(raw(
                BitgetPrivateSurface::Positions,
                json!({"list":[]}),
            )?)?),
            BitgetPrivateFace::RegularOrders(vec![parse_regular_order_page(raw(
                BitgetPrivateSurface::RegularOrders,
                json!({"list":[], "cursor":null}),
            )?)?]),
            BitgetPrivateFace::Fills(vec![parse_fill_page(raw(
                BitgetPrivateSurface::Fills,
                json!({"list":[], "cursor":null}),
            )?)?]),
        ])?)
    }

    fn scope() -> Result<BitgetOrderFamilyScope, Box<dyn std::error::Error>> {
        Ok(BitgetOrderFamilyScope {
            binding: binding()?,
            profile_version: BITGET_ORDER_PROFILE_VERSION,
            attempt_id: 9,
            generation: 7,
            observed_at_ms: 100,
            expires_at_ms: 900,
        })
    }

    fn complete_evidence(
        candidate: BitgetPrivateGenerationCandidate,
    ) -> [BitgetOrderFamilyEvidence; 3] {
        [
            BitgetOrderFamilyEvidence::Regular(Box::new(candidate)),
            BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::conditional(
                BITGET_ORDER_PROFILE_VERSION,
            )),
            BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::algo(
                BITGET_ORDER_PROFILE_VERSION,
            )),
        ]
    }

    #[test]
    fn five_faces_and_all_three_families_share_one_scope() -> Result<(), Box<dyn std::error::Error>>
    {
        let candidate = validate_order_families(
            scope()?,
            &rules()?,
            200,
            complete_evidence(private_candidate()?),
        )?;
        assert_eq!(candidate.private().raw_pages.len(), 5);
        assert_eq!(candidate.private().positions.len(), 2);
        assert_eq!(
            candidate.conditional().family,
            BitgetUnsupportedOrderFamily::Conditional
        );
        assert_eq!(candidate.algo().family, BitgetUnsupportedOrderFamily::Algo);
        assert_ne!(candidate.regular_payload_digest(), [0; 32]);
        Ok(())
    }

    #[test]
    fn missing_family_cross_generation_and_projection_tamper_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let candidate = private_candidate()?;
        assert_eq!(
            validate_order_families(
                scope()?,
                &rules()?,
                200,
                [
                    BitgetOrderFamilyEvidence::Regular(Box::new(candidate.clone())),
                    BitgetOrderFamilyEvidence::Unsupported(BitgetUnsupportedEvidence::conditional(
                        BITGET_ORDER_PROFILE_VERSION
                    ))
                ]
            ),
            Err(BitgetOrderFamilyError::MissingFamily)
        );
        let mut cross_generation = candidate.clone();
        cross_generation.generation = 8;
        assert_eq!(
            validate_order_families(
                scope()?,
                &rules()?,
                200,
                complete_evidence(cross_generation)
            ),
            Err(BitgetOrderFamilyError::Scope)
        );
        let mut tampered = candidate;
        tampered.balance.available_balance -= rust_decimal::Decimal::ONE;
        assert_eq!(
            validate_order_families(scope()?, &rules()?, 200, complete_evidence(tampered)),
            Err(BitgetOrderFamilyError::Projection)
        );
        Ok(())
    }
}
