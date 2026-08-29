use rust_decimal::Decimal;
use thiserror::Error;
use venue_domain::domain::{
    Amount, InstrumentIdentity, InstrumentSnapshot, InstrumentSnapshotError, InstrumentValueError,
    MarketKind, OrderSide, Price,
};

use crate::TargetExposurePlan;

/// Frozen price evidence used only for deterministic sizing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferencePriceSnapshot {
    pub instrument_identity: InstrumentIdentity,
    pub instrument_generation: u64,
    pub price: Price,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
}

/// Fully frozen input to the pure sizing reducer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticSizingRequest {
    pub exposure: TargetExposurePlan,
    pub instrument: InstrumentSnapshot,
    pub reference_price: ReferencePriceSnapshot,
    /// Reviewed native reduce-only support for this derivative binding.
    pub reduce_only_supported: bool,
    pub now_ms: u64,
}

/// Canonical quantity semantics. Adapters remain responsible for native request encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SizedQuantity {
    Base(Decimal),
    ContractLots(Decimal),
}

impl SizedQuantity {
    #[must_use]
    pub const fn value(self) -> Decimal {
        match self {
            Self::Base(value) | Self::ContractLots(value) => value,
        }
    }
}

/// Semantic sizing only; this is neither an execution command nor mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticSizingPlan {
    pub instrument_identity: InstrumentIdentity,
    pub instrument_generation: u64,
    pub side: OrderSide,
    pub reducing: bool,
    pub reduce_only: bool,
    pub quantity: SizedQuantity,
    pub reference_price: Price,
    pub requested_quote_notional: Amount,
    pub normalized_quote_notional: Amount,
    pub residual_quote_notional: Amount,
}

/// Converts an already-approved target exposure delta into canonical base quantity or lots.
pub fn plan_semantic_size(
    request: &SemanticSizingRequest,
) -> Result<SemanticSizingPlan, SizingError> {
    if !reference_is_fresh(&request.reference_price, request.now_ms) {
        return Err(SizingError::ReferencePrice);
    }
    if request.instrument.metadata.identity() != request.reference_price.instrument_identity
        || request.instrument.metadata.instrument.generation
            != request.reference_price.instrument_generation
    {
        return Err(SizingError::ReferenceMismatch);
    }
    let metadata = request
        .instrument
        .require(
            &request.reference_price.instrument_identity,
            request.reference_price.instrument_generation,
            request.now_ms,
        )
        .map_err(map_snapshot_error)?;
    validate_exposure(&request.exposure, metadata.instrument.symbol.quote())?;

    let delta = request.exposure.delta_exposure.value;
    if delta == Decimal::ZERO {
        return Err(SizingError::NoChange);
    }
    let managed = request
        .exposure
        .target_exposure
        .value
        .checked_sub(delta)
        .ok_or(SizingError::ArithmeticOverflow)?;
    let target = request.exposure.target_exposure.value;
    if crosses_zero(managed, target) {
        return Err(SizingError::DirectionFlipRequiresSplit);
    }
    if matches!(metadata.instrument.market, MarketKind::Spot)
        && (managed < Decimal::ZERO || target < Decimal::ZERO)
    {
        return Err(SizingError::SpotShort);
    }

    let side = if delta > Decimal::ZERO {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let reducing = reduces(managed, target);
    let derivative = matches!(metadata.instrument.market, MarketKind::LinearPerpetual);
    if reducing && derivative && !request.reduce_only_supported {
        return Err(SizingError::ReduceOnlyUnsupported);
    }

    let requested_value = if delta > Decimal::ZERO {
        delta
    } else {
        delta
            .checked_mul(Decimal::NEGATIVE_ONE)
            .ok_or(SizingError::ArithmeticOverflow)?
    };
    let requested = Amount::new(
        request.exposure.delta_exposure.asset.clone(),
        requested_value,
    );
    let quantity_value = metadata
        .quantity_for_quote_notional(&requested, Some(request.reference_price.price))
        .map_err(map_value_error)?;
    let normalized = metadata
        .quote_notional(quantity_value, Some(request.reference_price.price))
        .map_err(map_value_error)?;
    if normalized.value > requested.value {
        return Err(SizingError::NormalizedExceedsRequested);
    }
    let residual = requested
        .value
        .checked_sub(normalized.value)
        .ok_or(SizingError::ArithmeticOverflow)?;
    let quantity = if metadata.contract.is_some() {
        SizedQuantity::ContractLots(quantity_value)
    } else {
        SizedQuantity::Base(quantity_value)
    };

    Ok(SemanticSizingPlan {
        instrument_identity: metadata.identity(),
        instrument_generation: metadata.instrument.generation,
        side,
        reducing,
        reduce_only: reducing && derivative,
        quantity,
        reference_price: request.reference_price.price,
        requested_quote_notional: requested,
        normalized_quote_notional: normalized,
        residual_quote_notional: Amount::new(
            request.exposure.delta_exposure.asset.clone(),
            residual,
        ),
    })
}

pub(crate) fn reference_is_fresh(reference: &ReferencePriceSnapshot, now_ms: u64) -> bool {
    reference.instrument_generation != 0
        && reference.observed_at_ms != 0
        && reference.expires_at_ms > reference.observed_at_ms
        && now_ms >= reference.observed_at_ms
        && now_ms < reference.expires_at_ms
}

fn validate_exposure(plan: &TargetExposurePlan, quote: &str) -> Result<(), SizingError> {
    let amounts = [
        &plan.safe_available_margin,
        &plan.effective_follower_capital,
        &plan.target_exposure,
        &plan.delta_exposure,
    ];
    if plan.snapshot_generation == 0
        || plan.safe_available_margin.value < Decimal::ZERO
        || plan.effective_follower_capital.value < Decimal::ZERO
        || amounts.iter().any(|amount| amount.asset.as_str() != quote)
    {
        return Err(SizingError::ExposurePlan);
    }
    Ok(())
}

const fn crosses_zero(managed: Decimal, target: Decimal) -> bool {
    (managed.is_sign_positive() && !managed.is_zero() && target.is_sign_negative())
        || (managed.is_sign_negative() && target.is_sign_positive() && !target.is_zero())
}

fn reduces(managed: Decimal, target: Decimal) -> bool {
    managed != Decimal::ZERO
        && (target == Decimal::ZERO
            || (managed.is_sign_positive() == target.is_sign_positive()
                && target.abs() < managed.abs()))
}

const fn map_snapshot_error(error: InstrumentSnapshotError) -> SizingError {
    match error {
        InstrumentSnapshotError::TradingDisabled => SizingError::InstrumentDisabled,
        _ => SizingError::InstrumentSnapshot,
    }
}

const fn map_value_error(error: InstrumentValueError) -> SizingError {
    match error {
        InstrumentValueError::Quantity | InstrumentValueError::Notional => {
            SizingError::BelowMinimum
        }
        InstrumentValueError::TradingDisabled => SizingError::InstrumentDisabled,
        InstrumentValueError::Overflow => SizingError::ArithmeticOverflow,
        _ => SizingError::InstrumentRules,
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SizingError {
    #[error("reference price is malformed, expired, or future-dated")]
    ReferencePrice,
    #[error("reference price identity or generation does not match instrument metadata")]
    ReferenceMismatch,
    #[error("instrument snapshot is malformed, expired, or future-dated")]
    InstrumentSnapshot,
    #[error("instrument is not enabled for trading")]
    InstrumentDisabled,
    #[error("target exposure plan is malformed or uses a non-quote asset")]
    ExposurePlan,
    #[error("target exposure has no change to size")]
    NoChange,
    #[error("cross-zero reversal must close and confirm before opening the opposite side")]
    DirectionFlipRequiresSplit,
    #[error("spot copy cannot create or preserve a short exposure")]
    SpotShort,
    #[error("derivative reduction requires reviewed reduce-only support")]
    ReduceOnlyUnsupported,
    #[error("requested notional rounds below an instrument minimum")]
    BelowMinimum,
    #[error("instrument conversion rules are invalid")]
    InstrumentRules,
    #[error("normalized notional unexpectedly exceeds the requested notional")]
    NormalizedExceedsRequested,
    #[error("copy sizing overflowed decimal precision")]
    ArithmeticOverflow,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{
        Asset, ContractSpec, Instrument, InstrumentMetadata, Precision, Symbol, ValueUnit,
    };

    use super::*;

    fn exposure(
        target: Decimal,
        delta: Decimal,
    ) -> Result<TargetExposurePlan, Box<dyn std::error::Error>> {
        let quote = Asset::new("USDT")?;
        Ok(TargetExposurePlan {
            snapshot_generation: 3,
            exposure_ratio: Decimal::ONE,
            safe_available_margin: Amount::new(quote.clone(), Decimal::from(100)),
            effective_follower_capital: Amount::new(quote.clone(), Decimal::from(100)),
            target_exposure: Amount::new(quote.clone(), target),
            delta_exposure: Amount::new(quote, delta),
        })
    }

    fn instrument(
        market: MarketKind,
        contract: Option<ContractSpec>,
        minimum_notional: Decimal,
    ) -> Result<InstrumentSnapshot, Box<dyn std::error::Error>> {
        let quote = Asset::new("USDT")?;
        let quantity = match &contract {
            Some(value) => value.lots.clone(),
            None => Precision::new(Decimal::ONE, Decimal::ONE)?,
        };
        let settlement_asset = matches!(market, MarketKind::LinearPerpetual).then(|| quote.clone());
        let metadata = InstrumentMetadata::new(
            Instrument {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                market,
                settlement_asset,
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 2))?,
                quantity_step: quantity.step,
                minimum_notional: Amount::new(quote, minimum_notional),
            },
            Precision::new(Decimal::new(1, 2), Decimal::new(1, 2))?,
            quantity,
            contract,
            true,
        )?;
        Ok(InstrumentSnapshot::new(metadata, 100, 300)?)
    }

    fn request(
        exposure: TargetExposurePlan,
        instrument: InstrumentSnapshot,
        price: Decimal,
    ) -> Result<SemanticSizingRequest, Box<dyn std::error::Error>> {
        Ok(SemanticSizingRequest {
            reference_price: ReferencePriceSnapshot {
                instrument_identity: instrument.metadata.identity(),
                instrument_generation: instrument.metadata.instrument.generation,
                price: Price::new(price)?,
                observed_at_ms: 110,
                expires_at_ms: 290,
            },
            exposure,
            instrument,
            reduce_only_supported: true,
            now_ms: 200,
        })
    }

    #[test]
    fn spot_buy_and_inventory_reduction_are_semantic_only() -> Result<(), Box<dyn std::error::Error>>
    {
        let spot = instrument(MarketKind::Spot, None, Decimal::ZERO)?;
        let buy = plan_semantic_size(&request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            spot.clone(),
            Decimal::from(5),
        )?)?;
        assert_eq!(buy.side, OrderSide::Buy);
        assert_eq!(buy.quantity, SizedQuantity::Base(Decimal::from(10)));
        assert!(!buy.reducing);
        assert!(!buy.reduce_only);

        let sell = plan_semantic_size(&request(
            exposure(Decimal::from(30), Decimal::from(-20))?,
            spot,
            Decimal::from(5),
        )?)?;
        assert_eq!(sell.side, OrderSide::Sell);
        assert!(sell.reducing);
        assert!(!sell.reduce_only);
        Ok(())
    }

    #[test]
    fn base_and_quote_contracts_use_authoritative_lots() -> Result<(), Box<dyn std::error::Error>> {
        let lots = Precision::new(Decimal::ONE, Decimal::ONE)?;
        let base_contract = ContractSpec::new(Decimal::new(1, 1), ValueUnit::Base, lots.clone())?;
        let base = plan_semantic_size(&request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            instrument(
                MarketKind::LinearPerpetual,
                Some(base_contract),
                Decimal::ZERO,
            )?,
            Decimal::from(5),
        )?)?;
        assert_eq!(
            base.quantity,
            SizedQuantity::ContractLots(Decimal::from(100))
        );

        let quote_contract = ContractSpec::new(Decimal::from(10), ValueUnit::Quote, lots)?;
        let quote = plan_semantic_size(&request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            instrument(
                MarketKind::LinearPerpetual,
                Some(quote_contract),
                Decimal::ZERO,
            )?,
            Decimal::from(5),
        )?)?;
        assert_eq!(
            quote.quantity,
            SizedQuantity::ContractLots(Decimal::from(5))
        );
        Ok(())
    }

    #[test]
    fn stale_and_cross_generation_references_fail_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        let base = request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            instrument(MarketKind::Spot, None, Decimal::ZERO)?,
            Decimal::from(5),
        )?;
        let mut stale = base.clone();
        stale.reference_price.expires_at_ms = stale.now_ms;
        assert_eq!(plan_semantic_size(&stale), Err(SizingError::ReferencePrice));

        let mut wrong_generation = base;
        wrong_generation.reference_price.instrument_generation = 8;
        assert_eq!(
            plan_semantic_size(&wrong_generation),
            Err(SizingError::ReferenceMismatch)
        );

        let mut stale_instrument = request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            instrument(MarketKind::Spot, None, Decimal::ZERO)?,
            Decimal::from(5),
        )?;
        stale_instrument.instrument.expires_at_ms = stale_instrument.now_ms;
        assert_eq!(
            plan_semantic_size(&stale_instrument),
            Err(SizingError::InstrumentSnapshot)
        );

        let mut disabled = request(
            exposure(Decimal::from(50), Decimal::from(50))?,
            instrument(MarketKind::Spot, None, Decimal::ZERO)?,
            Decimal::from(5),
        )?;
        disabled.instrument.metadata.trading_enabled = false;
        assert_eq!(
            plan_semantic_size(&disabled),
            Err(SizingError::InstrumentDisabled)
        );
        Ok(())
    }

    #[test]
    fn zero_spot_short_and_cross_zero_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let spot = instrument(MarketKind::Spot, None, Decimal::ZERO)?;
        assert_eq!(
            plan_semantic_size(&request(
                exposure(Decimal::from(10), Decimal::ZERO)?,
                spot.clone(),
                Decimal::ONE,
            )?),
            Err(SizingError::NoChange)
        );
        assert_eq!(
            plan_semantic_size(&request(
                exposure(Decimal::from(-10), Decimal::from(-10))?,
                spot,
                Decimal::ONE,
            )?),
            Err(SizingError::SpotShort)
        );
        let derivative = instrument(MarketKind::LinearPerpetual, None, Decimal::ZERO)?;
        assert_eq!(
            plan_semantic_size(&request(
                exposure(Decimal::from(-10), Decimal::from(-20))?,
                derivative,
                Decimal::ONE,
            )?),
            Err(SizingError::DirectionFlipRequiresSplit)
        );
        Ok(())
    }

    #[test]
    fn derivative_reduction_requires_reduce_only_support() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut input = request(
            exposure(Decimal::from(30), Decimal::from(-20))?,
            instrument(MarketKind::LinearPerpetual, None, Decimal::ZERO)?,
            Decimal::ONE,
        )?;
        input.reduce_only_supported = false;
        assert_eq!(
            plan_semantic_size(&input),
            Err(SizingError::ReduceOnlyUnsupported)
        );
        input.reduce_only_supported = true;
        let plan = plan_semantic_size(&input)?;
        assert!(plan.reducing);
        assert!(plan.reduce_only);
        assert_eq!(plan.side, OrderSide::Sell);
        Ok(())
    }

    #[test]
    fn minimum_and_rounding_residual_are_explicit() -> Result<(), Box<dyn std::error::Error>> {
        let spot = instrument(MarketKind::Spot, None, Decimal::from(5))?;
        assert_eq!(
            plan_semantic_size(&request(
                exposure(Decimal::from(4), Decimal::from(4))?,
                spot.clone(),
                Decimal::ONE,
            )?),
            Err(SizingError::BelowMinimum)
        );
        let plan = plan_semantic_size(&request(
            exposure(Decimal::from(52), Decimal::from(52))?,
            spot,
            Decimal::from(5),
        )?)?;
        assert_eq!(plan.quantity, SizedQuantity::Base(Decimal::from(10)));
        assert_eq!(plan.normalized_quote_notional.value, Decimal::from(50));
        assert_eq!(plan.residual_quote_notional.value, Decimal::from(2));
        Ok(())
    }
}
