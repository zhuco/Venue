use rust_decimal::Decimal;
use thiserror::Error;
use venue_domain::domain::{InstrumentIdentity, OrderSide, Precision, Price};

use crate::sizing::{ReferencePriceSnapshot, reference_is_fresh};

/// Frozen inputs for preserving a leader limit's signed relative offset across venues.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossVenueLimitRequest {
    pub leader_limit_price: Price,
    pub leader_reference: ReferencePriceSnapshot,
    pub follower_reference: ReferencePriceSnapshot,
    pub expected_leader_generation: u64,
    pub expected_follower_generation: u64,
    /// Maximum accepted absolute relative offset in `[0, 1)`.
    pub max_absolute_deviation: Decimal,
    pub now_ms: u64,
}

/// Unrounded follower price boundary. It is price data only, not an order instruction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossVenueLimitPlan {
    pub instrument_identity: InstrumentIdentity,
    pub leader_generation: u64,
    pub follower_generation: u64,
    pub relative_offset: Decimal,
    pub follower_risk_boundary: Price,
}

/// Converts a frozen leader limit to a follower risk boundary without reading markets or time.
pub fn convert_cross_venue_limit(
    request: &CrossVenueLimitRequest,
) -> Result<CrossVenueLimitPlan, LimitPriceError> {
    if request.expected_leader_generation == 0 || request.expected_follower_generation == 0 {
        return Err(LimitPriceError::GenerationMismatch);
    }
    if request.leader_reference.instrument_generation != request.expected_leader_generation
        || request.follower_reference.instrument_generation != request.expected_follower_generation
    {
        return Err(LimitPriceError::GenerationMismatch);
    }
    if !reference_is_fresh(&request.leader_reference, request.now_ms)
        || !reference_is_fresh(&request.follower_reference, request.now_ms)
    {
        return Err(LimitPriceError::ReferencePrice);
    }
    if request.leader_reference.instrument_identity
        != request.follower_reference.instrument_identity
    {
        return Err(LimitPriceError::IdentityMismatch);
    }
    if request.max_absolute_deviation < Decimal::ZERO
        || request.max_absolute_deviation >= Decimal::ONE
    {
        return Err(LimitPriceError::DeviationBound);
    }

    let ratio = request
        .leader_limit_price
        .value()
        .checked_div(request.leader_reference.price.value())
        .ok_or(LimitPriceError::ArithmeticOverflow)?;
    let relative_offset = ratio
        .checked_sub(Decimal::ONE)
        .ok_or(LimitPriceError::ArithmeticOverflow)?;
    if relative_offset.abs() > request.max_absolute_deviation {
        return Err(LimitPriceError::DeviationExceeded);
    }
    let boundary = request
        .follower_reference
        .price
        .value()
        .checked_mul(ratio)
        .ok_or(LimitPriceError::ArithmeticOverflow)?;
    let follower_risk_boundary = Price::new(boundary).map_err(|_| LimitPriceError::InvalidPrice)?;

    Ok(CrossVenueLimitPlan {
        instrument_identity: request.leader_reference.instrument_identity.clone(),
        leader_generation: request.expected_leader_generation,
        follower_generation: request.expected_follower_generation,
        relative_offset,
        follower_risk_boundary,
    })
}

/// Normalizes a risk boundary to the follower tick: BUY floors and SELL ceils.
pub fn normalize_limit_price(
    precision: &Precision,
    side: OrderSide,
    risk_boundary: Price,
) -> Result<Price, LimitPriceError> {
    precision
        .validate()
        .map_err(|_| LimitPriceError::Precision)?;
    let boundary = risk_boundary.value();
    let floor = precision
        .floor(boundary)
        .map_err(|_| LimitPriceError::Precision)?;
    let normalized = match side {
        OrderSide::Buy => floor,
        OrderSide::Sell if floor == boundary => floor,
        OrderSide::Sell => floor
            .checked_add(precision.step)
            .ok_or(LimitPriceError::ArithmeticOverflow)?,
    };
    if normalized <= Decimal::ZERO
        || !precision
            .accepts(normalized)
            .map_err(|_| LimitPriceError::Precision)?
    {
        return Err(LimitPriceError::BelowMinimum);
    }
    if (matches!(side, OrderSide::Buy) && normalized > boundary)
        || (matches!(side, OrderSide::Sell) && normalized < boundary)
    {
        return Err(LimitPriceError::RiskBoundaryCrossed);
    }
    Price::new(normalized).map_err(|_| LimitPriceError::InvalidPrice)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LimitPriceError {
    #[error("reference price is malformed, expired, or future-dated")]
    ReferencePrice,
    #[error("reference generation does not match the explicitly expected generation")]
    GenerationMismatch,
    #[error("leader and follower references do not share one stable instrument identity")]
    IdentityMismatch,
    #[error("maximum absolute deviation must be in [0, 1)")]
    DeviationBound,
    #[error("leader limit exceeds the reviewed relative-deviation bound")]
    DeviationExceeded,
    #[error("price precision is invalid")]
    Precision,
    #[error("normalized price is below the precision minimum")]
    BelowMinimum,
    #[error("normalized tick crossed the follower risk boundary")]
    RiskBoundaryCrossed,
    #[error("derived price is not positive")]
    InvalidPrice,
    #[error("limit price arithmetic overflowed decimal precision")]
    ArithmeticOverflow,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{Asset, MarketKind, Symbol};

    use super::*;

    fn identity() -> Result<InstrumentIdentity, Box<dyn std::error::Error>> {
        Ok(InstrumentIdentity {
            symbol: "BTC/USDT".parse::<Symbol>()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(Asset::new("USDT")?),
        })
    }

    fn reference(
        identity: InstrumentIdentity,
        generation: u64,
        price: Decimal,
    ) -> Result<ReferencePriceSnapshot, Box<dyn std::error::Error>> {
        Ok(ReferencePriceSnapshot {
            instrument_identity: identity,
            instrument_generation: generation,
            price: Price::new(price)?,
            observed_at_ms: 100,
            expires_at_ms: 300,
        })
    }

    fn request() -> Result<CrossVenueLimitRequest, Box<dyn std::error::Error>> {
        let identity = identity()?;
        Ok(CrossVenueLimitRequest {
            leader_limit_price: Price::new(Decimal::from(101))?,
            leader_reference: reference(identity.clone(), 3, Decimal::from(100))?,
            follower_reference: reference(identity, 9, Decimal::from(200))?,
            expected_leader_generation: 3,
            expected_follower_generation: 9,
            max_absolute_deviation: Decimal::new(5, 2),
            now_ms: 200,
        })
    }

    #[test]
    fn preserves_signed_relative_offset_across_venues() -> Result<(), Box<dyn std::error::Error>> {
        let plan = convert_cross_venue_limit(&request()?)?;
        assert_eq!(plan.relative_offset, Decimal::new(1, 2));
        assert_eq!(plan.follower_risk_boundary.value(), Decimal::from(202));
        assert_eq!(plan.leader_generation, 3);
        assert_eq!(plan.follower_generation, 9);
        Ok(())
    }

    #[test]
    fn buy_floors_and_sell_ceils_without_crossing_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let precision = Precision::new(Decimal::new(3, 1), Decimal::new(3, 1))?;
        let boundary = Price::new(Decimal::new(25125, 2))?;
        let buy = normalize_limit_price(&precision, OrderSide::Buy, boundary)?;
        let sell = normalize_limit_price(&precision, OrderSide::Sell, boundary)?;
        assert_eq!(buy.value(), Decimal::new(2511, 1));
        assert_eq!(sell.value(), Decimal::new(2514, 1));
        assert!(buy.value() <= boundary.value());
        assert!(sell.value() >= boundary.value());
        Ok(())
    }

    #[test]
    fn expiry_identity_and_generation_mismatch_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let base = request()?;
        let mut stale = base.clone();
        stale.leader_reference.expires_at_ms = stale.now_ms;
        assert_eq!(
            convert_cross_venue_limit(&stale),
            Err(LimitPriceError::ReferencePrice)
        );

        let mut wrong_generation = base.clone();
        wrong_generation.expected_follower_generation = 8;
        assert_eq!(
            convert_cross_venue_limit(&wrong_generation),
            Err(LimitPriceError::GenerationMismatch)
        );

        let mut wrong_identity = base;
        wrong_identity.follower_reference.instrument_identity.symbol = "ETH/USDT".parse()?;
        assert_eq!(
            convert_cross_venue_limit(&wrong_identity),
            Err(LimitPriceError::IdentityMismatch)
        );
        Ok(())
    }

    #[test]
    fn deviation_bounds_and_extreme_offsets_are_rejected() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut invalid_bound = request()?;
        invalid_bound.max_absolute_deviation = Decimal::ONE;
        assert_eq!(
            convert_cross_venue_limit(&invalid_bound),
            Err(LimitPriceError::DeviationBound)
        );

        let mut exceeded = request()?;
        exceeded.max_absolute_deviation = Decimal::new(5, 3);
        assert_eq!(
            convert_cross_venue_limit(&exceeded),
            Err(LimitPriceError::DeviationExceeded)
        );
        Ok(())
    }

    #[test]
    fn overflow_and_invalid_precision_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let mut overflow = request()?;
        overflow.leader_limit_price = Price::new(Decimal::new(15, 1))?;
        overflow.leader_reference.price = Price::new(Decimal::ONE)?;
        overflow.follower_reference.price = Price::new(Decimal::MAX)?;
        overflow.max_absolute_deviation = Decimal::new(9, 1);
        assert_eq!(
            convert_cross_venue_limit(&overflow),
            Err(LimitPriceError::ArithmeticOverflow)
        );

        let invalid = Precision {
            step: Decimal::ZERO,
            minimum: Decimal::ZERO,
        };
        assert_eq!(
            normalize_limit_price(&invalid, OrderSide::Buy, Price::new(Decimal::ONE)?),
            Err(LimitPriceError::Precision)
        );
        Ok(())
    }
}
