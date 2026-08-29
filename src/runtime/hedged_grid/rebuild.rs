use rust_decimal::Decimal;

use crate::{
    domain::{Instrument, Price},
    strategy::hedged_grid::{GridEpoch, HedgedGridError, HedgedGridState},
};

/// Builds one physical epoch from a strategy-selected logical anchor. Exchange tick/step
/// normalization happens here, while the reducer retains the exact triggering fill price in its
/// durable recovery state.
pub(crate) fn epoch_at_anchor(
    state: &HedgedGridState,
    instrument: &Instrument,
    logical_anchor: Price,
) -> Result<GridEpoch, HedgedGridError> {
    let anchor = align_down(logical_anchor.value(), instrument.price_tick.value())?;
    let step = align_up(
        anchor * state.params.spacing_rate,
        instrument.price_tick.value(),
    )?;
    let quantity = align_up(
        state.params.order_notional.value / anchor,
        instrument.quantity_step,
    )?;
    let epoch = match state.epoch.as_ref() {
        Some(current) => current.epoch.checked_add(1).ok_or(HedgedGridError::Epoch)?,
        None => 1,
    };
    let epoch = GridEpoch {
        epoch,
        anchor_price: Price::new(anchor).map_err(|_| HedgedGridError::Epoch)?,
        step: Price::new(step).map_err(|_| HedgedGridError::Epoch)?,
        grid_quantity: quantity,
        passive_book_fallback: None,
    };
    epoch.validate(state.params.grid_count)?;
    Ok(epoch)
}

pub(crate) fn epoch_at_midpoint(
    state: &HedgedGridState,
    instrument: &Instrument,
    bid: Price,
    ask: Price,
) -> Result<GridEpoch, HedgedGridError> {
    let midpoint = Price::new((bid.value() + ask.value()) / Decimal::TWO)
        .map_err(|_| HedgedGridError::Epoch)?;
    epoch_at_anchor(state, instrument, midpoint)
}

fn align_up(value: Decimal, step: Decimal) -> Result<Decimal, HedgedGridError> {
    if !value.is_sign_positive() || !step.is_sign_positive() {
        return Err(HedgedGridError::Epoch);
    }
    let remainder = value % step;
    if remainder.is_zero() {
        Ok(value)
    } else {
        value
            .checked_add(step - remainder)
            .ok_or(HedgedGridError::Epoch)
    }
}

fn align_down(value: Decimal, step: Decimal) -> Result<Decimal, HedgedGridError> {
    if !value.is_sign_positive() || !step.is_sign_positive() {
        return Err(HedgedGridError::Epoch);
    }
    let aligned = value - (value % step);
    if aligned.is_sign_positive() {
        Ok(aligned)
    } else {
        Err(HedgedGridError::Epoch)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::{
        domain::{Amount, Asset, Instrument, MarketKind},
        strategy::hedged_grid::{HedgedGridBinding, HedgedGridParams},
    };

    use super::*;

    #[test]
    fn fill_anchor_is_normalized_without_using_midpoint() -> Result<(), Box<dyn std::error::Error>>
    {
        let state = HedgedGridState::new_with_params(
            HedgedGridBinding {
                strategy_instance_id: "hedged_grid_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "gate".to_owned(),
                account: "usdt_futures_dual".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                config_version: "v1".to_owned(),
                owner_scope: "scope_1".to_owned(),
            },
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
        )?;
        let asset = Asset::new("USDT")?;
        let instrument = Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(asset.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 4))?,
            quantity_step: Decimal::ONE,
            minimum_notional: Amount::new(asset, Decimal::ONE),
        };

        let epoch = epoch_at_anchor(&state, &instrument, Price::new(Decimal::new(123_456, 6))?)?;

        assert_eq!(epoch.anchor_price.value(), Decimal::new(1234, 4));
        assert_ne!(epoch.anchor_price.value(), Decimal::new(1200, 4));
        Ok(())
    }
}
