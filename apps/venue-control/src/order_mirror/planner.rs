use rust_decimal::Decimal;
use venue_control_protocol::follow_sizing::FollowSizing;
use venue_control_protocol::kol::TerminalOpenOrder;
use venue_domain::{OrderSide, PositionSide};

use crate::kol_executor::{BinanceCommandLedgerError, scaled_copy_quantity};

/// A minimum-notional opening can be larger than its proportional source size.
/// If the leader subsequently closes substantially the same source notional,
/// the follower must unwind the established child lot rather than leave the
/// minimum-up remainder open forever.
#[derive(Clone, Debug)]
pub(super) struct MinimumUpliftedOpen {
    pub source: TerminalOpenOrder,
    pub child_quantity: Decimal,
}

pub(super) fn reducing(order: &TerminalOpenOrder) -> bool {
    matches!(
        (order.position_side, order.order_side),
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
    )
}

pub(super) fn same_terms(left: &TerminalOpenOrder, right: &TerminalOpenOrder) -> bool {
    left.native_order_id == right.native_order_id
        && left.client_order_id == right.client_order_id
        && left.symbol == right.symbol
        && left.order_side == right.order_side
        && left.position_side == right.position_side
        && left.quantity == right.quantity
        && left.limit_price == right.limit_price
        && left.post_only == right.post_only
        && left.time_in_force == right.time_in_force
        && reducing(left) == reducing(right)
}

pub(super) fn eligible(order: &TerminalOpenOrder, cutoff: u64) -> bool {
    order.created_ms.is_some_and(|time| time > cutoff)
        && matches!(
            (order.time_in_force, order.post_only),
            (Some(venue_domain::LimitTimeInForce::Gtc), false)
                | (Some(venue_domain::LimitTimeInForce::PostOnly), true)
        )
        && order
            .native_order_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
        && order.quantity > Decimal::ZERO
        && order
            .filled_quantity
            .is_some_and(|filled| filled >= Decimal::ZERO && filled < order.quantity)
        && order.limit_price.is_some_and(|price| price > Decimal::ZERO)
        && matches!(
            order.position_side,
            PositionSide::Long | PositionSide::Short
        )
}

// Source fills do not reduce an already established child order's quantity. The child has its
// own fills; replacing after an amendment subtracts the fills of all prior child attempts.
pub(super) fn replacement_quantity(
    order: &TerminalOpenOrder,
    allocated: Decimal,
    capital: Decimal,
    multiplier: Decimal,
    sizing: FollowSizing,
    prior_filled: Decimal,
) -> Result<Decimal, BinanceCommandLedgerError> {
    if prior_filled < Decimal::ZERO {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let scaled = match sizing {
        FollowSizing::Proportional => {
            scaled_copy_quantity(order.quantity, allocated, capital, multiplier)?
        }
        FollowSizing::FixedNotional { notional } => {
            let price = order
                .limit_price
                .filter(|price| *price > Decimal::ZERO)
                .ok_or(BinanceCommandLedgerError::Conflict)?;
            if notional <= Decimal::ZERO {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            notional
                .checked_div(price)
                .ok_or(BinanceCommandLedgerError::Conflict)?
        }
    };
    Ok(scaled
        .checked_sub(prior_filled)
        .ok_or(BinanceCommandLedgerError::Conflict)?
        .max(Decimal::ZERO))
}

pub(super) fn close_quantity_with_minimum_uplift(
    close: &TerminalOpenOrder,
    proportional_quantity: Decimal,
    openings: &[MinimumUpliftedOpen],
) -> Decimal {
    if !reducing(close) {
        return proportional_quantity;
    }
    let Some(opening) = openings
        .iter()
        .filter(|opening| {
            !reducing(&opening.source)
                && opening.source.symbol == close.symbol
                && opening.source.position_side == close.position_side
                && opening.source.order_side != close.order_side
                && opening
                    .source
                    .created_ms
                    .zip(close.created_ms)
                    .is_some_and(|(opened, closed)| opened < closed)
                && substantially_same_source_notional(close, &opening.source)
        })
        .max_by_key(|opening| opening.source.created_ms)
    else {
        return proportional_quantity;
    };
    proportional_quantity.max(opening.child_quantity)
}

fn substantially_same_source_notional(
    close: &TerminalOpenOrder,
    opening: &TerminalOpenOrder,
) -> bool {
    let Some(opening_notional) = opening
        .limit_price
        .and_then(|price| opening.quantity.checked_mul(price))
    else {
        return false;
    };
    let Some(close_notional) = close
        .limit_price
        .and_then(|price| close.quantity.checked_mul(price))
    else {
        return false;
    };
    let Some(lower) = opening_notional.checked_mul(Decimal::new(99, 2)) else {
        return false;
    };
    let Some(upper) = opening_notional.checked_mul(Decimal::new(101, 2)) else {
        return false;
    };
    close_notional >= lower && close_notional <= upper
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::kol::TerminalOrderState;
    fn order() -> Result<TerminalOpenOrder, Box<dyn std::error::Error>> {
        Ok(TerminalOpenOrder {
            client_order_id: "leader-1".into(),
            native_order_id: Some("100".into()),
            symbol: "BTC/USDT".parse()?,
            order_side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::from(10),
            filled_quantity: Some(Decimal::ZERO),
            limit_price: Some(Decimal::from(100)),
            post_only: true,
            time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
            reduce_only: false,
            state: TerminalOrderState::New,
            created_ms: Some(1001),
        })
    }
    #[test]
    fn old_orders_and_missing_creation_proof_are_not_copied()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = order()?;
        assert!(eligible(&source, 1000));
        assert!(!eligible(&source, 1001));
        source.created_ms = None;
        assert!(!eligible(&source, 1000));
        source.created_ms = Some(1001);
        source.time_in_force = None;
        assert!(!eligible(&source, 1000));
        source.time_in_force = Some(venue_domain::LimitTimeInForce::Gtc);
        assert!(!eligible(&source, 1000));
        source.post_only = false;
        assert!(eligible(&source, 1000));
        Ok(())
    }
    #[test]
    fn leader_partial_fill_does_not_cancel_or_rescale_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = order()?;
        let mut partial = original.clone();
        partial.filled_quantity = Some(Decimal::from(6));
        partial.state = TerminalOrderState::PartiallyFilled;
        assert!(same_terms(&original, &partial));
        assert_eq!(
            replacement_quantity(
                &partial,
                Decimal::ONE,
                Decimal::from(2),
                Decimal::ONE,
                FollowSizing::Proportional,
                Decimal::ZERO
            )?,
            Decimal::from(5)
        );
        Ok(())
    }
    #[test]
    fn amendment_preserves_child_fills_and_never_overfills_new_total()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = order()?;
        let mut amended = original.clone();
        amended.quantity = Decimal::from(4);
        assert!(!same_terms(&original, &amended));
        assert_eq!(
            replacement_quantity(
                &amended,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                FollowSizing::Proportional,
                Decimal::from(3)
            )?,
            Decimal::ONE
        );
        assert_eq!(
            replacement_quantity(
                &amended,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                FollowSizing::Proportional,
                Decimal::from(6)
            )?,
            Decimal::ZERO
        );
        Ok(())
    }
    #[test]
    fn hedge_close_is_derived_from_leg_and_side() -> Result<(), Box<dyn std::error::Error>> {
        let mut source = order()?;
        assert!(!reducing(&source));
        source.order_side = OrderSide::Sell;
        assert!(reducing(&source));
        source.position_side = PositionSide::Short;
        assert!(!reducing(&source));
        source.order_side = OrderSide::Buy;
        assert!(reducing(&source));
        Ok(())
    }

    #[test]
    fn near_full_close_unwinds_a_minimum_uplifted_child_lot()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut open = order()?;
        open.quantity = Decimal::from(281);
        open.limit_price = Some(Decimal::new(8876, 5));
        open.created_ms = Some(1001);
        let mut close = open.clone();
        close.native_order_id = Some("close-1".into());
        close.client_order_id = "close-client".into();
        close.order_side = OrderSide::Sell;
        close.quantity = Decimal::from(278);
        close.limit_price = Some(Decimal::new(8983, 5));
        close.created_ms = Some(1002);
        let opening = MinimumUpliftedOpen {
            source: open,
            child_quantity: Decimal::from(57),
        };
        assert_eq!(
            close_quantity_with_minimum_uplift(
                &close,
                Decimal::new(279_926_854_480, 10),
                std::slice::from_ref(&opening),
            ),
            Decimal::from(57)
        );
        close.quantity = Decimal::from(200);
        assert_eq!(
            close_quantity_with_minimum_uplift(&close, Decimal::from(20), &[opening]),
            Decimal::from(20)
        );
        Ok(())
    }

    #[test]
    fn fixed_notional_ignores_source_size_and_multiplier_but_subtracts_own_fills()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = order()?;
        let sizing = FollowSizing::FixedNotional {
            notional: Decimal::new(55, 1),
        };
        let quantity = |order: &TerminalOpenOrder, filled| {
            replacement_quantity(
                order,
                Decimal::from(999),
                Decimal::ONE,
                Decimal::from(7),
                sizing,
                filled,
            )
        };
        assert_eq!(quantity(&source, Decimal::ZERO)?, Decimal::new(55, 3));
        source.quantity = Decimal::from(20);
        source.filled_quantity = Some(Decimal::from(4));
        assert_eq!(quantity(&source, Decimal::ZERO)?, Decimal::new(55, 3));
        source.limit_price = Some(Decimal::from(50));
        assert_eq!(quantity(&source, Decimal::new(55, 3))?, Decimal::new(55, 3));
        assert_eq!(quantity(&source, Decimal::ONE)?, Decimal::ZERO);
        source.limit_price = Some(Decimal::ZERO);
        assert!(quantity(&source, Decimal::ZERO).is_err());
        Ok(())
    }
}
