use rust_decimal::Decimal;
use venue_control_protocol::{
    inventory_mm::InventoryMmConfig,
    kol::{TerminalAccountProjection, TerminalOpenOrder, TerminalOrderState},
};
use venue_domain::domain::{
    Amount, Asset, FieldState, Order, OrderSide, OrderState, PositionSide, Price,
};
use venue_strategies::inventory_mm::MmConfig;

use super::InventoryMmRuntimeError as Error;

pub(super) const PRIVATE_MAX_AGE_MS: u64 = 5_000;
pub(super) const MARKET_MAX_AGE_MS: u64 = 5_000;

pub(super) fn amount(value: Decimal, asset: &str) -> Result<Amount, Error> {
    Ok(Amount {
        value,
        asset: Asset::new(asset).map_err(|_| Error::Facts)?,
    })
}

pub(super) fn config(source: &InventoryMmConfig) -> Result<MmConfig, Error> {
    source.validate().map_err(|_| Error::Facts)?;
    let quote = source.symbol.quote();
    Ok(MmConfig {
        symbol: source.symbol.clone(),
        order_notional: amount(source.order_notional, quote)?,
        max_leg_notional: amount(source.max_leg_notional, quote)?,
        max_gross_notional: amount(source.max_gross_notional, quote)?,
        max_net_notional: amount(source.max_net_notional, quote)?,
        base_half_spread_bps: source.base_half_spread_bps,
        inventory_skew_bps: source.inventory_skew_bps,
        volatility_multiplier: source.volatility_multiplier,
        volatility_period: 20,
        refresh_interval_ms: source.quote_refresh_ms,
        max_market_age_ms: MARKET_MAX_AGE_MS,
        max_private_age_ms: PRIVATE_MAX_AGE_MS,
        // These are deliberately account-wide USD circuit breakers, including the existing SOL
        // strategy, fees and funding. They are not represented as XRP-only realized performance.
        max_loss_quote: amount(source.max_loss_quote, "USD")?,
        max_drawdown_quote: amount(source.max_drawdown_quote, "USD")?,
    })
}

pub(super) fn order(source: &TerminalOpenOrder) -> Result<Order, Error> {
    if source.position_side == PositionSide::Net
        || source.client_order_id.trim().is_empty()
        || source
            .native_order_id
            .as_ref()
            .is_none_or(|id| id.trim().is_empty())
        || source.limit_price.is_none()
    {
        return Err(Error::Facts);
    }
    let close = matches!(
        (source.order_side, source.position_side),
        (OrderSide::Sell, PositionSide::Long) | (OrderSide::Buy, PositionSide::Short)
    );
    let result = Order {
        order_id: source.native_order_id.clone().ok_or(Error::Facts)?,
        client_order_id: FieldState::Known(source.client_order_id.clone()),
        symbol: source.symbol.clone(),
        side: source.order_side,
        position_side: FieldState::Known(source.position_side),
        purpose: FieldState::Missing,
        state: match source.state {
            TerminalOrderState::New => OrderState::New,
            TerminalOrderState::PartiallyFilled => OrderState::PartiallyFilled,
        },
        quantity: source.quantity,
        filled_quantity: source.filled_quantity.ok_or(Error::Facts)?,
        limit_price: source
            .limit_price
            .map(Price::new)
            .transpose()
            .map_err(|_| Error::Facts)?,
        time_in_force: source
            .time_in_force
            .map_or(FieldState::Missing, FieldState::Known),
        average_price: FieldState::Missing,
        // Binance Hedge close identity is direction+positionSide; native reduceOnly is absent.
        reduce_only: close,
    };
    result.validate().map_err(|_| Error::Facts)?;
    Ok(result)
}

pub(super) fn positions(
    projection: &TerminalAccountProjection,
    symbol: &venue_domain::Symbol,
) -> Result<(Decimal, Decimal), Error> {
    if projection.position_mode != venue_control_protocol::kol::TerminalPositionMode::Hedge
        || projection.private_generation == 0
    {
        return Err(Error::Facts);
    }
    let (mut long, mut short) = (None, None);
    for position in projection.positions.iter().filter(|p| &p.symbol == symbol) {
        let slot = match position.position_side {
            PositionSide::Long => &mut long,
            PositionSide::Short => &mut short,
            PositionSide::Net => return Err(Error::Facts),
        };
        if slot.is_some() || position.quantity < Decimal::ZERO {
            return Err(Error::Facts);
        }
        *slot = Some(position.quantity);
    }
    // Absence is zero only inside the caller's verified complete Hedge account projection.
    Ok((
        long.unwrap_or(Decimal::ZERO),
        short.unwrap_or(Decimal::ZERO),
    ))
}

pub(super) fn fresh(observed: u64, now: u64, age: u64) -> bool {
    observed > 0 && observed <= now && now - observed <= age
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::kol::{
        TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalPosition, TerminalPositionMode,
    };

    pub(super) fn projection() -> TerminalAccountProjection {
        TerminalAccountProjection {
            schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: "00000000-0000-4000-8000-000000000001".into(),
            trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
            observed_ms: 100,
            persisted_ms: 100,
            private_generation: 1,
            position_mode: TerminalPositionMode::Hedge,
            positions: vec![],
            position_history: vec![],
            open_orders: vec![],
            conditional_orders: vec![],
            fills: vec![],
            assets: vec![],
        }
    }

    #[test]
    fn complete_hedge_absence_is_zero_but_duplicates_net_and_negative_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut p = projection();
        let symbol = "XRP/USDC".parse()?;
        assert_eq!(positions(&p, &symbol)?, (Decimal::ZERO, Decimal::ZERO));
        let position = TerminalPosition {
            symbol: symbol.clone(),
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: Some(Decimal::from(2)),
        };
        p.positions.push(position.clone());
        assert_eq!(positions(&p, &symbol)?, (Decimal::ONE, Decimal::ZERO));
        p.positions.push(position);
        assert_eq!(positions(&p, &symbol), Err(Error::Facts));
        p.positions.pop();
        p.positions[0].quantity = -Decimal::ONE;
        assert_eq!(positions(&p, &symbol), Err(Error::Facts));
        p.positions.clear();
        p.position_mode = TerminalPositionMode::Net;
        assert_eq!(positions(&p, &symbol), Err(Error::Facts));
        Ok(())
    }

    #[test]
    fn hedge_close_uses_direction_and_preserves_subminimum_notional()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut source = TerminalOpenOrder {
            client_order_id: "mm-owned".into(),
            native_order_id: Some("123".into()),
            symbol: "XRP/USDC".parse()?,
            order_side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 1),
            filled_quantity: Some(Decimal::ZERO),
            limit_price: Some(Decimal::from(2)),
            time_in_force: None,
            post_only: true,
            reduce_only: false,
            state: TerminalOrderState::New,
            created_ms: Some(1),
        };
        let q = order(&source)?;
        assert!(q.reduce_only);
        assert_eq!(q.quantity, Decimal::new(1, 1));
        source.filled_quantity = None;
        assert_eq!(order(&source), Err(Error::Facts));
        source.filled_quantity = Some(Decimal::ZERO);
        source.native_order_id = None;
        assert_eq!(order(&source), Err(Error::Facts));
        Ok(())
    }

    #[test]
    fn freshness_does_not_accept_future_or_zero_observations() {
        assert!(!fresh(0, 100, 10));
        assert!(!fresh(101, 100, 10));
        assert!(!fresh(89, 100, 10));
        assert!(fresh(90, 100, 10));
    }
}
