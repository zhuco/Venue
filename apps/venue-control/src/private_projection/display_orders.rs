use super::*;
use rust_decimal::Decimal;

impl BinancePrivateProjectionStore {
    pub(super) async fn apply_display_fills(
        &self,
        owner: &str,
        projection: &mut TerminalAccountProjection,
    ) -> Result<(), PrivateProjectionError> {
        // Persisted authenticated cumulative quantities can update the display while the
        // complete inventory projection is still waiting for ACCOUNT_UPDATE. No dispatch gate changes.
        let rows: Vec<(serde_json::Value, String, String)> = sqlx::query_as(
            "SELECT fill_json, original_quantity::text, cumulative_filled_quantity::text \
             FROM venue_binance_account_fills WHERE owner_user_id=$1 AND trading_account_id=$2 \
             AND baseline_private_generation=$3 AND original_quantity IS NOT NULL \
             AND cumulative_filled_quantity IS NOT NULL ORDER BY cumulative_filled_quantity::numeric DESC LIMIT 500"
        ).bind(owner).bind(&projection.trading_account_id)
            .bind(i64::try_from(projection.private_generation).map_err(|_| PrivateProjectionError::Invalid)?)
            .fetch_all(&self.pool).await.map_err(|_| PrivateProjectionError::Unavailable)?;
        for (payload, original, cumulative) in rows {
            let fill: TerminalFill =
                serde_json::from_value(payload).map_err(|_| PrivateProjectionError::Invalid)?;
            let original = original
                .parse::<Decimal>()
                .map_err(|_| PrivateProjectionError::Invalid)?;
            let cumulative = cumulative
                .parse::<Decimal>()
                .map_err(|_| PrivateProjectionError::Invalid)?;
            apply(projection, &fill, original, cumulative);
        }
        Ok(())
    }
}

fn apply(
    projection: &mut TerminalAccountProjection,
    fill: &TerminalFill,
    original: Decimal,
    cumulative: Decimal,
) {
    if cumulative < Decimal::ZERO || cumulative > original {
        return;
    }
    projection.open_orders.retain_mut(|order| {
        if order.native_order_id.as_deref() != Some(fill.native_order_id.as_str())
            || order.symbol != fill.symbol
            || order.order_side != fill.order_side
            || order.position_side != fill.position_side
            || order.quantity != original
        {
            return true;
        }
        if order.filled_quantity.is_none_or(|old| cumulative > old) {
            order.filled_quantity = Some(cumulative);
            order.state = venue_control_protocol::kol::TerminalOrderState::PartiallyFilled;
        }
        cumulative < original
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn display_cumulative_never_regresses_and_does_not_change_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = TerminalAccountProjection {
            balance_observed_ms: None,
            schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: "account".into(),
            trading_account_id: "account".into(),
            observed_ms: 1,
            persisted_ms: 1,
            private_generation: 1,
            position_mode: TerminalPositionMode::Hedge,
            positions: vec![],
            position_history: vec![],
            open_orders: vec![],
            conditional_orders: vec![],
            fills: vec![],
            assets: vec![],
        };
        let inventory = projection.positions.clone();
        let fill = TerminalFill {
            native_trade_id: "t".into(),
            native_order_id: "o".into(),
            symbol: "BTC/USDT".parse()?,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: 1.into(),
            price: 10.into(),
            maker: None,
            occurred_ms: Some(1),
        };
        projection
            .open_orders
            .push(venue_control_protocol::kol::TerminalOpenOrder {
                client_order_id: "c".into(),
                native_order_id: Some("o".into()),
                symbol: fill.symbol.clone(),
                order_side: fill.order_side,
                position_side: fill.position_side,
                quantity: 50.into(),
                filled_quantity: Some(0.into()),
                limit_price: Some(10.into()),
                time_in_force: None,
                post_only: true,
                reduce_only: false,
                state: venue_control_protocol::kol::TerminalOrderState::New,
                created_ms: None,
            });
        apply(&mut projection, &fill, 50.into(), 1.into());
        assert_eq!(
            projection
                .open_orders
                .last()
                .and_then(|o| o.filled_quantity),
            Some(1.into())
        );
        apply(&mut projection, &fill, 50.into(), 0.into());
        assert_eq!(
            projection
                .open_orders
                .last()
                .and_then(|o| o.filled_quantity),
            Some(1.into())
        );
        apply(&mut projection, &fill, 50.into(), 50.into());
        assert!(
            !projection
                .open_orders
                .iter()
                .any(|o| o.native_order_id.as_deref() == Some("o"))
        );
        assert_eq!(projection.positions, inventory);
        Ok(())
    }
}
