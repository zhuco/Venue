use venue_control_protocol::kol::TerminalAccountProjection;

#[derive(Debug)]
pub(super) struct Changes {
    pub fills: bool,
    pub cycles: bool,
    pub orders: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_view::tests::private_projection;

    #[test]
    fn clock_assets_and_marks_do_not_rebuild_history_but_identity_does()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut old = private_projection("00000000-0000-4000-8000-000000000003", 10_000);
        old.positions
            .push(venue_control_protocol::kol::TerminalPosition {
                symbol: "BTC/USDC".parse()?,
                position_side: venue_domain::PositionSide::Long,
                quantity: 1.into(),
                entry_price: Some(10.into()),
                mark_price: Some(11.into()),
            });
        let mut next = old.clone();
        next.observed_ms += 1;
        next.persisted_ms += 1;
        next.positions[0].mark_price = Some(12.into());
        next.assets
            .push(venue_control_protocol::kol::TerminalAsset {
                asset: "USD".into(),
                equity: 100.into(),
                available_margin: None,
            });
        let changes = Changes::between(Some(&old), &next);
        assert!(!changes.fills && !changes.cycles && !changes.orders);
        next.positions[0].quantity = 2.into();
        assert!(Changes::between(Some(&old), &next).cycles);
        next = old.clone();
        next.private_generation += 1;
        let changes = Changes::between(Some(&old), &next);
        assert!(changes.fills && changes.cycles && changes.orders);
        next = old.clone();
        next.trading_account_id = "00000000-0000-4000-8000-000000000004".into();
        assert!(Changes::between(Some(&old), &next).fills);
        Ok(())
    }

    #[test]
    fn same_length_fill_correction_and_future_time_boundary_invalidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut old = private_projection("00000000-0000-4000-8000-000000000003", 10_000);
        old.fills.push(venue_control_protocol::kol::TerminalFill {
            symbol: "BTC/USDC".parse()?,
            native_order_id: "order".into(),
            native_trade_id: "trade".into(),
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: 1.into(),
            price: 10.into(),
            maker: None,
            occurred_ms: Some(10_001),
        });
        let mut next = old.clone();
        next.observed_ms = 10_001;
        let changes = Changes::between(Some(&old), &next);
        assert!(!changes.fills && changes.cycles);
        next.fills[0].price = 11.into();
        let changes = Changes::between(Some(&old), &next);
        assert!(changes.fills && changes.cycles);
        Ok(())
    }
}

impl Changes {
    pub(super) fn between(
        old: Option<&TerminalAccountProjection>,
        next: &TerminalAccountProjection,
    ) -> Self {
        let Some(old) = old.filter(|old| {
            old.credential_id == next.credential_id
                && old.trading_account_id == next.trading_account_id
                && old.private_generation == next.private_generation
                && old.position_mode == next.position_mode
        }) else {
            return Self {
                fills: true,
                cycles: true,
                orders: true,
            };
        };
        let fills = old.fills != next.fills;
        // Cycles use inventory and zero observations, not entry/mark estimates.
        let inventory_changed = old.positions.len() != next.positions.len()
            || old.positions.iter().zip(&next.positions).any(|(a, b)| {
                (&a.symbol, a.position_side, a.quantity) != (&b.symbol, b.position_side, b.quantity)
            });
        // Time is a dependency only when a previously future fact becomes eligible.
        let became_observed = |time: u64| time > old.observed_ms && time <= next.observed_ms;
        let time_boundary = next
            .fills
            .iter()
            .any(|f| f.occurred_ms.is_some_and(became_observed))
            || next
                .position_history
                .iter()
                .any(|p| became_observed(p.observed_ms));
        let history_changed = old.position_history.len() != next.position_history.len()
            || old
                .position_history
                .iter()
                .zip(&next.position_history)
                .any(|(a, b)| {
                    (
                        a.observed_ms,
                        &a.position.symbol,
                        a.position.position_side,
                        a.position.quantity,
                    ) != (
                        b.observed_ms,
                        &b.position.symbol,
                        b.position.position_side,
                        b.position.quantity,
                    )
                });
        Self {
            fills,
            cycles: fills || inventory_changed || history_changed || time_boundary,
            orders: old.open_orders != next.open_orders,
        }
    }
}
