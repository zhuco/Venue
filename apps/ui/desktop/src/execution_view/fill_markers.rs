use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use venue_control_protocol::kol::TerminalFill;

#[derive(Debug, Default)]
pub(super) struct FillMarkers {
    symbols: BTreeMap<String, Vec<TerminalFill>>,
    notionals: BTreeMap<(String, String), Decimal>,
}

impl FillMarkers {
    pub(super) fn rebuild(fills: &[TerminalFill]) -> Self {
        let symbols: BTreeSet<_> = fills.iter().map(|fill| fill.symbol.to_string()).collect();
        let mut result = Self::default();
        for symbol in symbols {
            let rows = aggregate_orders(fills, &symbol)
                .into_iter()
                .map(|(fill, notional)| {
                    result
                        .notionals
                        .insert((symbol.clone(), fill.native_order_id.clone()), notional);
                    fill
                })
                .collect();
            result.symbols.insert(symbol, rows);
        }
        result
    }

    pub(super) fn for_symbol(&self, symbol: &str) -> &[TerminalFill] {
        self.symbols.get(symbol).map_or(&[], Vec::as_slice)
    }

    pub(super) fn execution(
        &self,
        symbol: &str,
        native_order_id: &str,
    ) -> Option<(Decimal, Decimal)> {
        let notional = *self
            .notionals
            .get(&(symbol.to_owned(), native_order_id.to_owned()))?;
        let fill = self
            .for_symbol(symbol)
            .iter()
            .find(|fill| fill.native_order_id == native_order_id)?;
        Some((notional, fill.price))
    }
}

// One account projection and one symbol per call. Rebuild from unique trade facts, never add
// quantities on repeated UI frames or move the marker when another partial fill arrives.
fn aggregate_orders(fills: &[TerminalFill], symbol: &str) -> Vec<(TerminalFill, Decimal)> {
    let mut seen = BTreeSet::new();
    let mut grouped: BTreeMap<&str, (TerminalFill, Option<Decimal>)> = BTreeMap::new();
    for fill in fills.iter().filter(|f| f.symbol.to_string() == symbol) {
        if fill.native_order_id.is_empty()
            || !seen.insert((&fill.native_order_id, &fill.native_trade_id))
        {
            continue;
        }
        let notional = fill.price.checked_mul(fill.quantity);
        match grouped.entry(&fill.native_order_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((fill.clone(), notional));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let (total, value) = entry.get_mut();
                *value = value.and_then(|old| notional.and_then(|new| old.checked_add(new)));
                if let Some(quantity) = total.quantity.checked_add(fill.quantity) {
                    total.quantity = quantity;
                } else {
                    *value = None;
                }
                total.occurred_ms = total.occurred_ms.into_iter().chain(fill.occurred_ms).min();
            }
        }
    }
    let mut result = grouped
        .into_values()
        .filter_map(|(mut total, notional)| {
            let notional = notional?;
            total.price = notional.checked_div(total.quantity)?;
            Some((total, notional))
        })
        .collect::<Vec<_>>();
    result.sort_by(|(a, _), (b, _)| {
        (a.occurred_ms, &a.native_order_id).cmp(&(b.occurred_ms, &b.native_order_id))
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ten_then_four_hundred_ninety_updates_the_same_marker()
    -> Result<(), Box<dyn std::error::Error>> {
        let a = TerminalFill {
            native_order_id: "o".into(),
            native_trade_id: "t1".into(),
            symbol: "BTC/USDT".parse()?,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: 1.into(),
            price: 10.into(),
            maker: Some(true),
            occurred_ms: Some(1),
        };
        let mut state = crate::execution_view::ExecutionViewState::default();
        let mut dock = crate::trading::TradeDockState::default();
        let mut projection = crate::account_scope::tests::projection(1);
        projection.fills.push(a.clone());
        state.apply_private(Some(projection.clone()), &mut dock);
        assert_eq!(
            state.fill_execution("BTC/USDT", "o").map(|v| v.0),
            Some(10.into())
        );
        let mut b = a.clone();
        b.native_trade_id = "t2".into();
        b.quantity = 49.into();
        b.occurred_ms = Some(2);
        projection.fills.extend([b, a]);
        // The same account timestamp and inventory must not suppress changed trade facts.
        state.apply_private(Some(projection), &mut dock);
        assert_eq!(
            state.fill_execution("BTC/USDT", "o").map(|v| v.0),
            Some(500.into())
        );
        assert_eq!(state.fill_markers("BTC/USDT").len(), 1);
        assert_eq!(state.fill_markers("BTC/USDT")[0].occurred_ms, Some(1));
        Ok(())
    }
    #[test]
    fn partial_fills_have_one_marker_total_quantity_and_weighted_price()
    -> Result<(), Box<dyn std::error::Error>> {
        let a = TerminalFill {
            native_trade_id: "t1".into(),
            native_order_id: "o1".into(),
            symbol: "DOGE/USDC".parse()?,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: 2.into(),
            price: 10.into(),
            maker: Some(true),
            occurred_ms: Some(1000),
        };
        let mut b = a.clone();
        b.native_trade_id = "t2".into();
        b.quantity = 3.into();
        b.price = 20.into();
        b.occurred_ms = Some(70000);
        let mut c = a.clone();
        c.native_order_id = "o2".into();
        let markers = FillMarkers::rebuild(&[b.clone(), a.clone(), a.clone(), c]);
        let rows = markers.for_symbol("DOGE/USDC");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].quantity, Decimal::from(5));
        assert_eq!(rows[0].price, Decimal::from(16));
        assert_eq!(rows[0].occurred_ms, Some(1000));
        assert!(markers.for_symbol("BTC/USDC").is_empty());
        assert_eq!(
            markers.execution("DOGE/USDC", "o1"),
            Some((80.into(), 16.into()))
        );
        assert_eq!(markers.execution("BTC/USDC", "o1"), None);
        assert_eq!(markers.execution("DOGE/USDC", "missing"), None);
        Ok(())
    }
}
