use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use venue_control_protocol::kol::TerminalFill;

// One account projection and one symbol per call. Rebuild from unique trade facts, never add
// quantities on repeated UI frames or move the marker when another partial fill arrives.
pub(super) fn by_order(fills: &[TerminalFill], symbol: &str) -> Vec<TerminalFill> {
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
            total.price = notional?.checked_div(total.quantity)?;
            Some(total)
        })
        .collect::<Vec<_>>();
    result.sort_by(|a, b| {
        (a.occurred_ms, &a.native_order_id).cmp(&(b.occurred_ms, &b.native_order_id))
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let rows = by_order(&[b.clone(), a.clone(), a.clone(), c], "DOGE/USDC");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].quantity, Decimal::from(5));
        assert_eq!(rows[0].price, Decimal::from(16));
        assert_eq!(rows[0].occurred_ms, Some(1000));
        assert!(by_order(&[a, b], "BTC/USDC").is_empty());
        Ok(())
    }
}
