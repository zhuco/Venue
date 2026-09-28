use super::SINGLE_SIDE_ROWS;
use crate::model::AppModel;
use eframe::egui;
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use venue_control_protocol::UiBookLevel;

type Totals = Arc<(Vec<Decimal>, Vec<Decimal>)>;
#[derive(Clone, Default)]
struct DepthCache {
    asks: Vec<UiBookLevel>,
    bids: Vec<UiBookLevel>,
    totals: Totals,
}
impl DepthCache {
    fn update(&mut self, asks: &[UiBookLevel], bids: &[UiBookLevel]) -> Totals {
        // Content is the revision for old Control/preview snapshots without field sequence IDs.
        // Compare only the bounded visible depth; no arithmetic on unchanged snapshots.
        let asks = &asks[..asks.len().min(SINGLE_SIDE_ROWS)];
        let bids = &bids[..bids.len().min(SINGLE_SIDE_ROWS)];
        if self.asks != asks || self.bids != bids {
            fn totals(levels: &[UiBookLevel]) -> Vec<Decimal> {
                let mut sum = Decimal::ZERO;
                levels
                    .iter()
                    .map(|level| {
                        sum += level.quantity;
                        sum
                    })
                    .collect()
            }
            self.totals = Arc::new((totals(asks), totals(bids)));
            self.asks = asks.to_vec();
            self.bids = bids.to_vec();
        }
        self.totals.clone()
    }
}

pub(super) fn depth(
    ctx: &egui::Context,
    model: &AppModel,
    symbol: &str,
    asks: &[UiBookLevel],
    bids: &[UiBookLevel],
) -> Totals {
    let id = egui::Id::new("shared-book-depth");
    ctx.data_mut(|data| {
        let caches = data.get_temp_mut_or_default::<BTreeMap<String, DepthCache>>(id);
        let key = format!("{}:{symbol}", model.preferences.market_server.label());
        if caches.len() >= 16 && !caches.contains_key(&key) {
            caches.clear();
        }
        caches.entry(key).or_default().update(asks, bids)
    })
}

#[derive(Clone, Default)]
struct OrderIndex {
    scope: Option<crate::account_scope::AccountScope>,
    revision: u64,
    prices: BTreeMap<String, (BTreeSet<Decimal>, BTreeSet<Decimal>)>,
}
pub(super) fn own_order_marks(
    ctx: &egui::Context,
    model: &AppModel,
    symbol: &str,
    asks: &[UiBookLevel],
    bids: &[UiBookLevel],
    limit: usize,
) -> ([bool; SINGLE_SIDE_ROWS], [bool; SINGLE_SIDE_ROWS]) {
    let mut marks = ([false; SINGLE_SIDE_ROWS], [false; SINGLE_SIDE_ROWS]);
    let scope = model.confirmed_account_scope();
    let revision = model.execution.open_orders_revision();
    ctx.data_mut(|data| {
        let index =
            data.get_temp_mut_or_default::<OrderIndex>(egui::Id::new("shared-own-order-prices"));
        if index.scope != scope || index.revision != revision {
            index.prices.clear();
            index.scope = scope.clone();
            index.revision = revision;
            if let Some(scope) = &scope {
                if let Some(projection) = model
                    .execution
                    .private_projection_for(Some(&scope.trading_account_id))
                    .filter(|p| p.credential_id == scope.credential_id)
                {
                    for order in &projection.open_orders {
                        if order.quantity <= Decimal::ZERO
                            || order.filled_quantity.is_some_and(|v| v >= order.quantity)
                        {
                            continue;
                        }
                        let Some(price) = order.limit_price.filter(|p| *p > Decimal::ZERO) else {
                            continue;
                        };
                        let sides = index.prices.entry(order.symbol.to_string()).or_default();
                        match order.order_side {
                            venue_domain::OrderSide::Sell => &mut sides.0,
                            venue_domain::OrderSide::Buy => &mut sides.1,
                        }
                        .insert(price);
                    }
                }
            }
        }
        if scope
            .as_ref()
            .is_none_or(|s| s.venue != model.preferences.market_server.venue())
        {
            return;
        }
        if let Some(prices) = index.prices.get(symbol) {
            for ((levels, prices), marks) in [(asks, &prices.0), (bids, &prices.1)]
                .into_iter()
                .zip([&mut marks.0, &mut marks.1])
            {
                for (i, level) in levels.iter().take(limit.min(SINGLE_SIDE_ROWS)).enumerate() {
                    marks[i] = prices.contains(&level.price);
                }
            }
        }
    });
    marks
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unchanged_depth_reuses_totals_but_quantity_change_recalculates() {
        let mut cache = DepthCache::default();
        let mut levels = vec![UiBookLevel {
            price: Decimal::ONE,
            quantity: Decimal::from(2),
        }];
        let first = cache.update(&levels, &[]);
        assert!(Arc::ptr_eq(&first, &cache.update(&levels, &[])));
        levels[0].quantity = Decimal::from(3);
        let changed = cache.update(&levels, &[]);
        assert!(!Arc::ptr_eq(&first, &changed));
        assert_eq!(changed.0, vec![Decimal::from(3)]);
        assert!(cache.update(&[], &[]).0.is_empty());
    }
    #[test]
    fn private_order_changes_invalidate_without_public_depth_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        use venue_control_protocol::kol::{TerminalOpenOrder, TerminalOrderState};
        let ctx = egui::Context::default();
        let mut model = crate::account_scope::tests::model();
        let mut projection = crate::account_scope::tests::projection(1);
        projection.open_orders.push(TerminalOpenOrder {
            client_order_id: "own".into(),
            native_order_id: Some("native".into()),
            symbol: "BTC/USDC".parse()?,
            order_side: venue_domain::OrderSide::Buy,
            position_side: venue_domain::PositionSide::Long,
            quantity: Decimal::ONE,
            filled_quantity: None,
            limit_price: Some(Decimal::from(100)),
            time_in_force: None,
            post_only: false,
            reduce_only: false,
            state: TerminalOrderState::New,
            created_ms: None,
        });
        let bids = [UiBookLevel {
            price: Decimal::from(100),
            quantity: Decimal::ONE,
        }];
        model
            .execution
            .apply_private(Some(projection.clone()), &mut model.trade_dock);
        assert!(own_order_marks(&ctx, &model, "BTC/USDC", &[], &bids, 6).1[0]);
        assert!(!own_order_marks(&ctx, &model, "ETH/USDC", &[], &bids, 6).1[0]);
        projection.open_orders[0].filled_quantity = Some(Decimal::ONE);
        model
            .execution
            .apply_private(Some(projection), &mut model.trade_dock);
        assert!(!own_order_marks(&ctx, &model, "BTC/USDC", &[], &bids, 6).1[0]);
        model.begin_account_selection(crate::account_scope::tests::id(2));
        assert!(!own_order_marks(&ctx, &model, "BTC/USDC", &[], &bids, 6).1[0]);
        Ok(())
    }
}
