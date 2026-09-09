use super::*;
use venue_domain::OrderSide;

pub(super) fn observe(
    model: &mut AppModel,
    symbol: &venue_domain::Symbol,
    trade: &venue_control_protocol::UiTrade,
    context: &egui::Context,
) {
    let now = crate::account_center::now_ms();
    if trade.occurred_ms > now || now.saturating_sub(trade.occurred_ms) > 5_000 {
        return;
    }
    let Some(projection) = model
        .execution
        .private_projection_for(model.preferences.execution_account_id.as_deref())
        .filter(|projection| {
            model
                .selected_execution_credential()
                .is_some_and(|credential| {
                    credential.venue == model.preferences.market_server.venue()
                        && credential.credential_id == projection.credential_id
                        && credential.trading_account_id.as_ref()
                            == Some(&projection.trading_account_id)
                })
        })
    else {
        return;
    };
    if !model
        .execution
        .private_ready(model.preferences.execution_account_id.as_deref(), now)
        || trade.occurred_ms <= projection.observed_ms
    {
        return;
    }
    let targets = projection
        .open_orders
        .iter()
        .filter(|order| {
            &order.symbol == symbol
                && order
                    .created_ms
                    .is_none_or(|created| trade.occurred_ms > created)
                && order
                    .filled_quantity
                    .is_none_or(|filled| filled < order.quantity)
                && order
                    .limit_price
                    .is_some_and(|limit| crossed(order.order_side, limit, trade.price))
        })
        .filter_map(|order| {
            Some(crate::trading::TerminalOrderSelection {
                credential_id: projection.credential_id.clone(),
                trading_account_id: projection.trading_account_id.clone(),
                symbol: order.symbol.clone(),
                native_order_id: order.native_order_id.clone()?,
            })
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return;
    }
    for target in targets {
        model
            .execution
            .chart_orders
            .crossed_price(target, trade.occurred_ms);
    }
    context.request_repaint();
}

fn crossed(side: OrderSide, limit: rust_decimal::Decimal, price: rust_decimal::Decimal) -> bool {
    limit > rust_decimal::Decimal::ZERO
        && price > rust_decimal::Decimal::ZERO
        && match side {
            OrderSide::Buy => price < limit,
            OrderSide::Sell => price > limit,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_strict_trade_through_on_the_fill_side_hides_a_limit() {
        for (side, price, expected) in [
            (OrderSide::Buy, 99, true),
            (OrderSide::Buy, 100, false),
            (OrderSide::Buy, 101, false),
            (OrderSide::Sell, 101, true),
            (OrderSide::Sell, 100, false),
            (OrderSide::Sell, 99, false),
            (OrderSide::Buy, 0, false),
        ] {
            assert_eq!(crossed(side, 100.into(), price.into()), expected);
        }
    }
}
