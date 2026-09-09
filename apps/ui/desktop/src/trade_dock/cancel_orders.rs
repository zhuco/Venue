use super::*;
use crate::trading::TerminalOrderSelection;

pub(super) fn targets(model: &AppModel, all: bool) -> Result<Vec<TerminalOrderSelection>, String> {
    let scope = model
        .confirmed_account_scope()
        .filter(|scope| {
            scope.venue == venue_control_protocol::VenueId::Binance
                && scope.venue == model.preferences.market_server.venue()
        })
        .ok_or("请先选择有效交易账户")?;
    if !model
        .execution
        .private_ready(Some(&scope.trading_account_id), now_ms())
    {
        return Err("账户数据待更新".into());
    }
    let projection = model
        .execution
        .private_projection_for(Some(&scope.trading_account_id))
        .filter(|p| p.credential_id == scope.credential_id)
        .ok_or("账户数据待更新")?;
    let targets = projection
        .open_orders
        .iter()
        .filter(|order| {
            (all || order.symbol.to_string() == model.preferences.selected_symbol)
                && order
                    .filled_quantity
                    .is_none_or(|filled| filled < order.quantity)
        })
        .filter_map(|order| {
            Some(TerminalOrderSelection {
                credential_id: scope.credential_id.clone(),
                trading_account_id: scope.trading_account_id.clone(),
                symbol: order.symbol.clone(),
                native_order_id: order.native_order_id.clone()?,
            })
        })
        .filter(|target| !model.execution.chart_orders.is_pending(target))
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err("当前范围没有可撤挂单".into());
    }
    Ok(targets)
}

pub(super) fn submit(
    model: &mut AppModel,
    client: &ControlClient,
    all: bool,
    context: &egui::Context,
) {
    model.synchronize_trading_scope();
    let targets = match targets(model, all) {
        Ok(targets) => targets,
        Err(error) => {
            local_failure(model, error);
            return;
        }
    };
    let scope = model.confirmed_account_scope();
    let mut submitted = 0;
    for target in targets {
        let request_id = model.next_terminal_request_id();
        let request = TerminalCancelRequest {
            replacement_price: None,
            schema_version: TERMINAL_SCHEMA_VERSION,
            request_id: request_id.clone(),
            credential_id: target.credential_id.clone(),
            symbol: target.symbol.clone(),
            native_order_id: target.native_order_id.clone(),
        };
        if let Err(error) = client.send_terminal_cancel(request, scope.clone()) {
            local_failure(
                model,
                format!("已提交 {submitted} 笔撤单，其余未提交：{error}"),
            );
            return;
        }
        model
            .execution
            .chart_orders
            .submitted_cancel(target, request_id.clone(), context);
        model.execution.begin_terminal_submission(request_id);
        submitted += 1;
    }
    model.trade_dock.clear_order_selection();
    model.notice(format!("已提交 {submitted} 笔撤单"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_scope::tests::{model, projection};
    use venue_control_protocol::kol::*;
    #[test]
    fn scopes_exclude_other_symbols_pending_orders_and_unconfirmed_accounts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut model = model();
        model.select_symbol("BTC/USDC".into());
        let mut facts = projection(1);
        for (id, symbol) in [("a", "BTC/USDC"), ("b", "BTC/USDC"), ("c", "ETH/USDC")] {
            facts.open_orders.push(TerminalOpenOrder {
                client_order_id: id.into(),
                native_order_id: Some(id.into()),
                symbol: symbol.parse()?,
                order_side: venue_domain::OrderSide::Buy,
                position_side: venue_domain::PositionSide::Long,
                quantity: 1.into(),
                filled_quantity: Some(0.into()),
                limit_price: Some(100.into()),
                time_in_force: None,
                post_only: true,
                reduce_only: false,
                state: TerminalOrderState::New,
                created_ms: Some(facts.observed_ms),
            });
        }
        model
            .execution
            .apply_private(Some(facts.clone()), &mut model.trade_dock);
        assert_eq!(targets(&model, false)?.len(), 2);
        assert_eq!(targets(&model, true)?.len(), 3);
        let target = targets(&model, false)?.remove(0);
        model.execution.chart_orders.submitted_cancel(
            target,
            "request".into(),
            &egui::Context::default(),
        );
        assert_eq!(targets(&model, false)?.len(), 1);
        assert_eq!(targets(&model, true)?.len(), 2);
        let (client, probe) = ControlClient::fixture();
        let scope = model.confirmed_account_scope().ok_or("account scope")?;
        client.subscribe_terminal(crate::account_scope::Scoped {
            value: TerminalProjectionRequest {
                schema_version: 1,
                credential_id: scope.credential_id.clone(),
                symbols: vec!["BTC/USDC".parse()?],
            },
            scope,
        });
        submit(&mut model, &client, true, &egui::Context::default());
        assert_eq!(probe.count(), 2);
        submit(&mut model, &client, true, &egui::Context::default());
        assert_eq!(probe.count(), 2, "pending batch must not be resent");
        assert_eq!(
            model
                .execution
                .private_projection_for(model.preferences.execution_account_id.as_deref()),
            Some(&facts)
        );
        model.begin_account_selection(crate::account_scope::tests::id(2));
        assert!(targets(&model, false).is_err());
        assert!(targets(&model, true).is_err());
        Ok(())
    }
}
