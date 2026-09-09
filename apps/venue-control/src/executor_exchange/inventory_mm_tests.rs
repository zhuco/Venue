use super::*;
use venue_control_protocol::kol::{TerminalOpenOrder, TerminalOrderState, TerminalPosition};
use venue_domain::{Order, Price};

fn fixture() -> Result<(ExecutionRequest, TerminalAccountProjection), Box<dyn std::error::Error>> {
    let mut request = super::super::tests::grid_place_request(0)?;
    request.origin = ExecutorCommandOrigin::InventoryMm;
    request.credential_id = "00000000-0000-4000-8000-000000000001".into();
    request.trading_account_id = "00000000-0000-4000-8000-000000000002".into();
    let projection = TerminalAccountProjection {
        schema_version: 1,
        credential_id: request.credential_id.clone(),
        trading_account_id: request.trading_account_id.clone(),
        observed_ms: 1_000,
        persisted_ms: 1_000,
        private_generation: 7,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![],
        position_history: vec![],
        open_orders: vec![],
        conditional_orders: vec![],
        fills: vec![],
        assets: vec![],
    };
    Ok((request, projection))
}

#[test]
fn stream_dispatch_requires_fresh_bound_hedge_facts_and_closes_respect_existing_reservations()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut request, mut p) = fixture()?;
    assert!(mm_available_quantity(&request, &p, 5_900).is_ok());
    assert!(mm_available_quantity(&request, &p, 6_001).is_err());
    assert!(mm_available_quantity(&request, &p, 999).is_err());
    p.trading_account_id = "00000000-0000-4000-8000-000000000003".into();
    assert!(mm_available_quantity(&request, &p, 1_001).is_err());
    p.trading_account_id = request.trading_account_id.clone();
    p.position_mode = TerminalPositionMode::Net;
    assert!(mm_available_quantity(&request, &p, 1_001).is_err());
    p.position_mode = TerminalPositionMode::Hedge;
    request.order_kind = ExecutionOrderKind::Limit {
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(4, 3),
        price: Decimal::from(50_000),
        reducing: true,
        time_in_force: LimitTimeInForce::PostOnly,
    };
    p.positions.push(TerminalPosition {
        symbol: request.symbol.clone(),
        position_side: PositionSide::Long,
        quantity: Decimal::new(3, 3),
        entry_price: None,
        mark_price: Some(Decimal::from(50_000)),
    });
    assert_eq!(
        mm_available_quantity(&request, &p, 1_001)?,
        Decimal::new(3, 3)
    );
    p.open_orders.push(TerminalOpenOrder {
        client_order_id: "existing-close".into(),
        native_order_id: Some("123".into()),
        symbol: request.symbol.clone(),
        order_side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(2, 3),
        filled_quantity: Some(Decimal::new(1, 3)),
        limit_price: Some(Decimal::from(50_000)),
        time_in_force: Some(LimitTimeInForce::PostOnly),
        post_only: true,
        reduce_only: true,
        state: TerminalOrderState::PartiallyFilled,
        created_ms: Some(999),
    });
    assert_eq!(
        mm_available_quantity(&request, &p, 1_001)?,
        Decimal::new(2, 3)
    );
    p.open_orders[0].filled_quantity = None;
    assert!(mm_available_quantity(&request, &p, 1_001).is_err());
    Ok(())
}

#[test]
fn full_result_confirms_exact_order_while_ack_or_mismatch_stays_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut request, _) = fixture()?;
    let rules = parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        request.symbol.clone(),
        7,
    )?;
    let mut order = Order {
        order_id: "native".into(),
        client_order_id: FieldState::Known(request.client_order_id.clone()),
        symbol: request.symbol.clone(),
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: Decimal::new(1, 3),
        filled_quantity: Decimal::ZERO,
        limit_price: Some(Price::new(Decimal::from(50_000))?),
        time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
        average_price: FieldState::Missing,
        reduce_only: false,
    };
    assert_eq!(
        mm_result(&request, &rules, "native", None).state,
        ExecutionReadback::Unknown
    );
    let accepted = mm_result(&request, &rules, "native", Some(&order));
    assert_eq!(accepted.state, ExecutionReadback::Reconciled);
    assert!(accepted.order_fact.is_some());
    assert_eq!(
        mm_result(&request, &rules, "wrong-native", Some(&order)).state,
        ExecutionReadback::Unknown
    );
    request.order_kind = ExecutionOrderKind::CancelExact {
        native_order_id: Some("native".into()),
        target_client_order_id: Some(request.client_order_id.clone()),
    };
    order.state = OrderState::Filled;
    order.filled_quantity = order.quantity;
    let filled_before_cancel = mm_result(&request, &rules, "native", Some(&order));
    assert_eq!(filled_before_cancel.state, ExecutionReadback::Reconciled);
    assert_eq!(
        filled_before_cancel
            .order_fact
            .ok_or("missing cumulative fill")?
            .filled_quantity,
        order.quantity
    );
    order.client_order_id = FieldState::Known("other-client".into());
    assert_eq!(
        mm_result(&request, &rules, "native", Some(&order)).state,
        ExecutionReadback::Unknown
    );
    Ok(())
}
