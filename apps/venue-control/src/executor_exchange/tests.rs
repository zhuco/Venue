use secrecy::SecretString;
use venue_domain::domain::{Order, Price};

use super::*;

fn credentials() -> Result<BinanceCredentials, Box<dyn std::error::Error>> {
    Ok(BinanceCredentials::from_secrets(
        SecretString::from("a".repeat(32)),
        SecretString::from("b".repeat(32)),
    )?)
}

#[tokio::test]
async fn mock_submit_is_stable_by_client_order_id() -> Result<(), Box<dyn std::error::Error>> {
    let request = ExecutionRequest {
        command_id: "command-a".into(),
        client_order_id: "client-a".into(),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::Market {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            reducing: false,
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    };
    let mut exchange = MockBinanceExecution::default();
    assert_eq!(
        exchange.submit(&request, credentials()?).await?,
        outcome(ExecutionReadback::Accepted, Some("mock-client-a".into()))
    );
    exchange.set_readback("client-a".into(), ExecutionReadback::Reconciled);
    assert_eq!(
        exchange.submit(&request, credentials()?).await?,
        outcome(ExecutionReadback::Reconciled, Some("mock-client-a".into()))
    );
    assert_eq!(
        exchange.readback(&request, credentials()?).await?,
        outcome(ExecutionReadback::Reconciled, Some("mock-client-a".into()))
    );
    Ok(())
}

fn grid_place_request(index: usize) -> Result<ExecutionRequest, Box<dyn std::error::Error>> {
    Ok(ExecutionRequest {
        command_id: format!("place-command-{index}"),
        client_order_id: format!("place-client-{index}"),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::LimitPostOnly {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            price: Decimal::new(50_000, 0),
            reducing: false,
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    })
}

fn grid_cancel_request(index: usize) -> Result<ExecutionRequest, Box<dyn std::error::Error>> {
    Ok(ExecutionRequest {
        command_id: format!("cancel-command-{index}"),
        client_order_id: format!("cancel-client-{index}"),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::CancelExact {
            native_order_id: Some(format!("native-target-{index}")),
            target_client_order_id: Some(format!("owned-target-{index}")),
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    })
}

fn grid_batch_context() -> GridBatchExecutionContext {
    GridBatchExecutionContext {
        batch_id: "grid-batch".to_owned(),
        owner_user_id: "owner".to_owned(),
        durable: None,
    }
}

fn hot_batch_context(now: u64) -> GridBatchExecutionContext {
    GridBatchExecutionContext {
        batch_id: "hot-grid-batch".to_owned(),
        owner_user_id: "owner".to_owned(),
        durable: Some(crate::kol_executor::GridBatchDispatchContext {
            batch_digest: [7; 32],
            private_generation: 9,
            private_observed_ms: now,
            instrument_generation: 7,
            source_event_received_ms: Some(now),
            private_projection_current: true,
        }),
    }
}

fn hot_token(now: u64) -> Result<crate::GridHotDispatchToken, Box<dyn std::error::Error>> {
    let rules = venue_gateway_binance::parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        "BTC/USDT".parse()?,
        7,
    )?;
    Ok(crate::GridHotDispatchToken {
        batch_id: "hot-grid-batch".to_owned(),
        batch_digest: [7; 32],
        owner_user_id: "owner".to_owned(),
        trading_account_id: "account-a".to_owned(),
        credential_id: "credential-a".to_owned(),
        symbol: "BTC/USDT".parse()?,
        private_generation: 9,
        private_observed_ms: now,
        source_event_received_ms: now,
        valid_until_ms: now.saturating_add(1_000),
        rules,
    })
}

#[test]
fn router_consumes_only_an_exact_durable_hot_token() -> Result<(), Box<dyn std::error::Error>> {
    let now = now_ms()?;
    let cache = crate::GridHotDispatchCache::new();
    let router = BinanceExecutionRouter::with_hot_dispatch(
        venue_gateway_binance::BinanceTransportLimits::new(
            std::time::Duration::from_secs(1),
            1_024,
        )?,
        cache.clone(),
    );
    let requests = vec![grid_place_request(0)?, grid_cancel_request(0)?];
    assert!(cache.publish(hot_token(now)?));
    assert!(
        router
            .take_matching_hot_token(&hot_batch_context(now), &requests)
            .is_some()
    );
    assert!(
        router
            .take_matching_hot_token(&hot_batch_context(now), &requests)
            .is_none(),
        "a committed hot token is one-shot"
    );

    assert!(cache.publish(hot_token(now)?));
    let mut mismatched = hot_batch_context(now);
    if let Some(durable) = &mut mismatched.durable {
        durable.batch_digest = [8; 32];
    }
    assert!(
        router
            .take_matching_hot_token(&mismatched, &requests)
            .is_none()
    );
    assert!(cache.take("hot-grid-batch").is_none());

    assert!(cache.publish(hot_token(now)?));
    let mut superseded = hot_batch_context(now);
    if let Some(durable) = &mut superseded.durable {
        durable.private_projection_current = false;
    }
    assert!(
        router
            .take_matching_hot_token(&superseded, &requests)
            .is_none(),
        "a superseded durable private projection must force signed cold preflight"
    );
    assert!(
        cache.take("hot-grid-batch").is_none(),
        "a superseded token must be discarded rather than retained for a later claim"
    );
    Ok(())
}

#[tokio::test]
async fn grid_batch_timing_tracks_send_span_without_hard_coding_fill_count()
-> Result<(), Box<dyn std::error::Error>> {
    for (places, cancels) in [(2_usize, 1_usize), (4_usize, 2_usize)] {
        let mut requests = (0..places)
            .map(grid_place_request)
            .collect::<Result<Vec<_>, _>>()?;
        requests.extend(
            (0..cancels)
                .map(grid_cancel_request)
                .collect::<Result<Vec<_>, _>>()?,
        );
        let mut exchange = MockBinanceExecution::default();
        let outcome = exchange
            .submit_grid_batch(&grid_batch_context(), &requests, credentials()?)
            .await?;
        assert_eq!(outcome.commands.len(), places + cancels);
        assert_eq!(
            usize::from(outcome.timing.outbound_attempts),
            places + cancels
        );
        assert!(outcome.timing.executor_start_to_first_submit_us.is_some());
        assert!(outcome.timing.executor_start_to_last_submit_us.is_some());
        assert!(outcome.timing.first_to_last_submit_us.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn grid_batch_rejects_any_place_after_cancel_before_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let requests = vec![
        grid_place_request(0)?,
        grid_cancel_request(0)?,
        grid_place_request(1)?,
    ];
    let mut exchange = MockBinanceExecution::default();
    assert_eq!(
        exchange
            .submit_grid_batch(&grid_batch_context(), &requests, credentials()?)
            .await,
        Err(GridBatchSubmitError::DefinitelyNotDispatched(
            BinanceExecutionError::Invalid,
        ))
    );
    assert!(exchange.orders.is_empty());
    Ok(())
}

#[test]
fn post_dispatch_ambiguity_never_becomes_explicit_rejection() {
    for error in [
        BinanceTransportError::Ack,
        BinanceTransportError::BodyTooLarge,
        BinanceTransportError::Clock,
    ] {
        assert_eq!(
            dispatch_failed(error, None).map(|outcome| outcome.state),
            Ok(ExecutionReadback::Unknown)
        );
    }
    for error in [
        BinanceTransportError::Timeout,
        BinanceTransportError::Disconnected,
        BinanceTransportError::AmbiguousStatus(503),
    ] {
        assert_eq!(
            dispatch_unknown(error, None).state,
            ExecutionReadback::Unknown
        );
    }
    assert_eq!(
        dispatch_failed(BinanceTransportError::HttpStatus(400), None).map(|outcome| outcome.state),
        Ok(ExecutionReadback::Rejected)
    );
    assert_eq!(
        dispatch_failed(BinanceTransportError::HttpStatus(429), None).map(|outcome| outcome.state),
        Ok(ExecutionReadback::Rejected)
    );
}

#[test]
fn transport_preflight_failure_stays_not_dispatched() {
    assert_eq!(
        dispatch_failed(BinanceTransportError::Binding, None),
        Err(BinanceExecutionError::Invalid)
    );
    assert_eq!(
        dispatch_failed(BinanceTransportError::Signing, None),
        Err(BinanceExecutionError::Unavailable)
    );
}

#[test]
fn not_dispatched_failures_are_distinct_from_dispatch_uncertainty() {
    assert_eq!(
        BinanceExecutionError::Invalid.not_dispatched_code(),
        "not_dispatched_invalid"
    );
    assert_eq!(
        BinanceExecutionError::Unavailable.not_dispatched_code(),
        "not_dispatched_unavailable"
    );
    assert_ne!(
        BinanceExecutionError::Invalid.not_dispatched_code(),
        "dispatch_unknown"
    );
}

#[test]
fn native_place_states_are_exhaustive_and_unknown_never_accepts_post_only() {
    assert_eq!(
        place_readback_decision(OrderState::Unknown, Decimal::ZERO),
        PlaceReadbackDecision::Unknown
    );
    assert_eq!(
        place_readback_decision(OrderState::New, Decimal::ZERO),
        PlaceReadbackDecision::Accepted
    );
    assert_eq!(
        place_readback_decision(OrderState::PartiallyFilled, Decimal::ONE),
        PlaceReadbackDecision::Accepted
    );
    assert_eq!(
        place_readback_decision(OrderState::Filled, Decimal::ONE),
        PlaceReadbackDecision::VerifyTerminal
    );
    for state in [
        OrderState::Cancelled,
        OrderState::Expired,
        OrderState::Rejected,
    ] {
        assert_eq!(
            place_readback_decision(state, Decimal::ZERO),
            PlaceReadbackDecision::Rejected
        );
    }
    for state in [OrderState::Cancelled, OrderState::Expired] {
        assert_eq!(
            place_readback_decision(state, Decimal::ONE),
            PlaceReadbackDecision::VerifyTerminal
        );
    }
    assert_eq!(
        place_readback_decision(OrderState::Rejected, Decimal::ONE),
        PlaceReadbackDecision::Unknown
    );
}

#[test]
fn restart_market_terminal_requires_a_persisted_pre_dispatch_position_baseline() {
    let market = ExecutionOrderKind::Market {
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        reducing: false,
    };
    assert_eq!(
        restart_terminal_decision(&market, true),
        ExecutionReadback::Unknown
    );
    let post_only = ExecutionOrderKind::LimitPostOnly {
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        price: Decimal::new(50_000, 0),
        reducing: false,
    };
    assert_eq!(
        restart_terminal_decision(&post_only, true),
        ExecutionReadback::Reconciled
    );
}

#[test]
fn projection_lag_reservation_requires_exact_persisted_context()
-> Result<(), Box<dyn std::error::Error>> {
    let request = ExecutionRequest {
        command_id: "current".into(),
        client_order_id: "current-client".into(),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::Market {
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            reducing: true,
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    };
    let mut reservation = ReconciledCloseReservation {
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        client_order_id: "prior-maker-close".into(),
        side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::new(5, 3),
        reconciled_ms: 101,
        projection_observed_ms: 100,
    };
    assert!(reservation_applies(
        &request,
        &reservation,
        PositionSide::Long,
        OrderSide::Sell
    )?);
    reservation.credential_id = "credential-b".into();
    assert_eq!(
        reservation_applies(&request, &reservation, PositionSide::Long, OrderSide::Sell),
        Err(BinanceExecutionError::Invalid)
    );
    Ok(())
}

#[test]
fn dual_cancel_selectors_must_identify_the_same_exchange_order() {
    assert!(cancel_selectors_match(
        "321",
        Some("grid-owned-order"),
        Some("321"),
        Some("grid-owned-order")
    ));
    assert!(!cancel_selectors_match(
        "999",
        Some("grid-owned-order"),
        Some("321"),
        Some("grid-owned-order")
    ));
    assert!(!cancel_selectors_match(
        "321",
        Some("different-order"),
        Some("321"),
        Some("grid-owned-order")
    ));
}

#[test]
fn exchange_info_rules_floor_quantity_and_reject_too_small_market_commands()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        "BTC/USDT".parse()?,
        7,
    )?;
    assert_eq!(
        normalize_quantity(Decimal::new(29, 4), &rules)?,
        Decimal::new(2, 3)
    );
    assert_eq!(
        normalize_quantity(Decimal::new(9, 4), &rules),
        Err(BinanceExecutionError::Invalid)
    );
    Ok(())
}

#[test]
fn close_only_market_can_clear_below_minimum_notional_after_quantity_validation() {
    assert!(opening_minimum_notional_required(false));
    assert!(!opening_minimum_notional_required(true));
}

#[test]
fn preflight_reads_open_orders_before_the_final_position_surface() {
    let source = include_str!("../executor_exchange.rs");
    let start = source
        .find("async fn snapshot_with_rules")
        .expect("snapshot boundary");
    let end = source[start..]
        .find("async fn snapshot(")
        .map(|offset| start + offset)
        .expect("snapshot end");
    let body = &source[start..end];
    let regular = body.find("build_regular_orders_request").expect("regular");
    let algo = body.find("build_algo_orders_request").expect("algo");
    let positions = body.find("build_positions_request").expect("positions");
    assert!(regular < positions && algo < positions);
}

#[test]
fn exact_limit_readback_requires_every_immutable_semantic() -> Result<(), Box<dyn std::error::Error>>
{
    let rules = parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        "BTC/USDT".parse()?,
        7,
    )?;
    let request = ExecutionRequest {
        command_id: "command-limit".into(),
        client_order_id: "client-limit".into(),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::LimitPostOnly {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(29, 4),
            price: Decimal::new(30_000, 0),
            reducing: false,
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    };
    let mut order = Order {
        order_id: "native-1".into(),
        client_order_id: FieldState::Known("client-limit".into()),
        symbol: "BTC/USDT".parse()?,
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: Decimal::new(2, 3),
        filled_quantity: Decimal::ZERO,
        limit_price: Some(Price::new(Decimal::new(30_000, 0))?),
        time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
        average_price: FieldState::Missing,
        reduce_only: false,
    };
    assert!(exact_place_matches(&request, &order, &rules)?);

    order.time_in_force = FieldState::Known(LimitTimeInForce::Gtc);
    assert!(!exact_place_matches(&request, &order, &rules)?);
    order.time_in_force = FieldState::Known(LimitTimeInForce::PostOnly);
    order.position_side = FieldState::Known(PositionSide::Short);
    assert!(!exact_place_matches(&request, &order, &rules)?);
    Ok(())
}

#[test]
fn exact_reducing_readback_accepts_only_a_normalized_downward_clip()
-> Result<(), Box<dyn std::error::Error>> {
    let rules = parse_instrument_rules(
        include_str!(
            "../../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
        ),
        "BTC/USDT".parse()?,
        7,
    )?;
    let request = ExecutionRequest {
        command_id: "command-close".into(),
        client_order_id: "client-close".into(),
        credential_id: "credential-a".into(),
        trading_account_id: "account-a".into(),
        symbol: "BTC/USDT".parse()?,
        order_kind: ExecutionOrderKind::LimitPostOnly {
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(5, 3),
            price: Decimal::new(30_000, 0),
            reducing: true,
        },
        known_native_order_id: None,
        reconciled_close_reservations: Vec::new(),
    };
    let mut order = Order {
        order_id: "native-close".into(),
        client_order_id: FieldState::Known("client-close".into()),
        symbol: "BTC/USDT".parse()?,
        side: OrderSide::Sell,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: Decimal::new(3, 3),
        filled_quantity: Decimal::ZERO,
        limit_price: Some(Price::new(Decimal::new(30_000, 0))?),
        time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
        average_price: FieldState::Missing,
        reduce_only: false,
    };
    assert!(exact_place_matches(&request, &order, &rules)?);

    order.quantity = Decimal::new(6, 3);
    assert!(!exact_place_matches(&request, &order, &rules)?);
    order.quantity = Decimal::new(5, 4);
    assert_eq!(
        exact_place_matches(&request, &order, &rules),
        Err(BinanceExecutionError::Invalid)
    );
    Ok(())
}
