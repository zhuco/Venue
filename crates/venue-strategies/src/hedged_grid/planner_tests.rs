use std::collections::BTreeSet;

use rust_decimal::Decimal;
use venue_domain::domain::{Asset, Instrument, MarketKind, Precision, RiskSourceStatus};

use super::*;

fn metadata() -> Result<InstrumentMetadata, Box<dyn std::error::Error>> {
    let quote = Asset::new("USDT")?;
    InstrumentMetadata::new(
        Instrument {
            symbol: "SOL/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(quote.clone()),
            generation: 7,
            price_tick: Price::new(Decimal::new(1, 1))?,
            quantity_step: Decimal::new(1, 3),
            minimum_notional: Amount::new(quote, Decimal::from(5)),
        },
        Precision::new(Decimal::new(1, 1), Decimal::new(1, 1))?,
        Precision::new(Decimal::new(1, 3), Decimal::new(1, 3))?,
        None,
        true,
    )
    .map_err(Into::into)
}

fn input() -> Result<GridPlannerInput, Box<dyn std::error::Error>> {
    let quote = Asset::new("USDT")?;
    Ok(GridPlannerInput {
        config: GridPlannerConfig {
            instance_id: "binance_grid_sol".to_owned(),
            revision: 11,
            symbol: "SOL/USDT".parse()?,
            order_notional: Amount::new(quote.clone(), Decimal::from(5)),
            maximum_grid_notional: Amount::new(quote.clone(), Decimal::from(50)),
            spacing_rate: Decimal::new(1, 2),
            grid_count: 3,
            replenishment: Some(GridReplenishmentPolicy {
                minimum_leg_notional: Amount::new(quote.clone(), Decimal::from(20)),
                target_leg_notional: Amount::new(quote.clone(), Decimal::from(30)),
                max_single_notional: Amount::new(quote.clone(), Decimal::from(7)),
            }),
            profit_reduction: None,
            reset_policy: GridResetPolicy {
                max_market_age_ms: 1_000,
                max_private_age_ms: 1_000,
                convergence_timeout_ms: 5_000,
                failure_threshold: 3,
            },
        },
        instrument: metadata()?,
        instrument_limits: GridInstrumentLimits {
            minimum_quantity: Decimal::new(1, 3),
            maximum_quantity: Decimal::from(10),
            minimum_price: Price::new(Decimal::new(1, 1))?,
            maximum_price: Price::new(Decimal::from(1_000))?,
        },
        book: Some(GridBestBook {
            bid: Price::new(Decimal::new(999, 1))?,
            ask: Price::new(Decimal::new(1001, 1))?,
            observed_at_ms: 10_000,
        }),
        reference_price: None,
        inventory: GridInventory {
            private_generation: 31,
            private_observed_at_ms: 10_000,
            mark_price: Price::new(Decimal::from(100))?,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
        },
        owned_orders: Vec::new(),
        maker_fills: Vec::new(),
        pending_place_keys: std::collections::BTreeSet::new(),
        other_close_reservations: GridCloseReservations::default(),
        rolling_anchor: None,
        convergence: GridConvergenceFacts::default(),
        risk: None,
        control: GridPlannerControl::Run,
        now_ms: 10_100,
    })
}

fn converge(plan: GridPlan) -> Result<(GridRollingAnchor, Vec<GridOrderIntent>), &'static str> {
    match plan.directive {
        GridPlanDirective::Converge {
            rolling_anchor,
            desired_orders,
        } => Ok((rolling_anchor, desired_orders)),
        _ => Err("expected converge directive"),
    }
}

fn initialized_input() -> Result<GridPlannerInput, Box<dyn std::error::Error>> {
    let mut value = input()?;
    let (anchor, orders) = converge(GridPlanner::plan(&value)?)?;
    value.rolling_anchor = Some(anchor);
    value.owned_orders = orders;
    Ok(value)
}

#[test]
fn reference_only_grid_starts_and_rolls_one_or_two_fills_without_bbo()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = input()?;
    value.book = None;
    value.reference_price = Some(GridReferencePrice {
        price: Price::new(Decimal::from(100))?,
        observed_at_ms: 10_000,
    });
    let (anchor, orders) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(orders.len(), 12);
    for count in [1_usize, 2] {
        let mut rolled = value.clone();
        rolled.rolling_anchor = Some(anchor.clone());
        let sources = [
            take_order(&orders, GridPosition::Long, GridOrderRole::Open, 1)?,
            take_order(&orders, GridPosition::Short, GridOrderRole::Close, 1)?,
        ];
        rolled.owned_orders = orders
            .iter()
            .filter(|order| {
                !sources[..count]
                    .iter()
                    .any(|source| source.key == order.key)
            })
            .cloned()
            .collect();
        rolled.maker_fills = sources[..count]
            .iter()
            .enumerate()
            .map(|(index, source)| GridMakerFill {
                fill_id: format!("reference-fill-{index}"),
                source_order: source.clone(),
                complete: true,
                maker: true,
            })
            .collect();
        let (_, desired) = converge(GridPlanner::plan(&rolled)?)?;
        assert_eq!(
            diff_counts(&rolled.owned_orders, &desired),
            (count * 2, count)
        );
        assert_unique_diff(&rolled.owned_orders, &desired)?;
    }
    Ok(())
}

#[test]
fn reference_only_grid_rejects_stale_future_and_ambiguous_price_sources()
-> Result<(), Box<dyn std::error::Error>> {
    for observed in [0, 9_000, 10_101] {
        let mut value = input()?;
        value.book = None;
        value.reference_price = Some(GridReferencePrice {
            price: Price::new(Decimal::from(100))?,
            observed_at_ms: observed,
        });
        assert!(matches!(
            GridPlanner::plan(&value)?.directive,
            GridPlanDirective::Blocked { .. }
        ));
    }
    let mut ambiguous = input()?;
    ambiguous.reference_price = Some(GridReferencePrice {
        price: Price::new(Decimal::from(100))?,
        observed_at_ms: 10_000,
    });
    assert!(matches!(
        GridPlanner::plan(&ambiguous)?.directive,
        GridPlanDirective::Blocked { .. }
    ));
    Ok(())
}

fn take_order(
    orders: &[GridOrderIntent],
    position: GridPosition,
    role: GridOrderRole,
    level: u64,
) -> Result<GridOrderIntent, &'static str> {
    orders
        .iter()
        .find(|order| {
            order.key.position == position && order.key.role == role && order.key.level == level
        })
        .cloned()
        .ok_or("missing source order")
}

fn assert_unique_diff(
    actual: &[GridOrderIntent],
    desired: &[GridOrderIntent],
) -> Result<(), &'static str> {
    let actual_keys = actual
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    let desired_keys = desired
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    if actual_keys.len() != actual.len() || desired_keys.len() != desired.len() {
        return Err("duplicate order identity");
    }
    let places = desired_keys
        .difference(&actual_keys)
        .collect::<BTreeSet<_>>();
    let cancels = actual_keys
        .difference(&desired_keys)
        .collect::<BTreeSet<_>>();
    if places.len() != desired_keys.difference(&actual_keys).count()
        || cancels.len() != actual_keys.difference(&desired_keys).count()
    {
        return Err("duplicate converge diff");
    }
    let lane_prices = desired
        .iter()
        .map(|order| (order.key.position, order.key.role, order.price))
        .collect::<BTreeSet<_>>();
    if lane_prices.len() != desired.len() {
        return Err("duplicate lane price");
    }
    Ok(())
}

fn diff_counts(actual: &[GridOrderIntent], desired: &[GridOrderIntent]) -> (usize, usize) {
    let actual_keys = actual
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    let desired_keys = desired
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    (
        desired_keys.difference(&actual_keys).count(),
        actual_keys.difference(&desired_keys).count(),
    )
}

#[test]
fn initial_plan_has_four_normalized_lanes_and_repeatable_output()
-> Result<(), Box<dyn std::error::Error>> {
    let value = input()?;
    let first = GridPlanner::plan(&value)?;
    let second = GridPlanner::plan(&value)?;
    assert_eq!(first, second);
    let (_, orders) = converge(first)?;
    assert_eq!(orders.len(), 12);
    for position in [GridPosition::Long, GridPosition::Short] {
        for role in [GridOrderRole::Open, GridOrderRole::Close] {
            assert_eq!(
                orders
                    .iter()
                    .filter(|order| order.key.position == position && order.key.role == role)
                    .count(),
                3
            );
        }
    }
    assert!(orders.iter().all(|order| {
        value
            .instrument
            .price
            .accepts(order.price.value())
            .unwrap_or(false)
            && value
                .instrument
                .quantity
                .accepts(order.quantity)
                .unwrap_or(false)
            && (order.key.role == GridOrderRole::Close) == order.reduce_only
    }));
    Ok(())
}

#[test]
fn simultaneous_open_and_close_fills_recompute_once_without_duplicate_diff()
-> Result<(), Box<dyn std::error::Error>> {
    let initialized = initialized_input()?;
    let open = take_order(
        &initialized.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    let close = take_order(
        &initialized.owned_orders,
        GridPosition::Long,
        GridOrderRole::Close,
        1,
    )?;
    let fills = vec![
        GridMakerFill {
            fill_id: "trade-open".to_owned(),
            source_order: open.clone(),
            complete: true,
            maker: true,
        },
        GridMakerFill {
            fill_id: "trade-close".to_owned(),
            source_order: close.clone(),
            complete: true,
            maker: true,
        },
    ];
    let mut forward = initialized.clone();
    forward
        .owned_orders
        .retain(|order| order.key != open.key && order.key != close.key);
    forward.maker_fills = fills.clone();
    let actual = forward.owned_orders.clone();
    let (_, forward_orders) = converge(GridPlanner::plan(&forward)?)?;
    assert_eq!(diff_counts(&actual, &forward_orders), (4, 2));

    let mut reverse = forward;
    reverse.maker_fills = fills.into_iter().rev().collect();
    let (_, reverse_orders) = converge(GridPlanner::plan(&reverse)?)?;
    assert_eq!(forward_orders, reverse_orders);
    assert_unique_diff(&actual, &forward_orders)?;
    assert_eq!(forward_orders.len(), 12);
    Ok(())
}

#[test]
fn one_complete_maker_fill_places_two_and_cancels_one_without_waiting_for_a_pair()
-> Result<(), Box<dyn std::error::Error>> {
    let initialized = initialized_input()?;
    let source = take_order(
        &initialized.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    let mut value = initialized;
    value.owned_orders.retain(|order| order.key != source.key);
    value.maker_fills.push(GridMakerFill {
        fill_id: "single-complete".to_owned(),
        source_order: source,
        complete: true,
        maker: true,
    });
    let actual = value.owned_orders.clone();
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_unique_diff(&actual, &desired)?;
    assert_eq!(diff_counts(&actual, &desired), (2, 1));
    Ok(())
}

#[test]
fn resting_crossed_orders_preserve_surface_until_signed_fills_arrive()
-> Result<(), Box<dyn std::error::Error>> {
    for (bid, ask) in [(98, 99), (101, 102)] {
        let mut value = initialized_input()?;
        let original = value.owned_orders.clone();
        let anchor = value.rolling_anchor.clone();
        value.book.as_mut().ok_or("book fixture")?.bid = Price::new(Decimal::from(bid))?;
        value.book.as_mut().ok_or("book fixture")?.ask = Price::new(Decimal::from(ask))?;
        let (after_anchor, desired) = converge(GridPlanner::plan(&value)?)?;
        assert_eq!(diff_counts(&original, &desired), (0, 0));
        assert_eq!(Some(after_anchor), anchor);
    }
    Ok(())
}

#[test]
fn first_fill_rolls_while_crossed_counterpart_is_still_in_signed_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = initialized_input()?;
    let source = take_order(
        &value.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    value.book.as_mut().ok_or("book fixture")?.bid =
        Price::new(source.price.value() - Decimal::new(1, 1))?;
    value.book.as_mut().ok_or("book fixture")?.ask = source.price;
    value.owned_orders.retain(|order| order.key != source.key);
    value.maker_fills.push(GridMakerFill {
        fill_id: "first-of-pair".to_owned(),
        source_order: source,
        complete: true,
        maker: true,
    });
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(diff_counts(&value.owned_orders, &desired), (2, 1));
    assert_unique_diff(&value.owned_orders, &desired)?;
    Ok(())
}

#[test]
fn crossing_new_maker_target_waits_without_consuming_fill_or_resetting_anchor()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = initialized_input()?;
    let source = take_order(
        &value.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    value.owned_orders.retain(|order| order.key != source.key);
    value.maker_fills.push(GridMakerFill {
        fill_id: "maker-wait".to_owned(),
        source_order: source,
        complete: true,
        maker: true,
    });
    let normal_book = value.book.clone();
    value.book.as_mut().ok_or("book fixture")?.bid = Price::new(Decimal::from(104))?;
    value.book.as_mut().ok_or("book fixture")?.ask = Price::new(Decimal::from(105))?;
    for _ in 0..5 {
        assert_eq!(
            GridPlanner::plan(&value)?.directive,
            GridPlanDirective::Blocked {
                reason: GridBlockedReason::MakerPriceWouldCrossBook,
            }
        );
    }
    value.book = normal_book;
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(diff_counts(&value.owned_orders, &desired), (2, 1));
    Ok(())
}

#[test]
fn crossed_pair_in_separate_batches_keeps_epoch_and_two_place_one_cancel_per_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = initialized_input()?;
    let first = take_order(
        &value.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    let second = take_order(
        &value.owned_orders,
        GridPosition::Short,
        GridOrderRole::Close,
        1,
    )?;
    value.book.as_mut().ok_or("book fixture")?.bid =
        Price::new(first.price.value() - Decimal::new(1, 1))?;
    value.book.as_mut().ok_or("book fixture")?.ask = first.price;
    value.owned_orders.retain(|order| order.key != first.key);
    value.maker_fills = vec![GridMakerFill {
        fill_id: "pair-first".to_owned(),
        source_order: first,
        complete: true,
        maker: true,
    }];
    let (anchor, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(diff_counts(&value.owned_orders, &desired), (2, 1));
    value.pending_place_keys = desired
        .iter()
        .filter(|order| !value.owned_orders.iter().any(|old| old.key == order.key))
        .map(|order| order.key.clone())
        .collect();
    value.owned_orders = desired
        .into_iter()
        .filter(|order| order.key != second.key)
        .collect();
    value.rolling_anchor = Some(anchor);
    value.maker_fills = vec![GridMakerFill {
        fill_id: "pair-second".to_owned(),
        source_order: second,
        complete: true,
        maker: true,
    }];
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(diff_counts(&value.owned_orders, &desired), (2, 1));
    assert!(
        desired
            .iter()
            .all(|order| order.key.epoch == value.config.revision)
    );
    assert_eq!(desired.len(), 12);
    Ok(())
}

#[test]
fn consecutive_complete_fills_keep_unconfirmed_places_non_cancellable()
-> Result<(), Box<dyn std::error::Error>> {
    let initialized = initialized_input()?;
    let first_source = take_order(
        &initialized.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    let mut first = initialized.clone();
    first
        .owned_orders
        .retain(|order| order.key != first_source.key);
    first.maker_fills = vec![GridMakerFill {
        fill_id: "consecutive-first".to_owned(),
        source_order: first_source,
        complete: true,
        maker: true,
    }];
    let first_actual = first.owned_orders.clone();
    let (first_anchor, first_desired) = converge(GridPlanner::plan(&first)?)?;
    let first_actual_keys = first_actual
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    let pending = first_desired
        .iter()
        .filter(|order| !first_actual_keys.contains(&order.key))
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(pending.len(), 2);

    let second_source = take_order(&first_desired, GridPosition::Short, GridOrderRole::Open, 1)?;
    assert!(!pending.contains(&second_source.key));
    let mut second = initialized;
    second.rolling_anchor = Some(first_anchor);
    second.owned_orders = first_desired.clone();
    second
        .owned_orders
        .retain(|order| order.key != second_source.key);
    second.pending_place_keys = pending.clone();
    second.maker_fills = vec![GridMakerFill {
        fill_id: "consecutive-second".to_owned(),
        source_order: second_source,
        complete: true,
        maker: true,
    }];
    let projected_after_fill = second.owned_orders.clone();
    let (_, second_desired) = converge(GridPlanner::plan(&second)?)?;
    assert_eq!(diff_counts(&projected_after_fill, &second_desired), (2, 1));
    let second_keys = second_desired
        .iter()
        .map(|order| order.key.clone())
        .collect::<BTreeSet<_>>();
    assert!(pending.iter().all(|key| second_keys.contains(key)));
    Ok(())
}

#[test]
fn two_same_lane_maker_fills_have_deterministic_unique_places_and_cancels()
-> Result<(), Box<dyn std::error::Error>> {
    let initialized = initialized_input()?;
    let first = take_order(
        &initialized.owned_orders,
        GridPosition::Short,
        GridOrderRole::Open,
        1,
    )?;
    let second = take_order(
        &initialized.owned_orders,
        GridPosition::Short,
        GridOrderRole::Open,
        2,
    )?;
    let mut value = initialized;
    // Both sell fills move the signed book upward while keeping every surviving maker passive.
    value.book.as_mut().ok_or("book fixture")?.bid = Price::new(Decimal::new(1005, 1))?;
    value.book.as_mut().ok_or("book fixture")?.ask = Price::new(Decimal::new(1015, 1))?;
    value
        .owned_orders
        .retain(|order| order.key != first.key && order.key != second.key);
    value.maker_fills = vec![
        GridMakerFill {
            fill_id: "same-lane-2".to_owned(),
            source_order: second,
            complete: true,
            maker: true,
        },
        GridMakerFill {
            fill_id: "same-lane-1".to_owned(),
            source_order: first,
            complete: true,
            maker: true,
        },
    ];
    let actual = value.owned_orders.clone();
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_unique_diff(&actual, &desired)?;
    assert_eq!(diff_counts(&actual, &desired), (4, 2));
    assert_eq!(desired.len(), 12);
    Ok(())
}

#[test]
fn stale_facts_block_without_mutation_while_convergence_failures_require_reset()
-> Result<(), Box<dyn std::error::Error>> {
    let mut stale = initialized_input()?;
    stale.book.as_mut().ok_or("book fixture")?.observed_at_ms = 1;
    assert_eq!(
        GridPlanner::plan(&stale)?.directive,
        GridPlanDirective::Blocked {
            reason: GridBlockedReason::StaleMarketFacts
        }
    );

    let mut timed_out = initialized_input()?;
    timed_out.convergence.pending_since_ms = Some(5_000);
    let GridPlanDirective::ResetRequired {
        trigger,
        cancel_owned_orders,
        keep_positions,
        require_fresh_facts,
    } = GridPlanner::plan(&timed_out)?.directive
    else {
        return Err("expected convergence reset".into());
    };
    assert_eq!(trigger, GridResetTrigger::ConvergenceTimedOut);
    assert!(cancel_owned_orders && keep_positions && require_fresh_facts);

    let mut failed = initialized_input()?;
    failed.convergence.consecutive_failures = 3;
    assert!(matches!(
        GridPlanner::plan(&failed)?.directive,
        GridPlanDirective::ResetRequired {
            trigger: GridResetTrigger::FailureThresholdReached,
            ..
        }
    ));
    Ok(())
}

#[test]
fn replenishment_and_profitable_reduction_use_configured_caps()
-> Result<(), Box<dyn std::error::Error>> {
    let mut replenish = input()?;
    replenish.inventory.long_quantity = Decimal::new(5, 2);
    replenish.inventory.short_quantity = Decimal::new(5, 2);
    let GridPlanDirective::Replenish {
        adjustments,
        cancel_owned_orders,
        require_fresh_private_facts,
    } = GridPlanner::plan(&replenish)?.directive
    else {
        return Err("expected replenishment".into());
    };
    assert_eq!(adjustments.len(), 2);
    assert!(adjustments.iter().all(|item| {
        item.target_notional.value == Decimal::from(7) && item.quantity == Decimal::new(7, 2)
    }));
    assert!(cancel_owned_orders && require_fresh_private_facts);

    let mut reduce = input()?;
    let quote = Asset::new("USDT")?;
    reduce.config.profit_reduction = Some(GridProfitReductionPolicy {
        inventory_equity_multiple: Decimal::from(3),
        minimum_profit_rate: Decimal::new(5, 2),
        reduction_fraction: Decimal::new(5, 1),
        max_single_notional: Amount::new(quote.clone(), Decimal::from(7)),
    });
    reduce.risk = Some(GridRiskFacts {
        account: AccountRiskSnapshot {
            exchange: "binance".to_owned(),
            account: "portfolio_um".to_owned(),
            risk_currency: quote.clone(),
            account_equity: Decimal::from(10),
            private_generation: 31,
            observed_at_ms: 10_000,
            source_status: RiskSourceStatus::Complete,
        },
        legs: vec![
            LegRiskSnapshot {
                symbol: "SOL/USDT".parse()?,
                position_side: PositionSide::Long,
                quantity: Decimal::ONE,
                mark_price: Price::new(Decimal::from(100))?,
                contract_multiplier: Decimal::ONE,
                notional: Decimal::from(100),
                unrealized_pnl: Decimal::from(10),
                risk_currency: quote.clone(),
                private_generation: 31,
                observed_at_ms: 10_000,
            },
            LegRiskSnapshot {
                symbol: "SOL/USDT".parse()?,
                position_side: PositionSide::Short,
                quantity: Decimal::ONE,
                mark_price: Price::new(Decimal::from(100))?,
                contract_multiplier: Decimal::ONE,
                notional: Decimal::from(100),
                unrealized_pnl: Decimal::ZERO,
                risk_currency: quote.clone(),
                private_generation: 31,
                observed_at_ms: 10_000,
            },
        ],
        conversion: GridRiskConversion {
            risk_currency: quote.clone(),
            quote_currency: quote,
            quote_per_risk_unit: Decimal::ONE,
            private_generation: 31,
            observed_at_ms: 10_000,
        },
    });
    let GridPlanDirective::ReduceExposure { reductions, .. } =
        GridPlanner::plan(&reduce)?.directive
    else {
        return Err("expected profitable reduction".into());
    };
    assert_eq!(reductions.len(), 1);
    assert_eq!(reductions[0].position, GridPosition::Long);
    assert_eq!(reductions[0].quantity, Decimal::new(7, 2));
    assert!(reductions[0].close_only);
    Ok(())
}

#[test]
fn profitable_reduction_converts_risk_currency_to_quote_without_parity_assumption()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = input()?;
    let quote = Asset::new("USDT")?;
    let risk_currency = Asset::new("USD")?;
    value.inventory.short_quantity = Decimal::ZERO;
    value.config.profit_reduction = Some(GridProfitReductionPolicy {
        inventory_equity_multiple: Decimal::from(3),
        minimum_profit_rate: Decimal::new(5, 2),
        reduction_fraction: Decimal::new(5, 1),
        max_single_notional: Amount::new(quote.clone(), Decimal::from(70)),
    });
    value.risk = Some(GridRiskFacts {
        account: AccountRiskSnapshot {
            exchange: "binance".to_owned(),
            account: "portfolio_um".to_owned(),
            risk_currency: risk_currency.clone(),
            account_equity: Decimal::from(10),
            private_generation: 31,
            observed_at_ms: 10_000,
            source_status: RiskSourceStatus::Complete,
        },
        legs: vec![LegRiskSnapshot {
            symbol: "SOL/USDT".parse()?,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            mark_price: Price::new(Decimal::from(100))?,
            contract_multiplier: Decimal::ONE,
            notional: Decimal::from(100),
            unrealized_pnl: Decimal::from(10),
            risk_currency: risk_currency.clone(),
            private_generation: 31,
            observed_at_ms: 10_000,
        }],
        conversion: GridRiskConversion {
            risk_currency,
            quote_currency: quote,
            quote_per_risk_unit: Decimal::new(5, 1),
            private_generation: 31,
            observed_at_ms: 10_000,
        },
    });

    let GridPlanDirective::ReduceExposure { reductions, .. } = GridPlanner::plan(&value)?.directive
    else {
        return Err("expected profitable reduction".into());
    };
    assert_eq!(reductions.len(), 1);
    assert_eq!(reductions[0].quantity, Decimal::new(25, 2));

    let valid_risk = value.risk.clone().ok_or("missing risk facts")?;
    let mut invalid_cases = Vec::new();
    let mut wrong_generation = valid_risk.clone();
    wrong_generation.conversion.private_generation = 30;
    invalid_cases.push(wrong_generation);
    let mut stale = valid_risk.clone();
    stale.conversion.observed_at_ms = 1;
    invalid_cases.push(stale);
    let mut wrong_source = valid_risk.clone();
    wrong_source.conversion.risk_currency = Asset::new("EUR")?;
    invalid_cases.push(wrong_source);
    let mut wrong_quote = valid_risk.clone();
    wrong_quote.conversion.quote_currency = Asset::new("USDC")?;
    invalid_cases.push(wrong_quote);
    let mut zero_rate = valid_risk;
    zero_rate.conversion.quote_per_risk_unit = Decimal::ZERO;
    invalid_cases.push(zero_rate);

    for risk in invalid_cases {
        value.risk = Some(risk);
        assert_eq!(
            GridPlanner::plan(&value)?.directive,
            GridPlanDirective::Blocked {
                reason: GridBlockedReason::InvalidRiskFacts
            }
        );
    }
    Ok(())
}

#[test]
fn rolled_orders_raise_fixed_quantity_to_meet_minimum_notional_at_new_price()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = initialized_input()?;
    let source = take_order(
        &value.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    assert_eq!(source.price, Price::new(Decimal::from(99))?);
    assert_eq!(
        value
            .rolling_anchor
            .as_ref()
            .map(|anchor| anchor.grid_quantity),
        Some(Decimal::new(52, 3))
    );
    value.owned_orders.retain(|order| order.key != source.key);
    value.book.as_mut().ok_or("book fixture")?.bid = Price::new(Decimal::new(995, 1))?;
    value.book.as_mut().ok_or("book fixture")?.ask = Price::new(Decimal::new(997, 1))?;
    value.maker_fills.push(GridMakerFill {
        fill_id: "roll-below-minimum".to_owned(),
        source_order: source,
        complete: true,
        maker: true,
    });

    let (anchor, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(anchor.grid_quantity, Decimal::new(53, 3));
    let rolled_open = take_order(&desired, GridPosition::Long, GridOrderRole::Open, 4)?;
    assert_eq!(rolled_open.price, Price::new(Decimal::from(96))?);
    assert_eq!(rolled_open.quantity, Decimal::new(53, 3));
    assert!(
        rolled_open.quantity * rolled_open.price.value()
            >= value.instrument.instrument.minimum_notional.value
    );

    value.instrument_limits.maximum_quantity = Decimal::new(52, 3);
    assert!(matches!(
        GridPlanner::plan(&value)?.directive,
        GridPlanDirective::ResetRequired {
            trigger: GridResetTrigger::InvalidOwnedOrder,
            ..
        }
    ));
    Ok(())
}

#[test]
fn stop_never_flattens_and_structural_conflicts_reset_only_owned_orders()
-> Result<(), Box<dyn std::error::Error>> {
    let mut stop = input()?;
    stop.book.as_mut().ok_or("book fixture")?.observed_at_ms = 1;
    stop.control = GridPlannerControl::Stop;
    assert_eq!(
        GridPlanner::plan(&stop)?.directive,
        GridPlanDirective::Stop {
            cancel_owned_orders: true,
            flatten_positions: false
        }
    );

    let mut conflict = initialized_input()?;
    conflict.owned_orders.push(conflict.owned_orders[0].clone());
    assert!(matches!(
        GridPlanner::plan(&conflict)?.directive,
        GridPlanDirective::ResetRequired {
            trigger: GridResetTrigger::DuplicateOwnedOrder,
            cancel_owned_orders: true,
            keep_positions: true,
            ..
        }
    ));
    Ok(())
}

#[test]
fn excessive_levels_fail_closed_at_the_total_open_notional_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = input()?;
    value.config.grid_count = 10;
    let first = GridPlanner::plan(&value);
    let second = GridPlanner::plan(&value);
    assert_eq!(first, Err(GridPlannerError::OpenNotionalLimit));
    assert_eq!(second, first);
    Ok(())
}

#[test]
fn semantic_key_separates_bounded_grid_rank_from_monotonic_lane_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let value = input()?;
    let (_, orders) = converge(GridPlanner::plan(&value)?)?;
    let closest = take_order(&orders, GridPosition::Long, GridOrderRole::Open, 1)?;
    assert_eq!(
        GridPlanner::semantic_order_key(&orders, &closest.key)?,
        GridSemanticOrderKey {
            revision: 11,
            position: GridPosition::Long,
            role: GridOrderRole::Open,
            grid_level: 1,
            sequence: 1,
        }
    );
    assert!(orders.iter().all(|order| {
        GridPlanner::semantic_order_key(&orders, &order.key)
            .is_ok_and(|key| (1..=3).contains(&key.grid_level))
    }));
    Ok(())
}

#[test]
fn adapter_price_and_quantity_boundaries_are_enforced_before_orders_escape()
-> Result<(), Box<dyn std::error::Error>> {
    let mut exact = input()?;
    exact.instrument_limits.maximum_quantity = Decimal::new(52, 3);
    exact.instrument_limits.maximum_price = Price::new(Decimal::from(103))?;
    assert!(GridPlanner::plan(&exact).is_ok());

    let mut quantity_rejected = exact.clone();
    quantity_rejected.instrument_limits.maximum_quantity = Decimal::new(51, 3);
    assert_eq!(
        GridPlanner::plan(&quantity_rejected),
        Err(GridPlannerError::OrderOutsideInstrumentLimits)
    );

    let mut price_rejected = exact;
    price_rejected.instrument_limits.maximum_price = Price::new(Decimal::new(1029, 1))?;
    assert_eq!(
        GridPlanner::plan(&price_rejected),
        Err(GridPlannerError::OrderOutsideInstrumentLimits)
    );
    Ok(())
}

#[test]
fn partial_fill_keeps_signed_remaining_quantity_without_rolling()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = initialized_input()?;
    let source = take_order(
        &value.owned_orders,
        GridPosition::Long,
        GridOrderRole::Open,
        1,
    )?;
    let remaining = Decimal::new(26, 3);
    value
        .owned_orders
        .iter_mut()
        .find(|order| order.key == source.key)
        .ok_or("missing partial order")?
        .quantity = remaining;
    value.maker_fills.push(GridMakerFill {
        fill_id: "partial-trade".to_owned(),
        source_order: source.clone(),
        complete: false,
        maker: true,
    });
    let (_, desired) = converge(GridPlanner::plan(&value)?)?;
    assert_eq!(
        desired
            .iter()
            .find(|order| order.key == source.key)
            .map(|order| order.quantity),
        Some(remaining)
    );
    assert_eq!(desired.len(), value.owned_orders.len());
    Ok(())
}

#[test]
fn stop_remains_available_when_non_identity_strategy_limits_are_invalid()
-> Result<(), Box<dyn std::error::Error>> {
    let mut value = input()?;
    value.config.maximum_grid_notional.value = Decimal::ZERO;
    value.instrument_limits.maximum_quantity = Decimal::ZERO;
    value.control = GridPlannerControl::Stop;
    assert!(matches!(
        GridPlanner::plan(&value)?.directive,
        GridPlanDirective::Stop {
            cancel_owned_orders: true,
            flatten_positions: false
        }
    ));
    Ok(())
}
