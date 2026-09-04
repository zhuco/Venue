use super::*;
use venue_control_protocol::grid::GridOrderSemanticKey;
use venue_control_protocol::kol::{TerminalOpenOrder, TerminalOrderState};
use venue_domain::OrderSide;
use venue_strategies::hedged_grid::{GridInventory, GridOrderRole};

#[test]
fn durable_ids_fit_binance_and_change_by_plan_for_mutations() {
    let first = durable_id("vgm", "instance", 2, 7, "replenish:long", 36);
    let replay = durable_id("vgm", "instance", 2, 7, "replenish:long", 36);
    let next = durable_id("vgm", "instance", 2, 8, "replenish:long", 36);
    assert_eq!(first, replay);
    assert_ne!(first, next);
    assert!(first.len() <= 36);
}

#[test]
fn desired_digest_binds_anchor_and_entire_surface() -> Result<(), Box<dyn std::error::Error>> {
    let anchor = GridRollingAnchor {
        revision: 3,
        instrument_generation: 41,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::ONE)?,
        grid_quantity: Decimal::new(5, 1),
    };
    let order = GridDesiredOrder {
        key: GridOrderSemanticKey {
            position_side: PositionSide::Long,
            role: ProtocolOrderRole::Open,
            level: 1,
            sequence: 1,
        },
        client_order_id: "vgp-one".into(),
        quantity: Decimal::new(5, 1),
        limit_price: Decimal::new(99, 0),
    };
    let digest = desired_digest(&anchor, std::slice::from_ref(&order));
    let mut scaled_anchor = anchor.clone();
    scaled_anchor.grid_quantity.rescale(4);
    let mut scaled_order = order.clone();
    scaled_order.quantity.rescale(4);
    scaled_order.limit_price.rescale(4);
    assert_eq!(digest, desired_digest(&scaled_anchor, &[scaled_order]));
    let mut changed_anchor = anchor.clone();
    changed_anchor.instrument_generation += 1;
    let mut changed_order = order.clone();
    changed_order.quantity += Decimal::new(1, 1);
    let mut close_order = order.clone();
    close_order.key.role = ProtocolOrderRole::Close;
    assert!(order_priority(&close_order) < order_priority(&order));
    assert_eq!(
        desired_digest(&anchor, &[order.clone(), close_order.clone()]),
        desired_digest(&anchor, &[close_order, order.clone()])
    );
    assert_ne!(
        digest,
        desired_digest(&changed_anchor, std::slice::from_ref(&order))
    );
    assert_ne!(digest, desired_digest(&anchor, &[changed_order]));
    let durable = GridAnchor {
        revision: 9,
        instrument_generation: anchor.instrument_generation,
        price: anchor.anchor_price.value(),
        price_step: anchor.step.value(),
        grid_quantity: anchor.grid_quantity,
        source_native_trade_id: Some("trade-1".into()),
        observed_ms: 10,
    };
    assert_eq!(planner_anchor(&durable, 3)?.revision, 3);
    Ok(())
}

#[test]
fn partial_or_still_open_fill_never_rolls() {
    assert_eq!(
        fill_complete(Decimal::new(4, 1), Decimal::ONE, false),
        Ok(false)
    );
    assert_eq!(fill_complete(Decimal::ONE, Decimal::ONE, true), Ok(false));
    assert_eq!(fill_complete(Decimal::ONE, Decimal::ONE, false), Ok(true));
    assert_eq!(
        fill_complete(Decimal::from(2), Decimal::ONE, false),
        Err(BinanceGridRuntimeError::Facts)
    );
}

#[test]
fn partial_fill_keeps_client_id_and_does_not_place_again() -> Result<(), Box<dyn std::error::Error>>
{
    let initial = vec![intent(
        GridPosition::Long,
        GridOrderRole::Open,
        1,
        99,
        Decimal::ONE,
    )?];
    let prior_orders = desired_orders("instance", 2, &initial, None, 7)?;
    let prior = surface(prior_orders.clone())?;
    let partial = vec![intent(
        GridPosition::Long,
        GridOrderRole::Open,
        1,
        99,
        Decimal::new(4, 1),
    )?];
    let next = desired_orders("instance", 2, &partial, Some(&prior), 8)?;
    assert_eq!(next[0].client_order_id, prior_orders[0].client_order_id);
    let signed_open_ids = BTreeSet::from([prior_orders[0].client_order_id.as_str()]);
    assert!(
        next.iter()
            .all(|order| signed_open_ids.contains(order.client_order_id.as_str()))
    );
    Ok(())
}

#[test]
fn rolling_reuses_lane_ids_despite_rank_changes() -> Result<(), Box<dyn std::error::Error>> {
    let initial = vec![
        intent(GridPosition::Long, GridOrderRole::Open, 1, 99, Decimal::ONE)?,
        intent(GridPosition::Long, GridOrderRole::Open, 2, 98, Decimal::ONE)?,
        intent(
            GridPosition::Long,
            GridOrderRole::Close,
            1,
            101,
            Decimal::ONE,
        )?,
        intent(
            GridPosition::Short,
            GridOrderRole::Open,
            1,
            101,
            Decimal::ONE,
        )?,
    ];
    let before = desired_orders("instance", 2, &initial, None, 7)?;
    let prior = surface(before.clone())?;
    let rolled = vec![
        intent(GridPosition::Long, GridOrderRole::Open, 2, 98, Decimal::ONE)?,
        intent(GridPosition::Long, GridOrderRole::Open, 3, 97, Decimal::ONE)?,
        intent(
            GridPosition::Long,
            GridOrderRole::Close,
            1,
            101,
            Decimal::ONE,
        )?,
        intent(
            GridPosition::Long,
            GridOrderRole::Close,
            2,
            102,
            Decimal::ONE,
        )?,
        intent(
            GridPosition::Short,
            GridOrderRole::Open,
            1,
            101,
            Decimal::ONE,
        )?,
    ];
    let after = desired_orders("instance", 2, &rolled, Some(&prior), 8)?;
    let before_ids = before
        .iter()
        .map(|order| order.client_order_id.as_str())
        .collect::<BTreeSet<_>>();
    let after_ids = after
        .iter()
        .map(|order| order.client_order_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(after_ids.difference(&before_ids).count(), 2);
    assert_eq!(before_ids.difference(&after_ids).count(), 1);
    let is_shifted_lane = |order: &&GridDesiredOrder| {
        order.key.position_side == PositionSide::Long
            && order.key.role == ProtocolOrderRole::Open
            && order.key.sequence == 2
    };
    let old_lane = before
        .iter()
        .find(is_shifted_lane)
        .ok_or_else(|| std::io::Error::other("missing old lane"))?;
    let new_lane = after
        .iter()
        .find(is_shifted_lane)
        .ok_or_else(|| std::io::Error::other("missing new lane"))?;
    assert_ne!(old_lane.key.level, new_lane.key.level);
    assert_eq!(old_lane.client_order_id, new_lane.client_order_id);
    Ok(())
}

#[test]
fn second_roll_finds_first_plan_survivor_and_treats_its_fill_as_new_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let lane = vec![intent(
        GridPosition::Long,
        GridOrderRole::Open,
        1,
        99,
        Decimal::ONE,
    )?];
    let first = desired_orders("instance", 2, &lane, None, 7)?;
    let first_surface = surface(first.clone())?;
    let second = desired_orders("instance", 2, &lane, Some(&first_surface), 8)?;
    let mut second_surface = surface(second.clone())?;
    second_surface.plan_revision = 8;
    let third = desired_orders("instance", 2, &lane, Some(&second_surface), 9)?;
    assert_eq!(first[0].client_order_id, third[0].client_order_id);
    let owner = GridOrderOwnership {
        instance_id: "instance".into(),
        trading_account_id: "00000000-0000-4000-8000-000000000001".into(),
        config_revision: 2,
        plan_revision: 7,
        key: first[0].key.clone(),
        place_command_id: "first-place".into(),
        client_order_id: first[0].client_order_id.clone(),
        symbol: "BTC/USDT".parse()?,
        quantity: first[0].quantity,
        filled_quantity: first[0].quantity,
        limit_price: first[0].limit_price,
        native_order_id: Some("native-1".into()),
        state: GridOwnedOrderState::Terminal,
        first_seen_ms: 1,
        last_seen_ms: 11,
    };
    let third_surface = GridDesiredSurface {
        plan_revision: 9,
        orders: third,
        ..surface(Vec::new())?
    };
    let ownership = BTreeMap::from([(owner.client_order_id.clone(), owner)]);
    assert_eq!(
        prior_command_surfaces(&third_surface, &ownership, 2, 9),
        BTreeSet::from([(2, 7)])
    );
    assert_eq!(
        missing_place_result(Some((ExecutorCommandState::Reconciled, 10)), 10, 5),
        MissingPlaceResult::Pending
    );
    assert_eq!(
        missing_place_result(Some((ExecutorCommandState::Reconciled, 10)), 11, 5),
        MissingPlaceResult::FactsChanged
    );
    Ok(())
}

#[test]
fn resume_after_pause_uses_a_new_plan_identity() -> Result<(), Box<dyn std::error::Error>> {
    let lane = vec![intent(
        GridPosition::Short,
        GridOrderRole::Open,
        1,
        101,
        Decimal::ONE,
    )?];
    let before_pause = desired_orders("instance", 2, &lane, None, 7)?;
    let after_resume = desired_orders("instance", 2, &lane, None, 8)?;
    assert_ne!(
        before_pause[0].client_order_id,
        after_resume[0].client_order_id
    );
    Ok(())
}

#[test]
fn rejected_market_waits_for_new_facts_and_is_a_failure() {
    assert_eq!(
        market_status([(ExecutorCommandState::Rejected, 20)].into_iter(), 19),
        (false, true, 20)
    );
    assert_eq!(
        market_status([(ExecutorCommandState::Rejected, 20)].into_iter(), 20),
        (false, true, 20)
    );
    assert_eq!(
        market_status([(ExecutorCommandState::Rejected, 20)].into_iter(), 21),
        (true, true, 20)
    );
    assert_eq!(
        market_status(
            [
                (ExecutorCommandState::Rejected, 20),
                (ExecutorCommandState::Reconciled, 30),
            ]
            .into_iter(),
            30,
        ),
        (false, true, 20)
    );
}

#[test]
fn unknown_market_and_missing_projection_lifecycle_paths_expire() {
    assert_eq!(
        market_status(
            [(ExecutorCommandState::ReconcileRequired, 20)].into_iter(),
            100
        ),
        (false, false, 0)
    );
    assert_eq!(
        lifecycle_timeout_code(GridInstanceState::StopPending, Some(10), 20, 31),
        Some("stop_convergence_timeout")
    );
    assert_eq!(
        lifecycle_timeout_code(GridInstanceState::ResetRequired, Some(10), 20, 31),
        Some("reset_convergence_timeout")
    );
    assert_eq!(
        lifecycle_timeout_code(GridInstanceState::Paused, Some(10), 20, 31),
        Some("pause_convergence_timeout")
    );
    assert_eq!(
        lifecycle_timeout_code(GridInstanceState::Running, Some(10), 20, 31),
        None
    );
}

#[test]
fn hot_update_waits_for_old_surface_and_signed_command_readback() {
    assert!(!signed_teardown_ready(false, false, Some(20), 20));
    assert!(!signed_teardown_ready(true, true, Some(20), 20));
    assert!(!signed_teardown_ready(true, false, Some(20), 19));
    assert!(!signed_teardown_ready(true, false, Some(20), 20));
    assert!(signed_teardown_ready(true, false, Some(20), 21));
}

#[test]
fn planner_diffs_are_bounded_and_never_cancel_before_all_places_fit() {
    assert_eq!(selected_batch_counts(40, 40, 0), (16, 0));
    assert_eq!(selected_batch_counts(24, 40, 7), (9, 0));
    assert_eq!(selected_batch_counts(1, 40, 15), (1, 0));
    assert_eq!(selected_batch_counts(0, 40, 0), (0, 16));
    assert_eq!(selected_batch_counts(2, 1, 0), (2, 1));
    assert_eq!(selected_batch_counts(4, 2, 0), (4, 2));
}

#[test]
fn persisted_close_surface_is_clipped_by_external_reservations()
-> Result<(), Box<dyn std::error::Error>> {
    let mut long_close = desired_orders(
        "instance",
        2,
        &[intent(
            GridPosition::Long,
            GridOrderRole::Close,
            1,
            101,
            Decimal::from(4),
        )?],
        None,
        7,
    )?;
    let desired = surface(std::mem::take(&mut long_close))?;
    let inventory = GridInventory {
        private_generation: 1,
        private_observed_at_ms: 10,
        mark_price: Price::new(Decimal::from(100))?,
        long_quantity: Decimal::from(5),
        short_quantity: Decimal::from(3),
    };
    let mut reservations = GridCloseReservations {
        long_quantity: Decimal::ONE,
        short_quantity: Decimal::ZERO,
    };
    assert!(desired_closes_fit(
        &desired,
        &inventory,
        &reservations,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeSet::new(),
    )?);
    reservations.long_quantity = Decimal::from(2);
    assert!(!desired_closes_fit(
        &desired,
        &inventory,
        &reservations,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeSet::new(),
    )?);
    Ok(())
}

#[test]
fn signed_partial_order_is_a_fact_change_not_a_surface_conflict()
-> Result<(), Box<dyn std::error::Error>> {
    let wanted = GridDesiredOrder {
        key: GridOrderSemanticKey {
            position_side: PositionSide::Long,
            role: ProtocolOrderRole::Close,
            level: 1,
            sequence: 1,
        },
        client_order_id: "partial-close".into(),
        quantity: Decimal::from(5),
        limit_price: Decimal::from(101),
    };
    let mut signed = TerminalOpenOrder {
        client_order_id: wanted.client_order_id.clone(),
        native_order_id: Some("native-1".into()),
        symbol: "BTC/USDT".parse()?,
        order_side: OrderSide::Sell,
        position_side: PositionSide::Long,
        quantity: Decimal::from(5),
        filled_quantity: Some(Decimal::ONE),
        limit_price: Some(wanted.limit_price),
        post_only: true,
        time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
        reduce_only: false,
        state: TerminalOrderState::PartiallyFilled,
        created_ms: Some(1),
    };
    assert_eq!(
        actual_matches_desired(&signed, &wanted)?,
        DesiredOrderMatch::Partial
    );
    let completed = missing_place_result(Some((ExecutorCommandState::Reconciled, 9)), 10, 5);
    let not_yet_visible = missing_place_result(Some((ExecutorCommandState::Accepted, 10)), 10, 5);
    assert_eq!(completed, MissingPlaceResult::FactsChanged);
    assert_eq!(not_yet_visible, MissingPlaceResult::Pending);
    let partial_surface = surface(vec![wanted.clone()])?;
    let partial_inventory = GridInventory {
        private_generation: 1,
        private_observed_at_ms: 10,
        mark_price: Price::new(Decimal::from(100))?,
        long_quantity: Decimal::from(4),
        short_quantity: Decimal::ZERO,
    };
    assert!(desired_closes_fit(
        &partial_surface,
        &partial_inventory,
        &GridCloseReservations::default(),
        &BTreeMap::from([(signed.client_order_id.clone(), signed.clone())]),
        &BTreeMap::new(),
        &BTreeSet::new(),
    )?);
    let completed_inventory = GridInventory {
        long_quantity: Decimal::ZERO,
        ..partial_inventory.clone()
    };
    assert!(desired_closes_fit(
        &partial_surface,
        &completed_inventory,
        &GridCloseReservations::default(),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeSet::from([wanted.client_order_id.clone()]),
    )?);
    signed.filled_quantity = Some(Decimal::ZERO);
    assert_eq!(
        actual_matches_desired(&signed, &wanted)?,
        DesiredOrderMatch::Exact
    );
    signed.filled_quantity = Some(signed.quantity);
    assert_eq!(
        actual_matches_desired(&signed, &wanted)?,
        DesiredOrderMatch::Conflict
    );
    Ok(())
}

fn intent(
    position: GridPosition,
    role: GridOrderRole,
    sequence: u64,
    price: i64,
    quantity: Decimal,
) -> Result<GridOrderIntent, Box<dyn std::error::Error>> {
    let side =
        match (position, role) {
            (GridPosition::Long, GridOrderRole::Open)
            | (GridPosition::Short, GridOrderRole::Close) => OrderSide::Buy,
            (GridPosition::Long, GridOrderRole::Close)
            | (GridPosition::Short, GridOrderRole::Open) => OrderSide::Sell,
        };
    Ok(GridOrderIntent {
        key: GridOrderKey {
            epoch: 2,
            position,
            role,
            level: sequence,
        },
        side,
        price: Price::new(Decimal::from(price))?,
        quantity,
        reduce_only: role == GridOrderRole::Close,
    })
}

fn surface(
    orders: Vec<GridDesiredOrder>,
) -> Result<GridDesiredSurface, Box<dyn std::error::Error>> {
    Ok(GridDesiredSurface {
        instance_id: "instance".into(),
        symbol: "BTC/USDT".parse()?,
        config_revision: 2,
        plan_revision: 7,
        desired_digest: [1; 32],
        orders,
    })
}
