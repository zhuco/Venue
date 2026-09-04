use super::*;
use bytes::Bytes;
use venue_execution::{SignedAccountBalance, SignedAccountPositionMode};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

fn state() -> Result<AccountStreamProjection, Box<dyn std::error::Error>> {
    let binding = GatewayBinding::new(
        VenueId::Binance,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        "SOL/USDC".parse()?,
    )?;
    let positions = [PositionSide::Long, PositionSide::Short]
        .into_iter()
        .map(|side| SignedAccountPositionFact {
            symbol: binding.symbol.clone(),
            position_side: side,
            quantity: Decimal::ONE,
            entry_price: Some(Decimal::from(100)),
            mark_price: Some(Decimal::from(100)),
        })
        .collect();
    let baseline = SignedAccountSnapshot::complete(
        binding,
        100,
        1,
        2,
        3,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        positions,
        "binance-fills-v1|SOLUSDC,100,,".into(),
        Vec::new(),
    )?
    .with_balances(vec![SignedAccountBalance {
        asset: "USD".parse()?,
        equity: Decimal::from(50),
        available_margin: Some(Decimal::from(40)),
    }])?;
    Ok(AccountStreamProjection::new(baseline))
}

fn apply(
    state: &mut AccountStreamProjection,
    payload: &str,
) -> Result<(), BinanceAccountGatewayError> {
    let frame = BinanceRawPrivateFrame {
        binding: state.baseline.binding().clone(),
        instrument_generation: 3,
        private_generation: 1,
        received_at_ms: 200,
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    };
    state.apply(
        &frame,
        &BTreeSet::from([state.baseline.binding().symbol.clone()]),
    )
}

const NEW: &str = r#"{"e":"ORDER_TRADE_UPDATE","fs":"UM","E":120,"T":119,"o":{"s":"SOLUSDC","c":"grid-new","i":44,"S":"BUY","ps":"LONG","x":"NEW","X":"NEW","o":"LIMIT","f":"GTX","q":"2","z":"0","p":"100","R":false}}"#;
const FILL: &str = r#"{"e":"ORDER_TRADE_UPDATE","fs":"UM","E":132,"T":130,"o":{"s":"SOLUSDC","c":"grid-new","i":44,"S":"BUY","ps":"LONG","x":"TRADE","t":45,"X":"FILLED","o":"LIMIT","f":"GTX","q":"2","z":"2","p":"100","R":false}}"#;
const POSITION: &str = r#"{"e":"ACCOUNT_UPDATE","fs":"UM","E":131,"T":130,"a":{"m":"ORDER","P":[{"s":"SOLUSDC","ps":"LONG","pa":"3","ep":"100","up":"0"}]}}"#;

#[test]
fn new_and_cancel_are_local_facts_not_rest_refreshes() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = state()?;
    apply(&mut state, NEW)?;
    let snapshot = state.snapshot(125, 2)?.ok_or("snapshot")?;
    assert_eq!(snapshot.open_orders().len(), 1);
    assert_eq!(
        snapshot.open_orders()[0].time_in_force,
        Some(LimitTimeInForce::PostOnly)
    );
    assert_eq!(snapshot.open_orders()[0].created_at_ms, Some(119));
    assert_eq!(snapshot.balance_observed_at_ms(), 100);
    let cancel = NEW
        .replace("\"E\":120", "\"E\":130")
        .replace("\"T\":119", "\"T\":129")
        .replace("\"x\":\"NEW\"", "\"x\":\"CANCELED\"")
        .replace("\"X\":\"NEW\"", "\"X\":\"CANCELED\"");
    apply(&mut state, &cancel)?;
    assert!(
        state
            .snapshot(135, 2)?
            .ok_or("snapshot")?
            .open_orders()
            .is_empty()
    );
    Ok(())
}

#[test]
fn fill_waits_for_matching_position_update_but_not_a_second_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state()?;
    apply(&mut state, NEW)?;
    apply(&mut state, FILL)?;
    assert!(state.snapshot(140, 2)?.is_none());
    apply(&mut state, POSITION)?;
    let snapshot = state.snapshot(150, 2)?.ok_or("snapshot")?;
    assert_eq!(snapshot.positions()[0].quantity, Decimal::from(3));
    assert!(snapshot.open_orders().is_empty());
    assert_eq!(snapshot.balance_observed_at_ms(), 100);
    // Old mark and equity are not manufactured from receipt time or rounded stream PnL.
    assert_eq!(snapshot.positions()[0].mark_price, Some(Decimal::from(100)));
    Ok(())
}

#[test]
fn position_may_precede_execution_and_idle_does_not_renew_baseline()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state()?;
    apply(&mut state, NEW)?;
    apply(&mut state, POSITION)?;
    assert!(
        state.snapshot(140, 2)?.is_none(),
        "position-first must not double apply a later execution"
    );
    apply(&mut state, FILL)?;
    let snapshot = state.snapshot(150, 2)?.ok_or("snapshot")?;
    state.last_published_ms = snapshot.observed_at_ms();
    assert!(state.snapshot(150, 2)?.is_none());
    assert!(state.snapshot(149, 2)?.is_none());
    assert!(state.snapshot(155, 2)?.is_some());
    assert!(apply(&mut state, NEW).is_err());
    Ok(())
}

#[test]
fn wrong_symbol_side_or_order_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    for changed in [
        NEW.replace("SOLUSDC", "ETHUSDC"),
        NEW.replace("LONG", "BOTH"),
        NEW.replace("\"q\":\"2\"", "\"q\":\"-2\""),
    ] {
        assert!(apply(&mut state()?, &changed).is_err());
    }
    let mut state = state()?;
    apply(&mut state, NEW)?;
    assert!(apply(&mut state, &FILL.replace("\"i\":44", "\"i\":45")).is_err());
    Ok(())
}

#[test]
fn missing_execution_after_position_update_becomes_a_gap_not_fresh_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state()?;
    apply(&mut state, POSITION)?;
    assert!(state.snapshot(200, 2)?.is_none());
    assert!(state.snapshot(5_201, 2).is_err());
    Ok(())
}

#[test]
fn partial_duplicate_and_cross_burst_second_leg_keep_exact_inventory()
-> Result<(), Box<dyn std::error::Error>> {
    let mut state = state()?;
    apply(&mut state, NEW)?;
    let partial = FILL
        .replace("\"z\":\"2\"", "\"z\":\"1\"")
        .replace("\"X\":\"FILLED\"", "\"X\":\"PARTIALLY_FILLED\"");
    apply(&mut state, &partial)?;
    apply(&mut state, &partial)?;
    apply(
        &mut state,
        &POSITION.replace("\"pa\":\"3\"", "\"pa\":\"2\""),
    )?;
    let first = state.snapshot(200, 2)?.ok_or("partial snapshot")?;
    assert_eq!(first.open_orders()[0].filled_quantity, Some(Decimal::ONE));
    assert_eq!(first.positions()[0].quantity, Decimal::from(2));
    state.last_published_ms = first.observed_at_ms();
    let short_fill = FILL
        .replace("\"E\":132", "\"E\":232")
        .replace("\"T\":130", "\"T\":230")
        .replace("grid-new", "grid-short")
        .replace("\"i\":44", "\"i\":46")
        .replace("\"t\":45", "\"t\":47")
        .replace("LONG", "SHORT")
        .replace("BUY", "SELL");
    let short_position = POSITION
        .replace("\"E\":131", "\"E\":231")
        .replace("\"T\":130", "\"T\":230")
        .replace("LONG", "SHORT")
        .replace("\"pa\":\"3\"", "\"pa\":\"-3\"");
    apply(&mut state, &short_position)?;
    assert!(state.snapshot(240, 2)?.is_none());
    apply(&mut state, &short_fill)?;
    let second = state.snapshot(245, 2)?.ok_or("second leg snapshot")?;
    assert_eq!(second.positions()[0].quantity, Decimal::from(2));
    assert_eq!(second.positions()[1].quantity, Decimal::from(3));
    assert_eq!(second.open_orders().len(), 1);
    assert!(second.fills_cursor().contains(",47,230"));
    Ok(())
}
