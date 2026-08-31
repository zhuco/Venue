use venue_domain::domain::{OrderSide, Price};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{SignedAccountOrderFact, SignedAccountPositionMode};

use super::*;
use crate::copy_semantic::tests::{delivery_and_request, fresh_facts};

fn fixture() -> Result<(ExecutionCommand, Fill), Box<dyn std::error::Error>> {
    let (delivery, request) =
        delivery_and_request(Decimal::ZERO, 10.into(), CopyExecutionPhase::Adjust, 1)?;
    let command = delivery.execution_command(&request, &fresh_facts()?)?;
    let fill = Fill {
        fill_id: "actual-fill-1".to_owned(),
        execution_sequence: FieldState::Missing,
        order_id: "actual-native-1".to_owned(),
        symbol: delivery.owner().symbol.clone(),
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        quantity: Decimal::new(4, 1),
        price: Price::new(Decimal::ONE)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(true),
        exchange_time_ms: Some(140),
    };
    Ok((command, fill))
}

fn snapshot(
    command: &ExecutionCommand,
    fills: Vec<Fill>,
    open: bool,
) -> Result<SignedAccountSnapshot, Box<dyn std::error::Error>> {
    let ExecutionCommand::PlaceLimit(row) = command else {
        return Err("expected limit".into());
    };
    let binding = GatewayBinding::new(
        VenueId::Okx,
        GatewayMode::Live,
        row.owner.account.clone(),
        row.owner.symbol.clone(),
    )?;
    let orders = if open {
        vec![SignedAccountOrderFact {
            created_at_ms: None,
            time_in_force: Some(row.time_in_force),
            client_order_id: row.client_order_id.as_str().to_owned(),
            venue_order_id: Some("actual-native-1".to_owned()),
            symbol: row.owner.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: row.side,
            position_side: row.position_side,
            quantity: row.quantity,
            limit_price: Some(row.limit_price.value()),
            reduce_only: false,
            owner: Some(row.owner.clone()),
            external: false,
            state: None,
            filled_quantity: None,
        }]
    } else {
        Vec::new()
    };
    Ok(SignedAccountSnapshot::complete_with_fills(
        binding,
        150,
        1,
        2,
        1,
        SignedAccountPositionMode::Hedge,
        orders,
        Vec::new(),
        fills,
        "native-cursor".to_owned(),
        Vec::new(),
    )?)
}

#[test]
fn open_ack_and_partial_fills_are_not_complete() -> Result<(), Box<dyn std::error::Error>> {
    let (command, fill) = fixture()?;
    let page = snapshot(&command, Vec::new(), true)?;
    let facts = execution_facts(&command, "actual-native-1", &page, &[], 100, 160)?;
    assert!(facts.open);
    assert!(!facts.fully_filled);
    let page = snapshot(&command, vec![fill], false)?;
    let facts = execution_facts(&command, "actual-native-1", &page, &[], 100, 160)?;
    assert!(!facts.open);
    assert!(!facts.fully_filled);
    Ok(())
}

#[test]
fn cursor_overlap_reuses_persisted_partial_fills_without_double_counting()
-> Result<(), Box<dyn std::error::Error>> {
    let (command, first) = fixture()?;
    let mut second = first.clone();
    second.fill_id = "actual-fill-2".to_owned();
    second.quantity = Decimal::new(6, 1);
    second.exchange_time_ms = Some(145);
    let page = snapshot(&command, vec![first.clone(), second.clone()], false)?;
    let facts = execution_facts(
        &command,
        "actual-native-1",
        &page,
        std::slice::from_ref(&first),
        100,
        160,
    )?;
    assert!(facts.fully_filled);
    assert_eq!(facts.fills.len(), 2);
    let next = snapshot(&command, vec![second], false)?;
    let replay = execution_facts(&command, "actual-native-1", &next, &facts.fills, 100, 160)?;
    assert_eq!(replay.fills, facts.fills);
    assert!(replay.fully_filled);
    Ok(())
}

#[test]
fn conflicting_overlap_and_foreign_retained_fact_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let (command, first) = fixture()?;
    let mut conflict = first.clone();
    conflict.quantity = Decimal::new(5, 1);
    let page = snapshot(&command, vec![conflict], false)?;
    assert!(
        execution_facts(
            &command,
            "actual-native-1",
            &page,
            std::slice::from_ref(&first),
            100,
            160
        )
        .is_err()
    );
    let page = snapshot(&command, Vec::new(), false)?;
    let mut foreign = first;
    foreign.order_id = "another-child".to_owned();
    assert!(execution_facts(&command, "actual-native-1", &page, &[foreign], 100, 160).is_err());
    Ok(())
}

#[test]
fn excess_future_and_wrong_direction_fills_cannot_prove_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let (command, first) = fixture()?;
    let mut excess = first.clone();
    excess.quantity = 2.into();
    let mut future = first.clone();
    future.exchange_time_ms = Some(161);
    let mut wrong_side = first;
    wrong_side.side = OrderSide::Sell;
    for bad in [excess, future, wrong_side] {
        let page = snapshot(&command, vec![bad], false)?;
        assert!(execution_facts(&command, "actual-native-1", &page, &[], 100, 160).is_err());
    }
    Ok(())
}

#[test]
fn client_and_venue_id_must_resolve_to_the_same_open_order()
-> Result<(), Box<dyn std::error::Error>> {
    let (command, _) = fixture()?;
    let page = snapshot(&command, Vec::new(), true)?;
    assert!(execution_facts(&command, "conflicting-native-id", &page, &[], 100, 160).is_err());
    Ok(())
}

#[test]
fn matching_copy_identity_does_not_hide_a_different_limit_policy()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut command, _) = fixture()?;
    let page = snapshot(&command, Vec::new(), true)?;
    let ExecutionCommand::PlaceLimit(place) = &mut command else {
        return Err("limit required".into());
    };
    place.time_in_force = venue_domain::LimitTimeInForce::Gtc;
    assert!(execution_facts(&command, "actual-native-1", &page, &[], 100, 160).is_err());
    let matching = snapshot(&command, Vec::new(), true)?;
    assert!(execution_facts(&command, "actual-native-1", &matching, &[], 100, 160)?.open);
    Ok(())
}
