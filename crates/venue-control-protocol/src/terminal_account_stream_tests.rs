use super::*;
use crate::kol::*;
use venue_domain::{OrderSide, PositionSide};

fn projection() -> Result<TerminalAccountProjection, Box<dyn std::error::Error>> {
    let position = TerminalPosition {
        symbol: "BTC/USDC".parse()?,
        position_side: PositionSide::Long,
        quantity: 1.into(),
        entry_price: Some(100.into()),
        mark_price: Some(101.into()),
    };
    Ok(TerminalAccountProjection {
        schema_version: 1,
        credential_id: "00000000-0000-4000-8000-000000000001".into(),
        trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
        observed_ms: 100,
        persisted_ms: 101,
        private_generation: 1,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![position.clone()],
        position_history: vec![TerminalPositionHistoryEntry {
            observed_ms: 90,
            position,
        }],
        open_orders: vec![],
        conditional_orders: vec![],
        fills: (0..500)
            .map(|i| TerminalFill {
                native_trade_id: i.to_string(),
                native_order_id: i.to_string(),
                symbol: "BTC/USDC".parse().unwrap(),
                order_side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: 1.into(),
                price: 100.into(),
                maker: Some(true),
                occurred_ms: Some(90),
            })
            .collect(),
        assets: vec![TerminalAsset {
            asset: "USD".into(),
            equity: 1000.into(),
            available_margin: Some(900.into()),
        }],
    })
}

#[test]
fn live_changes_preserve_history_without_retransmitting_it()
-> Result<(), Box<dyn std::error::Error>> {
    let original = projection()?;
    let mut current = original.clone();
    current.observed_ms += 10;
    current.persisted_ms += 10;
    current.positions[0].mark_price = Some(102.into());
    current.assets[0].equity = 1001.into();
    let event = TerminalAccountStreamEvent::between(Some(&original), Some(&current));
    let encoded = serde_json::to_vec(&event)?;
    assert!(encoded.len() * 20 < serde_json::to_vec(&current)?.len());
    let mut received = Some(original);
    serde_json::from_slice::<TerminalAccountStreamEvent>(&encoded)?.apply(&mut received)?;
    assert_eq!(received, Some(current));
    Ok(())
}

#[test]
fn changed_and_empty_history_replace_the_previous_history() -> Result<(), Box<dyn std::error::Error>>
{
    let mut received = Some(projection()?);
    for clear in [false, true] {
        let mut current = received.clone().ok_or("baseline")?;
        current.observed_ms += 10;
        current.persisted_ms += 10;
        if clear {
            current.fills.clear();
            current.position_history.clear();
        } else {
            current.fills[0].price = 200.into();
            current.position_history[0].position.quantity = 2.into();
        }
        TerminalAccountStreamEvent::between(received.as_ref(), Some(&current))
            .apply(&mut received)?;
        assert_eq!(received, Some(current));
    }
    Ok(())
}

#[test]
fn missing_or_wrong_baseline_never_borrows_another_accounts_history()
-> Result<(), Box<dyn std::error::Error>> {
    let original = projection()?;
    let mut current = original.clone();
    current.observed_ms += 10;
    current.persisted_ms += 10;
    let event = TerminalAccountStreamEvent::between(Some(&original), Some(&current));
    for changed in 0..5 {
        let mut base = Some(original.clone());
        match changed {
            0 => base = None,
            1 => {
                base.as_mut().ok_or("baseline")?.credential_id = current.trading_account_id.clone()
            }
            2 => {
                base.as_mut().ok_or("baseline")?.trading_account_id = current.credential_id.clone()
            }
            3 => base.as_mut().ok_or("baseline")?.private_generation += 1,
            _ => base.as_mut().ok_or("baseline")?.observed_ms += 1,
        }
        let unchanged = base.clone();
        assert!(event.clone().apply(&mut base).is_err());
        assert_eq!(base, unchanged);
    }
    let mut base = Some(original.clone());
    let mut invalid = event;
    if let TerminalAccountStreamEvent::Update { projection, .. } = &mut invalid {
        projection.observed_ms = 1;
    }
    assert!(invalid.apply(&mut base).is_err());
    assert_eq!(base, Some(original));
    Ok(())
}

#[test]
fn reconnect_generation_change_and_empty_projection_reset_history()
-> Result<(), Box<dyn std::error::Error>> {
    let original = projection()?;
    let event = TerminalAccountStreamEvent::between(None, Some(&original));
    assert!(matches!(
        &event,
        TerminalAccountStreamEvent::Snapshot(Some(_))
    ));
    let mut base = None;
    event.apply(&mut base)?;
    assert_eq!(base, Some(original.clone()));
    let mut changed = original.clone();
    changed.private_generation += 1;
    assert!(matches!(
        TerminalAccountStreamEvent::between(Some(&original), Some(&changed)),
        TerminalAccountStreamEvent::Snapshot(Some(_))
    ));
    TerminalAccountStreamEvent::between(base.as_ref(), None).apply(&mut base)?;
    assert!(base.is_none());
    Ok(())
}
