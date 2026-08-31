use super::*;

#[test]
fn limit_normalization_uses_same_side_bbo_and_floors_price_and_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, bbo) = limit_facts()?;
    let intent = limit_intent(Decimal::new(100, 0))?;
    let ExecutionCommand::PlaceLimit(command) =
        normalize_limit_from_bbo(&binding, &rules, &intent, &bbo, 10_010)?
    else {
        return Err("expected limit".into());
    };
    assert_eq!(command.limit_price.value(), Decimal::new(654_854, 1));
    assert_eq!(command.quantity, Decimal::new(1, 3));
    assert_eq!(command.command_id, intent.command_id);
    assert_eq!(command.client_order_id, intent.client_order_id);
    assert_eq!(command.owner, intent.owner);
    Ok(())
}

#[test]
fn limit_normalization_rejects_minimum_symbol_direction_and_stale_bbo()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, rules, bbo) = limit_facts()?;
    assert!(
        normalize_limit_from_bbo(
            &binding,
            &rules,
            &limit_intent(Decimal::new(4, 0))?,
            &bbo,
            10_010,
        )
        .is_err()
    );
    let mut wrong_symbol = limit_intent(Decimal::new(100, 0))?;
    wrong_symbol.owner.symbol = "ETH/USDT".parse()?;
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_symbol, &bbo, 10_010).is_err());
    let mut wrong_account = limit_intent(Decimal::new(100, 0))?;
    wrong_account.owner.account = "00000000-0000-4000-8000-000000000002".to_owned();
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_account, &bbo, 10_010).is_err());
    let mut wrong_leg = limit_intent(Decimal::new(100, 0))?;
    wrong_leg.position_side = PositionSide::Short;
    assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_leg, &bbo, 10_010).is_err());
    assert!(
        normalize_limit_from_bbo(
            &binding,
            &rules,
            &limit_intent(Decimal::new(100, 0))?,
            &bbo,
            11_001,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn snapshot_fills_deduplicates_closed_account_wide_pages_and_keeps_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, _, _) = limit_facts()?;
    let first = String::from_utf8(EXECUTIONS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    let (fills, cursor) = snapshot_fills(
        &[
            account_wide_execution(&binding, 0, None, first.into_bytes()),
            account_wide_execution(&binding, 1, Some("next"), EXECUTIONS.to_vec()),
        ],
        None,
    )?;
    assert_eq!(fills.len(), 3);
    assert_eq!(fills[0].execution_sequence, FieldState::Known(103));
    assert_eq!(fills[0].exchange_time_ms, Some(2_000));
    assert_eq!(cursor, "bybit-exec:2000:c");
    Ok(())
}

#[test]
fn snapshot_fills_rejects_unclosed_or_unknown_account_symbols()
-> Result<(), Box<dyn std::error::Error>> {
    let (binding, _, _) = limit_facts()?;
    let unclosed = String::from_utf8(EXECUTIONS.to_vec())?
        .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
    assert!(
        snapshot_fills(
            &[account_wide_execution(
                &binding,
                0,
                None,
                unclosed.into_bytes(),
            )],
            None
        )
        .is_err()
    );
    let wrong_symbol = String::from_utf8(EXECUTIONS.to_vec())?.replace("BTCUSDT", "BTCUSDC");
    assert!(
        snapshot_fills(
            &[account_wide_execution(
                &binding,
                0,
                None,
                wrong_symbol.into_bytes(),
            )],
            None
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn fills_cursor_restarts_with_overlap_and_rejects_missing_window()
-> Result<(), Box<dyn std::error::Error>> {
    let resumed = fills_history_window(1_000_000, Some("bybit-exec:990000:exec-9"))?;
    assert_eq!(resumed.start_ms, 930_000);
    assert!(fills_history_window(HISTORY_WINDOW_MS + 2, Some("bybit-exec:1:exec-1")).is_err());
    assert!(fills_history_window(1_000_000, Some("okx-bill:99")).is_err());
    Ok(())
}
