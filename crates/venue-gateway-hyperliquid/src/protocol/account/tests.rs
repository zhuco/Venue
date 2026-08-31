use super::*;
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const NOW: u64 = 1_700_000_010_000;
const FILLS: &[u8] = include_bytes!("../../../fixtures/fills-page.json");

fn setup() -> Result<(HyperliquidPerpMeta, PerpUniverse), Box<dyn std::error::Error>> {
    let gateway = GatewayBinding::new(
        VenueId::Hyperliquid,
        GatewayMode::Live,
        "00000000-0000-4000-8000-000000000001",
        "BTC/USDC".parse()?,
    )?;
    let binding = HyperliquidReadBinding::new(
        crate::HyperliquidGatewayBinding::new(gateway)?,
        "0x0000000000000000000000000000000000000001",
    )?;
    let universe = parse_universe(include_bytes!("../../../fixtures/perp-meta.json"), &binding)?;
    Ok((universe.get("BTC").ok_or("BTC")?.clone(), universe))
}

fn row(time: u64, tid: u64, coin: &str) -> serde_json::Value {
    serde_json::json!({"closedPnl":"0", "coin":coin, "crossed":false,
        "dir":"Open Long", "fee":"0.1", "feeToken":"USDC", "oid":tid,
        "px":"65000", "side":"B", "sz":"0.001", "time":time, "tid":tid})
}

#[test]
fn snapshot_keeps_sibling_positions_net_sign_and_actual_currency() -> TestResult {
    let (meta, universe) = setup()?;
    let state = parse_account_state(
        include_bytes!("../../../fixtures/clearinghouse-state.json"),
        &universe,
        &meta,
    )?;
    assert_eq!(state.positions.len(), 2);
    let btc = state
        .positions
        .iter()
        .find(|position| position.symbol == meta.scope.symbol().clone())
        .ok_or("BTC position")?;
    assert_eq!(btc.position_side, PositionSide::Net);
    assert_eq!(btc.quantity, Decimal::new(-335, 4));
    assert!(btc.mark_price.is_some());
    assert_eq!(state.balance.asset.as_str(), "USDC");
    assert_eq!(state.balance.equity, Decimal::new(13_109_482_328, 6));
    assert_eq!(
        state.balance.available_margin,
        Some(Decimal::new(13_104_514_502, 6))
    );
    Ok(())
}

#[test]
fn account_state_preserves_negative_signed_balance_for_fail_closed_risk_refresh() -> TestResult {
    let (meta, universe) = setup()?;
    let mut state: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../fixtures/clearinghouse-state.json"))?;
    state["marginSummary"]["accountValue"] = "-3.5".into();
    state["withdrawable"] = "-4.0".into();
    let parsed = parse_account_state(&serde_json::to_vec(&state)?, &universe, &meta)?;
    assert_eq!(parsed.balance.equity, Decimal::new(-35, 1));
    assert_eq!(parsed.balance.available_margin, Some(Decimal::new(-40, 1)));
    Ok(())
}

#[test]
fn absent_selected_position_is_a_zero_net_leg_and_unknown_coin_is_rejected() -> TestResult {
    let (meta, universe) = setup()?;
    let mut state: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../fixtures/clearinghouse-state.json"))?;
    state["assetPositions"]
        .as_array_mut()
        .ok_or("positions")?
        .remove(0);
    let parsed = parse_account_state(&serde_json::to_vec(&state)?, &universe, &meta)?;
    assert_eq!(parsed.positions.len(), 2);
    assert!(
        parsed
            .positions
            .iter()
            .any(|p| p.symbol == *meta.scope.symbol() && p.quantity.is_zero())
    );
    state["assetPositions"][0]["position"]["coin"] = "UNSUPPORTED".into();
    assert!(parse_account_state(&serde_json::to_vec(&state)?, &universe, &meta).is_err());
    Ok(())
}

#[test]
fn account_orders_include_siblings_and_preserve_partial_fills() -> TestResult {
    let (_, universe) = setup()?;
    let mut rows: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../../fixtures/open-orders.json"))?;
    let row = rows
        .as_array_mut()
        .ok_or("orders")?
        .first_mut()
        .ok_or("order")?;
    row["coin"] = "ETH".into();
    row["origSz"] = "2".into();
    row["sz"] = "1".into();
    let orders = parse_account_orders(&serde_json::to_vec(&rows)?, &universe, 1_800_000_000_000)?;
    let eth = orders
        .iter()
        .find(|order| order.order.symbol.base() == "ETH")
        .ok_or("ETH order")?;
    assert_eq!(eth.order.state, OrderState::PartiallyFilled);
    assert_eq!(eth.order.filled_quantity, Decimal::ONE);
    Ok(())
}

#[test]
fn cursor_restarts_with_overlap_and_collects_every_perpetual_coin() -> TestResult {
    let (meta, universe) = setup()?;
    let mut initial = FillCollection::resume(None, &meta, NOW)?;
    assert!(initial.ingest(FILLS, &meta, &universe)?);
    let (fills, cursor) = initial.finish()?;
    assert_eq!(fills.len(), 3);
    assert!(fills.iter().any(|fill| fill.symbol.base() == "ETH"));
    let mut restored = FillCollection::resume(Some(&cursor), &meta, NOW + 1)?;
    assert_eq!(restored.query(&meta)?.begin_ms(), 1_700_000_000_002);
    let rows: Vec<serde_json::Value> = serde_json::from_slice(FILLS)?;
    let anchor = rows.last().ok_or("anchor")?.clone();
    let next = row(1_700_000_000_002, 6003, "ETH");
    assert!(restored.ingest(&serde_json::to_vec(&vec![anchor, next])?, &meta, &universe)?);
    let (fills, cursor2) = restored.finish()?;
    assert_eq!(fills.len(), 2);
    assert_ne!(cursor, cursor2);
    assert!(FillCollection::resume(Some(&cursor2), &meta, NOW).is_err());
    Ok(())
}

#[test]
fn lost_retention_anchor_cannot_advance_cursor_even_on_empty_terminal_page() -> TestResult {
    let (meta, universe) = setup()?;
    let mut initial = FillCollection::resume(None, &meta, NOW)?;
    initial.ingest(FILLS, &meta, &universe)?;
    let (_, cursor) = initial.finish()?;
    let mut restored = FillCollection::resume(Some(&cursor), &meta, NOW + 1)?;
    assert!(restored.ingest(b"[]", &meta, &universe).is_err());
    assert!(restored.finish().is_err());
    Ok(())
}

#[test]
fn capped_history_is_not_all_time_coverage_and_cannot_finish_early() -> TestResult {
    let (meta, universe) = setup()?;
    let mut collection = FillCollection::resume(None, &meta, NOW + 20_000)?;
    for page in 0..5_u64 {
        let rows = (1..=2_000)
            .map(|n| row(NOW + page * 2000 + n, page * 2000 + n, "BTC"))
            .collect::<Vec<_>>();
        let result = collection.ingest(&serde_json::to_vec(&rows)?, &meta, &universe);
        if page == 4 {
            assert!(result.is_err());
        } else {
            assert!(!result?);
        }
    }
    assert!(collection.finish().is_err());
    Ok(())
}

#[test]
fn cursor_scope_unknown_formats_future_rows_and_foreign_perps_fail_closed() -> TestResult {
    let (meta, universe) = setup()?;
    assert!(FillCollection::resume(Some("opaque-sha"), &meta, NOW).is_err());
    let mut collection = FillCollection::resume(None, &meta, NOW)?;
    assert!(
        collection
            .ingest(
                &serde_json::to_vec(&vec![row(NOW + 1, 1, "BTC")])?,
                &meta,
                &universe
            )
            .is_err()
    );
    let mut collection = FillCollection::resume(None, &meta, NOW)?;
    assert!(
        collection
            .ingest(
                &serde_json::to_vec(&vec![row(NOW, 1, "FOREIGN")])?,
                &meta,
                &universe
            )
            .is_err()
    );
    let mut collection = FillCollection::resume(None, &meta, NOW)?;
    assert!(collection.ingest(
        &serde_json::to_vec(&vec![row(NOW, 1, "@1")])?,
        &meta,
        &universe
    )?);
    let (fills, cursor) = collection.finish()?;
    assert!(fills.is_empty());
    let wrong = cursor.replace(
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
    );
    assert!(FillCollection::resume(Some(&wrong), &meta, NOW + 1).is_err());
    Ok(())
}
